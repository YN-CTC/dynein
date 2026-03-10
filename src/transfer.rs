/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use console::Term;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::{
    collections::HashMap,
    fs,
    io::{Error as IOError, Write},
    path::Path,
};

use dialoguer::Confirm;
use log::{debug, error};
use serde_json::{de::StrRead, Deserializer, StreamDeserializer, Value as JsonValue};
use tokio::sync::Mutex as TokioMutex;
use tokio::time::sleep;

use aws_sdk_dynamodb::{
    operation::scan::ScanOutput,
    types::{AttributeValue, WriteRequest},
    Client as DynamoDbSdkClient,
};
use thiserror::Error;

use super::app;
use super::batch;
use super::control;
use super::data;
use super::ddb::table;

#[derive(Error, Debug)]
pub enum DyneinExportError {
    #[error("io error")]
    IO(#[from] std::io::Error),
    #[error("serde error")]
    SerdeError(#[from] serde_json::Error),
}

impl From<dialoguer::Error> for DyneinExportError {
    fn from(e: dialoguer::Error) -> Self {
        match e {
            dialoguer::Error::IO(e) => DyneinExportError::IO(e),
        }
    }
}

#[derive(Debug)]
struct SuggestedAttribute {
    name: String,
    type_str: String,
}

#[derive(Clone, Debug, Hash, PartialOrd, PartialEq)]
struct ProgressState {
    processed_items: usize,
    recent_processed_items: VecDeque<(Instant, usize)>,
    max_recordable_observations: usize,
}

impl ProgressState {
    fn new(max_recordable_observations: usize) -> ProgressState {
        ProgressState {
            processed_items: 0,
            recent_processed_items: VecDeque::with_capacity(max_recordable_observations),
            max_recordable_observations,
        }
    }

    fn add_observation(&mut self, processed_items: usize) {
        self.add_observation_with_time(processed_items, Instant::now())
    }

    fn add_observation_with_time(&mut self, processed_items: usize, at: Instant) {
        self.processed_items += processed_items;

        if self.recent_processed_items.len() == self.max_recordable_observations {
            self.recent_processed_items.pop_back();
        }
        self.recent_processed_items
            .push_front((at, processed_items));
    }

    fn processed_items(&self) -> usize {
        self.processed_items
    }

    fn recent_average_processed_items_per_second(&self) -> f64 {
        self.recent_average_processed_items_per_second_with_time(Instant::now())
    }

    fn recent_average_processed_items_per_second_with_time(&self, at: Instant) -> f64 {
        let mut sum = 0.0;
        for v in &self.recent_processed_items {
            sum += v.1 as f64
        }
        if let Some((oldest_time, _)) = self.recent_processed_items.back() {
            if at == *oldest_time {
                f64::NAN
            } else {
                sum / at.duration_since(*oldest_time).as_secs_f64()
            }
        } else {
            0.0
        }
    }

    fn show(&self) {
        let items = self.processed_items();
        let items_per_sec = self.recent_average_processed_items_per_second();
        let mut term = Term::stdout();
        term.clear_line().expect("Failed to clear line");
        write!(
            term,
            "{} items processed ({:.2} items/sec)",
            items, items_per_sec
        )
        .expect("Failed to update message");
        term.flush().expect("Failed to flush");
    }
}

const MAX_NUMBER_OF_OBSERVES: usize = 10;

/// Token-bucket rate limiter for parallel write operations.
/// Tokens represent capacity units (WCU) replenished at `rate` per second.
struct TokenBucket {
    tokens: f64,
    rate: f64,
    last_refill: Instant,
    max_burst: f64,
}

impl TokenBucket {
    fn new(rate: f64) -> Self {
        TokenBucket {
            tokens: rate.min(100.0), // start with at most 100 tokens (avoid initial burst)
            rate,
            last_refill: Instant::now(),
            max_burst: rate, // allow up to 1 second of accumulated tokens
        }
    }

    /// Try to consume `amount` tokens.
    /// Returns `Duration::ZERO` if successful, otherwise how long to wait before retrying.
    fn try_consume(&mut self, amount: f64) -> Duration {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        self.tokens = (self.tokens + elapsed * self.rate).min(self.max_burst);
        self.last_refill = now;

        if self.tokens >= amount {
            self.tokens -= amount;
            Duration::ZERO
        } else {
            let deficit = amount - self.tokens;
            Duration::from_secs_f64(deficit / self.rate)
        }
    }
}

/// Acquire `amount` tokens from the shared token bucket, sleeping if the bucket is empty.
/// The Mutex is held only briefly (for the `try_consume` calculation), then released before sleeping.
async fn acquire_tokens(tb: &Arc<TokioMutex<TokenBucket>>, amount: f64) {
    loop {
        let wait = {
            let mut guard = tb.lock().await;
            guard.try_consume(amount)
        }; // lock released here
        if wait == Duration::ZERO {
            break;
        }
        debug!("Token bucket empty; sleeping {:?} before next batch", wait);
        sleep(wait).await;
    }
}

/// Rate limiter for controlling throughput based on capacity unit percentage.
/// Can be used for both write (WCU) and read (RCU) operations.
#[derive(Clone, Debug)]
struct RateLimiter {
    /// Target operations (capacity units) per second
    target_ops: f64,
    /// Last operation timestamp
    last_operation_time: Option<Instant>,
    /// Number of items processed in the last batch (used for import/purge)
    last_batch_size: usize,
    /// Actual capacity units consumed by the last API call (RCU for export, WCU for import)
    last_consumed_cu: f64,
}

impl RateLimiter {
    /// Create a new rate limiter based on capacity units and percentage.
    /// Each capacity unit allows 1 operation per second for items up to 1KB (WCU) or 4KB (RCU).
    fn new(capacity_units: i64, percent: u8) -> Self {
        let target_ops = (capacity_units as f64) * (percent as f64 / 100.0);
        debug!(
            "RateLimiter initialized: CU={}, percent={}%, target_ops={:.2}",
            capacity_units, percent, target_ops
        );
        RateLimiter {
            target_ops,
            last_operation_time: None,
            last_batch_size: 0,
            last_consumed_cu: 0.0,
        }
    }

    /// Calculate delay needed before the next batch write/delete to maintain the target WCU rate.
    /// Uses item count as a proxy for capacity unit consumption (1 item ≈ 1 WCU for ≤1 KB items).
    async fn wait_if_needed(&mut self, batch_size: usize) {
        if let Some(last_time) = self.last_operation_time {
            // Calculate the minimum time that should have elapsed for the last batch
            let min_duration_secs = self.last_batch_size as f64 / self.target_ops;
            let min_duration = Duration::from_secs_f64(min_duration_secs);
            let elapsed = last_time.elapsed();

            if elapsed < min_duration {
                let sleep_duration = min_duration - elapsed;
                debug!(
                    "Rate limiting: sleeping for {:?} (min_duration={:?}, elapsed={:?})",
                    sleep_duration, min_duration, elapsed
                );
                sleep(sleep_duration).await;
            }
        }

        self.last_operation_time = Some(Instant::now());
        self.last_batch_size = batch_size;
    }

    /// Calculate delay needed before the next API call to maintain the target CU rate.
    /// Uses the *actual* consumed capacity units returned by the previous API call,
    /// which correctly accounts for item size (1 RCU = 4 KB, 1 WCU = 1 KB).
    /// On the very first call `consumed_cu` should be 0.0, so no delay is applied.
    async fn wait_based_on_cu(&mut self, consumed_cu: f64) {
        if let Some(last_time) = self.last_operation_time {
            if self.last_consumed_cu > 0.0 {
                let min_duration_secs = self.last_consumed_cu / self.target_ops;
                let min_duration = Duration::from_secs_f64(min_duration_secs);
                let elapsed = last_time.elapsed();

                if elapsed < min_duration {
                    let sleep_duration = min_duration - elapsed;
                    debug!(
                        "Rate limiting: sleeping for {:?} (last_cu={:.1}, target_cu/s={:.1}, elapsed={:?})",
                        sleep_duration, self.last_consumed_cu, self.target_ops, elapsed
                    );
                    sleep(sleep_duration).await;
                }
            }
        }

        self.last_operation_time = Some(Instant::now());
        self.last_consumed_cu = consumed_cu;
    }
}

/* =================================================
Public functions
================================================= */

/// Export items in a DynamoDB table into specified format (JSON, JSONL, JSON compact, or CSV. default is JSON).
/// As CSV is a kind of "structured" format, you cannot export DynamoDB's NoSQL-ish "unstructured" data into CSV without any instruction from users.
/// Thus as an "instruction" this function takes --attributes or --keys-only options. If neither of them are given, dynein "guesses" attributes to export from the first item.
pub async fn export(
    cx: &app::Context,
    given_attributes: Option<String>,
    keys_only: bool,
    output_file: String,
    format: Option<String>,
    rcu_percent: Option<u8>,
) -> Result<(), DyneinExportError> {
    let ts: app::TableSchema = app::table_schema(cx).await;
    let format_str: Option<&str> = format.as_deref();

    // Initialize rate limiter if rcu_percent is specified
    let mut rate_limiter: Option<RateLimiter> = if let Some(percent) = rcu_percent {
        if ts.mode == table::Mode::OnDemand {
            println!("WARN: --rcu-percent is specified but the table is in OnDemand mode. Rate limiting will be skipped.");
            None
        } else {
            let desc = control::describe_table_api(cx, ts.name.clone()).await;
            let rcu = desc
                .provisioned_throughput
                .as_ref()
                .and_then(|pt| pt.read_capacity_units)
                .unwrap_or(0);

            if rcu == 0 {
                println!("WARN: Table RCU is 0. Rate limiting will be skipped.");
                None
            } else {
                println!(
                    "Rate limiting enabled: using {}% of {} RCU ({:.1} reads/sec)",
                    percent,
                    rcu,
                    (rcu as f64) * (percent as f64 / 100.0)
                );
                Some(RateLimiter::new(rcu, percent))
            }
        }
    } else {
        // No rate limiting, but still show warning for Provisioned mode
        if ts.mode == table::Mode::Provisioned {
            let msg = "WARN: For the best performance on import/export, dynein recommends OnDemand mode. However the target table is Provisioned mode now. Proceed anyway?";
            if !Confirm::new().with_prompt(msg).interact()? {
                app::bye(0, "Operation has been cancelled.");
            }
        }
        None
    };

    // Basically given_attributes would be used, but on CSV format, it can be overwritten by suggested attributes
    let attributes: Option<String> = match format_str {
        Some("csv") => {
            if !keys_only && given_attributes.is_none() {
                overwrite_attributes_or_exit(cx, &ts)
                    .await
                    .expect("failed to overwrite attributes based on a scanned item")
            } else {
                given_attributes
            }
        }
        None | Some(_) => {
            if keys_only || given_attributes.is_some() {
                app::bye(
                    1,
                    "You can use --keys-only and --attributes only with CSV format.",
                )
            }
            given_attributes
        }
    };

    // Create output file. If target file already exists, ask users if it's ok to delete contents of the file.
    let f: fs::File = if Path::new(&output_file).exists() {
        let msg = "Specified output file already exists. Is it OK to truncate contents?";
        if !Confirm::new().with_prompt(msg).interact()? {
            app::bye(0, "Operation has been cancelled.");
        }
        debug!("truncating existing output file.");
        let _f = fs::OpenOptions::new().append(true).open(&output_file)?;
        _f.set_len(0)?;
        _f
    } else {
        fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&output_file)?
    };

    let tmp_output_filename: &str = &format!("{}_tmp", output_file);
    let mut tmp_output_file: fs::File = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(tmp_output_filename)?;
    tmp_output_file.set_len(0)?;

    let mut last_evaluated_key: Option<HashMap<String, AttributeValue>> = None;
    let mut progress_status = ProgressState::new(MAX_NUMBER_OF_OBSERVES);
    // Tracks the actual RCU consumed by the previous scan page, fed into the rate limiter
    // before issuing the next scan. Starts at 0.0 so the very first scan is never delayed.
    let mut last_actual_rcu: f64 = 0.0;
    loop {
        if let Some(ref mut rl) = rate_limiter {
            rl.wait_based_on_cu(last_actual_rcu).await;
        }

        let scan_output: ScanOutput = data::scan_api(
            cx,
            None,  /* index */
            false, /* consistent_read */
            &attributes,
            keys_only,
            None,               /* limit */
            last_evaluated_key, /* exclusive_start_key */
        )
        .await;

        last_actual_rcu = scan_output
            .consumed_capacity
            .as_ref()
            .and_then(|c| c.capacity_units)
            .unwrap_or(0.0);
        debug!("Scan consumed {:.1} RCU this page (actual)", last_actual_rcu);

        let items = scan_output
            .items
            .expect("Scan result items should be 'Some' even if no item returned.");

        progress_status.add_observation(items.len());
        match format_str {
            None | Some("json") => {
                let s = serde_json::to_string_pretty(&data::convert_to_json_vec(&items))?;
                tmp_output_file.write_all(connectable_json(s, false).as_bytes())?;
            }
            Some("jsonl") => {
                let mut s: String = String::new();
                for item in &items {
                    s.push_str(&serde_json::to_string(&data::convert_to_json(item))?);
                    s.push('\n');
                }
                tmp_output_file.write_all(s.as_bytes())?;
            }
            Some("json-compact") => {
                let s = serde_json::to_string(&data::convert_to_json_vec(&items))?;
                tmp_output_file.write_all(connectable_json(s, true).as_bytes())?;
            }
            Some("csv") => {
                let s = data::convert_items_to_csv_lines(
                    &items,
                    &ts,
                    &attrs_to_append(&ts, &attributes),
                    keys_only,
                );
                tmp_output_file.write_all(s.as_bytes())?;
            }
            Some(o) => panic!("Invalid output format is given: {}", o),
        }
        progress_status.show();

        debug!(
            "scan_output.last_evaluated_key is: {:?}",
            &scan_output.last_evaluated_key
        );
        match scan_output.last_evaluated_key {
            None => break,
            Some(lek) => last_evaluated_key = Some(lek),
        }
    }

    match format_str {
        None | Some("json") => json_finish(f, tmp_output_filename)?.write_all(b"\n]")?,
        Some("json-compact") => json_finish(f, tmp_output_filename)?.write_all(b"]")?,
        Some("jsonl") => jsonl_finish(f, tmp_output_filename)?,
        Some("csv") => csv_finish(
            f,
            tmp_output_filename,
            &ts,
            attrs_to_append(&ts, &attributes),
            keys_only,
        )?
        .write_all(b"\n")?,
        Some(o) => panic!("Invalid output format is given: {}", o),
    };

    fs::remove_file(tmp_output_filename)?;

    Ok(())
}

pub async fn import(
    cx: &app::Context,
    input_file: String,
    format: Option<String>,
    enable_set_inference: bool,
    wcu_percent: Option<u8>,
    workers: usize,
) -> Result<(), batch::DyneinBatchError> {
    let format_str: Option<&str> = format.as_deref();

    let ts: app::TableSchema = app::table_schema(cx).await;

    // Initialize token-bucket rate limiter if wcu_percent is specified.
    // For JSON/JSONL paths the token bucket is shared across parallel workers.
    // For CSV the existing sequential RateLimiter is used instead.
    let token_bucket: Option<Arc<TokioMutex<TokenBucket>>> = if let Some(percent) = wcu_percent {
        if ts.mode == table::Mode::OnDemand {
            println!("WARN: --wcu-percent is specified but the table is in OnDemand mode. Rate limiting will be skipped.");
            None
        } else {
            let desc = control::describe_table_api(cx, ts.name.clone()).await;
            let wcu = desc
                .provisioned_throughput
                .as_ref()
                .and_then(|pt| pt.write_capacity_units)
                .unwrap_or(0);

            if wcu == 0 {
                println!("WARN: Table WCU is 0. Rate limiting will be skipped.");
                None
            } else {
                let target = (wcu as f64) * (percent as f64 / 100.0);
                println!(
                    "Rate limiting enabled: using {}% of {} WCU ({:.1} writes/sec), workers={}",
                    percent, wcu, target, workers
                );
                Some(Arc::new(TokioMutex::new(TokenBucket::new(target))))
            }
        }
    } else {
        // No rate limiting, but still show warning for Provisioned mode
        if ts.mode == table::Mode::Provisioned {
            let msg = "WARN: For the best performance on import/export, dynein recommends OnDemand mode. However the target table is Provisioned mode now. Proceed anyway?";
            if !Confirm::new().with_prompt(msg).interact()? {
                println!("Operation has been cancelled.");
                return Ok(());
            }
        }
        None
    };

    let input_string: String = if Path::new(&input_file).exists() {
        fs::read_to_string(&input_file)?
    } else {
        error!("Couldn't find the input file '{}'.", &input_file);
        std::process::exit(1);
    };

    match format_str {
        None | Some("json") | Some("json-compact") => {
            let array_of_json_obj: Vec<JsonValue> = serde_json::from_str(&input_string)?;
            write_array_of_jsons_with_chunked_25(
                cx,
                array_of_json_obj,
                enable_set_inference,
                token_bucket,
                workers,
            )
            .await?;
        }
        Some("jsonl") => {
            let array_of_json_obj: StreamDeserializer<'_, StrRead<'_>, JsonValue> =
                Deserializer::from_str(&input_string).into_iter::<JsonValue>();
            let array_of_valid_json_obj: Vec<JsonValue> =
                array_of_json_obj.filter_map(Result::ok).collect();
            write_array_of_jsons_with_chunked_25(
                cx,
                array_of_valid_json_obj,
                enable_set_inference,
                token_bucket,
                workers,
            )
            .await?;
        }
        Some("csv") => {
            // CSV import remains sequential; use a shared client + RateLimiter for WCU control.
            let mut rate_limiter_csv: Option<RateLimiter> = token_bucket.as_ref().map(|_| {
                RateLimiter {
                    target_ops: 0.0, // will be overridden below
                    last_operation_time: None,
                    last_batch_size: 0,
                    last_consumed_cu: 0.0,
                }
            });
            // Re-derive target_ops for CSV rate limiting from wcu_percent if set
            if let (Some(percent), Some(ref mut rl)) = (wcu_percent, rate_limiter_csv.as_mut()) {
                let desc = control::describe_table_api(cx, ts.name.clone()).await;
                let wcu = desc
                    .provisioned_throughput
                    .as_ref()
                    .and_then(|pt| pt.write_capacity_units)
                    .unwrap_or(0);
                rl.target_ops = (wcu as f64) * (percent as f64 / 100.0);
            }

            let lines: Vec<&str> = input_string
                .split('\n')
                .collect::<Vec<&str>>()
                .into_iter()
                .filter(|&x| !x.is_empty())
                .collect::<Vec<&str>>();
            let headers: Vec<&str> = lines[0].split(',').collect::<Vec<&str>>();
            let mut matrix: Vec<Vec<&str>> = vec![];
            // Create a single client to reuse TCP connections across all batch writes.
            let shared_client = batch::create_batch_client(cx).await;
            let mut progress_status = ProgressState::new(MAX_NUMBER_OF_OBSERVES);
            // WCU consumed by the previous BatchWriteItem call; 0.0 on the first batch (no delay).
            let mut last_actual_wcu: f64 = 0.0;
            for (i, line) in lines.iter().enumerate().skip(1) {
                let cells: Vec<&str> = line.split(',').collect::<Vec<&str>>();
                debug!("splitted line => {:?}", cells);
                matrix.push(cells);
                if i % 25 == 0 {
                    if let Some(ref mut rl) = rate_limiter_csv {
                        rl.wait_based_on_cu(last_actual_wcu).await;
                    }
                    last_actual_wcu =
                        write_csv_matrix(cx, &shared_client, &matrix, &headers, enable_set_inference).await?;
                    progress_status.add_observation(25);
                    progress_status.show();
                    matrix.clear();
                }
            }
            debug!("rest of matrix => {:?}", matrix);
            if !matrix.is_empty() {
                if let Some(ref mut rl) = rate_limiter_csv {
                    rl.wait_based_on_cu(last_actual_wcu).await;
                }
                write_csv_matrix(cx, &shared_client, &matrix, &headers, enable_set_inference).await?;
                progress_status.add_observation(matrix.len());
                progress_status.show();
            }
        }
        Some(o) => panic!("Invalid input format is given: {}", o),
    }
    Ok(())
}

/// Delete items from a DynamoDB table using parallel BatchWriteItem DeleteRequests.
///
/// - Without `input_file`: scans the **entire** table (keys_only) and deletes ALL items.
/// - With `input_file`: reads the specified file (JSON / JSONL / CSV) and deletes only those items.
///   Only primary key(s) are extracted from each record; all other attributes are ignored.
///
/// In both modes, deletions are fanned out to `workers` concurrent BatchWriteItem requests
/// using a shared DynamoDB client and TokenBucket (same pattern as `import`).
pub async fn purge(
    cx: &app::Context,
    yes: bool,
    input_file: Option<String>,
    format: Option<String>,
    wcu_percent: Option<u8>,
    workers: usize,
) -> Result<(), batch::DyneinBatchError> {
    let ts: app::TableSchema = app::table_schema(cx).await;

    // Confirm before deleting unless --yes is specified
    if !yes {
        let msg = if input_file.is_some() {
            format!(
                "This will delete items listed in the input file from table '{}'. Continue?",
                ts.name
            )
        } else {
            format!(
                "This will DELETE ALL items from table '{}'. Are you sure? (This cannot be undone)",
                ts.name
            )
        };
        if !Confirm::new().with_prompt(msg).interact()? {
            println!("Operation has been cancelled.");
            return Ok(());
        }
    }

    if let Some(file) = input_file {
        // File-based: delete only items listed in the export file
        purge_from_file(cx, &ts, file, format.as_deref(), wcu_percent, workers).await
    } else {
        // Scan-based: delete ALL items in the table
        purge_all(cx, &ts, wcu_percent, workers).await
    }
}

/// Delete ALL items in a DynamoDB table by scanning and issuing parallel BatchWriteItem DeleteRequests.
///
/// The scan retrieves only primary key(s) (keys_only=true) to minimise RCU.
async fn purge_all(
    cx: &app::Context,
    ts: &app::TableSchema,
    wcu_percent: Option<u8>,
    workers: usize,
) -> Result<(), batch::DyneinBatchError> {
    // Setup token bucket (same pattern as import)
    let token_bucket: Option<Arc<TokioMutex<TokenBucket>>> = if let Some(percent) = wcu_percent {
        if ts.mode == table::Mode::OnDemand {
            println!("WARN: --wcu-percent is specified but the table is in OnDemand mode. Rate limiting will be skipped.");
            None
        } else {
            let desc = control::describe_table_api(cx, ts.name.clone()).await;
            let wcu = desc
                .provisioned_throughput
                .as_ref()
                .and_then(|pt| pt.write_capacity_units)
                .unwrap_or(0);
            if wcu == 0 {
                println!("WARN: Table WCU is 0. Rate limiting will be skipped.");
                None
            } else {
                let target = (wcu as f64) * (percent as f64 / 100.0);
                println!(
                    "Rate limiting enabled: using {}% of {} WCU ({:.1} writes/sec), workers={}",
                    percent, wcu, target, workers
                );
                Some(Arc::new(TokioMutex::new(TokenBucket::new(target))))
            }
        }
    } else {
        None
    };

    // Build shared DDB client ONCE for the entire purge.
    // All parallel workers share this client → connection pool is reused → no TLS timeout.
    let retry_config = cx
        .retry
        .as_ref()
        .map(|v| v.batch_write_item.as_ref().unwrap_or(&v.default));
    let sdk_config = cx
        .effective_sdk_config_with_retry(retry_config.cloned())
        .await;
    let shared_ddb = Arc::new(DynamoDbSdkClient::new(&sdk_config));

    let progress = Arc::new(TokioMutex::new(ProgressState::new(MAX_NUMBER_OF_OBSERVES)));
    let mut in_flight: FuturesUnordered<_> = FuturesUnordered::new();

    let mut last_evaluated_key: Option<HashMap<String, AttributeValue>> = None;

    loop {
        // Scan with keys_only=true to minimise RCU consumption.
        let scan_output = data::scan_api(
            cx,
            None,  /* index */
            false, /* consistent_read */
            &None, /* attributes */
            true,  /* keys_only */
            None,  /* limit */
            last_evaluated_key,
        )
        .await;

        let items = scan_output
            .items
            .expect("Scan result items should be 'Some' even if no item returned.");

        // Fan-out deletions in chunks of 25 (BatchWriteItem limit)
        for chunk in items.chunks(25) {
            let chunk_vec = chunk.to_vec();
            let count = chunk_vec.len();

            // Back-pressure: wait for one in-flight batch to complete before submitting another.
            if in_flight.len() >= workers {
                if let Some(result) = in_flight.next().await {
                    result?;
                }
            }

            // Rate-limit: acquire tokens based on estimated WCU (1 token ≈ 1 WCU for ≤1KB items).
            if let Some(ref tb) = token_bucket {
                acquire_tokens(tb, count as f64).await;
            }

            let ddb_clone = Arc::clone(&shared_ddb);
            let tb_clone = token_bucket.clone();
            let prog_clone = Arc::clone(&progress);
            let ts_clone = ts.clone();
            let table_name = cx.effective_table_name();

            in_flight.push(async move {
                let request_items = batch::build_delete_request_items_from_attrmap(
                    table_name,
                    chunk_vec,
                    &ts_clone,
                );
                let actual_wcu =
                    batch::batch_write_until_processed_with_client(&ddb_clone, request_items)
                        .await?;
                debug!(
                    "BatchWriteItem (delete) consumed {:.1} WCU this batch (actual)",
                    actual_wcu
                );
                // Reconcile actual vs estimated WCU consumption in the token bucket.
                if let Some(ref tb) = tb_clone {
                    let estimated = count as f64;
                    let diff = actual_wcu - estimated;
                    if diff > 0.0 {
                        acquire_tokens(tb, diff).await;
                    } else if diff < 0.0 {
                        let mut guard = tb.lock().await;
                        guard.tokens = (guard.tokens + (-diff)).min(guard.max_burst);
                    }
                }
                {
                    let mut prog = prog_clone.lock().await;
                    prog.add_observation(count);
                    prog.show();
                }
                Ok::<(), batch::DyneinBatchError>(())
            });
        }

        match scan_output.last_evaluated_key {
            None => break,
            Some(lek) => last_evaluated_key = Some(lek),
        }
    }

    // Drain remaining in-flight futures.
    while let Some(result) = in_flight.next().await {
        result?;
    }

    println!(); // newline after progress display
    Ok(())
}

/// Delete only the items listed in `input_file` from a DynamoDB table.
///
/// The file is typically produced by `dy export`. Only primary key(s) are extracted from each
/// record; other attributes are silently ignored. Supports JSON, JSONL, JSON-compact, and CSV.
/// Deletions are fanned out to `workers` concurrent BatchWriteItem requests (same as `import`).
async fn purge_from_file(
    cx: &app::Context,
    ts: &app::TableSchema,
    input_file: String,
    format: Option<&str>,
    wcu_percent: Option<u8>,
    workers: usize,
) -> Result<(), batch::DyneinBatchError> {
    // Setup token bucket (same pattern as import)
    let token_bucket: Option<Arc<TokioMutex<TokenBucket>>> = if let Some(percent) = wcu_percent {
        if ts.mode == table::Mode::OnDemand {
            println!("WARN: --wcu-percent is specified but the table is in OnDemand mode. Rate limiting will be skipped.");
            None
        } else {
            let desc = control::describe_table_api(cx, ts.name.clone()).await;
            let wcu = desc
                .provisioned_throughput
                .as_ref()
                .and_then(|pt| pt.write_capacity_units)
                .unwrap_or(0);
            if wcu == 0 {
                println!("WARN: Table WCU is 0. Rate limiting will be skipped.");
                None
            } else {
                let target = (wcu as f64) * (percent as f64 / 100.0);
                println!(
                    "Rate limiting enabled: using {}% of {} WCU ({:.1} writes/sec), workers={}",
                    percent, wcu, target, workers
                );
                Some(Arc::new(TokioMutex::new(TokenBucket::new(target))))
            }
        }
    } else {
        None
    };

    let input_string: String = if Path::new(&input_file).exists() {
        fs::read_to_string(&input_file)?
    } else {
        error!("Couldn't find the input file '{}'.", &input_file);
        std::process::exit(1);
    };

    match format {
        None | Some("json") | Some("json-compact") => {
            let array_of_json_obj: Vec<JsonValue> = serde_json::from_str(&input_string)?;
            delete_array_of_jsons_with_chunked_25(cx, ts, array_of_json_obj, token_bucket, workers)
                .await?;
        }
        Some("jsonl") => {
            let array_of_json_obj: StreamDeserializer<'_, StrRead<'_>, JsonValue> =
                Deserializer::from_str(&input_string).into_iter::<JsonValue>();
            let array_of_valid_json_obj: Vec<JsonValue> =
                array_of_json_obj.filter_map(Result::ok).collect();
            delete_array_of_jsons_with_chunked_25(
                cx,
                ts,
                array_of_valid_json_obj,
                token_bucket,
                workers,
            )
            .await?;
        }
        Some("csv") => {
            // CSV deletion is sequential (mirrors CSV import).
            let mut rate_limiter_csv: Option<RateLimiter> = token_bucket.as_ref().map(|_| {
                RateLimiter {
                    target_ops: 0.0,
                    last_operation_time: None,
                    last_batch_size: 0,
                    last_consumed_cu: 0.0,
                }
            });
            if let (Some(percent), Some(ref mut rl)) = (wcu_percent, rate_limiter_csv.as_mut()) {
                let desc = control::describe_table_api(cx, ts.name.clone()).await;
                let wcu = desc
                    .provisioned_throughput
                    .as_ref()
                    .and_then(|pt| pt.write_capacity_units)
                    .unwrap_or(0);
                rl.target_ops = (wcu as f64) * (percent as f64 / 100.0);
            }

            let lines: Vec<&str> = input_string
                .split('\n')
                .filter(|x| !x.is_empty())
                .collect();
            let headers: Vec<&str> = lines[0].split(',').collect();
            let mut matrix: Vec<Vec<&str>> = vec![];
            let shared_client = batch::create_batch_client(cx).await;
            let mut progress_status = ProgressState::new(MAX_NUMBER_OF_OBSERVES);
            let mut last_actual_wcu: f64 = 0.0;
            for (i, line) in lines.iter().enumerate().skip(1) {
                let cells: Vec<&str> = line.split(',').collect();
                matrix.push(cells);
                if i % 25 == 0 {
                    if let Some(ref mut rl) = rate_limiter_csv {
                        rl.wait_based_on_cu(last_actual_wcu).await;
                    }
                    last_actual_wcu = delete_csv_matrix(cx, &shared_client, ts, &matrix, &headers)
                        .await?;
                    progress_status.add_observation(25);
                    progress_status.show();
                    matrix.clear();
                }
            }
            if !matrix.is_empty() {
                if let Some(ref mut rl) = rate_limiter_csv {
                    rl.wait_based_on_cu(last_actual_wcu).await;
                }
                delete_csv_matrix(cx, &shared_client, ts, &matrix, &headers).await?;
                progress_status.add_observation(matrix.len());
                progress_status.show();
            }
        }
        Some(o) => panic!("Invalid input format is given: {}", o),
    }

    println!(); // newline after progress display
    Ok(())
}

/* =================================================
Private functions
================================================= */

async fn overwrite_attributes_or_exit(
    cx: &app::Context,
    ts: &app::TableSchema,
) -> Result<Option<String>, dialoguer::Error> {
    println!("As neither --keys-only nor --attributes options are given, fetching an item to understand attributes to export...");
    let suggested_attributes: Vec<SuggestedAttribute> = suggest_attributes(cx, ts).await;

    println!("Found following attributes in the first item in the table:");
    for preview_attribute in &suggested_attributes {
        println!(
            "  - {} ({})",
            preview_attribute.name, preview_attribute.type_str
        );
    }
    let msg = "Are you OK to export items in CSV with columns(attributes) above?";
    if !Confirm::new().with_prompt(msg).interact()? {
        app::bye(0, "Operation has been cancelled. You can use --keys-only or --attributes option to specify columns explicitly.");
    }

    Ok(Some(
        suggested_attributes
            .into_iter()
            .map(|sa| sa.name)
            .collect::<Vec<String>>()
            .join(","),
    ))
}

/// This function scan the fisrt item from the target table and use it as a source of attributes.
async fn suggest_attributes(cx: &app::Context, ts: &app::TableSchema) -> Vec<SuggestedAttribute> {
    let mut attributes_suggestion = vec![];

    let items = data::scan_api(
        cx,
        None,    /* index */
        false,   /* consistent_read */
        &None,   /* attributes */
        false,   /* keys_only */
        Some(1), /* limit */
        None,    /* esk */
    )
    .await
    .items
    .expect("items should be 'Some' even if there's no item in the table.");

    if items.is_empty() {
        app::bye(0, "No item to export in this table. Quit the operation.");
    }

    let primary_keys = [
        Some(ts.pk.name.to_owned()),
        ts.sk.to_owned().map(|x| x.name),
    ];
    let non_key_attributes = items[0]
        .iter()
        .filter(|(attr, _)| {
            !primary_keys
                .iter()
                .any(|key| Some(attr.to_owned()) == key.as_ref())
        })
        .collect::<Vec<(&String, &AttributeValue)>>();

    for (attr, attrval) in non_key_attributes {
        attributes_suggestion.push(SuggestedAttribute {
            name: attr.to_owned(),
            type_str: data::attrval_to_type(attrval).expect("attrval should be mapped"),
        });
    }

    debug!("Suggested attributes to use: {:?}", attributes_suggestion);
    attributes_suggestion
}

fn attrs_to_append(ts: &app::TableSchema, attributes: &Option<String>) -> Option<Vec<String>> {
    attributes
        .as_ref()
        .map(|ats| filter_attributes_to_append(ts, ats))
}

/// This function takes list of attributes separated by comma (e.g. "name,age,address")
/// and return vec of these strings, filtering pk/sk.
fn filter_attributes_to_append(ts: &app::TableSchema, ats: &str) -> Vec<String> {
    let mut attributes_to_append: Vec<String> = vec![];
    let splitted_attributes: Vec<String> = ats.split(',').map(|x| x.trim().to_owned()).collect();
    for attr in splitted_attributes {
        if attr == ts.pk.name || (ts.sk.is_some() && attr == ts.sk.as_ref().unwrap().name) {
            println!("NOTE: primary keys are included by default and you don't need to give them as a part of --attributes.");
            continue;
        }
        attributes_to_append.push(attr);
    }
    attributes_to_append
}

/// This function tweaks scan output items.
fn connectable_json(mut s: String, compact: bool) -> String {
    s.remove(0); // remove first char "["
    let len = s.len();
    if compact || len == 1 {
        s.truncate(len - 1); // remove last char "]"
    } else {
        s.truncate(len - 2); // remove last char "]" and newline
    }
    s.push(','); // add last "," so that continue to next iteration
    s
}

fn json_finish(mut f: fs::File, tmp_output_filename: &str) -> Result<fs::File, IOError> {
    f.write_all(b"[")?;
    let mut contents = fs::read_to_string(tmp_output_filename)?;
    let len = contents.len();
    contents.truncate(len - 1); // remove last ","
    f.write_all(contents.as_bytes())?;
    Ok(f)
}

fn jsonl_finish(mut f: fs::File, tmp_output_filename: &str) -> Result<(), IOError> {
    let contents = fs::read_to_string(tmp_output_filename)?;
    f.write_all(contents.as_bytes())?;
    Ok(())
}

fn csv_finish(
    mut f: fs::File,
    tmp_output_filename: &str,
    ts: &app::TableSchema,
    attributes_to_append: Option<Vec<String>>,
    keys_only: bool,
) -> Result<fs::File, IOError> {
    f.write_all(build_csv_header(ts, attributes_to_append, keys_only).as_bytes())?;
    let contents = fs::read_to_string(tmp_output_filename)?;
    f.write_all(contents.as_bytes())?;
    Ok(f)
}

fn build_csv_header(
    ts: &app::TableSchema,
    attributes_to_append: Option<Vec<String>>,
    keys_only: bool,
) -> String {
    let mut header_str: String = ts.pk.name.clone();
    if let Some(sk) = &ts.sk {
        header_str.push(',');
        header_str.push_str(&sk.name);
    };

    if keys_only {
    } else if let Some(attrs) = attributes_to_append {
        header_str.push(',');
        header_str.push_str(&attrs.join(","));
    }

    header_str.push('\n');
    header_str
}

/// Write items via parallel BatchWriteItem requests (up to `workers` in-flight at a time).
///
/// A single `DynamoDbSdkClient` is created once at the start and shared across all workers
/// via `Arc`. This ensures the underlying HTTP connection pool is reused, avoiding the
/// "HTTP connect timeout" errors that occur when every request creates a new client/connection.
///
/// Rate limiting is handled by a shared token-bucket (`token_bucket`), which is checked before
/// each batch is submitted. The Mutex lock is held only briefly; sleeping happens outside the lock.
async fn write_array_of_jsons_with_chunked_25(
    cx: &app::Context,
    array_of_json_obj: Vec<JsonValue>,
    enable_set_inference: bool,
    token_bucket: Option<Arc<TokioMutex<TokenBucket>>>,
    workers: usize,
) -> Result<(), batch::DyneinBatchError> {
    // Build the SDK config and shared client ONCE for the entire import.
    // All parallel workers share this client → the connection pool is reused → no TLS timeout.
    let retry_config = cx
        .retry
        .as_ref()
        .map(|v| v.batch_write_item.as_ref().unwrap_or(&v.default));
    let sdk_config = cx
        .effective_sdk_config_with_retry(retry_config.cloned())
        .await;
    let shared_ddb = Arc::new(DynamoDbSdkClient::new(&sdk_config));

    let progress = Arc::new(TokioMutex::new(ProgressState::new(MAX_NUMBER_OF_OBSERVES)));
    // FuturesUnordered drives up to `workers` BatchWriteItem futures concurrently.
    let mut in_flight: FuturesUnordered<_> = FuturesUnordered::new();

    for chunk in array_of_json_obj.chunks(25) {
        let items = chunk.to_vec();
        let count = items.len();

        // When at capacity: wait for one in-flight batch to finish before submitting another.
        if in_flight.len() >= workers {
            if let Some(result) = in_flight.next().await {
                result?;
            }
        }

        // Rate-limit: acquire tokens based on estimated WCU (1 token ≈ 1 WCU for ≤1KB items).
        // The lock is held only briefly; sleeping happens outside the lock.
        if let Some(ref tb) = token_bucket {
            acquire_tokens(tb, count as f64).await;
        }

        let ddb_clone = Arc::clone(&shared_ddb);
        let tb_clone = token_bucket.clone();
        let prog_clone = Arc::clone(&progress);

        // cx is &app::Context (a fat pointer), captured by copy into the async move block.
        in_flight.push(async move {
            let request_items = batch::convert_jsonvals_to_request_items(
                cx,
                items,
                enable_set_inference,
            )
            .await?;
            let actual_wcu =
                batch::batch_write_until_processed_with_client(&ddb_clone, request_items).await?;
            debug!(
                "BatchWriteItem consumed {:.1} WCU this batch (actual)",
                actual_wcu
            );
            // If there's a token bucket, reconcile actual vs estimated WCU consumption.
            if let Some(ref tb) = tb_clone {
                let estimated = count as f64;
                let diff = actual_wcu - estimated;
                if diff > 0.0 {
                    // Consumed more than estimated: deduct the extra from the bucket.
                    acquire_tokens(tb, diff).await;
                } else if diff < 0.0 {
                    // Consumed less: refund tokens back (capped at max_burst).
                    let mut guard = tb.lock().await;
                    guard.tokens = (guard.tokens + (-diff)).min(guard.max_burst);
                }
            }
            {
                let mut prog = prog_clone.lock().await;
                prog.add_observation(count);
                prog.show();
            }
            Ok::<(), batch::DyneinBatchError>(())
        });
    }

    // Drain any remaining in-flight futures.
    while let Some(result) = in_flight.next().await {
        result?;
    }

    Ok(())
}

/// Delete items (from a JSON/JSONL export file) via parallel BatchWriteItem DeleteRequests
/// (up to `workers` in-flight at a time). Mirrors `write_array_of_jsons_with_chunked_25`.
async fn delete_array_of_jsons_with_chunked_25(
    cx: &app::Context,
    ts: &app::TableSchema,
    array_of_json_obj: Vec<JsonValue>,
    token_bucket: Option<Arc<TokioMutex<TokenBucket>>>,
    workers: usize,
) -> Result<(), batch::DyneinBatchError> {
    let retry_config = cx
        .retry
        .as_ref()
        .map(|v| v.batch_write_item.as_ref().unwrap_or(&v.default));
    let sdk_config = cx
        .effective_sdk_config_with_retry(retry_config.cloned())
        .await;
    let shared_ddb = Arc::new(DynamoDbSdkClient::new(&sdk_config));

    let progress = Arc::new(TokioMutex::new(ProgressState::new(MAX_NUMBER_OF_OBSERVES)));
    let mut in_flight: FuturesUnordered<_> = FuturesUnordered::new();

    for chunk in array_of_json_obj.chunks(25) {
        let items = chunk.to_vec();
        let count = items.len();

        if in_flight.len() >= workers {
            if let Some(result) = in_flight.next().await {
                result?;
            }
        }

        if let Some(ref tb) = token_bucket {
            acquire_tokens(tb, count as f64).await;
        }

        let ddb_clone = Arc::clone(&shared_ddb);
        let tb_clone = token_bucket.clone();
        let prog_clone = Arc::clone(&progress);
        let ts_clone = ts.clone();

        in_flight.push(async move {
            let request_items = batch::convert_jsonvals_to_delete_request_items(
                cx,
                items,
                &ts_clone,
            )
            .await?;
            let actual_wcu =
                batch::batch_write_until_processed_with_client(&ddb_clone, request_items).await?;
            debug!(
                "BatchWriteItem (delete from file) consumed {:.1} WCU this batch (actual)",
                actual_wcu
            );
            if let Some(ref tb) = tb_clone {
                let estimated = count as f64;
                let diff = actual_wcu - estimated;
                if diff > 0.0 {
                    acquire_tokens(tb, diff).await;
                } else if diff < 0.0 {
                    let mut guard = tb.lock().await;
                    guard.tokens = (guard.tokens + (-diff)).min(guard.max_burst);
                }
            }
            {
                let mut prog = prog_clone.lock().await;
                prog.add_observation(count);
                prog.show();
            }
            Ok::<(), batch::DyneinBatchError>(())
        });
    }

    while let Some(result) = in_flight.next().await {
        result?;
    }

    Ok(())
}

/// Deletes a CSV matrix chunk via BatchWriteItem using a shared pre-built client.
/// Returns the actual WCU consumed (used by the sequential CSV rate limiter).
async fn delete_csv_matrix(
    cx: &app::Context,
    ddb: &DynamoDbSdkClient,
    ts: &app::TableSchema,
    matrix: &[Vec<&str>],
    headers: &[&str],
) -> Result<f64, batch::DyneinBatchError> {
    let request_items: HashMap<String, Vec<WriteRequest>> =
        batch::csv_matrix_to_delete_request_items(cx, matrix, headers, ts).await?;
    let consumed_wcu = batch::batch_write_until_processed_with_client(ddb, request_items).await?;
    debug!(
        "BatchWriteItem (delete CSV) consumed {:.1} WCU this batch (actual)",
        consumed_wcu
    );
    Ok(consumed_wcu)
}

/// Writes a CSV matrix chunk via BatchWriteItem using a shared pre-built client.
/// Returns the actual WCU consumed (used by the sequential CSV rate limiter).
async fn write_csv_matrix(
    cx: &app::Context,
    ddb: &DynamoDbSdkClient,
    matrix: &[Vec<&str>],
    headers: &[&str],
    enable_set_inference: bool,
) -> Result<f64, batch::DyneinBatchError> {
    let request_items: HashMap<String, Vec<WriteRequest>> =
        batch::csv_matrix_to_request_items(cx, matrix, headers, enable_set_inference).await?;
    let consumed_wcu = batch::batch_write_until_processed_with_client(ddb, request_items).await?;
    debug!("BatchWriteItem consumed {:.1} WCU this CSV batch (actual)", consumed_wcu);
    Ok(consumed_wcu)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ops::Add;
    use std::time::Duration;

    // --- RateLimiter ---

    #[test]
    fn test_rate_limiter_initialization() {
        let rl = RateLimiter::new(1000, 50);
        assert_eq!(rl.target_ops, 500.0);
        assert!(rl.last_operation_time.is_none());
        assert_eq!(rl.last_batch_size, 0);
        assert_eq!(rl.last_consumed_cu, 0.0);
    }

    #[test]
    fn test_rate_limiter_full_percentage() {
        let rl = RateLimiter::new(500, 100);
        assert_eq!(rl.target_ops, 500.0);
    }

    #[test]
    fn test_rate_limiter_half_percentage() {
        let rl = RateLimiter::new(10000, 50);
        assert_eq!(rl.target_ops, 5000.0);
    }

    #[test]
    fn test_rate_limiter_zero_percentage() {
        let rl = RateLimiter::new(10000, 0);
        assert_eq!(rl.target_ops, 0.0);
    }

    /// Verify that the first call to `wait_if_needed` does not block and correctly
    /// updates internal state (last_operation_time and last_batch_size).
    #[tokio::test]
    async fn test_rate_limiter_first_call_updates_state() {
        let mut rl = RateLimiter::new(1000, 100);
        assert!(rl.last_operation_time.is_none());
        assert_eq!(rl.last_batch_size, 0);

        // First call should not sleep because last_operation_time is None.
        rl.wait_if_needed(25).await;

        assert!(rl.last_operation_time.is_some());
        assert_eq!(rl.last_batch_size, 25);
    }

    /// Verify that each call to `wait_if_needed` updates last_batch_size.
    #[tokio::test]
    async fn test_rate_limiter_batch_size_updated_each_call() {
        let mut rl = RateLimiter::new(1000, 100);

        rl.wait_if_needed(10).await;
        assert_eq!(rl.last_batch_size, 10);

        tokio::time::sleep(Duration::from_millis(20)).await;
        rl.wait_if_needed(25).await;
        assert_eq!(rl.last_batch_size, 25);
    }

    /// Verify that a low WCU rate causes a measurable delay.
    #[tokio::test]
    async fn test_rate_limiter_imposes_delay() {
        // target_ops = 10 WCU * 100% = 10 items/sec → min_duration for 5 items = 0.5s
        let mut rl = RateLimiter::new(10, 100);
        rl.wait_if_needed(5).await; // first call: no delay
        let before = Instant::now();
        rl.wait_if_needed(1).await; // second call: should sleep ~0.5s - elapsed
        let elapsed = before.elapsed();

        assert!(
            elapsed >= Duration::from_millis(400),
            "Expected delay >=400ms, got {:?}",
            elapsed
        );
    }

    /// Verify that wait_based_on_cu updates last_consumed_cu correctly (first call = no delay).
    #[tokio::test]
    async fn test_rate_limiter_rcu_first_call_no_delay() {
        let mut rl = RateLimiter::new(3000, 90); // target = 2700 CU/sec
        assert!(rl.last_operation_time.is_none());
        assert_eq!(rl.last_consumed_cu, 0.0);

        // First call: last_consumed_cu == 0 so no delay, but stores the value.
        rl.wait_based_on_cu(256.0).await;

        assert!(rl.last_operation_time.is_some());
        assert_eq!(rl.last_consumed_cu, 256.0);
    }

    /// Verify that wait_based_on_cu imposes a delay based on actual consumed CU (RCU/WCU).
    #[tokio::test]
    async fn test_rate_limiter_rcu_imposes_delay() {
        // target = 10 CU/sec; last call consumed 5 CU → min_duration = 0.5s
        let mut rl = RateLimiter::new(10, 100); // target_ops = 10
        rl.wait_based_on_cu(5.0).await; // first call: no delay, stores 5.0 CU
        let before = Instant::now();
        rl.wait_based_on_cu(1.0).await; // second call: should sleep ~0.5s - elapsed
        let elapsed = before.elapsed();

        assert!(
            elapsed >= Duration::from_millis(400),
            "Expected CU-based delay >=400ms, got {:?}",
            elapsed
        );
    }

    // --- ProgressState ---

    #[test]
    fn test_progress_status() {
        let mut progress = ProgressState::new(2);

        let first_observation = Instant::now();
        progress.add_observation_with_time(10, first_observation);
        assert_eq!(progress.processed_items(), 10);
        assert_eq!(progress.recent_processed_items.len(), 1);
        assert!(progress
            .recent_average_processed_items_per_second_with_time(first_observation)
            .is_nan());

        let second_observation = first_observation.add(Duration::from_millis(500));
        progress.add_observation_with_time(10, second_observation);
        assert_eq!(progress.processed_items(), 20);
        assert_eq!(progress.recent_processed_items.len(), 2);
        assert_eq!(
            progress.recent_average_processed_items_per_second_with_time(second_observation),
            (10.0 + 10.0) / 0.5
        );

        let third_observation = first_observation.add(Duration::from_millis(1000));
        progress.add_observation_with_time(12, third_observation);
        assert_eq!(progress.processed_items(), 32);
        assert_eq!(progress.recent_processed_items.len(), 2);
        assert_eq!(
            progress.recent_average_processed_items_per_second_with_time(third_observation),
            (10.0 + 12.0) / 0.5
        );
    }
}
