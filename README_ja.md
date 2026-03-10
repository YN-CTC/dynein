# dynein — DynamoDB CLI ツール (日本語ドキュメント)

dynein は DynamoDB のテーブルやデータを簡潔なインターフェースで操作するコマンドラインツールです。  
設定ファイルは `$HOME/.dynein/` ディレクトリに保存されます。

---

## 目次

- [インストール](#インストール)
- [基本的な使い方](#基本的な使い方)
- [export — テーブルをファイルにエクスポート](#export--テーブルをファイルにエクスポート)
- [import — ファイルからテーブルにインポート](#import--ファイルからテーブルにインポート)
- [purge — アイテムを一括削除](#purge--アイテムを一括削除)
- [パフォーマンスチューニング](#パフォーマンスチューニング)

---

## インストール

```bash
cargo install dynein
```

または GitHub からビルド:

```bash
git clone https://github.com/YN-CTC/dynein.git
cd dynein
cargo build --release
cp target/release/dy /usr/local/bin/
```

---

## 基本的な使い方

```bash
# 操作対象テーブルを設定（以降のコマンドで --table を省略可能）
dy use --table MyTable

# テーブル一覧
dy list

# テーブル情報の確認
dy desc
```

---

## export — テーブルをファイルにエクスポート

テーブル内のアイテムを CSV / JSON / JSONL ファイルとして出力します。

```
dy export [OPTIONS] --output-file <FILE>
```

### オプション

| オプション | 短縮形 | 説明 |
|---|---|---|
| `--output-file <FILE>` | `-o` | 出力ファイルパス（必須） |
| `--format <FORMAT>` | `-f` | 出力フォーマット: `json` / `jsonl` / `json-compact` / `csv` （省略時: `json`） |
| `--keys-only` | | PKとSKのみ出力（CSV専用） |
| `--attributes <ATTRS>` | `-a` | 出力カラムをカンマ区切りで指定（CSV専用、例: `name,age,address`） |
| `--rcu-percent <1-100>` | | 使用するRCUの割合を指定（Provisioned モード専用） |

### 使用例

```bash
# JSONL 形式で全アイテムをエクスポート
dy export -o items.jsonl -f jsonl

# JSON 形式でエクスポート
dy export -o items.json -f json

# CSV でキーのみエクスポート（後でpurgeに使用可能）
dy export -o keys.csv -f csv --keys-only

# RCUの50%だけを使ってエクスポート（他の処理と共存）
dy export -o items.jsonl -f jsonl --rcu-percent 50
```

### フォーマットの説明

| フォーマット | 説明 |
|---|---|
| `json` | インデント付きの通常のJSON（配列形式） |
| `jsonl` | 1行1アイテムのJSON Lines形式。大容量データに適している |
| `json-compact` | 1行に全アイテムをまとめたコンパクトなJSON |
| `csv` | ヘッダー付きCSV。`--keys-only` または `--attributes` と組み合わせて使用 |

> **注意**: JSON/JSONL 形式では全属性が出力されます。CSV 形式ではカラム指定が必要です。

---

## import — ファイルからテーブルにインポート

CSV / JSON / JSONL ファイルからアイテムをテーブルに書き込みます。  
複数の並列ワーカーで高速にインポートできます。

```
dy import [OPTIONS] --input-file <FILE>
```

### オプション

| オプション | 短縮形 | 説明 |
|---|---|---|
| `--input-file <FILE>` | `-i` | 入力ファイルパス（必須） |
| `--format <FORMAT>` | `-f` | 入力フォーマット: `json` / `jsonl` / `json-compact` / `csv` |
| `--wcu-percent <1-100>` | | 使用するWCUの割合を指定（Provisioned モード専用） |
| `--workers <N>` | | 並列リクエスト数（デフォルト: `8`） |
| `--enable-set-inference` | | セット型の型推論を有効化（後方互換用） |

### 使用例

```bash
# JSONL ファイルをインポート（安全・安定な設定）
dy import -i items.jsonl -f jsonl --workers 2

# 速度を出したい場合（workers 3〜4）
dy import -i items.jsonl -f jsonl --workers 3 --wcu-percent 80

# WCU 3000 の50%（1500 writes/sec）で2並列インポート
dy import -t MyTable -i items.jsonl -f jsonl --workers 2 --wcu-percent 50

# CSV ファイルをインポート
dy import -i items.csv -f csv
```

### スループットの目安

`BatchWriteItem` は1リクエスト最大25アイテムです。

```
スループット ≈ workers × 25 / RTT (秒)
```

| workers | 目安スループット | 推奨用途 |
|---|---|---|
| 2 | ~500–1,500 items/sec | **通常運用（安全・安定）** |
| 3 | ~1,000–2,500 items/sec | 速度を出したいとき |
| 4 | ~1,500–3,500 items/sec | さらに速度が必要なとき |

> **基本は `--workers 2` が安全です。** スロットリングが起きない範囲で速度を上げたい場合は `3`〜`4` を試してください。  
> OnDemand モードのテーブルではWCU上限がないため、`--wcu-percent` を省略して最大スループットを発揮できます。

---

## purge — アイテムを一括削除

テーブルのアイテムを `BatchWriteItem DeleteRequest` で高速に削除します。  
**2つのモード**があります。

```
dy purge [OPTIONS]
```

### オプション

| オプション | 短縮形 | 説明 |
|---|---|---|
| `--yes` | `-y` | 確認プロンプトをスキップ |
| `--input-file <FILE>` | `-i` | 削除対象ファイル（省略時はテーブル全件削除） |
| `--format <FORMAT>` | `-f` | 入力フォーマット: `json` / `jsonl` / `json-compact` / `csv` |
| `--wcu-percent <1-100>` | | 使用するWCUの割合を指定（Provisioned モード専用） |
| `--workers <N>` | | 並列リクエスト数（デフォルト: `8`） |

---

### モード 1: テーブル全件削除（`--input-file` 省略）

テーブルを Scan して全アイテムを削除します。

```bash
# 確認プロンプト付きで全件削除（安全・安定な設定）
dy purge --workers 2

# 確認スキップで全件削除
dy purge --yes --workers 2

# 速度を出したい場合（workers 3〜4）
dy purge --yes --workers 3 --wcu-percent 80
```

---

### モード 2: ファイルを指定して削除（`--input-file` 指定）

エクスポートファイルに記載されたアイテムのみ削除します。  
ファイルから **PKとSK以外の属性は無視** されます。

#### 典型的なワークフロー

```bash
# 1. テーブル全件をエクスポート
dy export -o all_items.jsonl -f jsonl

# 2. 残したいアイテムをファイルから手動削除
vi all_items.jsonl   # 削除したくない行を消す

# 3. ファイルに残ったアイテムだけをDynamoDBから削除
dy purge -i all_items.jsonl -f jsonl --yes
```

#### CSV を使った例

```bash
# PKとSKのみのCSVをエクスポート（ファイルサイズが小さい）
dy export -o keys.csv -f csv --keys-only

# 残したいキーを削除してからpurge
vi keys.csv
dy purge -i keys.csv -f csv --yes --wcu-percent 80 --workers 2
```

#### レート制限付きの削除

```bash
# WCUの50%を使って削除（安全・安定）
dy purge -i items.jsonl -f jsonl --wcu-percent 50 --workers 2

# 出力例:
# Rate limiting enabled: using 50% of 3000 WCU (1500.0 writes/sec), workers=2
# 50000 items processed (1487.23 items/sec)
```

---

## パフォーマンスチューニング

### OnDemand vs Provisioned

| モード | 推奨設定 |
|---|---|
| **OnDemand** | `--workers 3〜4`（WCU制限なし） |
| **Provisioned** | `--workers 2`（安全）、速度を出す場合は `3〜4` + `--wcu-percent 50〜80` |

### workers の選び方

```bash
# 基本（安全・安定）
--workers 2

# 速度を出したいとき
--workers 3 --wcu-percent 80

# さらに速度が必要なとき
--workers 4 --wcu-percent 80
```

### テーブルモードの切り替え

インポート/エクスポート/パージ前に OnDemand モードに切り替えると最高速度が出ます:

```bash
# OnDemand に切り替え
dy admin update table MyTable --mode ondemand

# 処理後に Provisioned に戻す
dy admin update table MyTable --mode provisioned --wcu 1000 --rcu 1000
```

---

## ライセンス

Apache License 2.0 — 詳細は [LICENSE](LICENSE) を参照してください。
