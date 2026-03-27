# Dragonfly5 Server

PostgreSQL / MySQL に HTTP API でアクセスするための**分散型データベースプロキシサーバー**です。複数ノードでクラスターを構成し、負荷に応じてリクエストを振り分けます。

## 軽量性

Go 製の単一バイナリで、**常駐メモリは目安として約 20 MB 前後**に抑えられます。プロキシ層のオーバーヘッドが小さく、**レイテンシへの影響はほぼ無視できる**レベルを想定した設計です（実効レイテンシは主にデータベース側の処理時間に依存します）。

## 主な機能

- **マルチノードクラスター**: 複数サーバー間で負荷分散
- **読み書き分離**: 読み取り専用クエリと書き込み（DML）を別々の接続プールで処理
- **トランザクション対応**: Begin / Query / Execute / Commit / Rollback / Close
- **ヘルスベースのルーティング**: 各ノードの負荷・レイテンシ・エラー率を考慮してノードを選択
- **307 リダイレクト**: 負荷が高いノードは他ノードへリダイレクト
- **ストリーミング応答**: 大量結果をメモリ効率よく返却（オプション）

## アーキテクチャ

```
[Client] --> [Node A] --> /healz でクラスター内のノード情報を収集
    |            |
    |            +--> 負荷が低い → 自ノードで処理
    |            |
    |            +--> 負荷が高い → 307 Redirect → [Node B] で処理
    |
    +------> [Node B] ...
```

- **Balancer**: リクエストごとに最適なノード・データソースを選択（TopK + 重み付きランダム）
- **RequestHandler**: SQL の実行と結果の JSON / バイナリストリーム返却
- **DsManager**: データソースごとの接続プールとトランザクション管理

## 必要条件

- Go 1.25.0 以上
- PostgreSQL または MySQL

## セットアップ

### 1. 依存関係のインストール

```bash
cd src
go mod download
```

### 2. 設定ファイルの編集

`config.yaml` を編集し、ノード名・ポート・データソース・クラスター構成を設定します。

```yaml
nodeName: "dsnode-1"
nodePort: 5678
secretKey: "secret-key"
maxHttpQueue: 10000
streamingResponseMode: false # only false in opensource version

logger:
  level: info       # debug, info, warn, error
  dir: "logs"       # ログ出力先（カレントからの相対パス）
  maxSizeMB: 100    # 1ファイル最大サイズ（MB）
  maxBackups: 10    # 保持する世代数
  maxAgeDays: 0     # 日数超過で削除（0=無制限）
  compress: false   # ローテート済みをgzip圧縮

clusterNodes:
  - "http://localhost:5678"
  - "http://localhost:5679"


myDatasources:
  - datasourceId: pg_master
    databaseName: crm-system
    driver: postgres # postgres, mysql
    dsn: "postgres://user:password@localhost:5432/dbname?sslmode=disable"
    maxConns: 20
    maxWriteConns: 10
    minWriteConns: 5
    maxConnLifetimeSec: 1800
    maxTxIdleTimeoutSec: 15
    defaultQueryTimeoutSec: 30
  - datasourceId: pg_slave01_readonly
    databaseName: crm-system
    driver: postgres
    dsn: "postgres://user:password@localhost2:5432/dbname?sslmode=disable"
    maxConns: 20
    maxWriteConns: 0
    minWriteConns: 0
    maxConnLifetimeSec: 1800
    maxTxIdleTimeoutSec: 15
    defaultQueryTimeoutSec: 30
```

### 3. ビルド・実行

```bash
cd src
go build -o dragonfly5
./dragonfly5                        # デフォルトで config.yaml を読み込み
./dragonfly5 /path/to/config.yaml   # 設定ファイルを指定
```

## API エンドポイント

### ヘルスチェック

| メソッド | パス | 説明 |
|---------|------|------|
| GET | `/healz` | ノードの状態・データソース統計を JSON で返却 |

### クエリ・実行

| メソッド | パス | 説明 |
|---------|------|------|
| POST | `/rdb/query` | 読み取り専用クエリ（SELECT） |
| POST | `/rdb/execute` | 書き込み（INSERT / UPDATE / DELETE） |

### トランザクション

| メソッド | パス | 説明 |
|---------|------|------|
| POST | `/rdb/tx/begin` | トランザクション開始 |
| POST | `/rdb/tx/query` | トランザクション内で SELECT |
| POST | `/rdb/tx/execute` | トランザクション内で DML |
| PUT | `/rdb/tx/commit` | コミット |
| PUT | `/rdb/tx/rollback` | ロールバック |
| PUT | `/rdb/tx/close` | トランザクション終了（接続解放） |

## リクエストヘッダー

| ヘッダー | 必須 | 説明 |
|---------|------|------|
| `_cy_SecretKey` | ✓ | 認証用シークレットキー |
| `_cy_DbName` | ✓ | 対象データベース名（`databaseName` と一致） |
| `_cy_TxID` | トランザクション時 | トランザクション ID（`/begin` のレスポンスで取得） |
| `_cy_RdCount` | - | リダイレクト許容回数（省略時 0 = リダイレクトしない） |
| `_cy_TimeoutSec` | - | クエリタイムアウト（秒） |
| `X-Trace-Id` | - | リクエストトレース ID |

## リクエストボディ（JSON）

```json
{
  "sql": "SELECT * FROM users WHERE id = ?",
  "params": [
    { "type": "INT", "value": 1 }
  ],
  "timeoutSec": 30,
  "offsetRows": 0,
  "limitRows": 100
}
```

### パラメータ型（`params[].type`）

`NULL`, `BOOL`, `INT`, `LONG`, `FLOAT`, `DOUBLE`, `DATE`, `DATETIME`, `STRING`, `BYTES`, `AS_IS`

## レスポンス

### クエリ成功時

```json
{
  "meta": [
    { "name": "id", "dbType": "INT8", "nullable": false },
    { "name": "name", "dbType": "VARCHAR", "nullable": true }
  ],
  "rows": [[1, "Alice"], [2, "Bob"]],
  "totalCount": 2,
  "elapsedTimeUs": 1234
}
```

### 実行成功時（Execute）

```json
{
  "effectedRows": 5,
  "elapsedTimeUs": 567
}
```

### エラー時

```json
{
  "errcode": "BAD_REQUEST",
  "message": "詳細メッセージ"
}
```

### 307 リダイレクト時

- HTTP ステータス: `307 Temporary Redirect`
- `Location` ヘッダー: リダイレクト先ノードの NodeID
- クライアントは `Location` の URL に再リクエストする必要があります


## 設定項目

| 項目 | 説明 |
|------|------|
| `nodeName` | ノード識別名 |
| `nodePort` | HTTP サーバの待ち受けポート |
| `secretKey` | API 認証キー |
| `maxHttpQueue` | ノードあたりの最大同時 HTTP リクエスト数 |
| `streamingResponseMode` | ストリーミング応答の有効化 |
| `clusterNodes` | クラスター内の他ノード URL 一覧、自ノード含めても可 |
| `myDatasources` | 自ノードが持つデータソース設定 |
| `logger` | ログレベル、出力先、ローテーション設定 |

## ライセンス

Apache License 2.0
