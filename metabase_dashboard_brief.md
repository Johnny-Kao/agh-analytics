# Metabase Dashboard 建置說明

## 環境資訊

| 項目 | 值 |
|------|-----|
| Metabase URL | http://YOUR_SERVER_IP:3001 |
| PostgreSQL Host | YOUR_SERVER_IP（從外部連）或 `postgres`（Docker 內部） |
| PostgreSQL Port | 5432 |
| Database | `agh_analytics` |
| User | `agh` |
| Password | 見 `.env` 中的 `PG_PASSWORD` |

資料來源：AdGuard Home DNS query log，每 5 分鐘自動增量入庫，保留 14 天。

---

## 資料庫 Schema

### 主表：`dns_queries`（raw 明細）

每一筆 DNS query 一個 row。

| 欄位 | 型別 | 說明 |
|------|------|------|
| `id` | bigint | PK |
| `event_time` | timestamptz | query 發生時間（UTC） |
| `event_date` | date | query 日期 |
| `event_hour` | smallint | 0–23 |
| `day_of_week` | smallint | 1=週一 … 7=週日（ISO） |
| `is_weekend` | boolean | 週六日為 true |
| `time_segment` | text | `late_night` / `morning` / `afternoon` / `evening` |
| `client_key` | text | 設備唯一識別鍵，格式見下方 |
| `client_name` | text | AGH 設定的設備名稱（可能為空） |
| `client_ip` | inet | 設備 IP |
| `qname` | text | 完整查詢域名，如 `www.google.com` |
| `root_domain` | text | 根域名，如 `google.com` |
| `qtype` | text | 查詢類型：`A` / `AAAA` / `PTR` / `HTTPS` 等 |
| `response_status` | text | `allowed` / `blocked` / `cached` / `rewrite` |
| `block_reason` | text | 被封鎖時的規則文字，如 `\|\|ads.com^` |
| `rcode` | text | DNS 回應碼：`NOERROR` / `NXDOMAIN` / `SERVFAIL` |
| `upstream` | text | 上游 DNS resolver，如 `1.1.1.1:53` |
| `elapsed_ms` | numeric(12,3) | 查詢耗時（毫秒） |
| `answers_json` | jsonb | DNS 回應內容（`[{type, value, ttl}]`） |
| `raw_json` | jsonb | 完整原始 API 資料 |
| `event_fingerprint` | text | SHA256 dedup key（unique） |

#### `client_key` 格式說明

```
agh:<id>       → AGH 設定了 persistent client ID
name:<名稱>    → 依 AGH 設備名稱識別
ip:<IP>        → 無名稱，依 IP 識別
unknown:<hash> → fallback
```

#### `time_segment` 對應

```
late_night  → 00:00–05:59
morning     → 06:00–11:59
afternoon   → 12:00–17:59
evening     → 18:00–23:59
```

---

### Aggregate 表（預計算，Dashboard 優先用這些）

所有 aggregate 表都有 `bucket_size` 欄位：
- `'1h'`：小時粒度，保留最近 3 天
- `'1d'`：天粒度，保留最近 14 天

**在 Metabase 用 aggregate 表而非 `dns_queries` raw 表，查詢速度快很多。**

---

#### `agg_overview` — 全局統計

| 欄位 | 說明 |
|------|------|
| `bucket_start` | timestamptz，時間桶起點 |
| `bucket_size` | `'1h'` 或 `'1d'` |
| `total_queries` | 該時間桶內總查詢數 |
| `blocked_queries` | 被封鎖數 |
| `cached_queries` | 快取命中數 |
| `unique_clients` | 活躍設備數 |
| `unique_domains` | 查詢的不同域名數 |
| `avg_elapsed_ms` | 平均耗時（ms） |
| `p95_elapsed_ms` | P95 耗時（ms） |

---

#### `agg_client_usage` — 每設備使用量

| 欄位 | 說明 |
|------|------|
| `bucket_start` | 時間桶起點 |
| `bucket_size` | `'1h'` 或 `'1d'` |
| `client_key` | 設備識別鍵 |
| `client_name` | 設備名稱 |
| `total_queries` | 該設備在該時間桶的查詢數 |
| `blocked_queries` | 被封鎖數 |
| `unique_domains` | 查詢的不同域名數 |
| `avg_elapsed_ms` | 平均耗時 |

---

#### `agg_client_domain_usage` — 設備 × 域名

| 欄位 | 說明 |
|------|------|
| `bucket_start` | 時間桶起點 |
| `bucket_size` | `'1h'` 或 `'1d'` |
| `client_key` | 設備識別鍵 |
| `root_domain` | 根域名 |
| `total_queries` | 查詢數 |
| `blocked_queries` | 被封鎖數 |

---

#### `agg_domain_usage` — 每域名統計

| 欄位 | 說明 |
|------|------|
| `bucket_start` | 時間桶起點 |
| `bucket_size` | `'1h'` 或 `'1d'` |
| `root_domain` | 根域名 |
| `total_queries` | 查詢數 |
| `blocked_queries` | 被封鎖數 |
| `unique_clients` | 有多少設備查過這個域名 |
| `time_segment` | 主要查詢時段（僅 `1h` 有值） |

---

#### `agg_domain_client_usage` — 域名 × 設備

| 欄位 | 說明 |
|------|------|
| `bucket_start` | 時間桶起點 |
| `bucket_size` | `'1h'` 或 `'1d'` |
| `root_domain` | 根域名 |
| `client_key` | 設備識別鍵 |
| `client_name` | 設備名稱 |
| `total_queries` | 查詢數 |
| `blocked_queries` | 被封鎖數 |

---

## 注意事項

1. **Metabase 連 PostgreSQL 時用外部 IP**：Host 填伺服器 IP，因為 Metabase 的 Database 設定是從 Metabase 自己的視角發出連線
2. **aggregate 表的時間篩選一定要加 `bucket_size` 條件**，否則會把 1h 和 1d 的資料加在一起
3. **`client_name` 可能為 null**（無名稱設備只有 IP），Metabase 中可用 `COALESCE(client_name, HOST(client_ip))` 顯示
4. **時區**：所有時間欄位存 UTC，Metabase 設定時把 Report Timezone 設成你當地時區
5. **資料更新頻率**：每 5 分鐘 ingest，每小時 aggregate
