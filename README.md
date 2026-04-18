# AGH Analytics

Turn your [AdGuard Home](https://adguard.com/adguard-home.html) DNS logs into a real-time analytics dashboard — automatically.

```
AdGuard Home  →  Python ETL  →  PostgreSQL  →  Metabase
  (DNS logs)     (every 5 min)   (14-day store)  (3 dashboards)
```

## Dashboards

### 🌐 Dashboard 1 — Global Overview
Today's KPIs, hourly query trends, block rate, and latency (avg + P95).

![Dashboard 1](docs/dash1_overview.jpg)

### 📱 Dashboard 2 — Device Analysis
Per-device query volume, block rankings, and top domains per device.

![Dashboard 2](docs/dash2_devices.jpg)

### 🔍 Dashboard 3 — Domain Analysis
Top queried / blocked domains, late-night traffic, and per-domain device breakdown.

![Dashboard 3](docs/dash3_domains.jpg)

All dashboards support **Date Range**, **Device IP**, and **Domain** filters.

---

## Why Build This?

AdGuard Home's built-in UI only shows the last few hundred log entries with no trend analysis or cross-device comparison. This project answers questions like:

- Which device is making the most DNS queries?
- Which domains are blocked most often, and by whom?
- Is there suspicious late-night traffic?
- How has my block rate changed over time?

---

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                  Docker Compose                      │
│                                                     │
│  ┌──────────┐   ┌──────────────┐   ┌─────────────┐ │
│  │PostgreSQL│   │  ETL (Python)│   │  Metabase   │ │
│  │          │←──│              │   │  port 3001  │ │
│  │ port 5432│   │ cron: 5 min  │   │             │ │
│  └──────────┘   └──────────────┘   └─────────────┘ │
└─────────────────────────────────────────────────────┘
         ↑                  ↑
    stores data        polls AGH API
                    /control/querylog
```

**ETL pipeline:**
1. Polls AGH `/control/querylog` every 5 minutes (cursor-based, no duplicates)
2. Normalises records — extracts `root_domain` (tldextract), maps AGH reasons → `response_status`, computes SHA-256 dedup fingerprint
3. Inserts into `dns_queries` raw table (`ON CONFLICT DO NOTHING`)
4. Hourly job rebuilds 5 aggregate tables (1h / 1d buckets) for fast dashboard queries

**Design choices:**

| Choice | Reason |
|--------|--------|
| Python ETL | AGH API needs cursor pagination and field normalisation |
| PostgreSQL | `inet` type, window functions, Metabase compatibility |
| Aggregate tables | Raw table grows to 100k+ rows; pre-aggregation keeps dashboards fast |
| Docker Compose | Single command deploy, auto-restart on reboot |
| Native SQL in Metabase | More flexible than GUI builder; supports template tag filters |

---

## Database Schema

### Raw table: `dns_queries`

| Column | Type | Description |
|--------|------|-------------|
| `event_time` | timestamptz | Query timestamp (UTC) |
| `client_key` | text | Device ID: `agh:<id>` / `name:<n>` / `ip:<ip>` |
| `client_name` | text | Device name from AGH (nullable) |
| `client_ip` | inet | Device IP |
| `root_domain` | text | Root domain, e.g. `google.com` |
| `qname` | text | Full query name |
| `qtype` | text | Query type: `A`, `AAAA`, `PTR`, `HTTPS`… |
| `response_status` | text | `allowed` / `blocked` / `cached` / `rewrite` |
| `elapsed_ms` | numeric | Query latency in ms |
| `event_fingerprint` | text | SHA-256 dedup key (unique) |

### Aggregate tables

- `agg_overview` — global stats per bucket
- `agg_client_usage` — per-device stats
- `agg_client_domain_usage` — device × domain cross
- `agg_domain_usage` — per-domain stats
- `agg_domain_client_usage` — domain × device cross

All aggregate tables have `bucket_size = '1h'` (last 3 days) or `'1d'` (last 14 days).

Full schema: see [`metabase_dashboard_brief.md`](metabase_dashboard_brief.md)

---

## Requirements

- Docker + Docker Compose
- AdGuard Home with API access enabled
- ~1 GB disk for PostgreSQL data (14-day retention)
- ~512 MB RAM for Metabase JVM

---

## Installation

### 1. Clone and configure

```bash
git clone https://github.com/Johnny-Kao/agh-analytics.git
cd agh-analytics/agh_pg_etl
cp .env.example .env
```

Edit `.env`:

```env
AGH_BASE_URL=http://YOUR_AGH_HOST/control   # AGH control API base URL
AGH_USERNAME=your-username
AGH_PASSWORD=your-password

PG_PASSWORD=change-me-strong-password       # Choose a strong password
```

### 2. Start the stack

```bash
docker compose up -d
```

On first start:
- PostgreSQL schema is created automatically
- ETL backfills the last 7 days of AGH query logs
- Metabase is available at `http://YOUR_HOST:3001`

### 3. Configure Metabase

1. Open `http://YOUR_HOST:3001` and complete the setup wizard
2. Add a PostgreSQL database:
   - Host: `YOUR_HOST` (use server IP, not `postgres` — Metabase connects from its own perspective)
   - Port: `5432`
   - Database: `agh_analytics`
   - Username: `agh`
   - Password: value of `PG_PASSWORD` in your `.env`
3. Create native SQL questions using the queries in `metabase_dashboard_brief.md`

### 4. Verify it's running

```bash
docker compose ps               # all 3 services should be Up
docker logs agh-etl --tail 20   # check ETL ingest logs
```

---

## Data Retention

| Data | Retention |
|------|-----------|
| `dns_queries` raw | 14 days |
| `agg_*` 1h buckets | 3 days |
| `agg_*` 1d buckets | 14 days |

Retention cleanup runs automatically after each hourly aggregate rebuild.

---

## Project Structure

```
agh_pg_etl/
├── app/
│   ├── main.py          # CLI entrypoint (ingest / backfill / aggregate / init-db)
│   ├── agh_client.py    # AGH HTTP API client (cursor pagination)
│   ├── transform.py     # AGH JSON → normalised DnsQueryRow (pydantic)
│   ├── loader.py        # PostgreSQL insert + ETL state management
│   └── aggregator.py    # Aggregate rebuild + retention cleanup
├── sql/
│   ├── 001_init.sql     # Core tables (dns_queries, etl_state)
│   ├── 002_indexes.sql  # Performance indexes
│   └── 003_aggregates.sql  # Aggregate table definitions
├── tests/
│   └── test_transform.py
├── Dockerfile
├── docker-compose.yml
├── entrypoint.sh        # Init DB → backfill → start cron
├── requirements.txt
└── .env.example
docs/
├── dash1_overview.jpg
├── dash2_devices.jpg
└── dash3_domains.jpg
metabase_dashboard_brief.md   # Full schema + Metabase query reference
```

---

## License

MIT
