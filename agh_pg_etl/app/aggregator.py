"""
Aggregation job.

Rebuilds aggregate tables for a rolling window:
  - 1h buckets: last 3 days
  - 1d buckets: last 14 days

Also runs a retention cleanup:
  - dns_queries older than RETENTION_DAYS are deleted
  - aggregate rows older than RETENTION_DAYS are deleted

Strategy: DELETE old rows in window → INSERT fresh aggregates from dns_queries.
"""

import logging
from datetime import datetime, timedelta, timezone

import psycopg

log = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────────────────

RETENTION_DAYS = 14   # delete raw + aggregate rows older than this

_WINDOW = {
    "1h": timedelta(days=3),
    "1d": timedelta(days=14),
}

# ──────────────────────────────────────────────────────────────────────────
# SQL templates
# ──────────────────────────────────────────────────────────────────────────

_DELETE_AGG = "DELETE FROM {table} WHERE bucket_size = %s AND bucket_start >= %s"

_AGG_OVERVIEW_1H = """
INSERT INTO agg_overview (bucket_start, bucket_size,
    total_queries, blocked_queries, cached_queries,
    unique_clients, unique_domains, avg_elapsed_ms, p95_elapsed_ms)
SELECT
    date_trunc('hour', event_time)  AS bucket_start,
    '1h'                            AS bucket_size,
    COUNT(*)                        AS total_queries,
    COUNT(*) FILTER (WHERE response_status = 'blocked') AS blocked_queries,
    COUNT(*) FILTER (WHERE response_status = 'cached')  AS cached_queries,
    COUNT(DISTINCT client_key)      AS unique_clients,
    COUNT(DISTINCT root_domain)     AS unique_domains,
    ROUND(AVG(elapsed_ms)::numeric, 3)                     AS avg_elapsed_ms,
    ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY elapsed_ms)::numeric, 3) AS p95_elapsed_ms
FROM dns_queries
WHERE event_time >= %(since)s
GROUP BY 1
"""

_AGG_OVERVIEW_1D = """
INSERT INTO agg_overview (bucket_start, bucket_size,
    total_queries, blocked_queries, cached_queries,
    unique_clients, unique_domains, avg_elapsed_ms, p95_elapsed_ms)
SELECT
    date_trunc('day', event_time)   AS bucket_start,
    '1d'                            AS bucket_size,
    COUNT(*)                        AS total_queries,
    COUNT(*) FILTER (WHERE response_status = 'blocked') AS blocked_queries,
    COUNT(*) FILTER (WHERE response_status = 'cached')  AS cached_queries,
    COUNT(DISTINCT client_key)      AS unique_clients,
    COUNT(DISTINCT root_domain)     AS unique_domains,
    ROUND(AVG(elapsed_ms)::numeric, 3)                     AS avg_elapsed_ms,
    ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY elapsed_ms)::numeric, 3) AS p95_elapsed_ms
FROM dns_queries
WHERE event_time >= %(since)s
GROUP BY 1
"""

_AGG_CLIENT_1H = """
INSERT INTO agg_client_usage (bucket_start, bucket_size,
    client_key, client_name, total_queries, blocked_queries,
    unique_domains, avg_elapsed_ms)
SELECT
    date_trunc('hour', event_time)  AS bucket_start,
    '1h'                            AS bucket_size,
    client_key,
    MAX(client_name)                AS client_name,
    COUNT(*)                        AS total_queries,
    COUNT(*) FILTER (WHERE response_status = 'blocked') AS blocked_queries,
    COUNT(DISTINCT root_domain)     AS unique_domains,
    ROUND(AVG(elapsed_ms)::numeric, 3)                     AS avg_elapsed_ms
FROM dns_queries
WHERE event_time >= %(since)s
GROUP BY 1, 2, 3
"""

_AGG_CLIENT_1D = """
INSERT INTO agg_client_usage (bucket_start, bucket_size,
    client_key, client_name, total_queries, blocked_queries,
    unique_domains, avg_elapsed_ms)
SELECT
    date_trunc('day', event_time)   AS bucket_start,
    '1d'                            AS bucket_size,
    client_key,
    MAX(client_name)                AS client_name,
    COUNT(*)                        AS total_queries,
    COUNT(*) FILTER (WHERE response_status = 'blocked') AS blocked_queries,
    COUNT(DISTINCT root_domain)     AS unique_domains,
    ROUND(AVG(elapsed_ms)::numeric, 3)                     AS avg_elapsed_ms
FROM dns_queries
WHERE event_time >= %(since)s
GROUP BY 1, 2, 3
"""

_AGG_CLIENT_DOMAIN_1H = """
INSERT INTO agg_client_domain_usage (bucket_start, bucket_size,
    client_key, root_domain, total_queries, blocked_queries)
SELECT
    date_trunc('hour', event_time)  AS bucket_start,
    '1h'                            AS bucket_size,
    client_key, root_domain,
    COUNT(*)                        AS total_queries,
    COUNT(*) FILTER (WHERE response_status = 'blocked') AS blocked_queries
FROM dns_queries
WHERE event_time >= %(since)s
GROUP BY 1, 2, 3, 4
"""

_AGG_CLIENT_DOMAIN_1D = """
INSERT INTO agg_client_domain_usage (bucket_start, bucket_size,
    client_key, root_domain, total_queries, blocked_queries)
SELECT
    date_trunc('day', event_time)   AS bucket_start,
    '1d'                            AS bucket_size,
    client_key, root_domain,
    COUNT(*)                        AS total_queries,
    COUNT(*) FILTER (WHERE response_status = 'blocked') AS blocked_queries
FROM dns_queries
WHERE event_time >= %(since)s
GROUP BY 1, 2, 3, 4
"""

_AGG_DOMAIN_1H = """
INSERT INTO agg_domain_usage (bucket_start, bucket_size,
    root_domain, total_queries, blocked_queries, unique_clients, time_segment)
SELECT
    date_trunc('hour', event_time)  AS bucket_start,
    '1h'                            AS bucket_size,
    root_domain,
    COUNT(*)                        AS total_queries,
    COUNT(*) FILTER (WHERE response_status = 'blocked') AS blocked_queries,
    COUNT(DISTINCT client_key)      AS unique_clients,
    MODE() WITHIN GROUP (ORDER BY time_segment) AS time_segment
FROM dns_queries
WHERE event_time >= %(since)s
GROUP BY 1, 2, 3
"""

_AGG_DOMAIN_1D = """
INSERT INTO agg_domain_usage (bucket_start, bucket_size,
    root_domain, total_queries, blocked_queries, unique_clients, time_segment)
SELECT
    date_trunc('day', event_time)   AS bucket_start,
    '1d'                            AS bucket_size,
    root_domain,
    COUNT(*)                        AS total_queries,
    COUNT(*) FILTER (WHERE response_status = 'blocked') AS blocked_queries,
    COUNT(DISTINCT client_key)      AS unique_clients,
    NULL                            AS time_segment
FROM dns_queries
WHERE event_time >= %(since)s
GROUP BY 1, 2, 3
"""

_AGG_DOMAIN_CLIENT_1H = """
INSERT INTO agg_domain_client_usage (bucket_start, bucket_size,
    root_domain, client_key, client_name, total_queries, blocked_queries)
SELECT
    date_trunc('hour', event_time)  AS bucket_start,
    '1h'                            AS bucket_size,
    root_domain, client_key,
    MAX(client_name)                AS client_name,
    COUNT(*)                        AS total_queries,
    COUNT(*) FILTER (WHERE response_status = 'blocked') AS blocked_queries
FROM dns_queries
WHERE event_time >= %(since)s
GROUP BY 1, 2, 3, 4
"""

_AGG_DOMAIN_CLIENT_1D = """
INSERT INTO agg_domain_client_usage (bucket_start, bucket_size,
    root_domain, client_key, client_name, total_queries, blocked_queries)
SELECT
    date_trunc('day', event_time)   AS bucket_start,
    '1d'                            AS bucket_size,
    root_domain, client_key,
    MAX(client_name)                AS client_name,
    COUNT(*)                        AS total_queries,
    COUNT(*) FILTER (WHERE response_status = 'blocked') AS blocked_queries
FROM dns_queries
WHERE event_time >= %(since)s
GROUP BY 1, 2, 3, 4
"""

# table → (1h SQL, 1d SQL)
_AGG_JOBS: list[tuple[str, str, str]] = [
    ("agg_overview",            _AGG_OVERVIEW_1H,       _AGG_OVERVIEW_1D),
    ("agg_client_usage",        _AGG_CLIENT_1H,         _AGG_CLIENT_1D),
    ("agg_client_domain_usage", _AGG_CLIENT_DOMAIN_1H,  _AGG_CLIENT_DOMAIN_1D),
    ("agg_domain_usage",        _AGG_DOMAIN_1H,         _AGG_DOMAIN_1D),
    ("agg_domain_client_usage", _AGG_DOMAIN_CLIENT_1H,  _AGG_DOMAIN_CLIENT_1D),
]


# ──────────────────────────────────────────────────────────────────────────
# Retention cleanup
# ──────────────────────────────────────────────────────────────────────────

def run_retention_cleanup(conn: psycopg.Connection) -> None:
    """Delete raw events and aggregate rows older than RETENTION_DAYS."""
    cutoff = datetime.now(tz=timezone.utc) - timedelta(days=RETENTION_DAYS)

    agg_tables = [t for t, _, _ in _AGG_JOBS]

    with conn.transaction():
        with conn.cursor() as cur:
            # Raw events
            cur.execute("DELETE FROM dns_queries WHERE event_time < %s", (cutoff,))
            raw_deleted = cur.rowcount
            log.info("Retention: deleted %d raw rows older than %s", raw_deleted, cutoff.date())

            # Aggregate tables
            for table in agg_tables:
                cur.execute(f"DELETE FROM {table} WHERE bucket_start < %s", (cutoff,))  # noqa: S608
                log.info("Retention: deleted %d rows from %s", cur.rowcount, table)


# ──────────────────────────────────────────────────────────────────────────
# Public entry point
# ──────────────────────────────────────────────────────────────────────────

def run_aggregation(conn: psycopg.Connection) -> None:
    """Rebuild all aggregate tables for the rolling window, then clean up old data."""
    now = datetime.now(tz=timezone.utc)

    for table, sql_1h, sql_1d in _AGG_JOBS:
        _rebuild(conn, table, "1h", _WINDOW["1h"], sql_1h, now)
        _rebuild(conn, table, "1d", _WINDOW["1d"], sql_1d, now)

    run_retention_cleanup(conn)
    log.info("Aggregation complete")


def _rebuild(
    conn: psycopg.Connection,
    table: str,
    bucket_size: str,
    window: timedelta,
    insert_sql: str,
    now: datetime,
) -> None:
    since = now - window
    delete_sql = _DELETE_AGG.format(table=table)

    try:
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute(delete_sql, (bucket_size, since))
                deleted = cur.rowcount
                cur.execute(insert_sql, {"since": since})
                inserted = cur.rowcount
        log.info("%s [%s]: deleted=%d inserted=%d (since %s)",
                 table, bucket_size, deleted, inserted, since.date())
    except Exception as exc:
        log.error("Aggregation failed for %s [%s]: %s", table, bucket_size, exc)
        raise
