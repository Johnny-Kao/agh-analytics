"""PostgreSQL loader — all SQL lives here."""

import logging
from datetime import datetime, timezone

import psycopg
from psycopg.rows import dict_row

from config import config
from transform import DnsQueryRow

log = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────────
# SQL constants
# ──────────────────────────────────────────────────────────────────────────

_INSERT_DNS_QUERY = """
INSERT INTO dns_queries (
    event_time, event_date, event_hour, day_of_week, is_weekend, time_segment,
    client_key, client_name, client_ip,
    qname, root_domain, qtype,
    response_status, block_reason, rcode, upstream, elapsed_ms,
    answers_json, raw_json, event_fingerprint
) VALUES (
    %(event_time)s, %(event_date)s, %(event_hour)s, %(day_of_week)s, %(is_weekend)s, %(time_segment)s,
    %(client_key)s, %(client_name)s, %(client_ip)s::inet,
    %(qname)s, %(root_domain)s, %(qtype)s,
    %(response_status)s, %(block_reason)s, %(rcode)s, %(upstream)s, %(elapsed_ms)s,
    %(answers_json)s::jsonb, %(raw_json)s::jsonb, %(event_fingerprint)s
)
ON CONFLICT (event_fingerprint) DO NOTHING
"""

_UPSERT_STATE = """
INSERT INTO etl_state (pipeline_name, last_cursor, last_run_at, rows_ingested, updated_at)
VALUES (%(pipeline_name)s, %(last_cursor)s, %(last_run_at)s, %(rows_ingested)s, now())
ON CONFLICT (pipeline_name) DO UPDATE SET
    last_cursor   = EXCLUDED.last_cursor,
    last_run_at   = EXCLUDED.last_run_at,
    rows_ingested = etl_state.rows_ingested + EXCLUDED.rows_ingested,
    updated_at    = now()
"""

_GET_STATE = """
SELECT last_cursor, last_run_at, rows_ingested
FROM etl_state
WHERE pipeline_name = %s
"""


# ──────────────────────────────────────────────────────────────────────────
# Connection helper
# ──────────────────────────────────────────────────────────────────────────

def get_connection() -> psycopg.Connection:
    return psycopg.connect(config.pg_dsn, row_factory=dict_row)


# ──────────────────────────────────────────────────────────────────────────
# State helpers
# ──────────────────────────────────────────────────────────────────────────

def get_state(conn: psycopg.Connection, pipeline: str = "querylog_ingest") -> dict:
    with conn.cursor() as cur:
        cur.execute(_GET_STATE, (pipeline,))
        row = cur.fetchone()
        return row or {"last_cursor": None, "last_run_at": None, "rows_ingested": 0}


def save_state(
    conn: psycopg.Connection,
    cursor: str | None,
    rows_ingested: int,
    pipeline: str = "querylog_ingest",
) -> None:
    with conn.cursor() as cur:
        cur.execute(_UPSERT_STATE, {
            "pipeline_name": pipeline,
            "last_cursor":   cursor,
            "last_run_at":   datetime.now(tz=timezone.utc),
            "rows_ingested": rows_ingested,
        })
    conn.commit()


# ──────────────────────────────────────────────────────────────────────────
# Batch insert
# ──────────────────────────────────────────────────────────────────────────

def insert_batch(conn: psycopg.Connection, rows: list[DnsQueryRow]) -> int:
    """
    Insert a batch of DnsQueryRow objects.
    Returns the number of rows actually inserted (conflicts excluded).
    """
    if not rows:
        return 0

    params_list = [
        {
            "event_time":        r.event_time,
            "event_date":        r.event_date,
            "event_hour":        r.event_hour,
            "day_of_week":       r.day_of_week,
            "is_weekend":        r.is_weekend,
            "time_segment":      r.time_segment,
            "client_key":        r.client_key,
            "client_name":       r.client_name,
            "client_ip":         r.client_ip,
            "qname":             r.qname,
            "root_domain":       r.root_domain,
            "qtype":             r.qtype,
            "response_status":   r.response_status,
            "block_reason":      r.block_reason,
            "rcode":             r.rcode,
            "upstream":          r.upstream,
            "elapsed_ms":        r.elapsed_ms,
            "answers_json":      r.answers_json,
            "raw_json":          r.raw_json,
            "event_fingerprint": r.event_fingerprint,
        }
        for r in rows
    ]

    try:
        with conn.transaction():
            with conn.cursor() as cur:
                cur.executemany(_INSERT_DNS_QUERY, params_list)
                # psycopg3 executemany doesn't expose rowcount reliably for ON CONFLICT
                # Use a heuristic: assume all inserted minus conflicts
                return cur.rowcount if cur.rowcount >= 0 else len(rows)
    except psycopg.errors.DeadlockDetected:
        log.warning("Deadlock detected, batch rolled back – will retry next run")
        return 0
    except Exception as exc:
        log.error("insert_batch failed: %s", exc)
        raise
