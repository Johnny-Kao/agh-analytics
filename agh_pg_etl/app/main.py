#!/usr/bin/env python3
"""
AGH → PostgreSQL ETL

Usage:
    python main.py ingest      # pull new records from AGH querylog (incremental)
    python main.py aggregate   # rebuild aggregate tables
    python main.py init-db     # apply SQL schema files in order
    python main.py backfill    # ingest ALL available history from AGH
"""

import logging
import sys
from pathlib import Path

import psycopg

from config import config
from agh_client import iter_querylog
from transform import transform_record
from loader import get_connection, get_state, save_state, insert_batch
from aggregator import run_aggregation

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)
log = logging.getLogger(__name__)

SQL_DIR = Path(__file__).parent / "sql"


# ──────────────────────────────────────────────────────────────────────────
# Commands
# ──────────────────────────────────────────────────────────────────────────

def cmd_init_db() -> None:
    """Apply SQL migration files in alphabetical order."""
    with get_connection() as conn:
        for sql_file in sorted(SQL_DIR.glob("*.sql")):
            log.info("Applying %s", sql_file.name)
            conn.execute(sql_file.read_text())
        conn.commit()
    log.info("Database initialised")


def cmd_ingest(backfill: bool = False) -> None:
    """
    Incremental ingest from AGH /querylog.

    Strategy:
      - AGH returns records newest-first.
      - We walk backwards using `older_than` cursor.
      - Dedup via event_fingerprint prevents double-inserts.
      - Incremental mode: stop when a full batch produces 0 new inserts
        (means we've reached records already in the DB).
      - Backfill mode: walk all the way to the oldest available record.

    Cursor stored in etl_state is the RFC3339 timestamp of the oldest
    record seen so far. Next backfill resumes from there.
    """
    with get_connection() as conn:
        state = get_state(conn)
        # For incremental: always start from now (no older_than) so we
        # pick up the latest records and stop when we hit the known DB content.
        # For backfill: resume from stored cursor (oldest seen so far).
        start_cursor: str | None = state["last_cursor"] if backfill else None

        log.info(
            "Ingest starting | backfill=%s cursor=%s db_total=%s",
            backfill, start_cursor, state["rows_ingested"],
        )

        total_inserted = 0
        oldest_cursor_seen: str | None = state["last_cursor"]

        for batch_num, (records, next_cursor) in enumerate(
            iter_querylog(start_older_than=start_cursor), start=1
        ):
            rows = [r for raw in records if (r := transform_record(raw)) is not None]
            bad = len(records) - len(rows)
            if bad:
                log.warning("Batch %d: %d records failed transform", batch_num, bad)

            inserted = insert_batch(conn, rows)
            total_inserted += inserted

            # Track oldest cursor for backfill resume
            if next_cursor:
                oldest_cursor_seen = next_cursor

            log.info(
                "Batch %d: fetched=%d transformed=%d inserted=%d cursor=%s",
                batch_num, len(records), len(rows), inserted, next_cursor,
            )

            # Incremental: stop when we're seeing only known records
            if not backfill and inserted == 0 and batch_num > 1:
                log.info("No new records in batch — caught up with existing data")
                break

        # Persist cursor only after successful run
        save_state(conn, oldest_cursor_seen, total_inserted)
        log.info("Ingest complete | total_inserted=%d", total_inserted)


def cmd_aggregate() -> None:
    with get_connection() as conn:
        run_aggregation(conn)


# ──────────────────────────────────────────────────────────────────────────
# Entrypoint
# ──────────────────────────────────────────────────────────────────────────

COMMANDS = {
    "ingest":    lambda: cmd_ingest(backfill=False),
    "backfill":  lambda: cmd_ingest(backfill=True),
    "aggregate": cmd_aggregate,
    "init-db":   cmd_init_db,
}

if __name__ == "__main__":
    if len(sys.argv) < 2 or sys.argv[1] not in COMMANDS:
        print(f"Usage: main.py [{' | '.join(COMMANDS)}]")
        sys.exit(1)

    try:
        COMMANDS[sys.argv[1]]()
    except KeyboardInterrupt:
        log.info("Interrupted")
    except psycopg.OperationalError as exc:
        log.error("PostgreSQL connection failed: %s", exc)
        sys.exit(2)
    except Exception as exc:
        log.exception("Unhandled error: %s", exc)
        sys.exit(1)
