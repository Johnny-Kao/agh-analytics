#!/bin/bash
set -e

echo "[entrypoint] Waiting for PostgreSQL..."
until python -c "
import psycopg, os, sys
try:
    psycopg.connect(
        host=os.environ['PG_HOST'],
        port=int(os.environ.get('PG_PORT', 5432)),
        dbname=os.environ['PG_DB'],
        user=os.environ['PG_USER'],
        password=os.environ['PG_PASSWORD'],
        connect_timeout=3,
    )
    sys.exit(0)
except Exception:
    sys.exit(1)
" 2>/dev/null; do
  echo "[entrypoint] PostgreSQL not ready, retrying in 3s..."
  sleep 3
done

echo "[entrypoint] Initialising database schema..."
python main.py init-db

echo "[entrypoint] Running initial backfill..."
python main.py backfill

echo "[entrypoint] Starting cron..."

# Write crontab (use printf to avoid heredoc variable expansion issues)
printf '%s\n' \
  '# Ingest every 5 minutes' \
  '*/5 * * * * root cd /app && python main.py ingest >> /var/log/agh-ingest.log 2>&1' \
  '# Aggregate every hour at :10' \
  '10 * * * * root cd /app && python main.py aggregate >> /var/log/agh-aggregate.log 2>&1' \
  '' \
  > /etc/cron.d/agh-etl

chmod 0644 /etc/cron.d/agh-etl
cron -f
