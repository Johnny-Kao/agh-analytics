FROM python:3.11-slim

WORKDIR /app

# System deps for psycopg[binary] and tldextract
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 curl cron \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ .
COPY sql/ /app/sql/
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
