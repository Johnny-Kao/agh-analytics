-- AGH Analytics: Core Tables
-- Run order: 001 → 002 → 003

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- ─────────────────────────────────────────────
-- Raw event table
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dns_queries (
    id                  bigserial PRIMARY KEY,

    -- Time
    event_time          timestamptz NOT NULL,
    event_date          date        NOT NULL,
    event_hour          smallint    NOT NULL,
    day_of_week         smallint    NOT NULL,   -- 1=Mon … 7=Sun (ISO)
    is_weekend          boolean     NOT NULL,
    time_segment        text        NOT NULL,   -- late_night / morning / afternoon / evening

    -- Client
    client_key          text        NOT NULL,   -- agh:id / name:x / ip:x
    client_name         text,
    client_ip           inet,

    -- Query
    qname               text        NOT NULL,
    root_domain         text        NOT NULL,
    qtype               text        NOT NULL,

    -- Result
    response_status     text        NOT NULL,   -- allowed / blocked / cached / ...
    block_reason        text,                   -- filter rule text
    rcode               text,
    upstream            text,
    elapsed_ms          numeric(12,3),

    -- Payload
    answers_json        jsonb,
    raw_json            jsonb       NOT NULL,

    -- Dedup
    event_fingerprint   text        NOT NULL UNIQUE
);

-- ─────────────────────────────────────────────
-- ETL state / cursor tracking
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS etl_state (
    pipeline_name   text        PRIMARY KEY,
    last_cursor     text,
    last_run_at     timestamptz,
    rows_ingested   bigint      NOT NULL DEFAULT 0,
    updated_at      timestamptz NOT NULL DEFAULT now()
);

INSERT INTO etl_state (pipeline_name) VALUES ('querylog_ingest')
ON CONFLICT DO NOTHING;
