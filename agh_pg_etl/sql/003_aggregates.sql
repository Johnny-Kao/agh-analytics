-- AGH Analytics: Aggregate Tables

-- ─────────────────────────────────────────────
-- agg_overview  — global totals per time bucket
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS agg_overview (
    bucket_start    timestamptz NOT NULL,
    bucket_size     text        NOT NULL,   -- '1h' | '1d'
    total_queries   bigint      NOT NULL DEFAULT 0,
    blocked_queries bigint      NOT NULL DEFAULT 0,
    cached_queries  bigint      NOT NULL DEFAULT 0,
    unique_clients  bigint      NOT NULL DEFAULT 0,
    unique_domains  bigint      NOT NULL DEFAULT 0,
    avg_elapsed_ms  numeric(12,3),
    p95_elapsed_ms  numeric(12,3),
    PRIMARY KEY (bucket_start, bucket_size)
);

-- ─────────────────────────────────────────────
-- agg_client_usage  — per client per bucket
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS agg_client_usage (
    bucket_start    timestamptz NOT NULL,
    bucket_size     text        NOT NULL,
    client_key      text        NOT NULL,
    client_name     text,
    total_queries   bigint      NOT NULL DEFAULT 0,
    blocked_queries bigint      NOT NULL DEFAULT 0,
    unique_domains  bigint      NOT NULL DEFAULT 0,
    avg_elapsed_ms  numeric(12,3),
    PRIMARY KEY (bucket_start, bucket_size, client_key)
);

-- ─────────────────────────────────────────────
-- agg_client_domain_usage  — per client + domain per bucket
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS agg_client_domain_usage (
    bucket_start    timestamptz NOT NULL,
    bucket_size     text        NOT NULL,
    client_key      text        NOT NULL,
    root_domain     text        NOT NULL,
    total_queries   bigint      NOT NULL DEFAULT 0,
    blocked_queries bigint      NOT NULL DEFAULT 0,
    PRIMARY KEY (bucket_start, bucket_size, client_key, root_domain)
);

-- ─────────────────────────────────────────────
-- agg_domain_usage  — per domain per bucket
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS agg_domain_usage (
    bucket_start    timestamptz NOT NULL,
    bucket_size     text        NOT NULL,
    root_domain     text        NOT NULL,
    total_queries   bigint      NOT NULL DEFAULT 0,
    blocked_queries bigint      NOT NULL DEFAULT 0,
    unique_clients  bigint      NOT NULL DEFAULT 0,
    time_segment    text,                   -- dominant segment (only for 1h rows)
    PRIMARY KEY (bucket_start, bucket_size, root_domain)
);

-- ─────────────────────────────────────────────
-- agg_domain_client_usage  — per domain + client per bucket
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS agg_domain_client_usage (
    bucket_start    timestamptz NOT NULL,
    bucket_size     text        NOT NULL,
    root_domain     text        NOT NULL,
    client_key      text        NOT NULL,
    client_name     text,
    total_queries   bigint      NOT NULL DEFAULT 0,
    blocked_queries bigint      NOT NULL DEFAULT 0,
    PRIMARY KEY (bucket_start, bucket_size, root_domain, client_key)
);

-- Indexes for aggregate tables
CREATE INDEX IF NOT EXISTS idx_agg_overview_bs         ON agg_overview (bucket_start DESC, bucket_size);
CREATE INDEX IF NOT EXISTS idx_agg_client_bs           ON agg_client_usage (bucket_start DESC, bucket_size, client_key);
CREATE INDEX IF NOT EXISTS idx_agg_cd_client           ON agg_client_domain_usage (client_key, bucket_start DESC, bucket_size);
CREATE INDEX IF NOT EXISTS idx_agg_domain_bs           ON agg_domain_usage (bucket_start DESC, bucket_size, root_domain);
CREATE INDEX IF NOT EXISTS idx_agg_dc_domain           ON agg_domain_client_usage (root_domain, bucket_start DESC, bucket_size);
