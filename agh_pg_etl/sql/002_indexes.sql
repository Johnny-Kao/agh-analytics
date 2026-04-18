-- AGH Analytics: Indexes

-- Primary lookup patterns
CREATE INDEX IF NOT EXISTS idx_dq_event_time      ON dns_queries (event_time DESC);
CREATE INDEX IF NOT EXISTS idx_dq_event_date      ON dns_queries (event_date DESC);
CREATE INDEX IF NOT EXISTS idx_dq_client_key      ON dns_queries (client_key);
CREATE INDEX IF NOT EXISTS idx_dq_root_domain     ON dns_queries (root_domain);
CREATE INDEX IF NOT EXISTS idx_dq_response_status ON dns_queries (response_status);
CREATE INDEX IF NOT EXISTS idx_dq_qtype           ON dns_queries (qtype);

-- Composite: time-bucketed queries per client (Metabase dashboards)
CREATE INDEX IF NOT EXISTS idx_dq_client_time     ON dns_queries (client_key, event_time DESC);
CREATE INDEX IF NOT EXISTS idx_dq_domain_time     ON dns_queries (root_domain, event_time DESC);

-- Blocked queries fast path
CREATE INDEX IF NOT EXISTS idx_dq_blocked
    ON dns_queries (event_date DESC)
    WHERE response_status = 'blocked';
