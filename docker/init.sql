-- PulseFlow Database Schema
-- Partitioned by event date for analytical query performance

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Raw events table partitioned by month
-- This is your "landing zone" - everything that comes in goes here first
CREATE TABLE raw_events (
    id              BIGSERIAL,
    event_id        UUID NOT NULL,
    event_type      VARCHAR(100) NOT NULL,
    user_id         VARCHAR(100),
    session_id      VARCHAR(100),
    product_id      VARCHAR(100),
    order_id        VARCHAR(100),
    amount          DECIMAL(12, 2),
    currency        VARCHAR(10) DEFAULT 'USD',
    properties      JSONB DEFAULT '{}',
    client_ip       INET,
    user_agent      TEXT,
    event_timestamp TIMESTAMPTZ NOT NULL,
    received_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed_at    TIMESTAMPTZ,
    is_duplicate    BOOLEAN DEFAULT FALSE,
    is_valid        BOOLEAN DEFAULT TRUE,
    validation_errors JSONB DEFAULT '[]',
    PRIMARY KEY (id, event_timestamp)
) PARTITION BY RANGE (event_timestamp);

-- Create monthly partitions (current + next 3 months)
CREATE TABLE raw_events_2026_07 PARTITION OF raw_events
    FOR VALUES FROM ('2026-07-01') TO ('2026-08-01');

CREATE TABLE raw_events_2026_08 PARTITION OF raw_events
    FOR VALUES FROM ('2026-08-01') TO ('2026-09-01');

CREATE TABLE raw_events_2026_09 PARTITION OF raw_events
    FOR VALUES FROM ('2026-09-01') TO ('2026-10-01');

CREATE TABLE raw_events_2026_10 PARTITION OF raw_events
    FOR VALUES FROM ('2026-10-01') TO ('2026-11-01');

-- Default partition catches anything outside the ranges above
CREATE TABLE raw_events_default PARTITION OF raw_events DEFAULT;

-- Indexes on the partitioned table
CREATE INDEX idx_raw_events_event_id ON raw_events (event_id);
CREATE INDEX idx_raw_events_user_id ON raw_events (user_id, event_timestamp DESC);
CREATE INDEX idx_raw_events_event_type ON raw_events (event_type, event_timestamp DESC);
CREATE INDEX idx_raw_events_order_id ON raw_events (order_id) WHERE order_id IS NOT NULL;
CREATE INDEX idx_raw_events_processed ON raw_events (processed_at) WHERE processed_at IS NULL;

-- Deduplication tracking table
-- Separate table so we can check quickly without scanning raw_events
CREATE TABLE processed_event_ids (
    event_id        UUID PRIMARY KEY,
    processed_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    partition_key   VARCHAR(20) -- month string for cleanup, e.g. '2026-08'
);

CREATE INDEX idx_processed_event_ids_partition ON processed_event_ids (partition_key);

-- Dead Letter Queue - events that failed processing after all retries
CREATE TABLE dead_letter_queue (
    id              BIGSERIAL PRIMARY KEY,
    event_id        UUID,
    raw_payload     JSONB NOT NULL,
    failure_reason  VARCHAR(500) NOT NULL,
    failure_details JSONB DEFAULT '{}',
    retry_count     INT DEFAULT 0,
    first_failed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_failed_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resolved        BOOLEAN DEFAULT FALSE,
    resolved_at     TIMESTAMPTZ,
    resolved_by     VARCHAR(100)
);

CREATE INDEX idx_dlq_event_id ON dead_letter_queue (event_id);
CREATE INDEX idx_dlq_unresolved ON dead_letter_queue (first_failed_at) WHERE resolved = FALSE;
CREATE INDEX idx_dlq_failure_reason ON dead_letter_queue (failure_reason);

-- Aggregated hourly metrics - pre-computed for fast dashboard queries
CREATE TABLE hourly_metrics (
    id              BIGSERIAL PRIMARY KEY,
    hour_bucket     TIMESTAMPTZ NOT NULL,
    event_type      VARCHAR(100) NOT NULL,
    event_count     BIGINT DEFAULT 0,
    unique_users    BIGINT DEFAULT 0,
    unique_sessions BIGINT DEFAULT 0,
    total_revenue   DECIMAL(15, 2) DEFAULT 0,
    avg_amount      DECIMAL(12, 2),
    error_count     BIGINT DEFAULT 0,
    computed_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (hour_bucket, event_type)
);

CREATE INDEX idx_hourly_metrics_hour ON hourly_metrics (hour_bucket DESC);
CREATE INDEX idx_hourly_metrics_type ON hourly_metrics (event_type, hour_bucket DESC);

-- Pipeline health tracking - so you can see processing lag, throughput, etc.
CREATE TABLE pipeline_metrics (
    id              BIGSERIAL PRIMARY KEY,
    recorded_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    metric_name     VARCHAR(100) NOT NULL,
    metric_value    DECIMAL(20, 4) NOT NULL,
    tags            JSONB DEFAULT '{}'
);

CREATE INDEX idx_pipeline_metrics_name ON pipeline_metrics (metric_name, recorded_at DESC);
CREATE INDEX idx_pipeline_metrics_recorded ON pipeline_metrics (recorded_at DESC);

-- Convenience view for recent event activity
CREATE VIEW recent_events AS
SELECT
    event_type,
    COUNT(*) AS event_count,
    COUNT(DISTINCT user_id) AS unique_users,
    SUM(CASE WHEN amount IS NOT NULL THEN amount ELSE 0 END) AS total_amount,
    MIN(event_timestamp) AS earliest,
    MAX(event_timestamp) AS latest,
    COUNT(*) FILTER (WHERE is_duplicate) AS duplicate_count,
    COUNT(*) FILTER (WHERE NOT is_valid) AS invalid_count
FROM raw_events
WHERE event_timestamp >= NOW() - INTERVAL '1 hour'
GROUP BY event_type
ORDER BY event_count DESC;

-- Funnel analysis view
CREATE VIEW conversion_funnel AS
WITH funnel_events AS (
    SELECT
        DATE_TRUNC('hour', event_timestamp) AS hour_bucket,
        COUNT(*) FILTER (WHERE event_type = 'product_view')     AS product_views,
        COUNT(*) FILTER (WHERE event_type = 'add_to_cart')      AS add_to_carts,
        COUNT(*) FILTER (WHERE event_type = 'checkout')         AS checkouts,
        COUNT(*) FILTER (WHERE event_type = 'payment_success')  AS payments_success,
        COUNT(*) FILTER (WHERE event_type = 'payment_failure')  AS payments_failed,
        COUNT(*) FILTER (WHERE event_type = 'order_created')    AS orders_created
    FROM raw_events
    WHERE event_timestamp >= NOW() - INTERVAL '24 hours'
    GROUP BY 1
)
SELECT
    hour_bucket,
    product_views,
    add_to_carts,
    checkouts,
    payments_success,
    payments_failed,
    orders_created,
    ROUND(
        CASE WHEN product_views > 0
        THEN (add_to_carts::DECIMAL / product_views * 100)
        ELSE 0 END, 2
    ) AS view_to_cart_pct,
    ROUND(
        CASE WHEN checkouts > 0
        THEN (payments_success::DECIMAL / checkouts * 100)
        ELSE 0 END, 2
    ) AS checkout_conversion_pct
FROM funnel_events
ORDER BY hour_bucket DESC;

COMMENT ON TABLE raw_events IS 'Landing zone for all incoming events. Partitioned by month for query performance.';
COMMENT ON TABLE processed_event_ids IS 'Deduplication ledger. Event IDs written here before raw_events to ensure idempotency.';
COMMENT ON TABLE dead_letter_queue IS 'Events that failed processing after all retries. Requires manual review or reprocessing.';
COMMENT ON TABLE hourly_metrics IS 'Pre-aggregated hourly rollups for fast dashboard queries.';
COMMENT ON TABLE pipeline_metrics IS 'Internal pipeline health metrics: throughput, lag, error rates.';
