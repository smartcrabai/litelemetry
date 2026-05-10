CREATE TABLE IF NOT EXISTS dashboard_definitions (
    id UUID PRIMARY KEY,
    slug TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    layout_json JSONB NOT NULL DEFAULT '{}',
    revision BIGINT NOT NULL,
    enabled BOOLEAN NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS viewer_definitions (
    id UUID PRIMARY KEY,
    slug TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    refresh_interval_ms INTEGER NOT NULL,
    lookback_ms BIGINT NOT NULL,
    signal_mask INTEGER NOT NULL,
    definition_json JSONB NOT NULL DEFAULT '{}',
    layout_json JSONB NOT NULL DEFAULT '{}',
    revision BIGINT NOT NULL,
    enabled BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS viewer_snapshots (
    viewer_id UUID PRIMARY KEY,
    revision BIGINT NOT NULL,
    last_cursor_json JSONB NOT NULL DEFAULT '{}',
    status_json JSONB NOT NULL DEFAULT '{"type":"ok"}',
    generated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS alert_definitions (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL,
    viewer_id UUID NOT NULL,
    condition_json JSONB NOT NULL,
    severity TEXT NOT NULL,
    evaluation_interval_ms INTEGER NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    revision BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS attribute_index (
    attribute_key TEXT NOT NULL,
    attribute_value TEXT NOT NULL,
    signal SMALLINT NOT NULL,
    stream_id TEXT NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (attribute_key, attribute_value, stream_id)
);
CREATE INDEX IF NOT EXISTS attribute_index_observed_at_idx ON attribute_index (observed_at);
CREATE INDEX IF NOT EXISTS attribute_index_key_idx ON attribute_index (attribute_key);

CREATE TABLE IF NOT EXISTS incidents (
    id UUID PRIMARY KEY,
    alert_id UUID,
    status TEXT NOT NULL DEFAULT 'open',
    severity TEXT NOT NULL,
    opened_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    acknowledged_at TIMESTAMPTZ,
    resolved_at TIMESTAMPTZ,
    details_json JSONB NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS incidents_status_idx ON incidents (status);

CREATE TABLE IF NOT EXISTS notification_channels (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL,
    kind TEXT NOT NULL,
    config_json JSONB NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS slo_definitions (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL,
    viewer_id UUID,
    target_pct DOUBLE PRECISION NOT NULL,
    window_ms BIGINT NOT NULL,
    success_filter_json JSONB NOT NULL,
    total_filter_json JSONB NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS error_groups (
    fingerprint TEXT PRIMARY KEY,
    signature TEXT NOT NULL,
    first_seen TIMESTAMPTZ NOT NULL,
    last_seen TIMESTAMPTZ NOT NULL,
    count BIGINT NOT NULL DEFAULT 0,
    sample_payload JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS error_groups_last_seen_idx ON error_groups (last_seen);

INSERT INTO viewer_definitions (
    id,
    slug,
    name,
    refresh_interval_ms,
    lookback_ms,
    signal_mask,
    definition_json,
    layout_json,
    revision,
    enabled
) VALUES (
    '5d03b66e-e580-4ba2-af5c-2c25d4bb2f17',
    'compose-seed-traces',
    'Compose Seed Traces',
    1000,
    86400000,
    1,
    '{"kind":"table","signal":"traces"}'::jsonb,
    '{"default_view":"table"}'::jsonb,
    1,
    true
) , (
    'ab419eb2-2f02-40e9-8cde-4e0e9d858e11',
    'compose-seed-metrics',
    'Compose Seed Metrics',
    1000,
    86400000,
    2,
    '{"kind":"table","signal":"metrics"}'::jsonb,
    '{"default_view":"table"}'::jsonb,
    1,
    true
) , (
    '2b146a3b-0dd6-4671-a5db-bc2cd2de6d5e',
    'compose-seed-logs',
    'Compose Seed Logs',
    1000,
    86400000,
    4,
    '{"kind":"table","signal":"logs"}'::jsonb,
    '{"default_view":"table"}'::jsonb,
    1,
    true
) , (
    '7a8c1f4e-3e29-4b6a-9d12-f1a2b3c4d5e6',
    'compose-seed-metrics-stacked-bar',
    'Compose Seed Metrics (Stacked Bar)',
    1000,
    86400000,
    2,
    '{"kind":"stacked_bar","signal":"metrics"}'::jsonb,
    '{"default_view":"table"}'::jsonb,
    1,
    true
) , (
    'c9d0e1f2-a3b4-5c6d-7e8f-091a2b3c4d5e',
    'compose-seed-metrics-line',
    'Compose Seed Metrics (Line)',
    1000,
    86400000,
    2,
    '{"kind":"line","signal":"metrics"}'::jsonb,
    '{"default_view":"table"}'::jsonb,
    1,
    true
) ON CONFLICT DO NOTHING;

INSERT INTO dashboard_definitions (id, slug, name, layout_json, revision, enabled) VALUES (
    'a1b2c3d4-e5f6-7890-abcd-ef1234567890',
    'compose-seed-overview',
    'Overview',
    '{"columns":2,"panels":[{"viewer_id":"5d03b66e-e580-4ba2-af5c-2c25d4bb2f17","position":0},{"viewer_id":"ab419eb2-2f02-40e9-8cde-4e0e9d858e11","position":1},{"viewer_id":"2b146a3b-0dd6-4671-a5db-bc2cd2de6d5e","position":2}]}'::jsonb,
    1,
    true
) ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS api_keys (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL,
    key_hash TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS api_keys_hash_idx ON api_keys (key_hash);
