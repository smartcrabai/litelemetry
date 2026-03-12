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
) ON CONFLICT DO NOTHING;
