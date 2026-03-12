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
    'Compose Seed Viewer',
    1000,
    300000,
    1,
    '{"kind":"table","signal":"traces"}'::jsonb,
    '{"default_view":"table"}'::jsonb,
    1,
    true
) ON CONFLICT DO NOTHING;
