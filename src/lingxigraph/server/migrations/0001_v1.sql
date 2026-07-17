CREATE SCHEMA IF NOT EXISTS {{schema}};

CREATE TABLE IF NOT EXISTS {{schema}}.graph_versions (
    tenant_id TEXT NOT NULL DEFAULT 'system',
    graph_id TEXT NOT NULL,
    version TEXT NOT NULL,
    schema_hash TEXT NOT NULL,
    manifest JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, graph_id, version)
);

CREATE TABLE IF NOT EXISTS {{schema}}.assistants (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    graph_id TEXT NOT NULL,
    graph_version TEXT NOT NULL,
    name TEXT,
    config JSONB NOT NULL DEFAULT '{}'::jsonb,
    context JSONB NOT NULL DEFAULT '{}'::jsonb,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS assistants_tenant ON {{schema}}.assistants (tenant_id, created_at DESC);

CREATE TABLE IF NOT EXISTS {{schema}}.threads (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS threads_tenant ON {{schema}}.threads (tenant_id, updated_at DESC);

CREATE TABLE IF NOT EXISTS {{schema}}.runs (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    thread_id TEXT,
    assistant_id TEXT NOT NULL,
    graph_id TEXT NOT NULL,
    graph_version TEXT NOT NULL,
    idempotency_key TEXT,
    request_digest TEXT,
    status TEXT NOT NULL CHECK (status IN (
        'pending','running','paused','succeeded','failed',
        'cancelling','cancelled','timed_out','dead_letter'
    )),
    input JSONB,
    context JSONB NOT NULL DEFAULT '{}'::jsonb,
    config JSONB NOT NULL DEFAULT '{}'::jsonb,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    resume JSONB,
    update JSONB,
    goto_node TEXT,
    durability TEXT NOT NULL DEFAULT 'sync',
    error JSONB,
    output JSONB,
    attempt INTEGER NOT NULL DEFAULT 0,
    lease_owner TEXT,
    lease_expires_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ
);
ALTER TABLE {{schema}}.runs ADD COLUMN IF NOT EXISTS idempotency_key TEXT;
ALTER TABLE {{schema}}.runs ADD COLUMN IF NOT EXISTS request_digest TEXT;
ALTER TABLE {{schema}}.runs DROP CONSTRAINT IF EXISTS runs_status_check;
ALTER TABLE {{schema}}.runs ADD CONSTRAINT runs_status_check CHECK (status IN (
    'pending','running','paused','succeeded','failed',
    'cancelling','cancelled','timed_out','dead_letter'
));
CREATE UNIQUE INDEX IF NOT EXISTS runs_idempotency
    ON {{schema}}.runs (tenant_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS runs_queue ON {{schema}}.runs (status, created_at);
CREATE INDEX IF NOT EXISTS runs_thread ON {{schema}}.runs (tenant_id, thread_id, created_at DESC);
CREATE UNIQUE INDEX IF NOT EXISTS one_active_run_per_thread
    ON {{schema}}.runs (tenant_id, thread_id)
    WHERE thread_id IS NOT NULL AND status IN ('running','cancelling');

CREATE TABLE IF NOT EXISTS {{schema}}.run_events (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    sequence BIGINT NOT NULL,
    kind TEXT NOT NULL,
    data JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, run_id, sequence)
);
CREATE INDEX IF NOT EXISTS run_events_replay ON {{schema}}.run_events (tenant_id, run_id, sequence);

CREATE TABLE IF NOT EXISTS {{schema}}.checkpoints (
    seq BIGSERIAL PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    thread_id TEXT NOT NULL,
    namespace TEXT NOT NULL DEFAULT '',
    checkpoint_id TEXT NOT NULL,
    ts TIMESTAMPTZ NOT NULL,
    step BIGINT NOT NULL,
    config_json BYTEA NOT NULL,
    checkpoint_json BYTEA NOT NULL,
    metadata_json BYTEA NOT NULL,
    UNIQUE (tenant_id, thread_id, namespace, checkpoint_id)
);
CREATE INDEX IF NOT EXISTS checkpoints_thread
    ON {{schema}}.checkpoints (tenant_id, thread_id, namespace, seq DESC);

CREATE TABLE IF NOT EXISTS {{schema}}.checkpoint_writes (
    tenant_id TEXT NOT NULL,
    thread_id TEXT NOT NULL,
    namespace TEXT NOT NULL DEFAULT '',
    checkpoint_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    write_index INTEGER NOT NULL,
    write_json BYTEA NOT NULL,
    PRIMARY KEY (tenant_id, thread_id, namespace, checkpoint_id, task_id, write_index)
);

CREATE TABLE IF NOT EXISTS {{schema}}.store_items (
    tenant_id TEXT NOT NULL,
    namespace TEXT[] NOT NULL,
    key TEXT NOT NULL,
    value JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ,
    PRIMARY KEY (tenant_id, namespace, key)
);
ALTER TABLE {{schema}}.store_items ADD COLUMN IF NOT EXISTS expires_at TIMESTAMPTZ;
CREATE INDEX IF NOT EXISTS store_items_namespace ON {{schema}}.store_items USING GIN (namespace);
CREATE INDEX IF NOT EXISTS store_items_value ON {{schema}}.store_items USING GIN (value);

CREATE TABLE IF NOT EXISTS {{schema}}.schedules (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    assistant_id TEXT NOT NULL,
    cron TEXT NOT NULL,
    timezone TEXT NOT NULL DEFAULT 'UTC',
    input JSONB NOT NULL DEFAULT '{}'::jsonb,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS {{schema}}.audit_records (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    actor TEXT NOT NULL,
    action TEXT NOT NULL,
    resource_type TEXT NOT NULL,
    resource_id TEXT,
    result TEXT NOT NULL,
    trace_id TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS audit_tenant_time ON {{schema}}.audit_records (tenant_id, created_at DESC);

ALTER TABLE {{schema}}.assistants ENABLE ROW LEVEL SECURITY;
ALTER TABLE {{schema}}.threads ENABLE ROW LEVEL SECURITY;
ALTER TABLE {{schema}}.runs ENABLE ROW LEVEL SECURITY;
ALTER TABLE {{schema}}.run_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE {{schema}}.checkpoints ENABLE ROW LEVEL SECURITY;
ALTER TABLE {{schema}}.checkpoint_writes ENABLE ROW LEVEL SECURITY;
ALTER TABLE {{schema}}.store_items ENABLE ROW LEVEL SECURITY;
ALTER TABLE {{schema}}.schedules ENABLE ROW LEVEL SECURITY;
ALTER TABLE {{schema}}.audit_records ENABLE ROW LEVEL SECURITY;

DO $policies$
DECLARE table_name TEXT;
BEGIN
  FOREACH table_name IN ARRAY ARRAY[
    'assistants','threads','runs','run_events','checkpoints',
    'checkpoint_writes','store_items','schedules','audit_records'
  ] LOOP
    IF NOT EXISTS (
      SELECT 1 FROM pg_policies
      WHERE schemaname='{{schema}}' AND tablename=table_name
        AND policyname='tenant_isolation'
    ) THEN
      EXECUTE format(
        'CREATE POLICY tenant_isolation ON {{schema}}.%I USING '
        || '(tenant_id = current_setting(''app.tenant_id'', true)) '
        || 'WITH CHECK (tenant_id = current_setting(''app.tenant_id'', true))',
        table_name
      );
    END IF;
  END LOOP;
END
$policies$;
