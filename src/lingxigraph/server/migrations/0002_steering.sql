-- Durable mid-run steering inbox (issue #16).
-- PostgreSQL remains the source of truth; Redis (eventbus) is only an
-- optional low-latency notify/accelerator layer on top of this table.

CREATE TABLE IF NOT EXISTS {{schema}}.run_steering_events (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    sequence BIGINT NOT NULL,
    kind TEXT NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    idempotency_key TEXT,
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'delivered', 'consumed', 'superseded')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    consumed_at TIMESTAMPTZ,
    UNIQUE (tenant_id, run_id, sequence)
);

-- Idempotency: the same (tenant, run, idempotency_key) can only ever
-- create one durable event, matching the Run-level idempotency pattern.
CREATE UNIQUE INDEX IF NOT EXISTS run_steering_events_idempotency
    ON {{schema}}.run_steering_events (tenant_id, run_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

CREATE INDEX IF NOT EXISTS run_steering_events_pending
    ON {{schema}}.run_steering_events (tenant_id, run_id, sequence)
    WHERE status IN ('pending', 'delivered');

ALTER TABLE {{schema}}.run_steering_events ENABLE ROW LEVEL SECURITY;

DO $policies$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_policies
    WHERE schemaname='{{schema}}' AND tablename='run_steering_events'
      AND policyname='tenant_isolation'
  ) THEN
    EXECUTE format(
      'CREATE POLICY tenant_isolation ON {{schema}}.%I USING '
      || '(tenant_id = current_setting(''app.tenant_id'', true)) '
      || 'WITH CHECK (tenant_id = current_setting(''app.tenant_id'', true))',
      'run_steering_events'
    );
  END IF;
END
$policies$;
