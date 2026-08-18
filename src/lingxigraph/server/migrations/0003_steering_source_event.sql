-- Stable logical identity for paused-run -> resumed-run steering transfers
-- (issue #16 PR #17 review point 3). ``source_event_id`` points at the
-- original (now ``superseded``) event's id so a client's ``/steer``
-- response on a paused run can be correlated end-to-end to the
-- ``run.steer.consumed`` event eventually emitted under the resumed run.

ALTER TABLE {{schema}}.run_steering_events
    ADD COLUMN IF NOT EXISTS source_event_id TEXT;

CREATE INDEX IF NOT EXISTS run_steering_events_source
    ON {{schema}}.run_steering_events (tenant_id, source_event_id)
    WHERE source_event_id IS NOT NULL;
