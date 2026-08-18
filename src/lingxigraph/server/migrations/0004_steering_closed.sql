-- Atomic finalization gate for steering admission (issue #16 PR #17
-- review round 6, point 3, BLOCKER). Once a worker's graph execution
-- has ended there is no safe point left for a newly accepted steering
-- event to ever be consumed -- even though the Run row may still read
-- ``running``/``cancelling`` while the final flush and finalization
-- commit are in flight. The owning worker sets this column atomically
-- (fenced on ``lease_owner``/``attempt``, see
-- ``PostgresRepository.close_steering``) the instant execution ends, and
-- ``/steer`` refuses new admission once it is set.

ALTER TABLE {{schema}}.runs
    ADD COLUMN IF NOT EXISTS steering_closed BOOLEAN NOT NULL DEFAULT FALSE;
