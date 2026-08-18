-- Typed control-plane lineage marker for resumed runs (issue #16 PR #17
-- review round 14, BLOCKER). ``superseded_by_run_id`` used to live in
-- ordinary, user-writable ``runs.metadata`` -- but ``/steer``, ``/cancel``,
-- and the concurrent-resume conflict gate all treat it as authoritative
-- Runtime control state. A caller could forge
-- ``metadata.superseded_by_run_id`` at run-creation time to falsely
-- trigger ``409 run_superseded`` / ``409 run_resume_conflict`` on a run
-- that was never actually resumed. This column is written exactly once,
-- atomically, by the resume transaction that creates a descendant run
-- from a paused run (see ``PostgresRepository`` resume/steer/cancel
-- sync helpers) and is never derived from user-submitted ``metadata``.

ALTER TABLE {{schema}}.runs
    ADD COLUMN IF NOT EXISTS superseded_by_run_id TEXT;
