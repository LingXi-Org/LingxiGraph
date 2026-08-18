"""Exceptions raised by the LingxiGraph runtime."""


class LingxiGraphError(Exception):
    """Base class for public runtime errors."""


class GraphValidationError(LingxiGraphError, ValueError):
    """Raised when a graph definition is invalid."""


class GraphRecursionError(LingxiGraphError, RecursionError):
    """Raised when a run exceeds its configured superstep limit."""


class InvalidUpdateError(LingxiGraphError, ValueError):
    """Raised when node updates cannot be merged into the state."""


class EmptyInputError(LingxiGraphError, ValueError):
    """Raised when a run has neither new input nor resumable state."""


class GraphCancelledError(LingxiGraphError):
    """Raised when cooperative or server-side cancellation is requested."""


class GraphTimeoutError(LingxiGraphError, TimeoutError):
    """Raised when a node, step, or complete run exceeds its deadline."""


class PersistenceError(LingxiGraphError):
    """Raised when a durable write cannot be completed safely."""


class BudgetExceededError(LingxiGraphError):
    """Raised when a run exceeds a configured tool, token, or cost budget."""


class ConcurrentRunError(LingxiGraphError):
    """Raised when a thread concurrency strategy rejects a new run."""


class IdempotencyConflictError(LingxiGraphError):
    """Raised when an idempotency key is reused with a different request."""


class RunTerminalError(LingxiGraphError):
    """Raised when an operation targets a Run that has already finished."""


class RunSupersededError(LingxiGraphError):
    """Raised when steering targets a paused Run that has already been resumed.

    Resume durably transfers any steering that was pending at resume time
    onto the new Run it creates (see
    ``repository.resume_run_with_pending_steering`` and issue #16's
    paused-run steering semantics) -- once that happens the old Run id is a dead end
    for *further* steering, so new steer attempts against it fail loudly
    instead of silently pending forever.
    """


class RunFinalizingError(LingxiGraphError):
    """Raised when steering targets a Run whose graph has already ended.

    Issue #16 PR #17 review round 6, point 3: once a worker's graph
    execution reaches its terminal outcome, the normal heartbeat/steering
    loop stops and there is no safe point left for the graph to ever
    consume a newly-accepted steering event -- even though the Run row
    may still read ``running``/``cancelling`` while the final flush and
    finalization commit are in flight. ``Repository.close_steering`` sets
    an atomic, lease-fenced gate the instant execution ends so a late
    ``/steer`` call during this window fails loudly with a stable error
    instead of being durably accepted into an inbox nobody will ever read.
    """


class RunResumeConflictError(LingxiGraphError):
    """Raised when a resume attempt loses a race against a concurrent resume.

    ``POST /runs/{id}/resume`` pre-checks that the target Run is
    ``paused`` before calling
    ``repository.resume_run_with_pending_steering``, but that pre-check is
    not itself atomic with the resume -- two concurrent resume requests can
    both pass it. The repository re-validates, *inside* the same locked
    critical section that creates the new Run, that the old Run is still
    ``paused`` and has no ``superseded_by_run_id`` set yet; the request
    that loses that race gets this error instead of silently creating a
    second descendant Run (issue #16 PR #17 review round 4, point 1).
    """


class GraphInterrupt(BaseException):
    """Internal control-flow signal used by :func:`interrupt`."""

    def __init__(self, interrupt: object) -> None:
        super().__init__("graph execution interrupted")
        self.interrupt = interrupt


__all__ = [
    "ConcurrentRunError",
    "BudgetExceededError",
    "EmptyInputError",
    "GraphCancelledError",
    "GraphInterrupt",
    "GraphRecursionError",
    "GraphTimeoutError",
    "GraphValidationError",
    "InvalidUpdateError",
    "IdempotencyConflictError",
    "LingxiGraphError",
    "PersistenceError",
    "RunFinalizingError",
    "RunResumeConflictError",
    "RunSupersededError",
    "RunTerminalError",
]
