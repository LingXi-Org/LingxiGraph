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
    ``repository.transfer_pending_steering`` and issue #16's paused-run
    steering semantics) -- once that happens the old Run id is a dead end
    for *further* steering, so new steer attempts against it fail loudly
    instead of silently pending forever.
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
    "RunSupersededError",
    "RunTerminalError",
]
