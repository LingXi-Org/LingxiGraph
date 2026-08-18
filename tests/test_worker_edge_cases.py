"""Direct unit tests for Worker lifecycle properties and small edge branches."""

import asyncio
import unittest

from lingxigraph.server.registry import GraphRegistry
from lingxigraph.server.repository import InMemoryRepository
from lingxigraph.server.worker import Worker


class WorkerLifecyclePropertyTests(unittest.TestCase):
    def test_draining_ready_and_live_properties(self) -> None:
        registry = GraphRegistry({})
        repository = InMemoryRepository()
        worker = Worker(registry, repository)

        self.assertFalse(worker.draining)
        self.assertTrue(worker.ready)
        self.assertTrue(worker.live)

        worker.stop()

        self.assertTrue(worker.draining)
        self.assertFalse(worker.ready)

    def test_run_once_returns_false_immediately_while_draining(self) -> None:
        async def scenario() -> None:
            registry = GraphRegistry({})
            repository = InMemoryRepository()
            worker = Worker(registry, repository)
            worker.stop()
            claimed = await worker.run_once()
            self.assertFalse(claimed)

        asyncio.run(scenario())

    def test_drain_returns_true_when_already_idle(self) -> None:
        async def scenario() -> None:
            registry = GraphRegistry({})
            repository = InMemoryRepository()
            worker = Worker(registry, repository)
            # No active _execute in flight -- _idle is already set.
            finished = await worker.drain(timeout=1.0)
            self.assertTrue(finished)
            self.assertTrue(worker.draining)

        asyncio.run(scenario())


class IsRetryableClassificationTests(unittest.TestCase):
    """Direct unit coverage of ``Worker._is_retryable``'s branches: it
    decides whether a node exception should route into the ordinary
    delivery-retry path or fail the run outright."""

    def test_validation_errors_are_never_retryable(self) -> None:
        from lingxigraph.errors import GraphValidationError, InvalidUpdateError

        self.assertFalse(Worker._is_retryable(GraphValidationError("bad graph")))
        self.assertFalse(Worker._is_retryable(InvalidUpdateError("bad update")))
        self.assertFalse(Worker._is_retryable(KeyError("missing")))
        self.assertFalse(Worker._is_retryable(ValueError("bad value")))

    def test_connection_and_persistence_errors_are_retryable(self) -> None:
        from lingxigraph.errors import PersistenceError

        self.assertTrue(Worker._is_retryable(ConnectionError("dropped")))
        self.assertTrue(Worker._is_retryable(TimeoutError("timed out")))
        self.assertTrue(Worker._is_retryable(PersistenceError("db hiccup")))

    def test_plain_runtime_error_is_retryable(self) -> None:
        self.assertTrue(Worker._is_retryable(RuntimeError("transient")))

    def test_httpx_module_exceptions_are_retryable(self) -> None:
        class FakeHttpxError(Exception):
            pass

        FakeHttpxError.__module__ = "httpx._exceptions"
        self.assertTrue(Worker._is_retryable(FakeHttpxError("network blip")))

    def test_name_based_transient_markers_are_retryable(self) -> None:
        class UpstreamTemporaryFailure(Exception):
            pass

        self.assertTrue(Worker._is_retryable(UpstreamTemporaryFailure("try again")))

    def test_unrelated_exception_is_not_retryable(self) -> None:
        class SomeDomainError(Exception):
            pass

        self.assertFalse(Worker._is_retryable(SomeDomainError("permanent")))


if __name__ == "__main__":
    unittest.main()
