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


if __name__ == "__main__":
    unittest.main()
