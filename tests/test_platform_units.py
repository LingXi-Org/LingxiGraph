import asyncio
import json
import os
import tempfile
import unittest
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import TypedDict
from unittest.mock import AsyncMock, MagicMock, patch

from lingxigraph import (
    END,
    START,
    ConcurrentRunError,
    GraphCancelledError,
    GraphTimeoutError,
    StateGraph,
    cli,
)
from lingxigraph.cache_redis import RedisCache
from lingxigraph.serialization import JsonSerializer
from lingxigraph.server.eventbus import InMemoryEventBus, RedisEventBus
from lingxigraph.server.models import (
    AssistantCreate,
    AuditRecord,
    RunCreate,
    ScheduleCreate,
    ThreadCreate,
    utcnow,
)
from lingxigraph.server.registry import GraphRegistry
from lingxigraph.server.repository import (
    InMemoryRepository,
    PostgresRepository,
    RepositoryLimits,
)
from lingxigraph.server.security import Authenticator, AuthSettings, Principal
from lingxigraph.server.worker import Worker
from lingxigraph.types import MultitaskStrategy, RunStatus


class State(TypedDict):
    value: int


def graph_for(action):
    builder = StateGraph(State)
    builder.add_node("node", action)
    builder.add_edge(START, "node")
    builder.add_edge("node", END)
    return builder.compile()


class RegistryCLITests(unittest.TestCase):
    def test_registry_manifest_validation_and_lookup(self) -> None:
        registry = GraphRegistry({"one": graph_for(lambda state: state)})
        self.assertIn("one", registry)
        self.assertEqual(registry.info("one").id, "one")
        with self.assertRaises(ValueError):
            registry.register("one", graph_for(lambda state: state))
        with self.assertRaises(KeyError):
            registry.get("missing")

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "manifest.json"
            path.write_text(
                json.dumps(
                    {
                        "graphs": {
                            "example": {
                                "path": "lingxigraph.examples.production_graph:graph"
                            }
                        }
                    }
                ),
                encoding="utf-8",
            )
            loaded = GraphRegistry.from_manifest(path)
            self.assertEqual(loaded.list()[0].id, "example")
            path.write_text("{}", encoding="utf-8")
            with self.assertRaises(ValueError):
                GraphRegistry.from_manifest(path)
            path.write_text(json.dumps({"graphs": {"bad": "missing"}}), encoding="utf-8")
            with self.assertRaises(ValueError):
                GraphRegistry.from_manifest(path)

    def test_cli_parser_doctor_server_worker_and_migrate(self) -> None:
        self.assertEqual(cli.main(["--version"]), 0)
        self.assertEqual(cli.main([]), 2)
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(cli.main(["doctor"]), 1)
        with patch.dict(
            os.environ,
            {
                "LINGXIGRAPH_POSTGRES_URL": "postgresql://test",
                "LINGXIGRAPH_INSECURE_DEV_AUTH": "true",
            },
            clear=True,
        ):
            self.assertEqual(cli.main(["doctor"]), 0)
        with self.assertRaises(RuntimeError):
            cli._required("UNSET_TEST_VALUE")

        with patch.dict(
            os.environ, {"LINGXIGRAPH_INSECURE_DEV_AUTH": "true"}, clear=True
        ), patch("uvicorn.run") as run:
            args = cli.build_parser().parse_args(["server", "--port", "9000"])
            self.assertEqual(cli._server(args), 0)
            run.assert_called_once()

        repository = MagicMock()
        repository.setup = AsyncMock()
        saver = MagicMock()
        store = MagicMock()
        with patch.dict(
            os.environ, {"LINGXIGRAPH_POSTGRES_URL": "postgresql://test"}, clear=True
        ), patch("lingxigraph.server.PostgresRepository", return_value=repository), patch(
            "lingxigraph.checkpoint.postgres.PostgresSaver", return_value=saver
        ), patch("lingxigraph.store.postgres.PostgresStore", return_value=store):
            self.assertEqual(asyncio.run(cli._migrate()), 0)
            repository.setup.assert_awaited_once()
            saver.setup.assert_called_once()
            store.setup.assert_called_once()

        fake_worker = MagicMock()
        fake_worker.run_forever = AsyncMock(side_effect=asyncio.CancelledError)
        with patch.dict(
            os.environ, {"LINGXIGRAPH_POSTGRES_URL": "postgresql://test"}, clear=True
        ), patch("lingxigraph.server.PostgresRepository", return_value=repository), patch(
            "lingxigraph.checkpoint.postgres.PostgresSaver", return_value=saver
        ), patch("lingxigraph.store.postgres.PostgresStore", return_value=store), patch(
            "lingxigraph.server.Worker", return_value=fake_worker
        ):
            args = cli.build_parser().parse_args(["worker"])
            self.assertEqual(asyncio.run(cli._worker(args)), 0)
            fake_worker.stop.assert_called_once()


class SecurityEventBusTests(unittest.TestCase):
    def test_auth_settings_principal_dev_keys_and_jwt(self) -> None:
        with patch.dict(
            os.environ,
            {
                "LINGXIGRAPH_DEV_API_KEY": "secret-key",
                "LINGXIGRAPH_DEV_TENANT": "acme",
                "LINGXIGRAPH_DEV_ROLES": "viewer,operator",
                "LINGXIGRAPH_INSECURE_DEV_AUTH": "true",
            },
            clear=True,
        ):
            settings = AuthSettings.from_env()
        authenticator = Authenticator(settings)

        async def authenticate():
            key = await authenticator.authenticate(None, api_key="secret-key")
            self.assertEqual(key.tenant_id, "acme")
            dev = await authenticator.authenticate(
                None, dev_tenant="dev", dev_roles="developer,unknown"
            )
            self.assertEqual(dev.roles, frozenset({"developer"}))

        asyncio.run(authenticate())
        Principal("admin", "acme", frozenset({"tenant-admin"})).require("operator")
        with self.assertRaises(PermissionError):
            Principal("viewer", "acme", frozenset({"viewer"})).require("operator")
        with self.assertRaises(PermissionError):
            asyncio.run(Authenticator(AuthSettings()).authenticate(None))

        jwt_auth = Authenticator(
            AuthSettings(
                issuer="https://issuer",
                audience="agents",
                jwks_url="https://issuer/jwks",
            )
        )
        jwt_auth._jwk_client = SimpleNamespace(
            get_signing_key_from_jwt=lambda _token: SimpleNamespace(key="public")
        )
        claims = {
            "sub": "user",
            "tenant_id": "tenant",
            "roles": "viewer operator unknown",
        }
        with patch("jwt.decode", return_value=claims):
            principal = asyncio.run(jwt_auth.authenticate("Bearer token"))
        self.assertEqual(principal.roles, frozenset({"viewer", "operator"}))
        with patch("jwt.decode", return_value={"sub": "user"}), self.assertRaises(
            PermissionError
        ):
            asyncio.run(jwt_auth.authenticate("Bearer token"))

    def test_in_memory_and_redis_event_buses(self) -> None:
        async def memory_scenario():
            bus = InMemoryEventBus()
            waiter = asyncio.create_task(bus.wait("tenant", "run", timeout=0.1))
            await asyncio.sleep(0)
            await bus.publish("tenant", "run", 1)
            await waiter
            await bus.wait("tenant", "other", timeout=0.001)

        asyncio.run(memory_scenario())

        class PubSub:
            async def subscribe(self, _channel):
                return None

            async def get_message(self, **_kwargs):
                return {"data": "event"}

            async def aclose(self):
                return None

        redis = SimpleNamespace(
            publish=AsyncMock(return_value=1),
            pubsub=lambda: PubSub(),
            aclose=AsyncMock(),
        )
        with patch("redis.asyncio.Redis.from_url", return_value=redis):
            bus = RedisEventBus("redis://test")
            asyncio.run(bus.publish("tenant", "run", 1))
            asyncio.run(bus.wait("tenant", "run", timeout=0.1))
            asyncio.run(bus.close())
        redis.publish.assert_awaited_once()

    def test_redis_cache_roundtrip_clear_and_sync_wrappers(self) -> None:
        serializer = JsonSerializer()

        class Redis:
            def __init__(self):
                self.values = {"prefix:key": serializer.dumps({"value": 1})}

            async def get(self, key):
                return self.values.get(key)

            async def set(self, key, value, **_kwargs):
                self.values[key] = value

            async def delete(self, key):
                self.values.pop(key, None)

            async def scan_iter(self, **_kwargs):
                for key in list(self.values):
                    yield key

            async def aclose(self):
                return None

        redis = Redis()
        with patch("redis.asyncio.Redis.from_url", return_value=redis):
            cache = RedisCache("redis://test", prefix="prefix")
            self.assertEqual(asyncio.run(cache.aget("key")), {"value": 1})
            asyncio.run(cache.aset("other", {"value": 2}, ttl=1))
            asyncio.run(cache.adelete("other"))
            cache.set("sync", {"value": 3})
            self.assertEqual(cache.get("sync"), {"value": 3})
            cache.delete("sync")
            cache.set("group:item", {"value": 4})
            cache.clear(namespace="group")
            asyncio.run(cache.aclear())
            asyncio.run(cache.close())
            self.assertIsNone(cache.get("missing"))


class RepositoryWorkerTests(unittest.TestCase):
    def test_postgres_repository_stats_uses_tenant_scoped_counts(self) -> None:
        repository = PostgresRepository.__new__(PostgresRepository)
        repository._schema = "lingxigraph"
        connection_manager = MagicMock()
        connection = connection_manager.__enter__.return_value
        cursor_manager = connection.cursor.return_value
        cursor = cursor_manager.__enter__.return_value
        cursor.fetchall.return_value = [
            {"status": "pending", "count": 3},
            {"status": "failed", "count": 1},
        ]
        cursor.fetchone.side_effect = [
            {"count": 9},
            {"count": 2},
            {"count": 1},
            {"count": 4},
        ]
        repository._connect = MagicMock(return_value=connection_manager)
        repository._tenant = MagicMock()

        stats = asyncio.run(repository.stats("tenant"))

        self.assertEqual(stats["runs"]["pending"], 3)
        self.assertEqual(stats["runs"]["failed"], 1)
        self.assertEqual(stats["events"], 9)
        self.assertEqual(stats["threads"], 2)
        self.assertEqual(stats["assistants"], 1)
        self.assertEqual(stats["schedules"], 4)
        repository._tenant.assert_called_once_with(cursor, "tenant")

    def test_repository_concurrency_lease_cancel_and_quotas(self) -> None:
        async def scenario():
            repository = InMemoryRepository()
            assistant = await repository.create_assistant(
                "tenant", AssistantCreate(graph_id="graph"), "1"
            )
            thread = await repository.create_thread("tenant", ThreadCreate())
            first = await repository.create_run(
                "tenant",
                thread.id,
                assistant,
                RunCreate(assistant_id=assistant.id, input={"value": 1}),
            )
            claimed = await repository.claim_run("worker", lease_seconds=30)
            self.assertEqual(claimed.id, first.id)
            self.assertTrue(await repository.heartbeat("tenant", first.id, "worker"))
            self.assertFalse(await repository.heartbeat("tenant", first.id, "wrong"))
            with self.assertRaises(ConcurrentRunError):
                await repository.delete_thread("tenant", thread.id)
            with self.assertRaises(ConcurrentRunError):
                await repository.create_run(
                    "tenant",
                    thread.id,
                    assistant,
                    RunCreate(
                        assistant_id=assistant.id,
                        multitask_strategy=MultitaskStrategy.REJECT,
                    ),
                )
            second = await repository.create_run(
                "tenant",
                thread.id,
                assistant,
                RunCreate(
                    assistant_id=assistant.id,
                    multitask_strategy=MultitaskStrategy.CANCEL_PREVIOUS,
                ),
            )
            self.assertTrue(await repository.is_cancel_requested("tenant", first.id))
            await repository.finish_run("tenant", first.id, RunStatus.CANCELLED)
            self.assertFalse(await repository.request_cancel("tenant", first.id))
            self.assertTrue(await repository.request_cancel("tenant", second.id))
            await repository.create_schedule(
                "tenant", ScheduleCreate(assistant_id=assistant.id, cron="* * * * *")
            )
            await repository.audit(
                AuditRecord(
                    tenant_id="tenant",
                    actor="test",
                    action="tested",
                    resource_type="run",
                )
            )
            stats = await repository.stats("tenant")
            self.assertEqual(stats["runs"]["cancelled"], 2)
            self.assertEqual(stats["threads"], 1)
            self.assertEqual(stats["assistants"], 1)
            self.assertEqual(stats["schedules"], 1)

            expired = await repository.create_run(
                "tenant", None, assistant, RunCreate(assistant_id=assistant.id)
            )
            claimed = await repository.claim_run("old-worker", lease_seconds=30)
            key = ("tenant", claimed.id)
            repository._runs[key] = repository._runs[key].model_copy(
                update={"lease_expires_at": utcnow() - timedelta(seconds=1)}
            )
            reclaimed = await repository.claim_run("new-worker", lease_seconds=30)
            self.assertEqual(reclaimed.id, expired.id)

            limited = InMemoryRepository(
                limits=RepositoryLimits(max_active_runs=0, max_queued_runs=1)
            )
            limited_assistant = await limited.create_assistant(
                "tenant", AssistantCreate(graph_id="graph"), "1"
            )
            with self.assertRaises(ConcurrentRunError):
                await limited.create_run(
                    "tenant", None, limited_assistant, RunCreate(assistant_id=limited_assistant.id)
                )

        asyncio.run(scenario())

    def test_worker_maps_runtime_failures_to_terminal_states(self) -> None:
        async def execute(action, expected_status, expected_code):
            registry = GraphRegistry({"graph": graph_for(action)})
            repository = InMemoryRepository()
            assistant = await repository.create_assistant(
                "tenant", AssistantCreate(graph_id="graph"), "1"
            )
            run = await repository.create_run(
                "tenant",
                None,
                assistant,
                RunCreate(assistant_id=assistant.id, input={"value": 1}),
            )
            worker = Worker(registry, repository, max_delivery_attempts=1)
            self.assertTrue(await worker.run_once())
            self.assertFalse(await worker.run_once())
            completed = await repository.get_run("tenant", run.id)
            self.assertEqual(completed.status, expected_status)
            self.assertEqual(completed.error["code"], expected_code)

        def failed(_state):
            raise RuntimeError("failed")

        def timed_out(_state):
            raise GraphTimeoutError("late")

        def cancelled(_state):
            raise GraphCancelledError("cancelled")

        asyncio.run(execute(failed, RunStatus.DEAD_LETTER, "dead_letter"))
        asyncio.run(execute(timed_out, RunStatus.TIMED_OUT, "run_timed_out"))
        asyncio.run(execute(cancelled, RunStatus.CANCELLED, "run_cancelled"))


if __name__ == "__main__":
    unittest.main()
