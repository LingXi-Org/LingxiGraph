import asyncio
import os
import unittest
from uuid import uuid4

POSTGRES_URL = os.getenv("LINGXIGRAPH_TEST_POSTGRES_URL")
REDIS_URL = os.getenv("LINGXIGRAPH_TEST_REDIS_URL")


@unittest.skipUnless(POSTGRES_URL, "PostgreSQL integration DSN not configured")
class PostgresIntegrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.schema = "lx_test_" + uuid4().hex[:12]

    def tearDown(self) -> None:
        import psycopg

        with psycopg.connect(POSTGRES_URL, autocommit=True) as conn:
            conn.execute(f'DROP SCHEMA IF EXISTS "{self.schema}" CASCADE')

    def test_repository_queue_checkpoint_store_and_rls(self) -> None:
        import psycopg
        from psycopg import sql
        from psycopg.conninfo import conninfo_to_dict, make_conninfo

        from lingxigraph import END, START, PostgresSaver, StateGraph
        from lingxigraph.server.models import AssistantCreate, RunCreate, ThreadCreate
        from lingxigraph.server.repository import PostgresRepository
        from lingxigraph.store.postgres import PostgresStore

        # Simulate a database created before store item TTL support. Repository
        # setup must upgrade it before PostgresStore starts using expires_at.
        with psycopg.connect(POSTGRES_URL) as conn:
            conn.execute(f'CREATE SCHEMA "{self.schema}"')
            conn.execute(
                f'''CREATE TABLE "{self.schema}".store_items (
                    tenant_id TEXT NOT NULL,
                    namespace TEXT[] NOT NULL,
                    key TEXT NOT NULL,
                    value JSONB NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (tenant_id, namespace, key)
                )'''
            )

        async def scenario():
            repository = PostgresRepository(POSTGRES_URL, schema=self.schema)
            await repository.setup()
            assistant = await repository.create_assistant(
                "tenant-a", AssistantCreate(graph_id="graph", name="a"), "1.0.0"
            )
            await repository.create_assistant(
                "tenant-b", AssistantCreate(graph_id="graph", name="b"), "1.0.0"
            )
            self.assertEqual(len(await repository.list_assistants("tenant-a")), 1)
            thread = await repository.create_thread("tenant-a", ThreadCreate())
            first = await repository.create_run(
                "tenant-a",
                thread.id,
                assistant,
                RunCreate(assistant_id=assistant.id, input={"value": 1}),
            )
            claimed = await repository.claim_run("worker-a", lease_seconds=30)
            self.assertEqual(claimed.id, first.id)
            await repository.create_run(
                "tenant-a",
                thread.id,
                assistant,
                RunCreate(assistant_id=assistant.id, input={"value": 2}),
            )
            self.assertIsNone(await repository.claim_run("worker-b", lease_seconds=30))

        asyncio.run(scenario())

        class State(dict):
            __annotations__ = {"value": int}
            __required_keys__ = frozenset({"value"})

        saver = PostgresSaver(POSTGRES_URL, schema=self.schema)
        saver.setup()
        graph_builder = StateGraph(State)
        graph_builder.add_node("increment", lambda state: {"value": state["value"] + 1})
        graph_builder.add_edge(START, "increment")
        graph_builder.add_edge("increment", END)
        graph = graph_builder.compile(checkpointer=saver)
        config = {
            "configurable": {
                "tenant_id": "tenant-a",
                "thread_id": "checkpoint-thread",
            }
        }
        self.assertEqual(graph.invoke({"value": 1}, config)["value"], 2)
        self.assertEqual(graph.get_state(config).values["value"], 2)

        store_a = PostgresStore(POSTGRES_URL, tenant_id="tenant-a", schema=self.schema)
        store_b = PostgresStore(POSTGRES_URL, tenant_id="tenant-b", schema=self.schema)
        store_a.put(("users",), "one", {"name": "Alice"})
        self.assertIsNone(store_b.get(("users",), "one"))

        role = "lx_api_" + uuid4().hex[:10]
        password = uuid4().hex
        with psycopg.connect(POSTGRES_URL, autocommit=True) as conn:
            conn.execute(
                sql.SQL("CREATE ROLE {} LOGIN PASSWORD {}").format(
                    sql.Identifier(role), sql.Literal(password)
                )
            )
            conn.execute(f'GRANT USAGE ON SCHEMA "{self.schema}" TO "{role}"')
            conn.execute(
                f"GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA "
                f'"{self.schema}" TO "{role}"'
            )
            conn.execute(
                f'GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA "{self.schema}" TO "{role}"'
            )
        params = conninfo_to_dict(POSTGRES_URL)
        params.update(user=role, password=password)
        try:
            with psycopg.connect(make_conninfo(**params)) as conn:
                conn.execute("SELECT set_config('app.tenant_id', %s, false)", ("tenant-a",))
                count_a = conn.execute(
                    f'SELECT count(*) FROM "{self.schema}".assistants'
                ).fetchone()[0]
                conn.execute("SELECT set_config('app.tenant_id', %s, false)", ("tenant-b",))
                count_b = conn.execute(
                    f'SELECT count(*) FROM "{self.schema}".assistants'
                ).fetchone()[0]
                self.assertEqual((count_a, count_b), (1, 1))
        finally:
            with psycopg.connect(POSTGRES_URL, autocommit=True) as conn:
                conn.execute(f'DROP OWNED BY "{role}"')
                conn.execute(f'DROP ROLE "{role}"')

    def test_resume_run_with_pending_steering_is_atomic_and_race_free(self) -> None:
        """Issue #16 PR #17 review point 1, PostgreSQL path.

        Regresses both halves of the finding against a real PostgreSQL
        transaction:

        * A worker hammering ``claim_run`` concurrently with
          ``resume_run_with_pending_steering`` must never observe the
          resumed Run before its migrated steering is already present --
          the previous two-transaction implementation (separate
          ``create_run`` + ``transfer_pending_steering`` commits) allowed a
          real window where the new Run was claimable (and even
          finishable) with nothing migrated onto it.
        * Sequence numbers assigned to the migrated events and to an
          ordinary steer submitted against the resumed run afterwards must
          never collide -- the previous ``_transfer_pending_steering_sync``
          only locked the *old* run row, not the new one, so its
          ``MAX(sequence)+1`` could race a concurrent ``_submit_steering_
          sync`` doing the same computation for the new run.
        """

        from lingxigraph.server.models import AssistantCreate, RunCreate, ThreadCreate
        from lingxigraph.server.repository import PostgresRepository

        async def scenario() -> None:
            repo = PostgresRepository(POSTGRES_URL, schema=self.schema)
            await repo.setup()
            assistant = await repo.create_assistant(
                "acme", AssistantCreate(graph_id="graph", name="a"), "1.0.0"
            )
            thread = await repo.create_thread("acme", ThreadCreate())
            old_run = await repo.create_run(
                "acme", thread.id, assistant, RunCreate(assistant_id=assistant.id, input={})
            )
            await repo.submit_steering("acme", old_run.id, kind="user_input", payload={"m": 1})
            await repo.submit_steering("acme", old_run.id, kind="user_input", payload={"m": 2})
            await repo.finish_run("acme", old_run.id, "paused", output={})

            claims: list[str] = []
            stop = asyncio.Event()

            async def hammer_claim() -> None:
                while not stop.is_set():
                    claimed = await repo.claim_run("worker-a", lease_seconds=30)
                    if claimed is not None:
                        claims.append(claimed.id)
                        if claimed.id != old_run.id:
                            pending = await repo.list_pending_steering("acme", claimed.id)
                            self.assertEqual(
                                len(pending),
                                2,
                                "resumed run was claimable before its steering migrated",
                            )
                    await asyncio.sleep(0)

            hammer = asyncio.create_task(hammer_claim())
            request = RunCreate(assistant_id=assistant.id, resume=1)
            new_run, transferred = await repo.resume_run_with_pending_steering(
                "acme", thread.id, assistant, request, old_run.id
            )
            self.assertEqual([event.sequence for event in transferred], [1, 2])
            for _ in range(1000):
                if new_run.id in claims:
                    break
                await asyncio.sleep(0.002)
            stop.set()
            await hammer
            self.assertIn(new_run.id, claims)

            extra, _ = await repo.submit_steering(
                "acme", new_run.id, kind="user_input", payload={"m": 3}
            )
            self.assertEqual(extra.sequence, 3)
            all_events = await repo.list_steering("acme", new_run.id)
            self.assertEqual(sorted(event.sequence for event in all_events), [1, 2, 3])

            # Identity survives the transfer (review point 3).
            self.assertTrue(
                all(event.source_event_id for event in all_events if event.sequence in (1, 2))
            )

        asyncio.run(scenario())

    def test_concurrent_resume_of_the_same_paused_run_creates_exactly_one_descendant(
        self,
    ) -> None:
        """Issue #16 PR #17 review round 4, point 1, PostgreSQL path.

        Two real concurrent transactions both calling
        ``resume_run_with_pending_steering`` against the same paused run
        must not both succeed: PostgreSQL's ``FOR UPDATE`` on the old run
        row serializes them, but the loser must be rejected by the locked
        revalidation (still ``paused``, no ``superseded_by_run_id`` yet)
        rather than silently creating a second descendant Run and
        overwriting the winner's ``superseded_by_run_id``.
        """

        from lingxigraph.errors import RunResumeConflictError
        from lingxigraph.server.models import AssistantCreate, RunCreate, ThreadCreate
        from lingxigraph.server.repository import PostgresRepository

        async def scenario() -> None:
            repo = PostgresRepository(POSTGRES_URL, schema=self.schema)
            await repo.setup()
            assistant = await repo.create_assistant(
                "acme", AssistantCreate(graph_id="graph", name="a"), "1.0.0"
            )
            thread = await repo.create_thread("acme", ThreadCreate())
            old_run = await repo.create_run(
                "acme", thread.id, assistant, RunCreate(assistant_id=assistant.id, input={})
            )
            await repo.finish_run("acme", old_run.id, "paused", output={})

            request = RunCreate(assistant_id=assistant.id, resume=1)
            results = await asyncio.gather(
                repo.resume_run_with_pending_steering(
                    "acme", thread.id, assistant, request, old_run.id
                ),
                repo.resume_run_with_pending_steering(
                    "acme", thread.id, assistant, request, old_run.id
                ),
                return_exceptions=True,
            )
            successes = [r for r in results if not isinstance(r, BaseException)]
            failures = [r for r in results if isinstance(r, BaseException)]
            self.assertEqual(len(successes), 1, results)
            self.assertEqual(len(failures), 1, results)
            self.assertIsInstance(failures[0], RunResumeConflictError)

            winner_run, _ = successes[0]
            all_runs = await repo.list_runs("acme", thread_id=thread.id)
            descendants = [r for r in all_runs if r.id != old_run.id]
            self.assertEqual(len(descendants), 1)
            self.assertEqual(descendants[0].id, winner_run.id)

            final_old = await repo.get_run("acme", old_run.id)
            self.assertEqual(final_old.metadata.get("superseded_by_run_id"), winner_run.id)

        asyncio.run(scenario())

    def test_steer_accept_and_accepted_event_commit_atomically(self) -> None:
        """Issue #16 PR #17 review round 4, point 2, PostgreSQL path.

        Injects a failure between the steering row commit and its
        ``run.steer.accepted`` append by directly deleting the accepted
        event row after a normal ``submit_steering`` call (simulating the
        pre-fix failure window where the row committed but the event append
        transiently failed), then verifies an idempotency-key retry repairs
        it -- exactly once, never duplicated -- and that consumption still
        produces a complete accepted -> consumed pair.
        """

        from lingxigraph.server.models import AssistantCreate, RunCreate, ThreadCreate
        from lingxigraph.server.repository import PostgresRepository
        from lingxigraph.steering import SteeringConsumption, SteeringEvent

        async def scenario() -> None:
            repo = PostgresRepository(POSTGRES_URL, schema=self.schema)
            await repo.setup()
            assistant = await repo.create_assistant(
                "acme", AssistantCreate(graph_id="graph", name="a"), "1.0.0"
            )
            thread = await repo.create_thread("acme", ThreadCreate())
            run = await repo.create_run(
                "acme", thread.id, assistant, RunCreate(assistant_id=assistant.id, input={})
            )

            event, created = await repo.submit_steering(
                "acme", run.id, kind="user_input", payload={"m": 1}, idempotency_key="key-1"
            )
            self.assertTrue(created)
            accepted = [
                e for e in await repo.list_events("acme", run.id) if e.kind == "run.steer.accepted"
            ]
            self.assertEqual(len(accepted), 1)

            # Simulate the pre-fix failure window: the steering row
            # committed, but its ``run.steer.accepted`` event never made it
            # (e.g. a transient failure right after the row's commit).
            import psycopg

            with psycopg.connect(POSTGRES_URL, autocommit=True) as conn:
                conn.execute(
                    f'DELETE FROM "{self.schema}".run_events '
                    "WHERE tenant_id=%s AND run_id=%s AND kind='run.steer.accepted'",
                    ("acme", run.id),
                )
            gap = [
                e for e in await repo.list_events("acme", run.id) if e.kind == "run.steer.accepted"
            ]
            self.assertEqual(gap, [])

            # Retry with the same Idempotency-Key: the row already exists
            # (``created`` is False) but the gap must be repaired.
            retried_event, retried_created = await repo.submit_steering(
                "acme", run.id, kind="user_input", payload={"m": 1}, idempotency_key="key-1"
            )
            self.assertFalse(retried_created)
            self.assertEqual(retried_event.id, event.id)
            repaired = [
                e for e in await repo.list_events("acme", run.id) if e.kind == "run.steer.accepted"
            ]
            self.assertEqual(len(repaired), 1)
            self.assertEqual(repaired[0].data["steering_event_id"], event.id)

            # A further retry must not duplicate it.
            await repo.submit_steering(
                "acme", run.id, kind="user_input", payload={"m": 1}, idempotency_key="key-1"
            )
            final_accepted = [
                e for e in await repo.list_events("acme", run.id) if e.kind == "run.steer.accepted"
            ]
            self.assertEqual(len(final_accepted), 1)

            # After consumption, a complete accepted -> consumed pair
            # exists (never duplicated by the repair above).
            consumption = SteeringConsumption(
                event=SteeringEvent(
                    id=event.id,
                    run_id=run.id,
                    sequence=event.sequence,
                    kind=event.kind,
                    payload=event.payload,
                    metadata=event.metadata,
                    created_at=event.created_at,
                ),
                node="n",
                namespace=(),
                task_id="t-0",
                queue_latency_seconds=0.01,
            )
            await repo.commit_steering_consumptions("acme", run.id, [consumption])
            all_events = await repo.list_events("acme", run.id)
            self.assertEqual(
                sum(1 for e in all_events if e.kind == "run.steer.accepted"), 1
            )
            self.assertEqual(
                sum(1 for e in all_events if e.kind == "run.steer.consumed"), 1
            )

        asyncio.run(scenario())

    def test_commit_steering_consumptions_is_atomic(self) -> None:
        """Issue #16 PR #17 review point 2, PostgreSQL path: status update
        and the ``run.steer.consumed`` lifecycle event must land in the
        same transaction."""

        from lingxigraph.server.models import AssistantCreate, RunCreate, ThreadCreate
        from lingxigraph.server.repository import PostgresRepository
        from lingxigraph.steering import SteeringConsumption, SteeringEvent

        async def scenario() -> None:
            repo = PostgresRepository(POSTGRES_URL, schema=self.schema)
            await repo.setup()
            assistant = await repo.create_assistant(
                "acme", AssistantCreate(graph_id="graph", name="a"), "1.0.0"
            )
            thread = await repo.create_thread("acme", ThreadCreate())
            run = await repo.create_run(
                "acme", thread.id, assistant, RunCreate(assistant_id=assistant.id, input={})
            )
            event, _ = await repo.submit_steering(
                "acme", run.id, kind="user_input", payload={"m": 1}
            )
            steering_event = SteeringEvent(
                id=event.id,
                run_id=run.id,
                sequence=event.sequence,
                kind=event.kind,
                payload=event.payload,
                metadata=event.metadata,
                created_at=event.created_at,
            )
            consumption = SteeringConsumption(
                event=steering_event,
                consumed_at=steering_event.created_at,
                node="n",
                namespace=(),
                task_id="t",
            )
            stored = await repo.commit_steering_consumptions("acme", run.id, [consumption])
            self.assertEqual(len(stored), 1)
            self.assertEqual(stored[0].kind, "run.steer.consumed")
            all_steering = await repo.list_steering("acme", run.id)
            self.assertEqual(all_steering[0].status, "consumed")
            all_events = await repo.list_events("acme", run.id)
            self.assertEqual(
                [e.kind for e in all_events if e.kind == "run.steer.consumed"],
                ["run.steer.consumed"],
            )

        asyncio.run(scenario())

    def test_alembic_upgrade_head_applies_source_event_id(self) -> None:
        """Issue #16 PR #17 review round 3, point 1.

        ``PostgresRepository._setup_sync()`` applies the SQL migration
        files directly and is *not* a substitute for driving the real
        Alembic revision chain that a production deployment uses
        (``alembic upgrade head``). This regresses that ``0002_steering``
        -> ``0003_steering_source_event`` is reachable through Alembic
        itself and actually creates the ``source_event_id`` column and its
        partial index.
        """

        import psycopg
        from alembic import command
        from alembic.config import Config

        repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        cfg = Config(os.path.join(repo_root, "alembic.ini"))
        cfg.set_main_option("script_location", os.path.join(repo_root, "migrations"))
        cfg.set_main_option("sqlalchemy.url", POSTGRES_URL)
        os.environ["LINGXIGRAPH_POSTGRES_SCHEMA"] = self.schema

        with psycopg.connect(POSTGRES_URL, autocommit=True) as conn:
            conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{self.schema}"')
        try:
            command.upgrade(cfg, "0002_steering")
            with psycopg.connect(POSTGRES_URL) as conn:
                exists = conn.execute(
                    """SELECT 1 FROM information_schema.columns
                    WHERE table_schema=%s AND table_name='run_steering_events'
                    AND column_name='source_event_id'""",
                    (self.schema,),
                ).fetchone()
                self.assertIsNone(exists, "source_event_id should not exist before head")

            command.upgrade(cfg, "head")
            with psycopg.connect(POSTGRES_URL) as conn:
                column = conn.execute(
                    """SELECT 1 FROM information_schema.columns
                    WHERE table_schema=%s AND table_name='run_steering_events'
                    AND column_name='source_event_id'""",
                    (self.schema,),
                ).fetchone()
                self.assertIsNotNone(column, "alembic head must create source_event_id")
                index = conn.execute(
                    """SELECT 1 FROM pg_indexes
                    WHERE schemaname=%s AND indexname='run_steering_events_source'""",
                    (self.schema,),
                ).fetchone()
                self.assertIsNotNone(
                    index, "alembic head must create the source_event_id index"
                )
        finally:
            os.environ.pop("LINGXIGRAPH_POSTGRES_SCHEMA", None)

    def test_pause_resume_pause_resume_preserves_root_source_event_id(self) -> None:
        """Issue #16 PR #17 review round 3, point 2.

        A -> B (resume) -> C (resume): ``source_event_id`` on C's migrated
        steering event must still point at A, not B, and the eventual
        ``run.steer.consumed`` event's ``queue_latency_seconds`` must be
        computed from A's original acceptance ``created_at``.
        """

        from lingxigraph.server.models import AssistantCreate, RunCreate, ThreadCreate
        from lingxigraph.server.repository import PostgresRepository
        from lingxigraph.steering import SteeringConsumption, SteeringEvent

        async def scenario() -> None:
            repo = PostgresRepository(POSTGRES_URL, schema=self.schema)
            await repo.setup()
            assistant = await repo.create_assistant(
                "acme", AssistantCreate(graph_id="graph", name="a"), "1.0.0"
            )
            thread = await repo.create_thread("acme", ThreadCreate())

            run_a = await repo.create_run(
                "acme", thread.id, assistant, RunCreate(assistant_id=assistant.id, input={})
            )
            event_a, _ = await repo.submit_steering(
                "acme", run_a.id, kind="user_input", payload={"m": 1}
            )
            self.assertIsNone(event_a.source_event_id)
            await repo.finish_run("acme", run_a.id, "paused", output={})

            run_b, transferred_b = await repo.resume_run_with_pending_steering(
                "acme",
                thread.id,
                assistant,
                RunCreate(assistant_id=assistant.id, resume=1),
                run_a.id,
            )
            self.assertEqual(len(transferred_b), 1)
            event_b = transferred_b[0]
            self.assertEqual(event_b.source_event_id, event_a.id)
            self.assertEqual(event_b.created_at, event_a.created_at)
            await repo.finish_run("acme", run_b.id, "paused", output={})

            run_c, transferred_c = await repo.resume_run_with_pending_steering(
                "acme",
                thread.id,
                assistant,
                RunCreate(assistant_id=assistant.id, resume=1),
                run_b.id,
            )
            self.assertEqual(len(transferred_c), 1)
            event_c = transferred_c[0]
            # Root identity A survives, not the intermediate hop B.
            self.assertEqual(event_c.source_event_id, event_a.id)
            self.assertEqual(event_c.created_at, event_a.created_at)

            steering_event = SteeringEvent(
                id=event_c.id,
                run_id=run_c.id,
                sequence=event_c.sequence,
                kind=event_c.kind,
                payload=event_c.payload,
                metadata=event_c.metadata,
                created_at=event_c.created_at,
                source_event_id=event_c.source_event_id,
            )
            consumption = SteeringConsumption(
                event=steering_event,
                consumed_at=steering_event.created_at,
                node="n",
                namespace=(),
                task_id="t",
            )
            stored = await repo.commit_steering_consumptions("acme", run_c.id, [consumption])
            self.assertEqual(len(stored), 1)
            self.assertEqual(stored[0].data["source_event_id"], event_a.id)

        asyncio.run(scenario())

    def test_commit_steering_consumptions_is_idempotent_on_retry(self) -> None:
        """Issue #16 PR #17 review round 3, point 3: a retried commit of
        the same consumption batch (simulating a worker that resends after
        an ack it never observed) must not produce a second
        ``run.steer.consumed`` lifecycle event."""

        from lingxigraph.server.models import AssistantCreate, RunCreate, ThreadCreate
        from lingxigraph.server.repository import PostgresRepository
        from lingxigraph.steering import SteeringConsumption, SteeringEvent

        async def scenario() -> None:
            repo = PostgresRepository(POSTGRES_URL, schema=self.schema)
            await repo.setup()
            assistant = await repo.create_assistant(
                "acme", AssistantCreate(graph_id="graph", name="a"), "1.0.0"
            )
            thread = await repo.create_thread("acme", ThreadCreate())
            run = await repo.create_run(
                "acme", thread.id, assistant, RunCreate(assistant_id=assistant.id, input={})
            )
            event, _ = await repo.submit_steering(
                "acme", run.id, kind="user_input", payload={"m": 1}
            )
            steering_event = SteeringEvent(
                id=event.id,
                run_id=run.id,
                sequence=event.sequence,
                kind=event.kind,
                payload=event.payload,
                metadata=event.metadata,
                created_at=event.created_at,
            )
            consumption = SteeringConsumption(
                event=steering_event,
                consumed_at=steering_event.created_at,
                node="n",
                namespace=(),
                task_id="t",
            )
            first = await repo.commit_steering_consumptions("acme", run.id, [consumption])
            self.assertEqual(len(first), 1)
            second = await repo.commit_steering_consumptions("acme", run.id, [consumption])
            self.assertEqual(second, [])

            all_events = await repo.list_events("acme", run.id)
            consumed = [e for e in all_events if e.kind == "run.steer.consumed"]
            self.assertEqual(len(consumed), 1)

        asyncio.run(scenario())

    @unittest.skipUnless(REDIS_URL, "Redis integration URL not configured")
    def test_redis_cache_pubsub_and_recovery_contract(self) -> None:
        from lingxigraph.cache_redis import RedisCache
        from lingxigraph.server.eventbus import RedisEventBus

        async def scenario() -> None:
            prefix = "lingxigraph-test-" + uuid4().hex
            cache = RedisCache(REDIS_URL, prefix=prefix)
            await cache.aset("node:key", {"value": 1}, ttl=30)
            self.assertEqual(await cache.aget("node:key"), {"value": 1})

            bus = RedisEventBus(REDIS_URL, prefix=prefix)
            waiter = asyncio.create_task(bus.wait("tenant", "run", timeout=2))
            await asyncio.sleep(0.05)
            await bus.publish("tenant", "run", 1)
            await waiter

            await cache.aclear()
            self.assertIsNone(await cache.aget("node:key"))
            await bus.close()
            await cache.close()

        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()
