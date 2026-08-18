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
                consumed_at=event.created_at,
                node="n",
                namespace=(),
                task_id="t-0",
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

    def test_stale_worker_cannot_commit_steering_consumptions_after_lease_takeover(
        self,
    ) -> None:
        """Issue #16 PR #17 review round 7, point 2 (BLOCKER), PostgreSQL
        path: a worker whose lease has been reclaimed by a new owner must
        never be able to durably commit a steering consumption -- its
        write must be rejected atomically, touching nothing."""

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

            claimed_a = await repo.claim_run("worker-a", lease_seconds=1)
            assert claimed_a is not None
            self.assertEqual(claimed_a.attempt, 1)

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

            # A's lease expires; B claims the same run at a new attempt.
            await asyncio.sleep(1.2)
            claimed_b = await repo.claim_run("worker-b", lease_seconds=30)
            assert claimed_b is not None
            self.assertEqual(claimed_b.attempt, 2)

            # A's now-stale commit must be rejected outright.
            result = await repo.commit_steering_consumptions_if_owned(
                "acme", run.id, "worker-a", claimed_a.attempt, [consumption]
            )
            self.assertIsNone(result)

            still_pending = await repo.list_pending_steering("acme", run.id)
            self.assertEqual([row.id for row in still_pending], [event.id])
            consumed_events = [
                e for e in await repo.list_events("acme", run.id) if e.kind == "run.steer.consumed"
            ]
            self.assertEqual(consumed_events, [])

            # B, the current owner, commits it normally.
            stored = await repo.commit_steering_consumptions_if_owned(
                "acme", run.id, "worker-b", claimed_b.attempt, [consumption]
            )
            self.assertEqual(len(stored), 1)
            self.assertEqual(await repo.list_pending_steering("acme", run.id), [])

        asyncio.run(scenario())

    def test_claim_run_recovers_expired_cancelling_lease_to_cancelled_and_thread_unwedges(
        self,
    ) -> None:
        """Issue #16 PR #17 review round 11 (BLOCKER), PostgreSQL path:
        a run stuck in ``cancelling`` whose owning worker crashed before
        finalizing (lease expired) must never be permanently stranded --
        ``claim_run`` must resolve it to terminal ``cancelled`` on the
        very next claim pass, clear its lease, and -- crucially -- stop
        wedging its thread so a separate queued run on the SAME thread
        can subsequently be claimed."""

        from lingxigraph.server.models import AssistantCreate, RunCreate, ThreadCreate
        from lingxigraph.server.repository import PostgresRepository

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
            # A second run queued on the same thread -- it must remain
            # blocked while ``run`` is active, and become claimable once
            # the stranded ``cancelling`` run is recovered.
            queued = await repo.create_run(
                "acme", thread.id, assistant, RunCreate(assistant_id=assistant.id, input={})
            )

            accepted, created = await repo.submit_steering(
                "acme", run.id, kind="user_input", payload={"m": 1}
            )
            self.assertTrue(created)

            claimed = await repo.claim_run("worker-a", lease_seconds=1)
            assert claimed is not None
            self.assertEqual(claimed.id, run.id)

            cancel_requested = await repo.request_cancel("acme", run.id)
            self.assertTrue(cancel_requested)
            mid_flight = await repo.get_run("acme", run.id)
            self.assertEqual(mid_flight.status, "cancelling")

            closed = await repo.close_steering("acme", run.id, "worker-a", claimed.attempt)
            self.assertTrue(closed)

            # Worker A crashes here, never reaching the fenced terminal
            # commit. Let the lease expire, then simulate worker B's next
            # claim attempt.
            await asyncio.sleep(1.2)

            reclaimed = await repo.claim_run("worker-b", lease_seconds=30)

            final = await repo.get_run("acme", run.id)
            self.assertEqual(final.status, "cancelled")
            self.assertIsNone(final.lease_owner)
            self.assertIsNone(final.lease_expires_at)
            self.assertIsNotNone(final.finished_at)
            assert final.error is not None
            self.assertEqual(final.error["code"], "run_cancelled")

            # The queued run on the same thread is no longer wedged --
            # either this very claim call already picked it up, or the
            # next one does.
            if reclaimed is not None and reclaimed.id == queued.id:
                thread_unwedged_run = reclaimed
            else:
                self.assertIsNone(reclaimed)
                thread_unwedged_run = await repo.claim_run("worker-c", lease_seconds=30)
            assert thread_unwedged_run is not None
            self.assertEqual(thread_unwedged_run.id, queued.id)
            self.assertEqual(thread_unwedged_run.status, "running")

            # Ordinary cancel already leaves pending steering as durable
            # history (never implicitly consumed/replayed) -- the
            # lease-recovery path must behave the same way here.
            pending_steering = await repo.list_pending_steering("acme", run.id)
            self.assertEqual([row.id for row in pending_steering], [accepted.id])

        asyncio.run(scenario())

    def test_cancel_paused_run_terminates_immediately_postgres_path(self) -> None:
        """Issue #16 PR #17 review round 12 (BLOCKER), PostgreSQL path:
        cancelling a genuinely ``paused`` run (produced by running a real
        interrupt graph through the Worker against live Postgres, not a
        hand-faked status) must resolve to ``cancelled`` immediately --
        never a lease-less ``cancelling`` that no recovery mechanism can
        ever unstick. Also confirms a same-thread queued run is not
        permanently wedged by this fix."""

        from typing import TypedDict

        from fastapi.testclient import TestClient

        from lingxigraph import END, START, StateGraph, interrupt
        from lingxigraph.server import GraphRegistry, create_app
        from lingxigraph.server.repository import PostgresRepository
        from lingxigraph.server.security import Authenticator

        class State(TypedDict):
            value: int

        def make_registry() -> GraphRegistry:
            paused = StateGraph(State, name="interrupt-test", version="1.0.0")

            def approval(_state):
                return {"value": int(interrupt({"question": "new value?"}))}

            paused.add_node("approval", approval)
            paused.add_edge(START, "approval")
            paused.add_edge("approval", END)
            return GraphRegistry({"approval": paused.compile()})

        def wait_for_status(client, run_id, headers, expected):
            value = None
            for _ in range(200):
                value = client.get(f"/v1/runs/{run_id}", headers=headers)
                if value.json()["status"] in expected:
                    return value
                asyncio.run(asyncio.sleep(0.01))
            return value

        repository = PostgresRepository(POSTGRES_URL, schema=self.schema)
        asyncio.run(repository.setup())
        app = create_app(
            registry=make_registry(),
            repository=repository,
            authenticator=Authenticator.insecure_dev(),
            embedded_worker=True,
        )
        headers = {"x-tenant-id": "acme"}
        with TestClient(app) as client:
            assistant = client.post(
                "/v1/assistants",
                headers=headers,
                json={"graph_id": "approval", "name": "approval"},
            ).json()
            thread = client.post("/v1/threads", headers=headers, json={}).json()
            paused = client.post(
                f"/v1/threads/{thread['id']}/runs",
                headers=headers,
                json={"assistant_id": assistant["id"], "input": {"value": 0}},
            ).json()
            paused = wait_for_status(client, paused["id"], headers, {"paused"}).json()
            self.assertEqual(paused["status"], "paused")

            # A second run queued on the same thread -- 'cancelling' counts
            # as ACTIVE, so it must not stay permanently wedged by this fix.
            queued = client.post(
                f"/v1/threads/{thread['id']}/runs",
                headers=headers,
                json={"assistant_id": assistant["id"], "input": {"value": 1}},
            ).json()

            cancelled = client.post(f"/v1/runs/{paused['id']}/cancel", headers=headers)
            self.assertEqual(cancelled.status_code, 200, cancelled.text)
            self.assertEqual(cancelled.json()["status"], "cancelled")
            self.assertIsNone(cancelled.json()["lease_owner"])

            final = asyncio.run(repository.get_run("acme", paused["id"]))
            self.assertEqual(final.status, "cancelled")
            self.assertIsNotNone(final.finished_at)
            self.assertIsNone(final.lease_owner)
            self.assertIsNone(final.lease_expires_at)

            resume_after_cancel = client.post(
                f"/v1/runs/{paused['id']}/resume",
                headers=headers,
                json={"resume": 7},
            )
            self.assertEqual(resume_after_cancel.status_code, 409)

            # The queued sibling run on the same thread eventually runs to
            # completion -- it is not wedged by the now-terminal paused run.
            queued_final = wait_for_status(
                client, queued["id"], headers, {"paused", "succeeded", "failed"}
            ).json()
            self.assertIn(queued_final["status"], {"paused", "succeeded"})

    def test_idempotency_key_replay_safe_across_finalizing_and_terminal_gates(self) -> None:
        """Issue #16 PR #17 review round 7, point 3, PostgreSQL path: a
        same-Idempotency-Key replay must return the existing event, never
        a fresh 409, even after the run has gone finalizing or terminal --
        only a genuinely new key against such a run still gets 409."""

        from lingxigraph.errors import RunFinalizingError, RunTerminalError
        from lingxigraph.server.models import AssistantCreate, RunCreate, ThreadCreate
        from lingxigraph.server.repository import PostgresRepository

        async def scenario() -> None:
            repo = PostgresRepository(POSTGRES_URL, schema=self.schema)
            await repo.setup()
            assistant = await repo.create_assistant(
                "acme", AssistantCreate(graph_id="graph", name="a"), "1.0.0"
            )
            thread = await repo.create_thread("acme", ThreadCreate())

            # --- finalizing gate ---
            run = await repo.create_run(
                "acme", thread.id, assistant, RunCreate(assistant_id=assistant.id, input={})
            )
            claimed = await repo.claim_run("worker-a", lease_seconds=30)
            assert claimed is not None
            original, created = await repo.submit_steering(
                "acme", run.id, kind="user_input", payload={"m": 1}, idempotency_key="key-1"
            )
            self.assertTrue(created)
            gate_closed = await repo.close_steering("acme", run.id, "worker-a", claimed.attempt)
            self.assertTrue(gate_closed)

            replayed, replayed_created = await repo.submit_steering(
                "acme", run.id, kind="user_input", payload={"m": 1}, idempotency_key="key-1"
            )
            self.assertFalse(replayed_created)
            self.assertEqual(replayed.id, original.id)
            self.assertEqual(replayed.sequence, original.sequence)

            with self.assertRaises(RunFinalizingError):
                await repo.submit_steering(
                    "acme", run.id, kind="user_input", payload={"m": 2}, idempotency_key="key-2"
                )

            # --- terminal gate ---
            terminal_run = await repo.create_run(
                "acme", thread.id, assistant, RunCreate(assistant_id=assistant.id, input={})
            )
            terminal_original, terminal_created = await repo.submit_steering(
                "acme",
                terminal_run.id,
                kind="user_input",
                payload={"m": 1},
                idempotency_key="key-3",
            )
            self.assertTrue(terminal_created)
            await repo.finish_run("acme", terminal_run.id, "succeeded", output={})

            terminal_replayed, terminal_replayed_created = await repo.submit_steering(
                "acme",
                terminal_run.id,
                kind="user_input",
                payload={"m": 1},
                idempotency_key="key-3",
            )
            self.assertFalse(terminal_replayed_created)
            self.assertEqual(terminal_replayed.id, terminal_original.id)
            self.assertEqual(terminal_replayed.sequence, terminal_original.sequence)

            with self.assertRaises(RunTerminalError):
                await repo.submit_steering(
                    "acme",
                    terminal_run.id,
                    kind="user_input",
                    payload={"m": 2},
                    idempotency_key="key-4",
                )

        asyncio.run(scenario())

    def test_supersede_pending_steering_if_owned_postgres_path(self) -> None:
        """Issue #16 PR #17 review round 8, point 1 (BLOCKER), PostgreSQL
        path: the fenced ``supersede_pending_steering_if_owned`` must
        durably transition still-pending steering rows to ``superseded``
        and record a matching ``run.steer.superseded`` lifecycle event, and
        must be rejected outright for a worker whose lease has already been
        reclaimed by a new owner."""

        from lingxigraph.server.models import AssistantCreate, RunCreate, ThreadCreate
        from lingxigraph.server.repository import PostgresRepository

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

            claimed_a = await repo.claim_run("worker-a", lease_seconds=1)
            assert claimed_a is not None

            # A's lease expires; B claims the same run at a new attempt.
            await asyncio.sleep(1.2)
            claimed_b = await repo.claim_run("worker-b", lease_seconds=30)
            assert claimed_b is not None
            self.assertEqual(claimed_b.attempt, claimed_a.attempt + 1)

            # A's now-stale attempt must be rejected outright, touching
            # nothing.
            stale_result = await repo.supersede_pending_steering_if_owned(
                "acme", run.id, "worker-a", claimed_a.attempt
            )
            self.assertIsNone(stale_result)
            self.assertEqual(
                [row.id for row in await repo.list_pending_steering("acme", run.id)],
                [event.id],
            )

            # B, the current owner, supersedes it normally.
            stored = await repo.supersede_pending_steering_if_owned(
                "acme", run.id, "worker-b", claimed_b.attempt
            )
            self.assertEqual(len(stored), 1)
            self.assertEqual(stored[0].kind, "run.steer.superseded")
            self.assertEqual(stored[0].data["steering_event_id"], event.id)
            self.assertEqual(stored[0].data["reason"], "unconsumed_at_final_boundary")
            self.assertIsNone(stored[0].data["superseded_by_run_id"])

            self.assertEqual(await repo.list_pending_steering("acme", run.id), [])
            all_steering = await repo.list_steering("acme", run.id)
            self.assertEqual(all_steering[0].status, "superseded")

            # Idempotent under retry: calling again must not duplicate the
            # lifecycle event (nothing left pending to supersede either).
            second = await repo.supersede_pending_steering_if_owned(
                "acme", run.id, "worker-b", claimed_b.attempt
            )
            self.assertEqual(second, [])
            superseded_events = [
                e
                for e in await repo.list_events("acme", run.id)
                if e.kind == "run.steer.superseded"
            ]
            self.assertEqual(len(superseded_events), 1)

        asyncio.run(scenario())

    def test_finalize_run_with_steering_disposition_if_owned_postgres_path(self) -> None:
        """Issue #16 PR #17 review round 9, point 2 (BLOCKER), PostgreSQL
        path: the merged ``finalize_run_with_steering_disposition_if_owned``
        must durably supersede leftover steering AND commit the run's
        terminal status together, and must be rejected outright for a
        worker whose lease has already been reclaimed by a new owner."""

        from lingxigraph.server.models import (
            AssistantCreate,
            RunCreate,
            RunStatus,
            ThreadCreate,
        )
        from lingxigraph.server.repository import PostgresRepository

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

            claimed_a = await repo.claim_run("worker-a", lease_seconds=1)
            assert claimed_a is not None

            # A's lease expires; B claims the same run at a new attempt.
            await asyncio.sleep(1.2)
            claimed_b = await repo.claim_run("worker-b", lease_seconds=30)
            assert claimed_b is not None
            self.assertEqual(claimed_b.attempt, claimed_a.attempt + 1)

            # A's now-stale attempt must be rejected outright, touching
            # neither the steering row nor the run's status.
            stale_result = await repo.finalize_run_with_steering_disposition_if_owned(
                "acme", run.id, "worker-a", claimed_a.attempt, RunStatus.SUCCEEDED, output={}
            )
            self.assertIsNone(stale_result)
            self.assertEqual(
                [row.id for row in await repo.list_pending_steering("acme", run.id)],
                [event.id],
            )
            still_running = await repo.get_run("acme", run.id)
            assert still_running is not None
            self.assertEqual(still_running.status, "running")

            # B, the current owner, finalizes normally: steering disposition
            # and terminal status commit together.
            result = await repo.finalize_run_with_steering_disposition_if_owned(
                "acme", run.id, "worker-b", claimed_b.attempt, RunStatus.SUCCEEDED, output={}
            )
            assert result is not None
            updated, superseded = result
            self.assertEqual(updated.status, "succeeded")
            self.assertEqual(len(superseded), 1)
            self.assertEqual(superseded[0].data["steering_event_id"], event.id)
            self.assertEqual(superseded[0].data["reason"], "unconsumed_at_final_boundary")
            self.assertEqual(superseded[0].data["sequence"], event.sequence)
            self.assertEqual(superseded[0].data["kind"], event.kind)

            final = await repo.get_run("acme", run.id)
            assert final is not None
            self.assertEqual(final.status, "succeeded")
            self.assertEqual(await repo.list_pending_steering("acme", run.id), [])
            all_steering = await repo.list_steering("acme", run.id)
            self.assertEqual(all_steering[0].status, "superseded")

        asyncio.run(scenario())

    def test_finalize_run_with_steering_disposition_rolls_back_atomically_on_failure(
        self,
    ) -> None:
        """Issue #16 PR #17 review round 9, point 2 (BLOCKER) -- the real
        Postgres rollback/takeover regression the review explicitly asked
        for: deliberately fail partway through the merged transaction
        (after the steering row has been superseded in-transaction, but
        before the run's terminal UPDATE and the transaction's COMMIT) and
        assert the WHOLE transaction rolls back -- the steering row is
        still pending/delivered and recoverable by the next owner, and the
        run's status is untouched. This must run against a real PostgreSQL
        connection to prove actual transactional atomicity; an
        InMemoryRepository ordering test cannot demonstrate this."""

        from lingxigraph.server.models import (
            AssistantCreate,
            RunCreate,
            RunStatus,
            ThreadCreate,
        )
        from lingxigraph.server.repository import PostgresRepository

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
            claimed = await repo.claim_run("worker-a", lease_seconds=30)
            assert claimed is not None

            class _InjectedFailure(Exception):
                pass

            real_ensure = repo._ensure_steer_superseded_event_sync

            def failing_ensure(cursor, tenant_id, run_id, steering_event, **kwargs):
                # Let the steering row's own UPDATE (to 'superseded') and
                # the superseded-event insert happen first -- exactly what
                # a real "fails right before the run's terminal UPDATE and
                # COMMIT" crash would leave in-flight -- then blow up
                # before the surrounding ``with self._connect()`` block
                # ever reaches the run status UPDATE or exits normally.
                real_ensure(cursor, tenant_id, run_id, steering_event, **kwargs)
                raise _InjectedFailure("simulated crash before terminal status commit")

            repo._ensure_steer_superseded_event_sync = failing_ensure  # type: ignore[method-assign]

            with self.assertRaises(_InjectedFailure):
                await repo.finalize_run_with_steering_disposition_if_owned(
                    "acme", run.id, "worker-a", claimed.attempt, RunStatus.SUCCEEDED, output={}
                )

            repo._ensure_steer_superseded_event_sync = real_ensure  # type: ignore[method-assign]

            # Everything must have rolled back together: the run is still
            # running (not succeeded), the steering row is still
            # pending/delivered (never left stranded as superseded), and no
            # run.steer.superseded event was recorded.
            still_running = await repo.get_run("acme", run.id)
            assert still_running is not None
            self.assertEqual(still_running.status, "running")
            self.assertEqual(
                [row.id for row in await repo.list_pending_steering("acme", run.id)],
                [event.id],
            )
            superseded_events = [
                e
                for e in await repo.list_events("acme", run.id)
                if e.kind == "run.steer.superseded"
            ]
            self.assertEqual(superseded_events, [])

            # A subsequent owner (the same worker, still holding its lease
            # here) can still finalize normally and recover the row -- it
            # was never lost.
            result = await repo.finalize_run_with_steering_disposition_if_owned(
                "acme", run.id, "worker-a", claimed.attempt, RunStatus.SUCCEEDED, output={}
            )
            assert result is not None
            updated, superseded = result
            self.assertEqual(updated.status, "succeeded")
            self.assertEqual(len(superseded), 1)

        asyncio.run(scenario())

    def test_retry_with_event_if_owned_postgres_path(self) -> None:
        """Issue #16 PR #17 review round 10, point 1 (BLOCKER), PostgreSQL
        path: the merged ``retry_run_with_event_if_owned`` must return the
        transaction's own resulting snapshot directly (no separate later
        ``get_run()`` read), reject a stale attempt outright, and -- the
        review's race B -- durably guarantee that ``worker_retrying``'s
        RunEvent sequence is strictly earlier than any event a subsequent
        owner's next attempt appends, because both writes are the same
        real SQL transaction."""

        from lingxigraph.server.models import (
            AssistantCreate,
            RunCreate,
            ThreadCreate,
        )
        from lingxigraph.server.repository import PostgresRepository

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
            claimed_a = await repo.claim_run("worker-a", lease_seconds=1)
            assert claimed_a is not None

            # A's lease expires; B claims the same run at a new attempt.
            await asyncio.sleep(1.2)
            claimed_b = await repo.claim_run("worker-b", lease_seconds=30)
            assert claimed_b is not None
            self.assertEqual(claimed_b.attempt, claimed_a.attempt + 1)

            # A's now-stale attempt must be rejected outright.
            stale_result = await repo.retry_run_with_event_if_owned(
                "acme",
                run.id,
                "worker-a",
                claimed_a.attempt,
                error={"code": "delivery_retry", "message": "transient"},
                max_attempts=5,
            )
            self.assertIsNone(stale_result)
            still_running = await repo.get_run("acme", run.id)
            assert still_running is not None
            self.assertEqual(still_running.status, "running")

            # B retries normally: status transition and worker_retrying
            # event commit together, and the returned Run is the
            # transaction's own snapshot.
            result = await repo.retry_run_with_event_if_owned(
                "acme",
                run.id,
                "worker-b",
                claimed_b.attempt,
                error={"code": "delivery_retry", "message": "transient"},
                max_attempts=5,
            )
            assert result is not None
            updated, event = result
            self.assertEqual(updated.status, "pending")
            assert event is not None
            self.assertEqual(event.kind, "worker_retrying")

            final = await repo.get_run("acme", run.id)
            assert final is not None
            self.assertEqual(final.status, "pending")

            # A third worker claims the retried run (attempt N+1 relative
            # to B's retry) and appends its own execution event -- proving
            # the earlier worker_retrying event's sequence is strictly
            # earlier, i.e. the durable event stream's causal order is
            # preserved even across a real Postgres transaction boundary.
            claimed_c = await repo.claim_run("worker-c", lease_seconds=30)
            assert claimed_c is not None
            next_event = await repo.append_event(
                "acme", run.id, "node_started", {"attempt": claimed_c.attempt}
            )
            self.assertLess(event.sequence, next_event.sequence)

        asyncio.run(scenario())

    def test_resume_transfer_emits_superseded_event_idempotently_postgres_path(self) -> None:
        """Issue #16 PR #17 review round 8, point 3 (BLOCKER), PostgreSQL
        path: the paused-run resume-transfer migration must record exactly
        one ``run.steer.superseded`` event per transferred steering row, in
        the same transaction as the status transition."""

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
            original, _ = await repo.submit_steering(
                "acme", old_run.id, kind="user_input", payload={"m": 1}
            )
            await repo.finish_run("acme", old_run.id, "paused", output={})

            new_run, transferred = await repo.resume_run_with_pending_steering(
                "acme",
                thread.id,
                assistant,
                RunCreate(assistant_id=assistant.id),
                old_run.id,
            )
            self.assertEqual(len(transferred), 1)

            old_events = await repo.list_events("acme", old_run.id)
            superseded_events = [e for e in old_events if e.kind == "run.steer.superseded"]
            self.assertEqual(len(superseded_events), 1)
            self.assertEqual(superseded_events[0].data["steering_event_id"], original.id)
            self.assertEqual(superseded_events[0].data["reason"], "resume_transfer")
            self.assertEqual(superseded_events[0].data["superseded_by_run_id"], new_run.id)
            self.assertEqual(
                superseded_events[0].data["replacement_steering_event_id"],
                transferred[0].id,
            )

            old_steering = await repo.list_steering("acme", old_run.id)
            self.assertEqual(old_steering[0].status, "superseded")

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

    @unittest.skipUnless(REDIS_URL, "Redis integration URL not configured")
    def test_redis_event_bus_wait_returns_promptly_when_heartbeat_is_cancelled(
        self,
    ) -> None:
        """Issue #16 PR #17 review round 6, point 4 (REQUIRED consistency
        fix): round 5 rewrote ``InMemoryEventBus.wait()`` away from
        ``asyncio.wait_for(...)`` because cancelling the *caller* while it
        is in-flight can hang forever on Python 3.11 -- but the production
        ``RedisEventBus.wait()`` still used that exact pattern, and
        ``Worker._heartbeat`` cancels a Redis-backed heartbeat exactly the
        same way as an in-memory one. Reproduce that cancellation directly
        against ``RedisEventBus.wait()`` (mirroring how ``Worker._execute``
        does ``heartbeat.cancel(); await asyncio.gather(heartbeat, ...)``)
        and assert it unblocks promptly instead of hanging.
        """

        from lingxigraph.server.eventbus import RedisEventBus

        async def scenario() -> None:
            prefix = "lingxigraph-test-" + uuid4().hex
            bus = RedisEventBus(REDIS_URL, prefix=prefix)
            try:
                # A long timeout: if cancellation does not propagate
                # promptly, this task hangs for (approximately) this long
                # instead of returning almost immediately below.
                waiter = asyncio.create_task(bus.wait("tenant", "run", timeout=30))
                await asyncio.sleep(0.1)
                waiter.cancel()
                start = asyncio.get_event_loop().time()
                with self.assertRaises(asyncio.CancelledError):
                    await asyncio.wait_for(waiter, timeout=2)
                elapsed = asyncio.get_event_loop().time() - start
                self.assertLess(elapsed, 2.0)
            finally:
                await bus.close()

        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()
