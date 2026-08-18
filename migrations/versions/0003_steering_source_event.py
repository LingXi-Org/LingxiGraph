"""Stable source_event_id for paused-run steering resume (issue #16).

Issue #16 PR #17 review round 3, point 1: the SQL file
``src/lingxigraph/server/migrations/0003_steering_source_event.sql`` shipped
without a matching Alembic revision, so ``alembic upgrade head`` stayed at
``0002_steering`` and never applied it -- even though
``PostgresRepository._setup_sync()`` (which scans the SQL files directly,
not through Alembic) did apply it, letting CI's real-Postgres integration
job pass while a deployment driven by ``alembic upgrade head`` would not.
This revision closes that gap.
"""

import os
import re
from importlib.resources import files

from alembic import op

revision = "0003_steering_source_event"
down_revision = "0002_steering"
branch_labels = None
depends_on = None


def upgrade() -> None:
    schema = os.getenv("LINGXIGRAPH_POSTGRES_SCHEMA", "lingxigraph")
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", schema):
        raise ValueError("invalid LINGXIGRAPH_POSTGRES_SCHEMA")
    sql = (
        files("lingxigraph.server")
        .joinpath("migrations/0003_steering_source_event.sql")
        .read_text(encoding="utf-8")
        .replace("{{schema}}", schema)
    )
    op.execute(sql)


def downgrade() -> None:
    raise RuntimeError("LingxiGraph v1 production migrations are forward-only")
