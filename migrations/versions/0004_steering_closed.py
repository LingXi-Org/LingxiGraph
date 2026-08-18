"""Atomic finalization gate for steering admission (issue #16 PR #17
review round 6, point 3, BLOCKER).

Adds ``runs.steering_closed`` -- see
``src/lingxigraph/server/migrations/0004_steering_closed.sql`` for the
full rationale.
"""

import os
import re
from importlib.resources import files

from alembic import op

revision = "0004_steering_closed"
down_revision = "0003_steering_source_event"
branch_labels = None
depends_on = None


def upgrade() -> None:
    schema = os.getenv("LINGXIGRAPH_POSTGRES_SCHEMA", "lingxigraph")
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", schema):
        raise ValueError("invalid LINGXIGRAPH_POSTGRES_SCHEMA")
    sql = (
        files("lingxigraph.server")
        .joinpath("migrations/0004_steering_closed.sql")
        .read_text(encoding="utf-8")
        .replace("{{schema}}", schema)
    )
    op.execute(sql)


def downgrade() -> None:
    raise RuntimeError("LingxiGraph v1 production migrations are forward-only")
