"""Durable mid-run steering inbox (issue #16)."""

import os
import re
from importlib.resources import files

from alembic import op

revision = "0002_steering"
down_revision = "0001_lingxigraph_v1"
branch_labels = None
depends_on = None


def upgrade() -> None:
    schema = os.getenv("LINGXIGRAPH_POSTGRES_SCHEMA", "lingxigraph")
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", schema):
        raise ValueError("invalid LINGXIGRAPH_POSTGRES_SCHEMA")
    sql = (
        files("lingxigraph.server")
        .joinpath("migrations/0002_steering.sql")
        .read_text(encoding="utf-8")
        .replace("{{schema}}", schema)
    )
    op.execute(sql)


def downgrade() -> None:
    raise RuntimeError("LingxiGraph v1 production migrations are forward-only")
