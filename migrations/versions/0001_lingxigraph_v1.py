"""LingxiGraph v1 control plane, queue, state and tenant isolation."""

import os
import re
from importlib.resources import files

from alembic import op

revision = "0001_lingxigraph_v1"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    schema = os.getenv("LINGXIGRAPH_POSTGRES_SCHEMA", "lingxigraph")
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", schema):
        raise ValueError("invalid LINGXIGRAPH_POSTGRES_SCHEMA")
    sql = (
        files("lingxigraph.server")
        .joinpath("migrations/0001_v1.sql")
        .read_text(encoding="utf-8")
        .replace("{{schema}}", schema)
    )
    op.execute(sql)


def downgrade() -> None:
    raise RuntimeError("LingxiGraph v1 production migrations are forward-only")
