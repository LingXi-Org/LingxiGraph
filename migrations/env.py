from __future__ import annotations

import os
from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool

config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)
dsn = os.environ.get("LINGXIGRAPH_POSTGRES_URL")
if dsn:
    config.set_main_option("sqlalchemy.url", dsn.replace("%", "%%"))


def _use_psycopg_driver(url: str) -> str:
    """Force the psycopg (v3) driver, matching the project's declared dependency.

    Plain ``postgresql://`` DSNs make SQLAlchemy default to the psycopg2
    dialect, which is not installed anywhere in this project (the ``postgres``
    extra pins ``psycopg[binary]``, i.e. psycopg 3). Callers -- including
    tests that set ``sqlalchemy.url`` directly via ``Config.set_main_option``
    -- may pass a plain ``postgresql://`` or ``postgres://`` DSN, so normalize
    the scheme here rather than relying on every caller to do so.
    """
    if url.startswith("postgresql+"):
        return url
    if url.startswith("postgresql://"):
        return "postgresql+psycopg://" + url[len("postgresql://") :]
    if url.startswith("postgres://"):
        return "postgresql+psycopg://" + url[len("postgres://") :]
    return url


def run_migrations_offline() -> None:
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=_use_psycopg_driver(url) if url else url,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    section = dict(config.get_section(config.config_ini_section, {}))
    url = section.get("sqlalchemy.url")
    if url:
        section["sqlalchemy.url"] = _use_psycopg_driver(url)
    connectable = engine_from_config(
        section,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(connection=connection)
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
