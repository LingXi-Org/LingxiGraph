"""Project scaffolding for ``lingxigraph new``.

Generates a minimal, immediately runnable multi-agent project: a trusted graph
module, a manifest, a Docker Compose single-server deployment, and the developer
entry points wired to ``lingxigraph dev`` / ``build`` / ``up``.
"""

from __future__ import annotations

import re
from pathlib import Path

_SLUG = re.compile(r"[^a-z0-9_]+")


def package_name(project: str) -> str:
    """Turn a project name into an importable Python package identifier."""

    slug = _SLUG.sub("_", project.strip().lower()).strip("_")
    if not slug:
        raise ValueError("project name must contain alphanumeric characters")
    if slug[0].isdigit():
        slug = f"agent_{slug}"
    return slug


def render(project: str) -> dict[str, str]:
    """Return a mapping of relative file paths to their rendered contents."""

    pkg = package_name(project)
    return {
        f"{pkg}/__init__.py": _INIT.format(pkg=pkg),
        f"{pkg}/graph.py": _GRAPH.format(pkg=pkg, project=project),
        "lingxigraph.json": _MANIFEST.format(pkg=pkg),
        "docker-compose.yml": _COMPOSE.format(project=project),
        "Dockerfile": _DOCKERFILE.format(pkg=pkg),
        "pyproject.toml": _PYPROJECT.format(pkg=pkg, project=project),
        "requirements.txt": _REQUIREMENTS,
        ".dockerignore": _DOCKERIGNORE,
        ".gitignore": _GITIGNORE,
        "README.md": _README.format(project=project, pkg=pkg),
    }


def scaffold(project: str, destination: Path, *, force: bool = False) -> list[Path]:
    """Materialize a new project under ``destination``.

    Returns the list of files written. Raises ``FileExistsError`` if any target
    file already exists and ``force`` is not set.
    """

    files = render(project)
    written: list[Path] = []
    for relative in files:
        target = destination / relative
        if target.exists() and not force:
            raise FileExistsError(str(target))
    for relative, content in files.items():
        target = destination / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8")
        written.append(target)
    return written


_INIT = '''"""Trusted graphs published with this deployment."""

from .graph import graph

__all__ = ["graph"]
'''

_GRAPH = '''"""The {project} agent graph.

This module is imported by the LingxiGraph Worker from ``lingxigraph.json``.
Only code shipped with the image is trusted; nothing is uploaded at runtime.
"""

from __future__ import annotations

from typing import TypedDict

from lingxigraph import END, START, Runtime, StateGraph


class State(TypedDict):
    request: str
    result: str


class Context(TypedDict, total=False):
    tenant: str


def respond(state: State, runtime: Runtime[Context]) -> dict[str, str]:
    tenant = (runtime.context or {{}}).get("tenant", "local")
    runtime.emit("progress", {{"stage": "respond", "tenant": tenant}})
    return {{"result": f"[{{tenant}}] handled: {{state['request']}}"}}


builder = StateGraph(State, context_schema=Context, name="{pkg}", version="1.0.0")
builder.add_node("respond", respond, timeout=30)
builder.add_edge(START, "respond")
builder.add_edge("respond", END)

graph = builder.compile()
'''

_MANIFEST = '''{{
  "$schema": "https://lingxigraph.dev/schemas/manifest-v1.json",
  "graphs": {{
    "{pkg}": {{
      "path": "{pkg}.graph:graph",
      "version": "1.0.0"
    }}
  }}
}}
'''

_COMPOSE = '''name: {project}

x-runtime: &runtime
  build:
    context: .
  restart: unless-stopped
  environment: &environment
    LINGXIGRAPH_POSTGRES_URL: postgresql://lingxigraph:lingxigraph@postgres:5432/lingxigraph
    LINGXIGRAPH_REDIS_URL: redis://redis:6379/0
    LINGXIGRAPH_INSECURE_DEV_AUTH: "true"
  depends_on:
    postgres:
      condition: service_healthy
    redis:
      condition: service_healthy
  security_opt:
    - no-new-privileges:true

services:
  postgres:
    image: postgres:16-alpine
    restart: unless-stopped
    environment:
      POSTGRES_DB: lingxigraph
      POSTGRES_USER: lingxigraph
      POSTGRES_PASSWORD: lingxigraph
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U lingxigraph -d lingxigraph"]
      interval: 5s
      timeout: 3s
      retries: 20
    volumes:
      - postgres-data:/var/lib/postgresql/data

  redis:
    image: redis:7.2-alpine
    restart: unless-stopped
    command: ["redis-server", "--appendonly", "yes"]
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 3s
      retries: 20
    volumes:
      - redis-data:/data

  migrate:
    <<: *runtime
    restart: "no"
    command: ["migrate"]

  api:
    <<: *runtime
    command: ["server", "--host", "0.0.0.0", "--port", "8124", "--embedded-worker"]
    ports:
      - "8124:8124"
    depends_on:
      migrate:
        condition: service_completed_successfully
      postgres:
        condition: service_healthy
      redis:
        condition: service_healthy

volumes:
  postgres-data:
  redis-data:
'''

_DOCKERFILE = '''FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
RUN pip install --no-cache-dir .

RUN useradd --uid 10001 --create-home app
USER 10001

ENTRYPOINT ["lingxigraph"]
CMD ["server", "--host", "0.0.0.0", "--port", "8124", "--embedded-worker"]
'''

_PYPROJECT = '''[build-system]
requires = ["setuptools>=68"]
build-backend = "setuptools.build_meta"

[project]
name = "{pkg}"
version = "1.0.0"
description = "{project} — a LingxiGraph multi-agent deployment"
requires-python = ">=3.11"
dependencies = ["lingxigraph[all]>=2.0.0"]

[tool.setuptools]
packages = ["{pkg}"]
'''

_REQUIREMENTS = "lingxigraph[all]>=2.0.0\n"

_DOCKERIGNORE = ".git\n.venv\n__pycache__\n*.pyc\ndist\nbuild\n"

_GITIGNORE = "__pycache__/\n*.pyc\n.venv/\ndist/\nbuild/\n.env\n"

_README = '''# {project}

A [LingxiGraph](https://lingxigraph.dev) multi-agent deployment.

## Develop

```bash
lingxigraph dev
```

Runs an in-memory Agent Server with an embedded Worker and opens the Studio at
http://localhost:8124/studio — no PostgreSQL or Redis required.

## Deploy (Docker Compose, single server)

```bash
lingxigraph up
```

Brings up PostgreSQL, Redis, migrations and the Agent Server (with embedded
Worker) on http://localhost:8124. Studio is served at `/studio`.

## Build the image

```bash
lingxigraph build
```

## Layout

- `{pkg}/graph.py` — your trusted agent graph.
- `lingxigraph.json` — the manifest the Worker imports at deploy time.
- `docker-compose.yml` — single-server production topology.
'''


__all__ = ["package_name", "render", "scaffold"]
