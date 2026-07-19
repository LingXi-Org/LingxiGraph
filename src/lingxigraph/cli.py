"""Production command line entry points."""

from __future__ import annotations

import argparse
import asyncio
import inspect
import os
import signal
import sys
from pathlib import Path
from typing import Any


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="lingxigraph")
    parser.add_argument("--version", action="store_true")
    commands = parser.add_subparsers(dest="command")

    server = commands.add_parser("server", help="run Agent Server")
    server.add_argument("--host", default="0.0.0.0")
    server.add_argument("--port", type=int, default=8124)
    server.add_argument("--manifest", default="lingxigraph.json")
    server.add_argument("--embedded-worker", action="store_true")

    worker = commands.add_parser("worker", help="run queue worker")
    worker.add_argument("--manifest", default="lingxigraph.json")
    worker.add_argument("--health-host", default="0.0.0.0")
    worker.add_argument("--health-port", type=int, default=8125)
    worker.add_argument("--drain-timeout", type=float, default=60.0)

    commands.add_parser("migrate", help="create or upgrade PostgreSQL schema")
    commands.add_parser("doctor", help="validate runtime configuration")

    new = commands.add_parser("new", help="scaffold a new agent project")
    new.add_argument("name", help="project name, e.g. my-agent")
    new.add_argument("--dir", default=None, help="target directory (default: ./<name>)")
    new.add_argument("--force", action="store_true", help="overwrite existing files")

    dev = commands.add_parser(
        "dev", help="run a local dev server (in-memory, embedded worker, Studio)"
    )
    dev.add_argument("--host", default="127.0.0.1")
    dev.add_argument("--port", type=int, default=8124)
    dev.add_argument("--manifest", default="lingxigraph.json")
    dev.add_argument("--reload", action="store_true", help="restart on source changes")
    dev.add_argument("--no-open", action="store_true", help="do not open a browser")

    build = commands.add_parser("build", help="build the deployment container image")
    build.add_argument("--tag", default=None, help="image tag (default: <project>:latest)")
    build.add_argument("--wheel", action="store_true", help="build a Python wheel instead")

    up = commands.add_parser("up", help="start the Docker Compose single-server stack")
    up.add_argument("--file", default="docker-compose.yml")
    up.add_argument("--detach", action="store_true", default=True)
    up.add_argument("--foreground", action="store_true", help="stream logs (no -d)")
    up.add_argument("--no-build", action="store_true", help="skip image build")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.version:
        from . import __version__

        print(__version__)
        return 0
    if args.command == "server":
        return _server(args)
    if args.command == "worker":
        return asyncio.run(_worker(args))
    if args.command == "migrate":
        return asyncio.run(_migrate())
    if args.command == "doctor":
        return _doctor()
    if args.command == "new":
        return _new(args)
    if args.command == "dev":
        return _dev(args)
    if args.command == "build":
        return _build(args)
    if args.command == "up":
        return _up(args)
    parser.print_help()
    return 2


def _server(args) -> int:
    _configure_observability()
    try:
        import uvicorn
    except ImportError as exc:
        raise RuntimeError("install lingxigraph[server] to run Agent Server") from exc
    from .server import GraphRegistry, create_app

    registry = GraphRegistry.from_manifest(args.manifest)
    postgres_url = os.getenv("LINGXIGRAPH_POSTGRES_URL")
    store_factory: Any = None
    if postgres_url:
        from .checkpoint.postgres import PostgresSaver
        from .server import PostgresRepository
        from .store.postgres import PostgresStore

        repository = PostgresRepository(postgres_url)
        asyncio.run(repository.setup())
        checkpointer = PostgresSaver(postgres_url)
        checkpointer.setup()

        def store_factory(tenant):
            return PostgresStore(postgres_url, tenant_id=tenant)
    else:
        repository = None
        checkpointer = None
    event_bus = None
    cache = None
    redis_url = os.getenv("LINGXIGRAPH_REDIS_URL")
    if redis_url:
        from .cache_redis import RedisCache
        from .server.eventbus import RedisEventBus

        event_bus = RedisEventBus(redis_url)
        cache = RedisCache(redis_url)
    app = create_app(
        registry=registry,
        repository=repository,
        checkpointer=checkpointer,
        store_factory=store_factory,
        event_bus=event_bus,
        cache=cache,
        embedded_worker=args.embedded_worker,
    )
    uvicorn.run(app, host=args.host, port=args.port)
    return 0


def _new(args) -> int:
    from .scaffold import package_name, scaffold

    try:
        pkg = package_name(args.name)
    except ValueError as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        return 1
    destination = Path(args.dir) if args.dir else Path.cwd() / args.name
    try:
        written = scaffold(args.name, destination, force=args.force)
    except FileExistsError as exc:
        print(f"FAIL: {exc} already exists (use --force to overwrite)", file=sys.stderr)
        return 1
    print(f"Created {len(written)} files in {destination}")
    print(f"  package: {pkg}")
    print("\nNext steps:")
    rel = destination if args.dir else args.name
    print(f"  cd {rel}")
    print("  pip install -e .")
    print("  lingxigraph dev")
    return 0


def _dev(args) -> int:
    """Run a zero-dependency local server: in-memory stores, embedded worker, Studio."""

    _configure_observability()
    try:
        import uvicorn
    except ImportError as exc:
        raise RuntimeError("install lingxigraph[server] to run the dev server") from exc
    from .server import GraphRegistry, create_app

    os.environ.setdefault("LINGXIGRAPH_INSECURE_DEV_AUTH", "true")
    manifest = Path(args.manifest)
    if not manifest.exists():
        print(f"FAIL: {manifest} not found — run `lingxigraph new` first", file=sys.stderr)
        return 1
    registry = GraphRegistry.from_manifest(manifest)
    url = f"http://{args.host}:{args.port}/studio/"
    print("LingxiGraph dev server (in-memory, embedded worker)")
    print(f"  graphs:  {len(registry.list())}")
    print(f"  studio:  {url}")
    print(f"  api:     http://{args.host}:{args.port}/v1")
    if not args.no_open:
        _open_browser(url)

    if args.reload:
        # uvicorn's reloader needs an import string; expose a factory via env.
        os.environ["LINGXIGRAPH_DEV_MANIFEST"] = str(manifest)
        uvicorn.run(
            "lingxigraph.cli:_dev_app",
            factory=True,
            host=args.host,
            port=args.port,
            reload=True,
        )
        return 0
    app = create_app(registry=registry, embedded_worker=True)
    uvicorn.run(app, host=args.host, port=args.port)
    return 0


def _dev_app():
    """Factory used by uvicorn --reload; reads the manifest from the environment."""

    from .server import GraphRegistry, create_app

    manifest = os.environ.get("LINGXIGRAPH_DEV_MANIFEST", "lingxigraph.json")
    registry = GraphRegistry.from_manifest(manifest)
    return create_app(registry=registry, embedded_worker=True)


def _build(args) -> int:
    import subprocess

    project = Path.cwd().name
    if args.wheel:
        print("Building Python wheel…")
        return subprocess.call([sys.executable, "-m", "build", "--wheel"])
    tag = args.tag or f"{project}:latest"
    if not Path("Dockerfile").exists():
        print("FAIL: no Dockerfile in the current directory", file=sys.stderr)
        return 1
    print(f"Building image {tag}…")
    return subprocess.call(["docker", "build", "-t", tag, "."])


def _up(args) -> int:
    import subprocess

    compose_file = Path(args.file)
    if not compose_file.exists():
        print(f"FAIL: {compose_file} not found", file=sys.stderr)
        return 1
    command = ["docker", "compose", "-f", str(compose_file), "up"]
    if not args.no_build:
        command.append("--build")
    if not args.foreground:
        command.append("-d")
    print(f"Starting stack from {compose_file}…")
    code = subprocess.call(command)
    if code == 0 and not args.foreground:
        print("\nStack is starting. Studio: http://localhost:8124/studio/")
    return code


def _open_browser(url: str) -> None:
    import threading
    import webbrowser

    def _open() -> None:
        import time

        time.sleep(1.0)
        try:
            webbrowser.open(url)
        except Exception:
            pass

    threading.Thread(target=_open, daemon=True).start()


async def _worker(args) -> int:
    _configure_observability()
    from .checkpoint.postgres import PostgresSaver
    from .server import GraphRegistry, PostgresRepository, Worker
    from .store.postgres import PostgresStore

    dsn = _required("LINGXIGRAPH_POSTGRES_URL")
    registry = GraphRegistry.from_manifest(args.manifest)
    repository = PostgresRepository(dsn)
    await repository.setup()
    saver = PostgresSaver(dsn)
    saver.setup()
    bus = None
    cache = None
    if os.getenv("LINGXIGRAPH_REDIS_URL"):
        from .cache_redis import RedisCache
        from .server.eventbus import RedisEventBus

        bus = RedisEventBus(os.environ["LINGXIGRAPH_REDIS_URL"])
        cache = RedisCache(os.environ["LINGXIGRAPH_REDIS_URL"])
    worker = Worker(
        registry,
        repository,
        checkpointer=saver,
        store_factory=lambda tenant: PostgresStore(dsn, tenant_id=tenant),
        cache=cache,
        event_bus=bus,
    )
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(signum, worker.stop)
        except (NotImplementedError, RuntimeError):
            pass
    health_server = await asyncio.start_server(
        lambda reader, writer: _worker_health(reader, writer, worker),
        args.health_host,
        args.health_port,
    )
    try:
        await worker.run_forever()
    except (KeyboardInterrupt, asyncio.CancelledError):
        worker.stop()
    finally:
        drained = worker.drain(timeout=args.drain_timeout)
        if inspect.isawaitable(drained):
            await drained
        health_server.close()
        await health_server.wait_closed()
    return 0


async def _worker_health(reader, writer, worker) -> None:
    try:
        first = await asyncio.wait_for(reader.readline(), timeout=2.0)
        path = first.decode("ascii", "ignore").split(" ")[1] if b" " in first else "/health"
        healthy = worker.live if path == "/health" else worker.ready
        status = "200 OK" if healthy else "503 Service Unavailable"
        body = b'{"status":"ok"}' if healthy else b'{"status":"unavailable"}'
        writer.write(
            f"HTTP/1.1 {status}\r\nContent-Type: application/json\r\n"
            f"Content-Length: {len(body)}\r\nConnection: close\r\n\r\n".encode("ascii")
            + body
        )
        await writer.drain()
    finally:
        writer.close()
        await writer.wait_closed()


async def _migrate() -> int:
    from .checkpoint.postgres import PostgresSaver
    from .server import PostgresRepository
    from .store.postgres import PostgresStore

    dsn = _required("LINGXIGRAPH_POSTGRES_URL")
    repository = PostgresRepository(dsn)
    await repository.setup()
    PostgresSaver(dsn).setup()
    PostgresStore(dsn, tenant_id="migration").setup()
    print("LingxiGraph schema is up to date")
    return 0


def _doctor() -> int:
    failures: list[str] = []
    manifest = Path("lingxigraph.json")
    if not manifest.exists():
        failures.append("lingxigraph.json is missing")
    else:
        try:
            from .server import GraphRegistry

            graphs = GraphRegistry.from_manifest(manifest).list()
            print(f"graphs: {len(graphs)}")
        except Exception as exc:
            failures.append(f"manifest: {exc}")
    if not os.getenv("LINGXIGRAPH_POSTGRES_URL"):
        failures.append("LINGXIGRAPH_POSTGRES_URL is not set")
    if not (
        os.getenv("LINGXIGRAPH_OIDC_ISSUER")
        or os.getenv("LINGXIGRAPH_DEV_API_KEY")
        or os.getenv("LINGXIGRAPH_INSECURE_DEV_AUTH", "false").lower() == "true"
    ):
        failures.append("no OIDC or development authentication is configured")
    if failures:
        for failure in failures:
            print(f"FAIL: {failure}", file=sys.stderr)
        return 1
    print("configuration: ok")
    return 0


def _required(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"{name} is required")
    return value


def _configure_observability() -> None:
    from .observability import configure_logging, configure_telemetry

    configure_logging()
    if os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT"):
        configure_telemetry()


if __name__ == "__main__":
    raise SystemExit(main())
