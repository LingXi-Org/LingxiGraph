"""Packaged Chainlit target configured entirely through environment variables."""

from .adapter import install_chainlit
from .loader import load_graph
from .settings import AdapterSettings

settings = AdapterSettings.from_env()
adapter = install_chainlit(
    load_graph(settings.graph_spec),
    sqlite_path=settings.sqlite_path,
    context=settings.context,
    observability=settings.observability,
)

__all__ = ["adapter"]

