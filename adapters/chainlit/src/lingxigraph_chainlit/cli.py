"""Console entry point forwarding options to Chainlit's supported CLI."""

from __future__ import annotations

import sys
from pathlib import Path


def main() -> None:
    from chainlit.cli import cli

    target = Path(__file__).with_name("host.py")
    cli.main(
        args=["run", str(target), *sys.argv[1:]],
        prog_name="lingxigraph-chainlit",
        standalone_mode=True,
    )


__all__ = ["main"]
