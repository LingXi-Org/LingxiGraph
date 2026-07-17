"""Structural checks for the core/platform dependency boundary."""

import ast
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src" / "lingxigraph"
OPTIONAL_INTEGRATION_MODULES = {
    "cache_redis.py",
    "cli.py",
    "observability.py",
    "sdk.py",
    "checkpoint/postgres.py",
    "store/postgres.py",
}
OPTIONAL_INTEGRATION_PACKAGES = {"integrations", "protocols", "server"}


class ArchitectureBoundaryTests(unittest.TestCase):
    def test_third_party_imports_are_isolated_from_the_embedded_core(self) -> None:
        """The embedded graph runtime remains importable without platform extras.

        v1 intentionally ships REST, Postgres, Redis, OTel, A2A and MCP adapters in
        the same distribution.  Their third-party imports must remain inside the
        explicit integration boundary so that ``pip install lingxigraph`` still
        provides a dependency-free embedded SDK.
        """
        violations: list[str] = []
        for path in SRC.rglob("*.py"):
            relative = path.relative_to(SRC).as_posix()
            if (
                relative in OPTIONAL_INTEGRATION_MODULES
                or relative.split("/", 1)[0] in OPTIONAL_INTEGRATION_PACKAGES
            ):
                continue
            tree = ast.parse(path.read_text(encoding="utf-8"), str(path))
            for node in ast.walk(tree):
                names: list[str] = []
                if isinstance(node, ast.Import):
                    names = [alias.name for alias in node.names]
                elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
                    names = [node.module]
                for name in names:
                    root_name = name.split(".", 1)[0]
                    if root_name not in sys.stdlib_module_names and root_name != "lingxigraph":
                        violations.append(f"{path.relative_to(ROOT)}: {name}")
        self.assertEqual(violations, [])

if __name__ == "__main__":
    unittest.main()
