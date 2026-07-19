"""v2 delivery: graph explanation (xray), scaffolding, CLI and Studio API."""

from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from typing import TypedDict

from fastapi.testclient import TestClient

from lingxigraph import END, START, StateGraph
from lingxigraph.cli import build_parser
from lingxigraph.examples.multi_agent_graph import graph as multi_agent_graph
from lingxigraph.scaffold import package_name, render, scaffold
from lingxigraph.server import GraphRegistry, create_app

DEV_HEADERS = {"x-tenant-id": "local", "x-roles": "viewer,developer,operator"}


class State(TypedDict):
    x: int


def _inner_node(state: State) -> dict:
    return {"x": state["x"] + 1}


def _outer_node(state: State) -> dict:
    return {"x": state["x"] * 2}


def _graph_with_subgraph():
    inner = StateGraph(State, name="inner")
    inner.add_node("step", _inner_node)
    inner.add_edge(START, "step").add_edge("step", END)
    sub = inner.compile()

    outer = StateGraph(State, name="outer")
    outer.add_node("double", _outer_node)
    outer.add_node("nested", sub)
    outer.add_edge(START, "double").add_edge("double", "nested").add_edge("nested", END)
    return outer.compile()


class XrayExplanationTest(unittest.TestCase):
    def test_flat_get_graph_has_debug_metadata(self) -> None:
        graph = _graph_with_subgraph()
        info = graph.get_graph()
        node = next(n for n in info.nodes if n.id == "double")
        self.assertEqual(node.kind, "node")
        self.assertEqual(node.debug["callable"], "_outer_node")
        self.assertFalse(node.debug["uses_runtime"])

    def test_xray_expands_subgraph(self) -> None:
        graph = _graph_with_subgraph()
        flat = graph.get_graph()
        nested = next(n for n in flat.nodes if n.id == "nested")
        self.assertTrue(nested.is_subgraph)
        self.assertIsNone(nested.subgraph)  # not expanded without xray

        info = graph.get_graph(xray=True)
        nested = next(n for n in info.nodes if n.id == "nested")
        self.assertEqual(nested.kind, "subgraph")
        self.assertIsNotNone(nested.subgraph)
        inner_ids = {n.id for n in nested.subgraph.nodes}
        self.assertIn("step", inner_ids)

    def test_xray_mermaid_nests_subgraph(self) -> None:
        graph = _graph_with_subgraph()
        mermaid = graph.draw_mermaid(xray=True)
        self.assertIn("subgraph", mermaid)
        self.assertIn("nested", mermaid)


class MultiAgentExampleTest(unittest.TestCase):
    def test_runs_parallel_agents_and_subgraph(self) -> None:
        result = multi_agent_graph.invoke(
            {"request": "topic", "sources": [], "brief": "", "findings": [], "report": ""}
        )
        self.assertEqual(len(result["sources"]), 3)  # from research subgraph
        self.assertEqual(len(result["findings"]), 2)  # analyst + critic merged
        self.assertIn("analyst", result["report"])
        self.assertIn("critic", result["report"])

    def test_xray_exposes_research_subgraph(self) -> None:
        info = multi_agent_graph.get_graph(xray=True)
        research = next(n for n in info.nodes if n.id == "research")
        self.assertTrue(research.is_subgraph)
        self.assertIsNotNone(research.subgraph)


class ScaffoldTest(unittest.TestCase):
    def test_package_name_slug(self) -> None:
        self.assertEqual(package_name("My Agent"), "my_agent")
        self.assertEqual(package_name("my-agent"), "my_agent")
        self.assertEqual(package_name("123bot"), "agent_123bot")
        with self.assertRaises(ValueError):
            package_name("---")

    def test_render_contains_expected_files(self) -> None:
        files = render("my-agent")
        self.assertIn("my_agent/graph.py", files)
        self.assertIn("lingxigraph.json", files)
        self.assertIn("docker-compose.yml", files)
        self.assertIn("Dockerfile", files)
        self.assertIn("my_agent.graph:graph", files["lingxigraph.json"])

    def test_scaffold_writes_and_refuses_overwrite(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            dest = Path(tmp) / "proj"
            written = scaffold("my-agent", dest)
            self.assertTrue((dest / "my_agent" / "graph.py").exists())
            self.assertGreater(len(written), 5)
            with self.assertRaises(FileExistsError):
                scaffold("my-agent", dest)
            scaffold("my-agent", dest, force=True)  # force succeeds


class CliParserTest(unittest.TestCase):
    def test_new_dev_build_up_registered(self) -> None:
        parser = build_parser()
        for command in ("server", "worker", "migrate", "doctor", "new", "dev", "build", "up"):
            args = parser.parse_args([command] if command != "new" else ["new", "x"])
            self.assertEqual(args.command, command)

    def test_dev_defaults(self) -> None:
        args = build_parser().parse_args(["dev"])
        self.assertEqual(args.port, 8124)
        self.assertEqual(args.host, "127.0.0.1")


class StructureEndpointTest(unittest.TestCase):
    def setUp(self) -> None:
        os.environ["LINGXIGRAPH_INSECURE_DEV_AUTH"] = "true"
        registry = GraphRegistry()
        registry.register("demo", _graph_with_subgraph())
        self.client = TestClient(create_app(registry=registry, embedded_worker=True))

    def test_structure_returns_debug_and_mermaid(self) -> None:
        response = self.client.get(
            "/v1/graphs/demo/structure?xray=true", headers=DEV_HEADERS
        )
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertIn("graph", body)
        self.assertIn("mermaid", body)
        nested = next(n for n in body["nodes"] if n["id"] == "nested")
        self.assertTrue(nested["is_subgraph"])
        self.assertIsNotNone(nested["subgraph"])

    def test_studio_is_served(self) -> None:
        response = self.client.get("/studio/", headers=DEV_HEADERS)
        self.assertEqual(response.status_code, 200)
        self.assertIn("LingxiGraph Studio", response.text)


if __name__ == "__main__":
    unittest.main()
