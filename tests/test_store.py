import unittest
from typing import TypedDict

from lingxigraph import END, START, InMemorySaver, InMemoryStore, StateGraph, get_store


class State(TypedDict, total=False):
    name: str
    recalled: str


class InMemoryStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.store = InMemoryStore()

    def test_put_get_roundtrip_and_timestamps(self) -> None:
        self.store.put(("users", "u1"), "profile", {"name": "Ada"})
        item = self.store.get(("users", "u1"), "profile")
        assert item is not None
        self.assertEqual(item.value, {"name": "Ada"})
        self.assertEqual(item.namespace, ("users", "u1"))
        created = item.created_at
        self.store.put(("users", "u1"), "profile", {"name": "Ada Lovelace"})
        updated = self.store.get(("users", "u1"), "profile")
        assert updated is not None
        self.assertEqual(updated.created_at, created)
        self.assertGreaterEqual(updated.updated_at, created)

    def test_get_missing_returns_none_and_delete_is_idempotent(self) -> None:
        self.assertIsNone(self.store.get(("users",), "ghost"))
        self.store.put(("users",), "a", {"x": 1})
        self.store.delete(("users",), "a")
        self.store.delete(("users",), "a")
        self.assertIsNone(self.store.get(("users",), "a"))
        self.assertEqual(self.store.list_namespaces(), [])

    def test_search_by_prefix_filter_and_query(self) -> None:
        self.store.put(("users", "u1", "memories"), "m1", {"topic": "food", "text": "likes pizza"})
        self.store.put(("users", "u1", "memories"), "m2", {"topic": "music", "text": "plays piano"})
        self.store.put(("users", "u2", "memories"), "m3", {"topic": "food", "text": "likes sushi"})

        everything = self.store.search(("users",), limit=10)
        self.assertEqual(len(everything), 3)

        u1_only = self.store.search(("users", "u1"))
        self.assertEqual({item.key for item in u1_only}, {"m1", "m2"})

        food = self.store.search(("users",), filter={"topic": "food"})
        self.assertEqual({item.key for item in food}, {"m1", "m3"})

        pizza = self.store.search(("users",), query="pizza")
        self.assertEqual([item.key for item in pizza], ["m1"])

        paged = self.store.search(("users",), limit=1, offset=1)
        self.assertEqual(len(paged), 1)

    def test_list_namespaces_with_prefix(self) -> None:
        self.store.put(("a", "x"), "k", {"v": 1})
        self.store.put(("a", "y"), "k", {"v": 2})
        self.store.put(("b",), "k", {"v": 3})
        self.assertEqual(self.store.list_namespaces(prefix=("a",)), [("a", "x"), ("a", "y")])

    def test_stored_values_are_isolated_copies(self) -> None:
        value = {"tags": ["one"]}
        self.store.put(("ns",), "k", value)
        value["tags"].append("two")
        item = self.store.get(("ns",), "k")
        assert item is not None
        self.assertEqual(item.value["tags"], ["one"])


class StoreInGraphTests(unittest.TestCase):
    def test_nodes_share_memories_across_threads(self) -> None:
        store = InMemoryStore()
        saver = InMemorySaver()

        def remember(state):
            get_store().put(("profiles",), "user", {"name": state["name"]})
            return {}

        def recall(state):
            item = get_store().get(("profiles",), "user")
            return {"recalled": item.value["name"] if item else "unknown"}

        writer = StateGraph(State).add_node("remember", remember)
        writer.add_edge(START, "remember").add_edge("remember", END)
        reader = StateGraph(State).add_node("recall", recall)
        reader.add_edge(START, "recall").add_edge("recall", END)

        writer.compile(checkpointer=saver, store=store).invoke(
            {"name": "Grace"}, {"configurable": {"thread_id": "thread-1"}}
        )
        result = reader.compile(checkpointer=saver, store=store).invoke(
            {}, {"configurable": {"thread_id": "thread-2"}}
        )
        self.assertEqual(result["recalled"], "Grace")

    def test_get_store_without_configuration_fails(self) -> None:
        builder = StateGraph(State).add_node("bad", lambda state: get_store().get(("x",), "y"))
        builder.add_edge(START, "bad").add_edge("bad", END)
        with self.assertRaisesRegex(RuntimeError, "no store is configured"):
            builder.compile().invoke({})

    def test_get_store_outside_node_fails(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "graph node is executing"):
            get_store()


if __name__ == "__main__":
    unittest.main()
