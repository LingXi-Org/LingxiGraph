"""Direct unit tests for lingxigraph.tools pure-function edge branches."""

import asyncio
import unittest

from lingxigraph import AIMessage, Command, InvalidUpdateError, ToolCall, tool
from lingxigraph.tools import ToolNode, _make_spec, validate_json_schema


def add(a: int, b: int = 1) -> int:
    """Add two numbers."""
    return a + b


def var_args_tool(a: int, *args: int, **kwargs: int) -> int:
    """A tool with *args/**kwargs, which must be skipped from the schema."""
    return a


class ToolDecoratorEdgeCaseTests(unittest.TestCase):
    def test_tool_decorator_with_explicit_string_name_positional(self) -> None:
        spec = tool("custom_name")(add)
        self.assertEqual(spec.name, "custom_name")

    def test_make_spec_skips_var_positional_and_var_keyword_parameters(self) -> None:
        spec = _make_spec(
            var_args_tool, name=None, return_direct=False
        )
        self.assertEqual(set(spec.parameters["properties"]), {"a"})


class ValidateJsonSchemaEdgeCaseTests(unittest.TestCase):
    def test_enum_violation_raises(self) -> None:
        with self.assertRaises(ValueError):
            validate_json_schema("z", {"enum": ["a", "b"]})

    def test_array_items_are_validated_recursively(self) -> None:
        schema = {"type": "array", "items": {"type": "integer"}}
        with self.assertRaises(ValueError):
            validate_json_schema([1, "not-an-int"], schema)
        validate_json_schema([1, 2, 3], schema)  # must not raise

    def test_object_missing_required_argument_raises(self) -> None:
        schema = {"type": "object", "properties": {"x": {"type": "integer"}}, "required": ["x"]}
        with self.assertRaises(ValueError):
            validate_json_schema({}, schema)

    def test_object_rejects_unknown_argument_when_additional_properties_false(self) -> None:
        schema = {
            "type": "object",
            "properties": {"x": {"type": "integer"}},
            "additionalProperties": False,
        }
        with self.assertRaises(ValueError):
            validate_json_schema({"x": 1, "y": 2}, schema)


class ToolNodeConstructionEdgeCaseTests(unittest.TestCase):
    def test_duplicate_tool_names_are_rejected(self) -> None:
        spec_a = tool("dup")(add)
        spec_b = tool("dup")(add)
        with self.assertRaises(ValueError):
            ToolNode([spec_a, spec_b])

    def test_non_positive_read_only_settings_are_rejected(self) -> None:
        spec = tool(add)
        with self.assertRaises(ValueError):
            ToolNode([spec], read_only_concurrency=0)
        with self.assertRaises(ValueError):
            ToolNode([spec], read_only_batch_size=0)

    def test_call_without_a_trailing_ai_message_raises(self) -> None:
        node = ToolNode([tool(add)])

        async def scenario() -> None:
            with self.assertRaises(InvalidUpdateError):
                await node({"messages": []})

        asyncio.run(scenario())

    def test_command_returning_tool_must_be_the_only_call(self) -> None:
        def make_command() -> Command:
            return Command(goto="next")

        node = ToolNode([tool(add), tool(make_command)])

        async def scenario() -> None:
            message = AIMessage(
                content="",
                tool_calls=[
                    ToolCall(id="1", name="add", args={"a": 1, "b": 2}),
                    ToolCall(id="2", name="make_command", args={}),
                ],
            )
            with self.assertRaises(InvalidUpdateError):
                await node({"messages": [message]})

        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()
