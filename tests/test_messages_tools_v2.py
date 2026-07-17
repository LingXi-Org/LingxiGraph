import asyncio
import unittest
from typing import Literal

from lingxigraph import (
    END,
    REMOVE_ALL_MESSAGES,
    AIMessage,
    AIMessageChunk,
    Command,
    HumanMessage,
    RemoveMessage,
    SystemMessage,
    ToolCall,
    ToolCallChunk,
    ToolMessage,
    add_messages,
    merge_chunks,
    tool,
)
from lingxigraph.messages import message_from_dict
from lingxigraph.tools import ToolNode, tools_condition


class MessagesAndToolsV2Tests(unittest.TestCase):
    def test_message_coercion_deletion_and_chunk_merge(self) -> None:
        values = [
            message_from_dict({"role": "system", "content": "s"}),
            message_from_dict({"role": "user", "content": "u"}),
            message_from_dict(
                {
                    "role": "assistant",
                    "content": "",
                    "tool_calls": [
                        {
                            "id": "c",
                            "function": {"name": "find", "arguments": {"q": "x"}},
                        }
                    ],
                }
            ),
            message_from_dict(
                {"role": "tool", "content": "done", "tool_call_id": "c"}
            ),
            message_from_dict({"type": "ai_chunk", "content": "x"}),
        ]
        self.assertIsInstance(values[0], SystemMessage)
        self.assertIsInstance(values[1], HumanMessage)
        self.assertIsInstance(values[2], AIMessage)
        self.assertIsInstance(values[3], ToolMessage)
        self.assertIsInstance(values[4], AIMessageChunk)
        with self.assertRaises(ValueError):
            message_from_dict({"type": "alien", "content": "x"})

        kept = add_messages("hello", AIMessage("answer", id="a"))
        self.assertEqual(len(add_messages(kept, RemoveMessage("a"))), 1)
        self.assertEqual(add_messages(kept, RemoveMessage(REMOVE_ALL_MESSAGES)), [])
        combined = merge_chunks(
            [
                AIMessageChunk(
                    "hel",
                    tool_call_chunks=(ToolCallChunk("find", '{"q":', "c", 0),),
                ),
                AIMessageChunk(
                    "lo",
                    tool_call_chunks=(ToolCallChunk(args='"x"}', index=0),),
                ),
            ]
        )
        self.assertEqual(combined.content, "hello")
        self.assertEqual(combined.tool_calls[0].args, {"q": "x"})

    def test_tool_schema_errors_commands_and_router(self) -> None:
        @tool(name="choose")
        def choose(value: Literal["a", "b"] = "a") -> str:
            """Choose a value.

            More details are deliberately excluded from the short description.
            """
            return value

        @tool
        def explode() -> str:
            """Raise an error."""
            raise RuntimeError("boom")

        self.assertEqual(choose.parameters["properties"]["value"]["enum"], ["a", "b"])
        self.assertEqual(choose.description, "Choose a value.")

        async def run() -> None:
            node = ToolNode([choose, explode])
            result = await node(
                {
                    "messages": [
                        AIMessage(
                            "",
                            tool_calls=(
                                ToolCall("choose", {"value": "b"}, "one"),
                                ToolCall("explode", {}, "two"),
                                ToolCall("missing", {}, "three"),
                            ),
                        )
                    ]
                }
            )
            messages = result["messages"]
            self.assertEqual(messages[0].content, "b")
            self.assertEqual(messages[1].status, "error")
            self.assertEqual(messages[2].status, "error")

            async def handoff():
                return Command(goto=END)

            command = await ToolNode([tool(handoff)])(
                {"messages": [AIMessage("", tool_calls=(ToolCall("handoff", {}, "h"),))]}
            )
            self.assertIsInstance(command, Command)

        asyncio.run(run())
        self.assertEqual(tools_condition({"messages": [AIMessage("done")]}), END)
        self.assertEqual(
            tools_condition({"messages": [AIMessage("", tool_calls=(ToolCall("x"),))]}),
            "tools",
        )


if __name__ == "__main__":
    unittest.main()
