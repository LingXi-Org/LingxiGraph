import asyncio
import json
import unittest
from typing import Annotated, Any, TypedDict

import httpx

from lingxigraph import (
    END,
    START,
    AIMessage,
    Command,
    HumanMessage,
    InMemorySaver,
    StateGraph,
    ToolCall,
    ToolMessage,
    add_messages,
    tool,
)
from lingxigraph.integrations.coze import (
    AsyncCozeClient,
    CozeAgentNode,
    CozeChatModel,
    CozeWorkflowNode,
    _message_to_coze,
    file_object,
    image_object,
)
from lingxigraph.integrations.openai_compat import OpenAICompatChatModel


class IntegrationV2Tests(unittest.TestCase):
    def test_coze_sse_chat_model(self) -> None:
        async def handler(request: httpx.Request) -> httpx.Response:
            self.assertEqual(request.headers["authorization"], "Bearer token")
            self.assertEqual(request.url.path, "/v3/chat")
            body = (
                'event: conversation.chat.created\n'
                'data: {"id":"chat-1","conversation_id":"conv-1"}\n\n'
                'event: conversation.message.delta\n'
                'data: {"id":"msg-1","content":"你"}\n\n'
                'event: conversation.message.delta\n'
                'data: {"id":"msg-1","content":"好"}\n\n'
                'event: done\n'
                'data: [DONE]\n\n'
            )
            return httpx.Response(200, text=body, headers={"content-type": "text/event-stream"})

        async def run() -> None:
            client = AsyncCozeClient("token", transport=httpx.MockTransport(handler))
            model = CozeChatModel("bot", client=client, user_id="user")
            result = await model.agenerate([HumanMessage("问候")])
            self.assertEqual(result.content, "你好")
            chunks = [chunk async for chunk in model.astream([HumanMessage("问候")])]
            self.assertEqual("".join(chunk.content for chunk in chunks), "你好")
            await client.aclose()

        asyncio.run(run())

    def test_openai_compatible_tool_roundtrip_and_stream(self) -> None:
        requests = []

        async def handler(request: httpx.Request) -> httpx.Response:
            payload = json.loads(request.content)
            requests.append(payload)
            if payload.get("stream"):
                body = (
                    'data: {"id":"c1","model":"test","choices":[{"delta":{"content":"O"}}]}\n\n'
                    'data: {"id":"c1","model":"test","choices":[{"delta":{"content":"K"}}]}\n\n'
                    'data: [DONE]\n\n'
                )
                return httpx.Response(200, text=body, headers={"content-type": "text/event-stream"})
            return httpx.Response(
                200,
                json={
                    "model": "test",
                    "choices": [
                        {
                            "finish_reason": "tool_calls",
                            "message": {
                                "content": None,
                                "tool_calls": [
                                    {
                                        "id": "call-1",
                                        "function": {"name": "lookup", "arguments": '{"q":"x"}'},
                                    }
                                ],
                            },
                        }
                    ],
                },
            )

        @tool
        def lookup(q: str) -> str:
            """Look up a value."""
            return q

        async def run() -> None:
            model = OpenAICompatChatModel(
                "test",
                base_url="https://example.test/v1",
                api_key="key",
                transport=httpx.MockTransport(handler),
            )
            response = await model.agenerate([HumanMessage("x")], tools=[lookup])
            self.assertEqual(response.tool_calls[0].args, {"q": "x"})
            chunks = [chunk async for chunk in model.astream([HumanMessage("x")])]
            self.assertEqual("".join(chunk.content for chunk in chunks), "OK")
            self.assertEqual(requests[0]["tools"][0]["function"]["name"], "lookup")
            await model.aclose()

        asyncio.run(run())

    def test_coze_client_endpoint_surface(self) -> None:
        paths = []

        async def handler(request: httpx.Request) -> httpx.Response:
            paths.append(request.url.path)
            if request.url.path in {"/v1/workflow/stream_run", "/v1/workflow/stream_resume"}:
                return httpx.Response(
                    200,
                    text='event: message\ndata: {"result":"ok"}\n\n',
                    headers={"content-type": "text/event-stream"},
                )
            if request.url.path == "/v3/chat/message/list":
                return httpx.Response(200, json={"code": 0, "data": [{"content": "ok"}]})
            return httpx.Response(200, json={"code": 0, "data": {"id": "id", "status": "completed"}})

        async def run() -> None:
            client = AsyncCozeClient(token_provider=lambda: "dynamic", transport=httpx.MockTransport(handler))
            await client.chat("bot", "user", stream=False)
            await client.chat_retrieve("conv", "chat")
            self.assertEqual((await client.chat_messages("conv", "chat"))[0]["content"], "ok")
            await client.submit_tool_outputs("conv", "chat", [{"tool_call_id": "x", "output": "y"}])
            await client.cancel_chat("conv", "chat")
            await client.create_conversation()
            await client.workflow_run("workflow", {"x": 1})
            self.assertEqual([item async for item in client.workflow_stream("workflow", {})][0]["data"]["result"], "ok")
            resumed = [
                item
                async for item in client.workflow_stream_resume("workflow", "event", 2, "answer")
            ]
            self.assertEqual(resumed[0]["data"]["result"], "ok")
            await client.aclose()

        asyncio.run(run())
        self.assertEqual(len(paths), 9)

    def test_coze_agent_local_tool_and_workflow_interrupt(self) -> None:
        class FakeClient:
            async def chat_stream(self, *args, **kwargs):
                del args, kwargs
                yield {
                    "event": "conversation.chat.created",
                    "data": {"id": "chat", "conversation_id": "conv"},
                }
                yield {
                    "event": "conversation.chat.requires_action",
                    "data": {
                        "id": "chat",
                        "conversation_id": "conv",
                        "required_action": {
                            "submit_tool_outputs": {
                                "tool_calls": [
                                    {
                                        "id": "call",
                                        "function": {
                                            "name": "lookup",
                                            "arguments": '{"q":"x"}',
                                        },
                                    }
                                ]
                            }
                        },
                    },
                }

            async def submit_tool_outputs(self, *args):
                self.outputs = args[-1]
                return {"status": "completed"}

            async def chat_messages(self, *args):
                del args
                return [{"role": "assistant", "content": "tool complete"}]

            async def workflow_stream(self, *args, **kwargs):
                del args, kwargs
                yield {
                    "event": "interrupt",
                    "data": {"event_id": "event", "interrupt_type": 2, "message": "answer?"},
                }

            async def workflow_stream_resume(self, *args):
                self.resume = args
                yield {"event": "done", "data": {"result": "resumed"}}

        @tool
        def lookup(q: str) -> str:
            """Look up a query."""
            return f"found:{q}"

        class AgentState(TypedDict):
            messages: Annotated[list[Any], add_messages]
            coze_conversations: dict[str, str]

        fake = FakeClient()
        node = CozeAgentNode("bot", client=fake, user_id="user", tools=[lookup])
        builder = StateGraph(AgentState)
        builder.add_node("coze", node).add_edge(START, "coze").add_edge("coze", END)
        result = builder.compile().invoke(
            {"messages": [HumanMessage("go")], "coze_conversations": {}}
        )
        self.assertEqual(result["messages"][-1].content, "tool complete")
        self.assertEqual(result["coze_conversations"], {"bot": "conv"})
        self.assertEqual(fake.outputs[0]["output"], "found:x")

        class WorkflowState(TypedDict):
            workflow_output: Any

        workflow = CozeWorkflowNode("workflow", client=fake, parameters={}, output_key="workflow_output")
        workflow_builder = StateGraph(WorkflowState)
        workflow_builder.add_node("workflow", workflow)
        workflow_builder.add_edge(START, "workflow").add_edge("workflow", END)
        config = {"configurable": {"thread_id": "workflow"}}
        graph = workflow_builder.compile(checkpointer=InMemorySaver())
        paused = graph.invoke({"workflow_output": None}, config)
        marker = paused["__interrupt__"][0].value
        resumed = graph.invoke(
            Command(
                resume={
                    "event_id": marker["event_id"],
                    "interrupt_type": marker["interrupt_type"],
                    "resume_data": "yes",
                }
            ),
            config,
        )
        self.assertEqual(resumed["workflow_output"], {"result": "resumed"})

    def test_coze_chat_model_continues_tool_submission(self) -> None:
        class FakeClient:
            async def submit_tool_outputs(self, conversation_id, chat_id, outputs):
                self.submitted = (conversation_id, chat_id, outputs)
                return {"status": "completed"}

            async def chat_messages(self, conversation_id, chat_id):
                return [{"role": "assistant", "content": "continued"}]

        async def run() -> None:
            client = FakeClient()
            model = CozeChatModel("bot", client=client, user_id="user")
            previous = AIMessage(
                "",
                tool_calls=(ToolCall("lookup", {"q": "x"}, "call"),),
                response_metadata={"conversation_id": "conv", "chat_id": "chat"},
            )
            result = await model.agenerate(
                [previous, ToolMessage("found", tool_call_id="call")]
            )
            self.assertEqual(result.content, "continued")
            self.assertEqual(client.submitted[0:2], ("conv", "chat"))

        asyncio.run(run())


class CozeCompleteFeatureTests(unittest.TestCase):
    def test_file_upload_multipart_and_object_string(self) -> None:
        captured: dict[str, Any] = {}

        async def handler(request: httpx.Request) -> httpx.Response:
            captured["path"] = request.url.path
            captured["content_type"] = request.headers.get("content-type", "")
            captured["body"] = request.content
            return httpx.Response(200, json={"code": 0, "data": {"id": "file-123"}})

        async def run() -> None:
            client = AsyncCozeClient("token", transport=httpx.MockTransport(handler))
            result = await client.upload_file(
                b"hello", filename="a.txt", content_type="text/plain"
            )
            self.assertEqual(result["id"], "file-123")
            await client.aclose()

        asyncio.run(run())
        self.assertEqual(captured["path"], "/v1/files/upload")
        self.assertTrue(captured["content_type"].startswith("multipart/form-data"))
        self.assertIn(b"hello", captured["body"])

        # An object_string message carries text plus file/image references.
        message = HumanMessage(
            "look at these",
            additional_kwargs={"objects": [file_object("f1"), image_object("i1")]},
        )
        encoded = _message_to_coze(message)
        self.assertEqual(encoded["content_type"], "object_string")
        items = json.loads(encoded["content"])
        self.assertEqual(items[0], {"type": "text", "text": "look at these"})
        self.assertEqual(items[1], {"type": "file", "file_id": "f1"})
        self.assertEqual(items[2], {"type": "image", "file_id": "i1"})

    def test_reasoning_and_follow_up_streaming(self) -> None:
        body = (
            "event: conversation.chat.created\n"
            'data: {"id":"chat-1","conversation_id":"conv-1"}\n\n'
            "event: conversation.message.delta\n"
            'data: {"id":"m1","reasoning_content":"let me think"}\n\n'
            "event: conversation.message.delta\n"
            'data: {"id":"m1","content":"answer"}\n\n'
            "event: conversation.message.completed\n"
            'data: {"id":"m2","type":"follow_up","content":"want more?"}\n\n'
            "event: done\ndata: [DONE]\n\n"
        )

        async def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200, text=body, headers={"content-type": "text/event-stream"}
            )

        async def run() -> None:
            client = AsyncCozeClient("token", transport=httpx.MockTransport(handler))
            model = CozeChatModel("bot", client=client, user_id="user")
            # agenerate splits reasoning, content, and follow-ups.
            result = await model.agenerate([HumanMessage("hi")])
            self.assertEqual(result.content, "answer")
            self.assertEqual(result.additional_kwargs["reasoning_content"], "let me think")
            self.assertEqual(result.additional_kwargs["follow_ups"], ("want more?",))
            self.assertEqual(result.response_metadata["follow_ups"], ("want more?",))
            # astream tags reasoning chunks so the UI can route them separately.
            chunks = [chunk async for chunk in model.astream([HumanMessage("hi")])]
            reasoning = [c.content for c in chunks if c.additional_kwargs.get("reasoning")]
            answer = [c.content for c in chunks if not c.additional_kwargs.get("reasoning")]
            self.assertEqual("".join(reasoning), "let me think")
            self.assertEqual("".join(answer), "answer")
            await client.aclose()

        asyncio.run(run())

    def test_agent_node_surfaces_reasoning_and_follow_ups(self) -> None:
        class FakeClient:
            async def chat_stream(self, *args, **kwargs):
                del args, kwargs
                yield {
                    "event": "conversation.chat.created",
                    "data": {"id": "chat", "conversation_id": "conv"},
                }
                yield {
                    "event": "conversation.message.delta",
                    "data": {"id": "chat", "reasoning_content": "thinking..."},
                }
                yield {
                    "event": "conversation.message.delta",
                    "data": {"id": "chat", "content": "done"},
                }
                yield {
                    "event": "conversation.message.completed",
                    "data": {"type": "follow_up", "content": "next?"},
                }

        class AgentState(TypedDict):
            messages: Annotated[list[Any], add_messages]
            coze_conversations: dict[str, str]
            coze_suggestions: tuple[str, ...]

        node = CozeAgentNode(
            "bot", client=FakeClient(), user_id="user", suggestions_key="coze_suggestions"
        )
        builder = StateGraph(AgentState)
        builder.add_node("coze", node).add_edge(START, "coze").add_edge("coze", END)
        result = builder.compile().invoke(
            {
                "messages": [HumanMessage("go")],
                "coze_conversations": {},
                "coze_suggestions": (),
            }
        )
        final = result["messages"][-1]
        self.assertEqual(final.content, "done")
        self.assertEqual(final.additional_kwargs["reasoning_content"], "thinking...")
        self.assertEqual(final.additional_kwargs["follow_ups"], ("next?",))
        self.assertEqual(result["coze_suggestions"], ("next?",))


if __name__ == "__main__":
    unittest.main()
