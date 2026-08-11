import asyncio
import json
import os
import tempfile
import unittest
from pathlib import Path

import httpx
from skills_ref.validator import validate as reference_validate

from lingxigraph import (
    AIMessage,
    AIMessageChunk,
    BudgetExceededError,
    FilesystemSkillSource,
    HumanMessage,
    InMemorySaver,
    SkillRegistry,
    SkillResourceError,
    SkillValidationError,
    ToolCall,
    ToolCallChunk,
    ToolMessage,
    create_agent,
    create_react_agent,
    tool,
    validate_skill,
)
from lingxigraph.integrations.openai_compat import OpenAICompatChatModel


def write_skill(
    root: Path,
    name: str = "hello",
    *,
    filename: str = "SKILL.md",
    description: str = "Greets people. Use when the user asks for a greeting.",
    body: str = "# Hello\n\nRead references/greetings.md when a localized greeting is needed.",
    extra: str = "",
) -> Path:
    skill_dir = root / name
    skill_dir.mkdir(parents=True)
    (skill_dir / filename).write_text(
        f"---\nname: {name}\ndescription: {description}\n{extra}---\n\n{body}\n",
        encoding="utf-8",
    )
    return skill_dir


class SkillParsingTests(unittest.TestCase):
    def test_parses_standard_frontmatter_and_block_scalars(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            skill_dir = Path(directory) / "café"
            skill_dir.mkdir()
            (skill_dir / "SKILL.md").write_text(
                """---
name: café
description: >
  Reviews café menus and
  suggests a greeting.
license: 'MIT'
compatibility: "Python 3.11+"
allowed-tools: Read Bash(git:*)
metadata:
  author: LingXi
  version: 1.0
---

# Café

Instructions.
""",
                encoding="utf-8",
            )
            self.assertEqual(validate_skill(skill_dir), ())
            spec = FilesystemSkillSource(skill_dir).load("café")
            self.assertEqual(spec.description, "Reviews café menus and suggests a greeting.")
            self.assertEqual(spec.allowed_tools, "Read Bash(git:*)")
            self.assertEqual(spec.extra_metadata, {"author": "LingXi", "version": "1.0"})
            self.assertIn("# Café", spec.body)
            self.assertTrue(spec.content.startswith("---\n"))

    def test_standard_fixture_passes_official_reference_validator(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            skill_dir = write_skill(
                root,
                extra=(
                    "license: MIT\n"
                    "compatibility: Python 3.11+\n"
                    "allowed-tools: Read Bash(git:*)\n"
                    "metadata:\n"
                    "  author: LingXi\n"
                    "  version: 2.1.0\n"
                ),
            )
            self.assertEqual(reference_validate(skill_dir), [])
            self.assertEqual(validate_skill(skill_dir), ())

    def test_lowercase_filename_is_accepted_but_uppercase_is_preferred(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            skill_dir = write_skill(root, filename="skill.md")
            self.assertEqual(FilesystemSkillSource(root).discover()[0].name, "hello")
            if os.path.normcase("SKILL.md") != os.path.normcase("skill.md"):
                (skill_dir / "SKILL.md").write_text(
                    "---\nname: hello\ndescription: Uppercase wins.\n---\nUppercase body",
                    encoding="utf-8",
                )
                self.assertEqual(
                    FilesystemSkillSource(root).load("hello").description,
                    "Uppercase wins.",
                )

    def test_validation_reports_all_relevant_errors(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            skill_dir = write_skill(
                root,
                "Wrong_Name",
                description="x" * 1025,
                extra="unknown: value\ncompatibility: ' '\n",
            )
            issues = validate_skill(skill_dir)
            codes = {issue.code for issue in issues}
            self.assertIn("unexpected-fields", codes)
            self.assertIn("invalid-name", codes)
            self.assertIn("invalid-description", codes)
            self.assertIn("invalid-compatibility", codes)

    def test_rejects_unsafe_or_complex_yaml(self) -> None:
        cases = {
            "missing": "# no frontmatter",
            "unclosed": "---\nname: unclosed\ndescription: Broken",
            "flow": "---\nname: flow\ndescription: [not, a, string]\n---\nBody",
            "anchor": "---\nname: anchor\ndescription: &value unsafe\n---\nBody",
            "duplicate": "---\nname: duplicate\nname: duplicate\ndescription: x\n---\nBody",
        }
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name, content in cases.items():
                skill_dir = root / name
                skill_dir.mkdir()
                (skill_dir / "SKILL.md").write_text(content, encoding="utf-8")
                self.assertTrue(validate_skill(skill_dir), name)

    def test_discovery_is_sorted_and_duplicates_fail(self) -> None:
        with tempfile.TemporaryDirectory() as first, tempfile.TemporaryDirectory() as second:
            first_root = Path(first)
            second_root = Path(second)
            write_skill(first_root, "zeta")
            write_skill(first_root, "alpha")
            write_skill(second_root, "alpha")
            source = FilesystemSkillSource(first_root)
            self.assertEqual([item.name for item in source.discover()], ["alpha", "zeta"])
            with self.assertRaises(SkillValidationError) as raised:
                SkillRegistry((source, FilesystemSkillSource(second_root)))
            self.assertEqual(raised.exception.issues[0].code, "duplicate-skill")

    def test_missing_root_and_size_limit_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with self.assertRaises(SkillValidationError):
                FilesystemSkillSource(root / "missing").discover()
            skill_dir = write_skill(root)
            source = FilesystemSkillSource(skill_dir, max_skill_bytes=8)
            with self.assertRaises(SkillValidationError):
                source.discover()


class FilesystemSkillSecurityTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.skill_dir = write_skill(self.root)
        (self.skill_dir / "references").mkdir()
        (self.skill_dir / "scripts").mkdir()
        (self.skill_dir / "assets").mkdir()
        (self.skill_dir / "references" / "greetings.md").write_text(
            "Hello / 你好", encoding="utf-8"
        )
        (self.skill_dir / "scripts" / "hello.py").write_text("print('hello')", encoding="utf-8")
        (self.skill_dir / "assets" / "pixel.bin").write_bytes(b"\x00\xff\x01")
        self.registry = SkillRegistry((FilesystemSkillSource(self.root),))

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_reads_text_script_and_binary_without_execution(self) -> None:
        self.assertEqual(
            self.registry.read_resource("hello", "references/greetings.md").content,
            "Hello / 你好".encode(),
        )
        self.assertEqual(
            self.registry.read_resource("hello", "scripts/hello.py").content,
            b"print('hello')",
        )
        resource_tool = self.registry.tool_specs()[1]
        result = resource_tool.func("hello", "assets/pixel.bin")
        envelope = json.loads(result)
        self.assertEqual(envelope["encoding"], "base64")
        self.assertEqual(envelope["content"], "AP8B")
        self.assertEqual(envelope["size"], 3)

    def test_rejects_traversal_absolute_and_unapproved_directories(self) -> None:
        attempts = [
            "../secret.txt",
            "references/../SKILL.md",
            "references/./greetings.md",
            "references//greetings.md",
            "/etc/passwd",
            r"C:\Windows\win.ini",
            r"\\server\share\file",
            "SKILL.md",
            "references/file:stream",
            "references/\x00secret",
        ]
        for path in attempts:
            with self.subTest(path=path), self.assertRaises(SkillResourceError):
                self.registry.read_resource("hello", path)

    def test_rejects_directories_and_oversized_resources(self) -> None:
        with self.assertRaises(SkillResourceError):
            self.registry.read_resource("hello", "references")
        small = SkillRegistry((FilesystemSkillSource(self.root, max_resource_bytes=2),))
        with self.assertRaises(SkillResourceError):
            small.read_resource("hello", "assets/pixel.bin")

    def test_rejects_symlink_escape(self) -> None:
        outside = self.root / "outside.txt"
        outside.write_text("secret", encoding="utf-8")
        link = self.skill_dir / "references" / "escape.md"
        try:
            link.symlink_to(outside)
        except OSError:
            self.skipTest("symlink creation is unavailable on this platform")
        with self.assertRaises(SkillResourceError):
            self.registry.read_resource("hello", "references/escape.md")


class SkillAgentIntegrationTests(unittest.TestCase):
    def test_progressive_disclosure_and_dynamic_resource_loading(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            skill_dir = write_skill(root, body="# Secret\n\nBODY_SENTINEL")
            (skill_dir / "references").mkdir()
            (skill_dir / "references" / "details.md").write_text(
                "RESOURCE_SENTINEL", encoding="utf-8"
            )

            class Model:
                def __init__(self) -> None:
                    self.calls = 0
                    self.seen_tools = []

                async def agenerate(self, messages, *, tools=None, **kwargs):
                    del kwargs
                    self.calls += 1
                    self.seen_tools.append(tuple(item.name for item in tools or ()))
                    combined = "\n".join(str(item.content) for item in messages)
                    if self.calls == 1:
                        self.first_prompt = combined
                        return AIMessage(
                            "",
                            tool_calls=(ToolCall("read_skill", {"skill_name": "hello"}, "s1"),),
                        )
                    if self.calls == 2:
                        self.second_prompt = combined
                        return AIMessage(
                            "",
                            tool_calls=(
                                ToolCall(
                                    "read_skill_resource",
                                    {"skill_name": "hello", "path": "references/details.md"},
                                    "s2",
                                ),
                            ),
                        )
                    self.third_prompt = combined
                    return AIMessage("finished")

            model = Model()
            graph = create_agent(model, skills=root, system_prompt="SYSTEM_SENTINEL")
            result = graph.invoke({"messages": [HumanMessage("Say hello")]})
            self.assertEqual(result["messages"][-1].content, "finished")
            self.assertIn("<name>hello</name>", model.first_prompt)
            self.assertIn("SYSTEM_SENTINEL", model.first_prompt)
            self.assertNotIn("BODY_SENTINEL", model.first_prompt)
            self.assertNotIn("RESOURCE_SENTINEL", model.first_prompt)
            self.assertIn("BODY_SENTINEL", model.second_prompt)
            self.assertIn("RESOURCE_SENTINEL", model.third_prompt)
            self.assertEqual(model.seen_tools[0], ("read_skill", "read_skill_resource"))

    def test_streaming_agent_uses_normal_tool_calling(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_skill(root)

            class StreamingModel:
                def __init__(self) -> None:
                    self.calls = 0

                async def astream(self, messages, *, tools=None, **kwargs):
                    del messages, tools, kwargs
                    self.calls += 1
                    if self.calls == 1:
                        yield AIMessageChunk(
                            "",
                            tool_call_chunks=(
                                ToolCallChunk(
                                    name="read_skill",
                                    args='{"skill_name":"hello"}',
                                    id="stream-skill",
                                    index=0,
                                ),
                            ),
                        )
                    else:
                        yield AIMessageChunk("streamed result")

                async def agenerate(self, messages, *, tools=None, **kwargs):
                    raise AssertionError("streaming path expected")

            graph = create_agent(StreamingModel(), skills=FilesystemSkillSource(root))
            result = graph.invoke({"messages": [HumanMessage("hello")]})
            self.assertEqual(result["messages"][-1].content, "streamed result")
            self.assertTrue(
                any(isinstance(message, ToolMessage) for message in result["messages"])
            )

    def test_allowed_tools_does_not_grant_runtime_permission(self) -> None:
        calls: list[str] = []

        @tool(permissions=("network",))
        def sensitive(value: str) -> str:
            """Perform a protected action."""

            calls.append(value)
            return value

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_skill(root, extra="allowed-tools: sensitive\n")

            class Model:
                def __init__(self) -> None:
                    self.calls = 0

                async def agenerate(self, messages, *, tools=None, **kwargs):
                    del messages, tools, kwargs
                    self.calls += 1
                    if self.calls == 1:
                        return AIMessage(
                            "",
                            tool_calls=(ToolCall("sensitive", {"value": "x"}, "protected"),),
                        )
                    return AIMessage("done")

            result = create_agent(Model(), [sensitive], skills=root).invoke(
                {"messages": [HumanMessage("run")]}
            )
            protected = next(
                message
                for message in result["messages"]
                if isinstance(message, ToolMessage) and message.name == "sensitive"
            )
            self.assertEqual(protected.status, "error")
            self.assertIn("PermissionError", protected.content)
            self.assertEqual(calls, [])

    def test_skill_tools_obey_hitl_authorizer_and_budget(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_skill(root)

            class Model:
                def __init__(self) -> None:
                    self.calls = 0

                async def agenerate(self, messages, *, tools=None, **kwargs):
                    del messages, tools, kwargs
                    self.calls += 1
                    if self.calls > 1:
                        return AIMessage("done")
                    return AIMessage(
                        "",
                        tool_calls=(ToolCall("read_skill", {"skill_name": "hello"}, "r1"),),
                    )

            denied = create_agent(
                Model(),
                skills=root,
                tool_authorize=lambda spec, call, runtime: spec.name != "read_skill",
            ).invoke({"messages": [HumanMessage("hello")]}, {"max_tool_calls": 1})
            self.assertTrue(
                any(
                    isinstance(item, ToolMessage)
                    and item.status == "error"
                    and "denied by policy" in item.content
                    for item in denied["messages"]
                )
            )

            approval = create_agent(
                Model(),
                skills=root,
                interrupt_on=["read_skill"],
                checkpointer=InMemorySaver(),
            )
            paused = approval.invoke(
                {"messages": [HumanMessage("hello")]},
                {"configurable": {"thread_id": "skill-approval"}},
            )
            self.assertIn("__interrupt__", paused)

            class BudgetModel:
                async def agenerate(self, messages, *, tools=None, **kwargs):
                    del messages, tools, kwargs
                    return AIMessage(
                        "",
                        tool_calls=(
                            ToolCall("read_skill", {"skill_name": "hello"}, "b1"),
                            ToolCall("read_skill", {"skill_name": "hello"}, "b2"),
                        ),
                    )

            with self.assertRaises(BudgetExceededError):
                create_agent(BudgetModel(), skills=root).invoke(
                    {"messages": [HumanMessage("hello")]}, {"max_tool_calls": 1}
                )

    def test_openai_compatible_models_receive_standard_tool_schemas(self) -> None:
        requests: list[dict] = []

        async def handler(request: httpx.Request) -> httpx.Response:
            payload = json.loads(request.content)
            requests.append(payload)
            if payload.get("stream"):
                return httpx.Response(
                    200,
                    text=(
                        'data: {"id":"skill","model":"deepseek-chat",'
                        '"choices":[{"delta":{"content":"done"},"finish_reason":"stop"}]}\n\n'
                        "data: [DONE]\n\n"
                    ),
                    headers={"content-type": "text/event-stream"},
                )
            return httpx.Response(
                200,
                json={
                    "model": "deepseek-chat",
                    "choices": [
                        {
                            "finish_reason": "stop",
                            "message": {"content": "done", "tool_calls": []},
                        }
                    ],
                    "usage": {},
                },
            )

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_skill(root)

            async def run() -> None:
                model = OpenAICompatChatModel(
                    "deepseek-chat",
                    base_url="https://api.deepseek.test/v1",
                    api_key="test",
                    transport=httpx.MockTransport(handler),
                )
                result = await create_agent(model, skills=root).ainvoke(
                    {"messages": [HumanMessage("hello")]}
                )
                self.assertEqual(result["messages"][-1].content, "done")
                await model.aclose()

            asyncio.run(run())
        names = [item["function"]["name"] for item in requests[0]["tools"]]
        self.assertEqual(names, ["read_skill", "read_skill_resource"])

    def test_old_api_and_tool_name_collisions_remain_deterministic(self) -> None:
        class Model:
            async def agenerate(self, messages, *, tools=None, **kwargs):
                del messages, tools, kwargs
                return AIMessage("legacy")

        self.assertIs(create_react_agent, create_agent)
        result = create_agent(Model()).invoke({"messages": [HumanMessage("hello")]})
        self.assertEqual(result["messages"][-1].content, "legacy")

        @tool(name="read_skill")
        def collision(skill_name: str) -> str:
            """Collide with the standard skill loader."""

            return skill_name

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_skill(root)
            with self.assertRaisesRegex(ValueError, "tool names must be unique"):
                create_agent(Model(), [collision], skills=root)

    def test_async_registry_tools_can_run_from_sync_graph(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_skill(root)

            async def run() -> None:
                registry = SkillRegistry((FilesystemSkillSource(root),))
                result = await asyncio.to_thread(registry.tool_specs()[0].func, "hello")
                self.assertIn("# Hello", result)

            asyncio.run(run())


if __name__ == "__main__":
    unittest.main()
