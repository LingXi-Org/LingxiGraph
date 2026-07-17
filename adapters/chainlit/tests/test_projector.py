from __future__ import annotations

from lingxigraph import AIMessage, AIMessageChunk, ToolCall, ToolCallChunk, ToolMessage
from lingxigraph.events import Event, EventKind

from lingxigraph_chainlit import ObservabilityOptions
from lingxigraph_chainlit.projector import EventProjector


def event(kind: EventKind, sequence: int, **values):
    return Event(kind, "run", sequence=sequence, **values)


def test_streaming_message_is_aggregated_and_duplicates_are_ignored() -> None:
    projector = EventProjector()
    first = event(
        EventKind.MESSAGE,
        1,
        step=1,
        task_id="agent",
        data={"value": (AIMessageChunk("hel", id="answer"), {"provider": "fake"})},
    )
    second = event(
        EventKind.MESSAGE,
        2,
        step=1,
        task_id="agent",
        data={"value": (AIMessageChunk("lo"), {})},
    )

    assert [item.content for item in projector.project(first)] == ["hel"]
    assert projector.project(first) == []
    assert [item.content for item in projector.project(second)] == ["lo"]
    assert projector.project(event(EventKind.RUN_COMPLETED, 3))[0].kind == "assistant_end"


def test_non_streaming_message_and_tool_roundtrip() -> None:
    projector = EventProjector(ObservabilityOptions(show_tool_io=True))
    emitted = projector.project(
        event(
            EventKind.MESSAGE,
            1,
            step=1,
            data={
                "value": (
                    AIMessage(
                        "checking",
                        id="answer",
                        tool_calls=(ToolCall("lookup", {"q": "x"}, "call-1"),),
                    ),
                    {},
                )
            },
        )
    )
    assert [item.kind for item in emitted] == ["assistant_message", "tool_start"]
    assert '"q": "x"' in emitted[1].content

    completed = projector.project(
        event(
            EventKind.NODE_COMPLETED,
            2,
            step=2,
            node="tools",
            task_id="tools",
            data={
                "update": {
                    "messages": [
                        ToolMessage("found", "call-1", name="lookup", status="success")
                    ]
                }
            },
        )
    )
    tool_end = next(item for item in completed if item.kind == "tool_end")
    assert tool_end.key == "tool:call-1"
    assert "found" in tool_end.content


def test_chunked_tool_arguments_are_accumulated() -> None:
    projector = EventProjector(ObservabilityOptions(show_tool_io=True))
    one = projector.project(
        event(
            EventKind.MESSAGE,
            1,
            step=1,
            data={
                "value": (
                    AIMessageChunk(
                        tool_call_chunks=(ToolCallChunk("lookup", '{"q":', "call", 0),)
                    ),
                    {},
                )
            },
        )
    )
    two = projector.project(
        event(
            EventKind.MESSAGE,
            2,
            step=1,
            data={
                "value": (
                    AIMessageChunk(
                        tool_call_chunks=(ToolCallChunk(args='"x"}', index=0),)
                    ),
                    {},
                )
            },
        )
    )
    assert one[-1].kind == "tool_start"
    assert '{\\"q\\":' in one[-1].content
    assert '{\\"q\\":\\"x\\"}' in two[-1].content


def test_observability_payloads_are_hidden_by_default_and_opt_in() -> None:
    hidden = EventProjector().project(
        event(EventKind.CUSTOM, 1, data={"channel": "progress", "value": {"secret": 1}})
    )[0]
    assert hidden.name == "progress"
    assert "secret" not in hidden.content

    visible = EventProjector(
        ObservabilityOptions(show_custom_payloads=True, show_state_updates=True)
    )
    custom = visible.project(
        event(EventKind.CUSTOM, 1, data={"channel": "progress", "value": {"secret": 1}})
    )[0]
    node = visible.project(
        event(
            EventKind.NODE_COMPLETED,
            2,
            node="agent",
            task_id="agent",
            data={"update": {"secret": 2}},
        )
    )[0]
    assert '"secret": 1' in custom.content
    assert '"secret": 2' in node.content


def test_interrupts_and_node_metadata_are_projected() -> None:
    from lingxigraph import Interrupt

    projector = EventProjector()
    node = projector.project(
        event(
            EventKind.NODE_STARTED,
            1,
            step=3,
            node="review",
            task_id="review",
            namespace=("team",),
            checkpoint_id="cp",
        )
    )[0]
    interrupt = projector.project(
        event(
            EventKind.INTERRUPT_RAISED,
            2,
            data={"interrupts": (Interrupt({"question": "Ship?"}, id="review:0"),)},
        )
    )[0]
    assert node.metadata["namespace"] == ["team"]
    assert node.metadata["checkpoint_id"] == "cp"
    assert interrupt.content == "Ship?"
    assert interrupt.metadata["interrupt_id"] == "review:0"
