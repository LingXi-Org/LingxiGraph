"""Offline ReAct smoke example with a scripted provider-neutral model."""

from lingxigraph import AIMessage, HumanMessage, ToolCall, create_agent, tool


@tool
def multiply(a: int, b: int) -> int:
    """Multiply two integers."""

    return a * b


class ScriptedModel:
    def __init__(self) -> None:
        self.turn = 0

    async def agenerate(self, messages, *, tools=None, **kwargs):
        del messages, tools, kwargs
        self.turn += 1
        if self.turn == 1:
            return AIMessage(
                "",
                tool_calls=(ToolCall("multiply", {"a": 6, "b": 7}, "multiply-1"),),
            )
        return AIMessage("The answer is 42.")


if __name__ == "__main__":
    graph = create_agent(ScriptedModel(), [multiply])
    result = graph.invoke({"messages": [HumanMessage("What is 6 × 7?")]})
    print(result["messages"][-1].content)
