"""Coze-backed agent example; requires COZE_API_TOKEN and COZE_BOT_ID."""

import asyncio
import os

from lingxigraph import HumanMessage, create_agent
from lingxigraph.integrations import AsyncCozeClient, CozeChatModel


async def main() -> None:
    client = AsyncCozeClient(os.environ["COZE_API_TOKEN"])
    try:
        model = CozeChatModel(
            os.environ["COZE_BOT_ID"],
            client=client,
            user_id=os.getenv("COZE_USER_ID", "lingxigraph-example"),
        )
        graph = create_agent(model, name="coze-agent")
        result = await graph.ainvoke({"messages": [HumanMessage("你好，请介绍你自己。")]})
        print(result["messages"][-1].content)
    finally:
        await client.aclose()


if __name__ == "__main__":
    asyncio.run(main())
