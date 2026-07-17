# LingxiGraph Chainlit Adapter

`lingxigraph-chainlit` hosts a compiled LingxiGraph `MessagesState` graph directly inside
Chainlit. It is a separate distribution: installing it does not add Chainlit to the
`lingxigraph` core package.

## Install and run

From this repository:

```bash
pip install -e "adapters/chainlit"
export LINGXIGRAPH_CHAINLIT_GRAPH="myapp.graph:graph"
lingxigraph-chainlit --host 0.0.0.0 --port 8000
```

PowerShell uses `$env:LINGXIGRAPH_CHAINLIT_GRAPH = "myapp.graph:graph"` for the environment
variable. The import target may be a `CompiledGraph` object or a zero-argument factory.

The graph must use the canonical message state:

```python
from lingxigraph import create_agent

# model implements the LingxiGraph ChatModel protocol.
graph = create_agent(model, system_prompt="You are a support assistant.")
```

The packaged host uses `.chainlit/lingxigraph.db` for graph checkpoints unless the graph
already owns a checkpointer. Override it with `LINGXIGRAPH_CHAINLIT_SQLITE_PATH`; use
`:memory:` only for disposable sessions.

## Programmatic setup

```python
from lingxigraph_chainlit import ObservabilityOptions, install_chainlit
from myapp.graph import graph

adapter = install_chainlit(
    graph,
    sqlite_path="var/chainlit-checkpoints.db",
    context={"department": "support"},
    observability=ObservabilityOptions(
        show_custom_payloads=True,
        show_tool_io=False,
    ),
)
```

Import this module through the normal `chainlit run app.py` command. For per-user context,
pass a synchronous or asynchronous `context_factory(session, latest_user_text)` instead of
static `context`. `session` exposes the Chainlit thread ID, websocket session ID, user
identifier, and selected chat profile.

## Environment variables

| Variable | Default | Meaning |
| --- | --- | --- |
| `LINGXIGRAPH_CHAINLIT_GRAPH` | required | Trusted `module:attribute` graph import path |
| `LINGXIGRAPH_CHAINLIT_SQLITE_PATH` | `.chainlit/lingxigraph.db` | Embedded checkpoint database |
| `LINGXIGRAPH_CHAINLIT_CONTEXT_JSON` | `{}` | Static JSON object passed as graph context |
| `LINGXIGRAPH_CHAINLIT_SHOW_STATE_UPDATES` | `false` | Show node state updates |
| `LINGXIGRAPH_CHAINLIT_SHOW_CUSTOM_PAYLOADS` | `false` | Show custom event values |
| `LINGXIGRAPH_CHAINLIT_SHOW_TOOL_IO` | `false` | Show tool arguments and results |
| `LINGXIGRAPH_CHAINLIT_MAX_PAYLOAD_CHARS` | `4000` | Maximum rendered payload length |
| `LINGXIGRAPH_CHAINLIT_DEFAULT_OPEN` | `false` | Expand observability steps by default |

Boolean variables accept `true/false`, `1/0`, `yes/no`, and `on/off`.

## Runtime behavior

- One stable Chainlit thread ID becomes the LingxiGraph checkpoint `thread_id`.
- Model chunks stream into one assistant message; node, tool, retry, cache, namespace, and
  custom activity appears as Chainlit steps.
- The Stop action cancels the active graph cooperatively.
- Dynamic interrupts prompt for text and resume using the interrupt ID. Multiple pending
  interrupts are collected before resuming.
- Existing pending interrupts are handled before a new user message is appended.

This adapter accepts text messages only. Chainlit authentication, its conversation-history
data layer, feedback, files, audio, theming, and deployment configuration remain the host
application's responsibility. SQLite here stores LingxiGraph checkpoints, not Chainlit UI
history.

## Development

```bash
uv sync --project adapters/chainlit --extra dev
uv run --project adapters/chainlit ruff check adapters/chainlit/src adapters/chainlit/tests
uv run --project adapters/chainlit pytest adapters/chainlit/tests
uv build --project adapters/chainlit
```

