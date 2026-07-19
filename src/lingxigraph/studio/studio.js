/* LingxiGraph Studio 1.0 — a real, API-connected debugger for compiled graphs. */
"use strict";

const $ = (s) => document.querySelector(s);
const $$ = (s) => [...document.querySelectorAll(s)];

// Dev auth headers. The dev/compose stack runs LINGXIGRAPH_INSECURE_DEV_AUTH=true,
// which honours x-tenant-id / x-roles. In a secured deployment a reverse proxy
// injects a bearer token instead and these headers are ignored.
const AUTH = { "x-tenant-id": "local", "x-roles": "viewer,developer,operator" };

const state = {
  graphs: [],
  graph: null,          // {id, version}
  structure: null,      // last fetched structure
  xray: false,
  assistantId: null,
  threadId: null,
  runId: null,
  events: [],
  nodeStatus: {},       // node id -> "active" | "done" | "failed"
  timeline: [],         // ordered trace items
  errors: [],
  startedAt: null,
  timer: null,
  es: null,             // EventSource
  selectedNode: null,
  history: [],
  drill: [],            // breadcrumb of subgraph names we've descended into
};

// The structure currently shown in the canvas, honouring any drill-in path.
function activeStructure() {
  let structure = state.structure;
  for (const name of state.drill) {
    const node = (structure.nodes || []).find((n) => n.id === name);
    if (node && node.subgraph) structure = node.subgraph;
    else break;
  }
  return structure;
}

function toast(message) {
  const el = $("#toast");
  el.textContent = message;
  el.classList.add("show");
  clearTimeout(toast._t);
  toast._t = setTimeout(() => el.classList.remove("show"), 1800);
}

async function api(path, options = {}) {
  const response = await fetch(path, {
    ...options,
    headers: { ...AUTH, "content-type": "application/json", ...(options.headers || {}) },
  });
  if (!response.ok) {
    let detail = `${response.status}`;
    try { detail = (await response.json()).detail || detail; } catch (_) {}
    throw new Error(detail);
  }
  if (response.status === 204) return null;
  return response.json();
}

function setConn(kind, text) {
  const dot = $("#connDot");
  dot.className = `conn ${kind}`;
  $("#connText").textContent = text;
}

/* ---------- bootstrap: discover graphs ---------- */

async function boot() {
  try {
    state.graphs = await api("/v1/graphs");
    setConn("on", "已连接");
  } catch (err) {
    setConn("err", "连接失败");
    $("#graphEmpty").textContent = `无法连接到 Agent Server：${err.message}`;
    return;
  }
  const select = $("#graphSelect");
  select.innerHTML = "";
  if (!state.graphs.length) {
    $("#graphEmpty").textContent = "未注册任何 Graph";
    return;
  }
  for (const graph of state.graphs) {
    const option = document.createElement("option");
    option.value = `${graph.id}::${graph.version}`;
    option.textContent = `${graph.id} · v${graph.version}`;
    select.appendChild(option);
  }
  await selectGraph(state.graphs[0]);
}

async function selectGraph(graph) {
  state.graph = { id: graph.id, version: graph.version };
  $("#graphName").textContent = graph.id;
  $("#graphVersion").textContent = `v${graph.version}`;
  await loadStructure();
}

async function loadStructure() {
  const { id, version } = state.graph;
  const query = `graph_version=${encodeURIComponent(version)}&xray=${state.xray}`;
  const structure = await api(`/v1/graphs/${encodeURIComponent(id)}/structure?${query}`);
  state.structure = structure;
  $("#nodeCount").textContent = structure.nodes.length;
  $("#edgeCount").textContent = structure.edges.length;
  $("#graphEmpty").hidden = true;
  renderGraph(structure);
}

/* ---------- graph rendering: layered layout, real topology ---------- */

function layerOf(structure) {
  // Longest-path layering from __start__ following edges.
  const adjacency = {};
  const nodeIds = structure.nodes.map((n) => n.id);
  for (const id of nodeIds) adjacency[id] = [];
  for (const edge of structure.edges) {
    if (adjacency[edge.source]) adjacency[edge.source].push(edge.target);
  }
  const layer = {};
  for (const id of nodeIds) layer[id] = 0;
  // iterate to relax (graph may have cycles; cap iterations)
  for (let pass = 0; pass < nodeIds.length; pass++) {
    let changed = false;
    for (const edge of structure.edges) {
      if (layer[edge.target] < layer[edge.source] + 1) {
        layer[edge.target] = layer[edge.source] + 1;
        changed = true;
      }
    }
    if (!changed) break;
  }
  // __end__ always in the last layer
  const maxLayer = Math.max(0, ...Object.values(layer));
  if (layer["__end__"] !== undefined) layer["__end__"] = maxLayer;
  return layer;
}

function renderGraph(_ignored) {
  const structure = activeStructure();
  const svg = $("#graphSvg");
  const canvas = $("#graphCanvas");
  renderBreadcrumb();
  const width = canvas.clientWidth || 520;
  const height = canvas.clientHeight || 460;
  svg.setAttribute("viewBox", `0 0 ${width} ${height}`);
  svg.innerHTML = "";

  const layer = layerOf(structure);
  const columns = {};
  for (const node of structure.nodes) {
    const l = layer[node.id] || 0;
    (columns[l] = columns[l] || []).push(node);
  }
  const layerKeys = Object.keys(columns).map(Number).sort((a, b) => a - b);
  const nodeW = 128, nodeH = 46;
  const padX = 30, padY = 24;
  const colGap = (width - 2 * padX - nodeW) / Math.max(1, layerKeys.length - 1);
  const pos = {};
  layerKeys.forEach((key, ci) => {
    const nodes = columns[key];
    const rowGap = (height - 2 * padY) / (nodes.length + 1);
    nodes.forEach((node, ri) => {
      pos[node.id] = {
        x: padX + ci * colGap,
        y: padY + rowGap * (ri + 1) - nodeH / 2,
        node,
      };
    });
  });

  const ns = "http://www.w3.org/2000/svg";
  // edges first
  for (const edge of structure.edges) {
    const a = pos[edge.source], b = pos[edge.target];
    if (!a || !b) continue;
    const x1 = a.x + nodeW, y1 = a.y + nodeH / 2;
    const x2 = b.x, y2 = b.y + nodeH / 2;
    const mx = (x1 + x2) / 2;
    const path = document.createElementNS(ns, "path");
    path.setAttribute("d", `M ${x1} ${y1} C ${mx} ${y1}, ${mx} ${y2}, ${x2} ${y2}`);
    path.setAttribute("class", `gedge ${edge.conditional ? "conditional" : ""}`);
    path.setAttribute("marker-end", "url(#arrow)");
    svg.appendChild(path);
    if (edge.label) {
      const label = document.createElementNS(ns, "text");
      label.setAttribute("x", mx);
      label.setAttribute("y", (y1 + y2) / 2 - 4);
      label.setAttribute("text-anchor", "middle");
      label.setAttribute("class", "gedge-label");
      label.textContent = edge.label;
      svg.appendChild(label);
    }
  }
  // arrow marker
  const defs = document.createElementNS(ns, "defs");
  defs.innerHTML =
    '<marker id="arrow" markerWidth="8" markerHeight="8" refX="7" refY="3" orient="auto">' +
    '<path d="M0,0 L7,3 L0,6 Z" fill="#5a5f72"/></marker>';
  svg.appendChild(defs);

  // nodes
  for (const node of structure.nodes) {
    const p = pos[node.id];
    if (!p) continue;
    const terminal = node.id === "__start__" || node.id === "__end__";
    const g = document.createElementNS(ns, "g");
    const cls = terminal ? "terminal" : (node.is_subgraph ? "subgraph" : "node");
    g.setAttribute("class", `gnode ${cls} ${state.nodeStatus[node.id] || ""} ${state.selectedNode === node.id ? "sel" : ""}`);
    g.dataset.node = node.id;
    const rect = document.createElementNS(ns, "rect");
    rect.setAttribute("x", p.x); rect.setAttribute("y", p.y);
    rect.setAttribute("width", nodeW); rect.setAttribute("height", nodeH);
    rect.setAttribute("rx", terminal ? 22 : 10);
    g.appendChild(rect);
    const title = document.createElementNS(ns, "text");
    title.setAttribute("x", p.x + nodeW / 2);
    title.setAttribute("y", p.y + (terminal ? 28 : 20));
    title.setAttribute("text-anchor", "middle");
    title.textContent = terminal ? node.id.replace(/__/g, "").toUpperCase() : node.id;
    g.appendChild(title);
    if (!terminal) {
      const sub = document.createElementNS(ns, "text");
      sub.setAttribute("x", p.x + nodeW / 2);
      sub.setAttribute("y", p.y + 34);
      sub.setAttribute("text-anchor", "middle");
      sub.setAttribute("class", "sub");
      sub.textContent = node.is_subgraph ? "subgraph" : (node.debug && node.debug.callable) || "node";
      g.appendChild(sub);
    }
    g.addEventListener("click", () => {
      if (node.is_subgraph && node.subgraph && state.xray) {
        state.drill.push(node.id);
        state.selectedNode = null;
        renderGraph();
        toast(`进入子图 ${node.id}`);
      } else {
        selectNode(node);
      }
    });
    g.addEventListener("dblclick", () => {
      if (node.is_subgraph && node.subgraph) {
        if (!state.xray) { state.xray = true; $("#xrayToggle").checked = true; }
        loadStructure().then(() => { state.drill.push(node.id); renderGraph(); });
      }
    });
    svg.appendChild(g);
  }
}

function renderBreadcrumb() {
  let bar = $("#breadcrumb");
  if (!bar) {
    bar = document.createElement("div");
    bar.id = "breadcrumb";
    bar.className = "breadcrumb";
    $("#graphCanvas").before(bar);
  }
  if (!state.drill.length) { bar.hidden = true; return; }
  bar.hidden = false;
  const crumbs = ["<button data-i='-1'>根图</button>"];
  state.drill.forEach((name, i) => crumbs.push(`<span>›</span><button data-i='${i}'>${escape(name)}</button>`));
  bar.innerHTML = crumbs.join("");
  bar.querySelectorAll("button").forEach((btn) => btn.addEventListener("click", () => {
    const i = parseInt(btn.dataset.i, 10);
    state.drill = i < 0 ? [] : state.drill.slice(0, i + 1);
    state.selectedNode = null;
    renderGraph();
  }));
}

function selectNode(node) {
  state.selectedNode = node.id;
  renderGraph(state.structure);
  const terminal = node.id === "__start__" || node.id === "__end__";
  $("#nodeDetail").hidden = false;
  $("#ndIcon").textContent = terminal ? "◉" : node.is_subgraph ? "▣" : "◇";
  $("#ndName").textContent = node.id;
  $("#ndKind").textContent = `${node.kind}${node.is_subgraph ? " · 子图" : ""}`;
  const body = $("#ndBody");
  body.innerHTML = "";
  const rows = [];
  const status = state.nodeStatus[node.id];
  if (status) rows.push(["运行状态", statusLabel(status)]);
  const debug = node.debug || {};
  if (debug.callable) rows.push(["实现", `<code>${escape(debug.callable)}</code>`]);
  if (debug.uses_runtime) rows.push(["Runtime", "注入 <code>runtime</code>"]);
  if (debug.timeout) rows.push(["超时", `${debug.timeout}s`]);
  if (debug.retry) rows.push(["重试", `最多 ${debug.retry.max_attempts ?? "?"} 次 · backoff ${debug.retry.backoff ?? "?"}`]);
  if (debug.max_concurrency) rows.push(["并发上限", debug.max_concurrency]);
  if (debug.cache) rows.push(["缓存", "已启用"]);
  if (debug.middleware) rows.push(["中间件", `${debug.middleware} 个`]);
  if (debug.defer) rows.push(["延迟执行", "是"]);
  if (debug.subgraph_persistence) rows.push(["子图持久化", `<code>${debug.subgraph_persistence}</code>`]);
  if (node.metadata && Object.keys(node.metadata).length) {
    rows.push(["元数据", `<code>${escape(JSON.stringify(node.metadata))}</code>`]);
  }
  const outgoing = (activeStructure().edges || []).filter((e) => e.source === node.id);
  if (outgoing.length) {
    rows.push(["后继", outgoing.map((e) => `<code>${escape(e.target)}</code>${e.conditional ? " ·条件" : ""}`).join(" ")]);
  }
  if (!rows.length) rows.push(["说明", terminal ? "图的入口/出口终端节点" : "普通节点"]);
  for (const [key, value] of rows) {
    body.innerHTML += `<dt>${key}</dt><dd>${value}</dd>`;
  }
  renderExplain(node);
}

function statusLabel(status) {
  return { active: "运行中", done: "已完成", failed: "失败" }[status] || status;
}

/* ---------- explain tab: narrate what the node does ---------- */

function renderExplain(node) {
  const view = $("#explainView");
  const debug = node.debug || {};
  const parts = [];
  const terminal = node.id === "__start__" || node.id === "__end__";
  if (terminal) {
    parts.push(block("终端节点",
      node.id === "__start__"
        ? "<code>__start__</code> 是编译图的虚拟入口。运行开始时，输入状态从这里进入第一个真实节点。"
        : "<code>__end__</code> 是编译图的虚拟出口。任意节点路由到这里即结束该超步链。"));
  } else if (node.is_subgraph) {
    parts.push(block("子图节点",
      `该节点是一个嵌套的编译子图（<code>${escape(debug.callable || node.id)}</code>）。它以自身的 plan → execute → commit 超步语义独立运行，` +
      (state.xray ? "并已在左侧图中展开。" : "开启顶部 X-ray 可展开其内部结构。")));
    if (debug.subgraph_persistence) {
      parts.push(block("持久化",
        `子图检查点策略为 <code>${escape(debug.subgraph_persistence)}</code>，决定其状态是每次调用重建还是跨调用保留。`));
    }
  } else {
    parts.push(block("执行语义",
      `节点 <code>${escape(node.id)}</code> 由 <code>${escape(debug.callable || "callable")}</code> 实现。` +
      (debug.uses_runtime
        ? "它接收 <code>runtime</code> 参数，可发射事件、读取 context 并使用幂等键。"
        : "它是纯状态函数，仅接收并返回 state 更新。")));
    const guards = [];
    if (debug.timeout) guards.push(`超时 <code>${debug.timeout}s</code>`);
    if (debug.retry) guards.push(`重试至多 <code>${debug.retry.max_attempts}</code> 次`);
    if (debug.max_concurrency) guards.push(`并发上限 <code>${debug.max_concurrency}</code>`);
    if (debug.cache) guards.push("结果缓存");
    if (guards.length) parts.push(block("可靠性护栏", guards.join("、") + "。"));
  }
  const incoming = (activeStructure().edges || []).filter((e) => e.target === node.id);
  const outgoing = (activeStructure().edges || []).filter((e) => e.source === node.id);
  parts.push(block("控制流",
    `${incoming.length} 条入边、${outgoing.length} 条出边` +
    (outgoing.some((e) => e.conditional) ? "，其中包含条件路由（运行时由路径函数决定去向）。" : "。")));
  view.innerHTML = parts.join("");
}

function block(title, html) {
  return `<div class="explain-block"><h4>${title}</h4><p>${html}</p></div>`;
}

/* ---------- run execution against the real API ---------- */

async function ensureAssistant() {
  if (state.assistantId) return state.assistantId;
  const assistant = await api("/v1/assistants", {
    method: "POST",
    body: JSON.stringify({
      graph_id: state.graph.id,
      graph_version: state.graph.version,
      name: `studio-${state.graph.id}`,
    }),
  });
  state.assistantId = assistant.id;
  return assistant.id;
}

async function ensureThread() {
  if (state.threadId) return state.threadId;
  const thread = await api("/v1/threads", {
    method: "POST",
    body: JSON.stringify({ metadata: { source: "studio" } }),
  });
  state.threadId = thread.id;
  return thread.id;
}

function parseInput() {
  const raw = $("#inputBox").value.trim();
  if (!raw) return {};
  try { return JSON.parse(raw); }
  catch (_) { throw new Error("输入不是合法 JSON"); }
}

async function runGraph() {
  let input;
  try { input = parseInput(); }
  catch (err) { toast(err.message); return; }

  resetRun();
  const button = $("#runButton");
  button.disabled = true;
  try {
    const assistantId = await ensureAssistant();
    const threadId = await ensureThread();
    const run = await api(`/v1/threads/${threadId}/runs`, {
      method: "POST",
      body: JSON.stringify({ assistant_id: assistantId, input, durability: "async" }),
    });
    state.runId = run.id;
    $("#runId").textContent = run.id.slice(0, 14) + "…";
    setStatus("running");
    $("#liveTag").hidden = false;
    $("#cancelButton").disabled = false;
    state.startedAt = performance.now();
    state.timer = setInterval(() => {
      $("#totalTime").textContent = ((performance.now() - state.startedAt) / 1000).toFixed(2) + "s";
    }, 60);
    streamRun(run.id);
  } catch (err) {
    toast("运行失败：" + err.message);
    setStatus("failed");
    button.disabled = false;
  }
}

function resetRun() {
  state.events = [];
  state.timeline = [];
  state.nodeStatus = {};
  state.errors = [];
  state.history = [];
  $("#traceList").innerHTML = "";
  $("#eventList").innerHTML = "";
  $("#errorList").innerHTML = '<div class="empty-hint">暂无错误</div>';
  $("#evBadge").textContent = "0";
  $("#errBadge").textContent = "0";
  $("#errBadge").classList.add("zero");
  $("#eventCount").textContent = "0";
  $("#totalTime").textContent = "0.00s";
  $("#interruptCard").hidden = true;
  renderGraph(state.structure);
}

function setStatus(status) {
  $("#statusDot").className = `status-dot ${status}`;
  $("#runStatus").textContent = {
    running: "运行中", succeeded: "已完成", failed: "失败",
    cancelled: "已取消", paused: "已暂停", idle: "空闲",
  }[status] || status;
}

/* ---------- live SSE trace ---------- */

function streamRun(runId) {
  const url = `/v1/runs/${runId}/stream`;
  // EventSource can't send headers; dev auth also accepts query fallback via proxy.
  // We use fetch + ReadableStream to attach auth headers.
  fetchStream(url);
}

async function fetchStream(url) {
  const foot = $("#streamState");
  foot.parentElement.classList.add("on");
  foot.innerHTML = "<i></i> 事件流已连接";
  let lastId = 0;
  try {
    const response = await fetch(url, { headers: { ...AUTH } });
    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let buffer = "";
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      let idx;
      while ((idx = buffer.indexOf("\n\n")) >= 0) {
        const chunk = buffer.slice(0, idx);
        buffer = buffer.slice(idx + 2);
        const event = parseSse(chunk);
        if (event && event.data) {
          lastId = event.id || lastId;
          handleEvent(event);
        }
      }
    }
  } catch (err) {
    foot.innerHTML = `<i></i> 事件流中断：${err.message}`;
  } finally {
    foot.parentElement.classList.remove("on");
    if ($("#runStatus").textContent === "运行中") foot.innerHTML = "<i></i> 事件流已结束";
    await finishRun();
  }
}

function parseSse(chunk) {
  const out = { id: null, event: null, data: null };
  for (const line of chunk.split("\n")) {
    if (line.startsWith("id:")) out.id = parseInt(line.slice(3).trim(), 10);
    else if (line.startsWith("event:")) out.event = line.slice(6).trim();
    else if (line.startsWith("data:")) {
      try { out.data = JSON.parse(line.slice(5).trim()); } catch (_) {}
    }
  }
  return out;
}

function handleEvent(sse) {
  // The SSE data field is the persisted RunEvent: {kind, sequence, data:{...}}.
  // The inner data carries the serialized runtime Event (node, step, payload).
  const persisted = sse.data;
  const inner = persisted.data || {};
  const event = {
    kind: persisted.kind,
    node: inner.node,
    step: inner.step,
    timestamp: inner.timestamp || persisted.created_at,
    data: inner.data || {},
  };
  state.events.push(event);
  $("#eventCount").textContent = state.events.length;
  $("#evBadge").textContent = state.events.length;
  appendEventRow(event);

  const kind = event.kind;
  const node = event.node;
  if (kind === "node_started" && node) {
    state.nodeStatus[node] = "active";
    pushTimeline(node, "active", "NODE STARTED", event);
  } else if (kind === "node_completed" && node) {
    state.nodeStatus[node] = "done";
    updateTimeline(node, "done", "NODE COMPLETED", event);
  } else if (kind === "node_failed" && node) {
    state.nodeStatus[node] = "failed";
    updateTimeline(node, "failed", "NODE FAILED", event);
    pushError(node, event);
  } else if (kind === "node_retrying" && node) {
    updateTimeline(node, "active", "NODE RETRYING", event);
  } else if (kind === "interrupt_raised" || kind === "run_paused") {
    showInterrupt(event);
  } else if (kind === "run_failed" || kind === "run_timed_out" || kind === "run_budget_exceeded") {
    pushError(event.node || "run", event);
  }
  if (["node_started", "node_completed", "node_failed"].includes(kind)) {
    renderGraph(state.structure);
  }
}

function nodeLabel(node) {
  const meta = (activeStructure().nodes || []).find((n) => n.id === node);
  return (meta && meta.debug && meta.debug.callable) || node;
}

function pushTimeline(node, status, phase, event) {
  const item = document.createElement("article");
  item.className = `trace-item ${status}`;
  item.dataset.node = node;
  item.innerHTML =
    `<div class="rail"><i>${status === "active" ? "◇" : "•"}</i></div>` +
    `<div class="trace-content"><div><b>${escape(node)}</b><span data-dur></span></div>` +
    `<small>${phase} · ${time(event.timestamp)}</small>` +
    `<p data-detail></p></div>`;
  item.dataset.start = String(performance.now());
  $("#traceList").appendChild(item);
  item.scrollIntoView({ block: "nearest" });
}

function updateTimeline(node, status, phase, event) {
  let item = [...$("#traceList").children].reverse().find((el) => el.dataset.node === node);
  if (!item) { pushTimeline(node, status, phase, event); item = $("#traceList").lastElementChild; }
  item.className = `trace-item ${status}`;
  item.querySelector(".rail i").textContent = status === "done" ? "✓" : status === "failed" ? "✕" : "◇";
  item.querySelector("small").textContent = `${phase} · ${time(event.timestamp)}`;
  if (item.dataset.start) {
    const ms = Math.round(performance.now() - Number(item.dataset.start));
    item.querySelector("[data-dur]").textContent = `${ms}ms`;
  }
  const data = event.data || {};
  const detail = item.querySelector("[data-detail]");
  if (data.update || data.output || data.result) {
    detail.innerHTML = `<span>输出</span><code>${escape(short(JSON.stringify(data.update || data.output || data.result)))}</code>`;
  } else if (data.error) {
    detail.innerHTML = `<span>错误</span>${escape(short(String(data.error)))}`;
  }
}

function appendEventRow(event) {
  const list = $("#eventList");
  const row = document.createElement("div");
  row.className = "event-row";
  row.innerHTML =
    `<span class="kind">${escape(event.kind)}</span>` +
    `<span class="node">${escape(event.node || (event.data && event.data.stage) || "—")}</span>` +
    `<span class="ts">${time(event.timestamp)}</span>`;
  list.appendChild(row);
  if (!$("#eventList").classList.contains("hidden")) row.scrollIntoView({ block: "nearest" });
}

function pushError(node, event) {
  state.errors.push(event);
  const list = $("#errorList");
  if (state.errors.length === 1) list.innerHTML = "";
  const data = event.data || {};
  const row = document.createElement("div");
  row.className = "error-row";
  row.innerHTML = `<b>${escape(node)} · ${escape(event.kind)}</b><pre>${escape(
    data.error || data.message || JSON.stringify(data, null, 2) || "unknown error")}</pre>`;
  list.appendChild(row);
  $("#errBadge").textContent = state.errors.length;
  $("#errBadge").classList.remove("zero");
}

function showInterrupt(event) {
  const card = $("#interruptCard");
  card.hidden = false;
  const data = event.data || {};
  $("#interruptWhy").textContent = data.value ? short(JSON.stringify(data.value)) : "运行已在中断点暂停";
  setStatus("paused");
}

async function finishRun() {
  clearInterval(state.timer);
  $("#liveTag").hidden = true;
  $("#cancelButton").disabled = true;
  $("#runButton").disabled = false;
  if (!state.runId) return;
  try {
    const run = await api(`/v1/runs/${state.runId}`);
    const status = run.status;
    if (status === "succeeded") setStatus("succeeded");
    else if (status === "paused") setStatus("paused");
    else if (status === "cancelled") setStatus("cancelled");
    else if (["failed", "timed_out", "dead_letter"].includes(status)) setStatus("failed");
    if (run.error && !state.errors.length) pushError("run", { kind: status, data: run.error });
    // Reconcile: a very fast run may terminate before every node_completed
    // event was streamed. On success, settle any still-active timeline items.
    if (status === "succeeded") {
      for (const item of $$("#traceList .trace-item.active")) {
        item.className = "trace-item done";
        item.querySelector(".rail i").textContent = "✓";
        const node = item.dataset.node;
        if (node) state.nodeStatus[node] = "done";
      }
      renderGraph(state.structure);
    }
  } catch (_) {}
  await loadState();
  await loadHistory();
}

async function cancelRun() {
  if (!state.runId) return;
  try {
    await api(`/v1/runs/${state.runId}/cancel`, { method: "POST" });
    toast("已请求取消");
  } catch (err) { toast("取消失败：" + err.message); }
}

async function resumeRun() {
  if (!state.runId) return;
  let value = $("#resumeValue").value.trim();
  let parsed = value;
  if (value) { try { parsed = JSON.parse(value); } catch (_) {} }
  try {
    const run = await api(`/v1/runs/${state.runId}/resume`, {
      method: "POST",
      body: JSON.stringify({ resume: parsed }),
    });
    state.runId = run.id;
    $("#interruptCard").hidden = true;
    setStatus("running");
    $("#liveTag").hidden = false;
    $("#cancelButton").disabled = false;
    state.startedAt = performance.now();
    state.timer = setInterval(() => {
      $("#totalTime").textContent = ((performance.now() - state.startedAt) / 1000).toFixed(2) + "s";
    }, 60);
    streamRun(run.id);
  } catch (err) { toast("恢复失败：" + err.message); }
}

/* ---------- state & checkpoint inspector ---------- */

async function loadState(checkpointId) {
  if (!state.threadId) return;
  try {
    const query = checkpointId ? `?checkpoint_id=${encodeURIComponent(checkpointId)}` : "";
    const snapshot = await api(`/v1/threads/${state.threadId}/state${query}`);
    renderState(snapshot);
  } catch (err) {
    $("#stateCode").innerHTML = `<span class="dim">无法获取状态：${escape(err.message)}</span>`;
  }
}

function renderState(snapshot) {
  const values = snapshot.values || snapshot;
  const json = JSON.stringify(values, null, 2);
  $("#stateCode").innerHTML = highlight(json);
  $("#stateLabel").textContent = "StateSnapshot";
  const bytes = new Blob([json]).size;
  $("#snapSize").textContent = bytes < 1024 ? `${bytes} B` : `${(bytes / 1024).toFixed(1)} KB`;
  const cpId = (snapshot.config && snapshot.config.configurable && snapshot.config.configurable.checkpoint_id);
  if (cpId) $("#stateLabel").textContent = `StateSnapshot · ${cpId.slice(0, 10)}`;
}

async function loadHistory() {
  if (!state.threadId) return;
  try {
    const history = await api(`/v1/threads/${state.threadId}/history`);
    state.history = history;
    $("#histBadge").textContent = history.length;
    renderHistory(history);
    renderCheckpointSelect(history);
  } catch (_) {}
}

function renderCheckpointSelect(history) {
  const select = $("#checkpointSelect");
  select.innerHTML = "";
  if (!history.length) { select.innerHTML = "<option>—</option>"; return; }
  history.forEach((snapshot, i) => {
    const cp = snapshot.config?.configurable?.checkpoint_id;
    const option = document.createElement("option");
    option.value = cp || "";
    const step = snapshot.metadata?.step ?? (history.length - 1 - i);
    option.textContent = `step ${step}${cp ? " · " + cp.slice(0, 8) : ""}`;
    select.appendChild(option);
  });
}

function renderHistory(history) {
  const view = $("#historyView");
  if (!history.length) { view.innerHTML = '<div class="empty-hint">暂无历史</div>'; return; }
  view.innerHTML = "";
  history.forEach((snapshot, i) => {
    const cp = snapshot.config?.configurable?.checkpoint_id;
    const step = snapshot.metadata?.step ?? (history.length - 1 - i);
    const next = (snapshot.next || []).join(", ") || "—";
    const item = document.createElement("div");
    item.className = `history-item ${i === 0 ? "current" : ""}`;
    item.innerHTML = `<div><b>step ${step}</b><br><small>next: ${escape(next)}</small></div>` +
      `<small>${cp ? cp.slice(0, 10) : ""}</small>`;
    item.addEventListener("click", () => { renderState(snapshot); toast(`已加载 step ${step}`); });
    view.appendChild(item);
  });
}

/* ---------- JSON highlighting ---------- */

function highlight(json) {
  return escape(json).replace(
    /("(\\u[a-zA-Z0-9]{4}|\\[^u]|[^\\"])*"(\s*:)?|\b(true|false|null)\b|-?\d+(\.\d+)?([eE][+-]?\d+)?)/g,
    (match) => {
      let cls = "num";
      if (/^"/.test(match)) cls = /:$/.test(match) ? "key" : "str";
      else if (/true|false/.test(match)) cls = "bool";
      else if (/null/.test(match)) cls = "null";
      return `<span class="${cls}">${match}</span>`;
    });
}

/* ---------- helpers ---------- */

function escape(value) {
  const el = document.createElement("span");
  el.textContent = value == null ? "" : String(value);
  return el.innerHTML;
}
function short(text, n = 90) {
  if (!text) return "";
  return text.length > n ? text.slice(0, n) + "…" : text;
}
function time(iso) {
  if (!iso) return "";
  const d = new Date(iso);
  return isNaN(d) ? "" : d.toLocaleTimeString("zh-CN", { hour12: false }) +
    "." + String(d.getMilliseconds()).padStart(3, "0");
}

/* ---------- wiring ---------- */

$("#runButton").addEventListener("click", runGraph);
$("#cancelButton").addEventListener("click", cancelRun);
$("#resumeButton").addEventListener("click", resumeRun);
$("#refreshBtn").addEventListener("click", () => { state.selectedNode = null; loadStructure().then(() => toast("结构已刷新")); });
$("#themeBtn").addEventListener("click", () => document.body.classList.toggle("light"));
$("#ndClose").addEventListener("click", () => { $("#nodeDetail").hidden = true; state.selectedNode = null; renderGraph(state.structure); });
$("#copyButton").addEventListener("click", async () => {
  await navigator.clipboard?.writeText($("#stateCode").innerText);
  toast("已复制状态快照");
});
$("#xrayToggle").addEventListener("change", (e) => { state.xray = e.target.checked; state.drill = []; loadStructure(); });
$("#graphSelect").addEventListener("change", (e) => {
  const [id, version] = e.target.value.split("::");
  const graph = state.graphs.find((g) => g.id === id && g.version === version);
  state.assistantId = null; state.threadId = null; state.drill = []; state.selectedNode = null;
  if (graph) selectGraph(graph);
});
$("#checkpointSelect").addEventListener("change", (e) => loadState(e.target.value || undefined));
$("#inputBox").addEventListener("keydown", (e) => { if (e.key === "Enter" && (e.metaKey || e.ctrlKey)) runGraph(); });

$$(".trace-tabs button").forEach((button) => button.addEventListener("click", () => {
  $$(".trace-tabs button").forEach((b) => b.classList.remove("active"));
  button.classList.add("active");
  const tab = button.dataset.trace;
  $("#traceList").classList.toggle("hidden", tab !== "timeline");
  $("#eventList").classList.toggle("hidden", tab !== "events");
  $("#errorList").classList.toggle("hidden", tab !== "errors");
}));
$$(".inspector-tabs button").forEach((button) => button.addEventListener("click", () => {
  $$(".inspector-tabs button").forEach((b) => b.classList.remove("active"));
  button.classList.add("active");
  const tab = button.dataset.tab;
  $("#stateView").classList.toggle("hidden", tab !== "state");
  $("#historyView").classList.toggle("hidden", tab !== "history");
  $("#explainView").classList.toggle("hidden", tab !== "explain");
}));

window.addEventListener("resize", () => { if (state.structure) renderGraph(state.structure); });
boot();
