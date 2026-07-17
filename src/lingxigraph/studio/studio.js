const $ = (selector) => document.querySelector(selector);
const $$ = (selector) => [...document.querySelectorAll(selector)];
const demoNodes = ["__start__", "understand", "decide", "tools", "answer", "__end__"];

function toast(message) {
  const el = $("#toast"); el.textContent = message; el.classList.add("show");
  setTimeout(() => el.classList.remove("show"), 1500);
}

async function api(path) {
  const response = await fetch(path, {headers: {"x-tenant-id": "local", "x-roles": "viewer,developer,operator"}});
  if (!response.ok) throw new Error(`${response.status}`);
  return response.json();
}

async function connectLocal() {
  try {
    const graphs = await api("/v1/graphs");
    if (!graphs.length) return;
    const graph = graphs[0];
    $("#graphName").textContent = graph.id;
    document.querySelector(".version").textContent = `v${graph.version}`;
    const structure = await api(`/v1/graphs/${encodeURIComponent(graph.id)}/structure?graph_version=${encodeURIComponent(graph.version)}`);
    $("#nodeCount").textContent = structure.nodes.length;
    $("#edgeCount").textContent = structure.edges.length;
    renderStructure(structure);
  } catch (_) {
    // The detailed demo remains useful when auth is configured or no run exists yet.
  }
}

function renderStructure(structure) {
  const canvas = $("#graphCanvas");
  if (!structure.nodes.length || structure.nodes.map(n => n.id).join() === demoNodes.join()) return;
  canvas.querySelectorAll(".node,.edge,.edge-label").forEach(el => el.remove());
  const count = structure.nodes.length;
  structure.nodes.forEach((node, index) => {
    const button = document.createElement("button");
    const terminal = node.id === "__start__" || node.id === "__end__";
    button.className = `node ${node.id === "__start__" ? "start" : node.id === "__end__" ? "end" : index === 1 ? "complete" : index === 2 ? "running" : "idle"}`;
    button.dataset.node = node.id;
    button.style.setProperty("--x", `${8 + (84 * index / Math.max(1, count - 1))}%`);
    button.style.setProperty("--y", `${43 + ((index % 2) ? -8 : 8)}%`);
    button.innerHTML = `<span>${terminal ? (node.id === "__start__" ? "→" : "■") : "◇"}</span><label>${escapeHtml(node.id)}${terminal ? "" : `<small>${node.is_subgraph ? "Subgraph" : "Graph Node"}</small>`}</label>`;
    canvas.appendChild(button);
  });
  bindNodes();
}

function escapeHtml(value) { const el = document.createElement("span"); el.textContent = value; return el.innerHTML; }

function bindNodes() {
  $$(".node").forEach(node => node.addEventListener("click", () => {
    $$(".node").forEach(n => n.style.outline = "");
    node.style.outline = "2px solid rgba(139,124,246,.35)";
    const name = node.dataset.node;
    $("#nodeDetail b").textContent = name;
    $("#nodeDetail small").textContent = `${name} · graph node`;
  }));
}

function simulateRun() {
  const button = $("#runButton"); button.disabled = true; button.textContent = "运行中…";
  $("#runStatus").textContent = "运行中"; $("#totalTime").textContent = "0.00s";
  const start = performance.now(); let stage = 0;
  $$(".trace-item").forEach((item, index) => { item.className = `trace-item ${index ? "pending" : "active"}`; });
  const timer = setInterval(() => { $("#totalTime").textContent = `${((performance.now()-start)/1000).toFixed(2)}s`; }, 40);
  const advance = () => {
    const items = $$(".trace-item");
    if (stage > 0) items[stage - 1].className = "trace-item done";
    if (stage < items.length) { items[stage].className = "trace-item active"; stage += 1; setTimeout(advance, 520 + Math.random()*420); }
    else { clearInterval(timer); $("#runStatus").textContent = "已完成"; button.disabled = false; button.innerHTML = "<span>▶</span> 再次运行"; toast("Graph 运行完成"); }
  };
  setTimeout(advance, 420);
}

$("#themeBtn").addEventListener("click", () => document.body.classList.toggle("light"));
$("#runButton").addEventListener("click", simulateRun);
$("#pauseButton").addEventListener("click", (event) => { event.currentTarget.textContent = event.currentTarget.textContent.includes("暂停") ? "▶ 继续追踪" : "Ⅱ 暂停追踪"; toast("追踪状态已切换"); });
$("#copyButton").addEventListener("click", async () => { await navigator.clipboard?.writeText($("#stateCode").innerText); toast("已复制状态快照"); });
$("#nodeDetail > button").addEventListener("click", () => $("#nodeDetail").style.display = "none");
$$('.inspector-tabs button').forEach(button => button.addEventListener('click', () => {
  $$('.inspector-tabs button').forEach(b => b.classList.remove('active')); button.classList.add('active');
  ['state','diff','history'].forEach(tab => $(`#${tab}View`).classList.toggle('hidden', tab !== button.dataset.tab));
}));
bindNodes(); connectLocal();
