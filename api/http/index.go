package http

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
)

// indexHTML is the embedded monitor page. {{MONITOR_PATH}} is substituted
// with the configured monitor WebSocket path at route-registration time.
// Log/analysis text is untrusted and is only ever rendered via
// textContent/createTextNode — never innerHTML.
const indexHTML = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>heimdall</title>
<style>
  :root {
    --bg: #1a1a2e; --bg2: #16213e; --fg: #e0e0e0; --fg-dim: #888;
    --accent: #0f3460; --blue: #53a8ff; --yellow: #e2b93d;
    --red: #e74c3c; --green: #2ecc71;
    --mono: 'SF Mono', 'Fira Code', 'Cascadia Code', monospace;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  html, body { height: 100%; }
  body { background: var(--bg); color: var(--fg); font-family: var(--mono); font-size: 13px; display: flex; flex-direction: column; }
  header { display: flex; align-items: center; gap: 10px; padding: 8px 14px; background: var(--bg2); border-bottom: 1px solid var(--accent); }
  header h1 { font-size: 14px; color: var(--blue); }
  #conn { width: 9px; height: 9px; border-radius: 50%; background: var(--red); flex: none; }
  #conn.up { background: var(--green); }
  #banner { color: var(--red); flex: 1; text-align: right; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
  main { flex: 1; display: flex; min-height: 0; }
  .pane { flex: 1; display: flex; flex-direction: column; min-width: 0; }
  .pane + .pane { border-left: 1px solid var(--accent); }
  h2 { font-size: 11px; text-transform: uppercase; color: var(--fg-dim); padding: 6px 12px; background: var(--bg2); flex: none; }
  .scroll { flex: 1; overflow: auto; padding: 8px 12px; }
  #logs div { white-space: pre-wrap; word-break: break-all; }
  .sev-error, .sev-fatal { color: var(--red); }
  .sev-warn, .sev-warning { color: var(--yellow); }
  .meta { color: var(--fg-dim); }
  #queuebox { border-bottom: 1px solid var(--accent); max-height: 40%; display: flex; flex-direction: column; }
  #controls { display: flex; gap: 8px; padding: 8px 12px; background: var(--bg2); border-top: 1px solid var(--accent); align-items: center; flex: none; }
  button { background: var(--accent); color: var(--fg); border: 1px solid var(--blue); padding: 5px 14px; font-family: var(--mono); font-size: 12px; cursor: pointer; border-radius: 3px; }
  button:disabled { opacity: 0.35; cursor: default; }
  #status { color: var(--yellow); white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
  #state { color: var(--blue); margin-left: auto; text-transform: uppercase; flex: none; }
  #analysis { word-break: break-word; }
  #analysis .mdseg { white-space: pre-wrap; }
  #analysis .tline { color: var(--fg-dim); margin: 8px 0; white-space: pre-wrap; }
  #analysis .tline.terr { color: var(--red); }
  #analysis .tline.tok { color: var(--green); }
  #analysis pre { background: var(--bg2); border: 1px solid var(--accent); border-radius: 3px; padding: 8px 10px; margin: 6px 0; overflow-x: auto; white-space: pre; }
  #analysis code { background: var(--bg2); border-radius: 2px; padding: 0 3px; color: var(--yellow); }
  #analysis .mdh { color: var(--blue); font-weight: bold; margin: 10px 0 4px; }
  #analysis strong { color: #fff; }
  .qrow { display: flex; align-items: center; gap: 8px; padding: 4px 0; border-bottom: 1px solid var(--accent); }
  .qrow .qmeta { color: var(--fg-dim); flex: none; }
  .qrow .qerr { flex: 1; color: var(--red); white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
  .qrow.running { opacity: 0.7; }
  .qrow button { padding: 2px 8px; font-size: 11px; }
  #queue .empty { color: var(--fg-dim); padding: 4px 0; }
</style>
</head>
<body>
<header>
  <h1>heimdall</h1><span id="conn"></span><span id="banner"></span>
</header>
<main>
  <section class="pane">
    <h2>logs</h2>
    <div id="logs" class="scroll"></div>
  </section>
  <section class="pane">
    <div id="queuebox">
      <h2>queue</h2>
      <div id="queue" class="scroll"></div>
    </div>
    <h2>analysis</h2>
    <div class="scroll"><div id="analysis"></div></div>
    <div id="controls">
      <button id="reset">reset</button>
      <button id="cancel" disabled>cancel</button>
      <span id="status"></span>
      <span id="state">idle</span>
    </div>
  </section>
</main>
<script>
"use strict";
var MONITOR_PATH = "{{MONITOR_PATH}}";
var MAX_ROWS = 2000;
var ws = null, runningId = null, retryMs = 1000;

function el(id) { return document.getElementById(id); }
function banner(msg) { el("banner").textContent = msg; }

function fmtRow(e) {
  var row = document.createElement("div");
  var sev = (e.severity || "").toLowerCase();
  if (sev) row.className = "sev-" + sev;
  var meta = document.createElement("span");
  meta.className = "meta";
  var ts = e.timestamp ? new Date(e.timestamp).toLocaleTimeString() : "";
  meta.textContent = ts + " " + (e.service || e.source || "") + " ";
  row.appendChild(meta);
  row.appendChild(document.createTextNode(
    (sev ? sev.toUpperCase() + " " : "") + (e.message || "")));
  return row;
}

function nearBottom(box) {
  return box.scrollHeight - box.scrollTop - box.clientHeight < 40;
}

function appendRow(box, node) {
  var stick = nearBottom(box);
  box.appendChild(node);
  while (box.childElementCount > MAX_ROWS) box.firstElementChild.remove();
  if (stick) box.scrollTop = box.scrollHeight;
}

function send(frame) {
  if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(frame));
  else banner("not connected");
}

// --- Analysis transcript -------------------------------------------------
// The pane is a sequence of blocks, TUI-style: markdown text segments from
// claude_delta chunks, interleaved with dim tool/status lines from
// claude_status. All rendering is DOM-built (textContent/createTextNode) —
// analyzer output is untrusted, never use innerHTML. The page is a Go raw
// string, so the backtick is built with fromCharCode instead of a literal.
var BT = String.fromCharCode(96);
var FENCE = BT + BT + BT;
var curSeg = null, curBuf = "";

function analysisAppend(node) {
  var box = el("analysis").parentElement;
  var stick = nearBottom(box);
  el("analysis").appendChild(node);
  if (stick) box.scrollTop = box.scrollHeight;
}

function clearAnalysis() {
  el("analysis").replaceChildren();
  curSeg = null;
  curBuf = "";
}

// Inline formatting: **bold** and inline code spans, built as DOM nodes.
function inlineCode(node, s) {
  s.split(BT).forEach(function (seg, i) {
    if (i % 2 === 1) {
      var c = document.createElement("code");
      c.textContent = seg;
      node.appendChild(c);
    } else if (seg) {
      node.appendChild(document.createTextNode(seg));
    }
  });
}

function inlineInto(node, s) {
  s.split("**").forEach(function (seg, i) {
    if (i % 2 === 1) {
      var b = document.createElement("strong");
      inlineCode(b, seg);
      node.appendChild(b);
    } else {
      inlineCode(node, seg);
    }
  });
}

// Minimal streaming markdown: fenced code blocks, #-headers, bullets,
// bold, inline code. Re-renders the whole segment on each delta so
// tokens split across chunks settle once the closing marker arrives.
function renderMarkdown(text, container) {
  container.replaceChildren();
  text.split(FENCE).forEach(function (part, i) {
    if (i % 2 === 1) {
      var pre = document.createElement("pre");
      var nl = part.indexOf("\n");
      // Drop a short language tag on the fence line (e.g. "go").
      pre.textContent = nl >= 0 && nl <= 15 ? part.slice(nl + 1) : part;
      container.appendChild(pre);
      return;
    }
    part.split("\n").forEach(function (line) {
      var div = document.createElement("div");
      var h = line.match(/^(#{1,4})\s+(.*)$/);
      if (h) {
        div.className = "mdh";
        inlineInto(div, h[2]);
      } else if (/^\s*[-*]\s+/.test(line)) {
        inlineInto(div, line.replace(/^(\s*)[-*]\s+/, "$1• "));
      } else {
        inlineInto(div, line);
      }
      container.appendChild(div);
    });
  });
}

// A status line ends the current text segment so the next delta starts a
// fresh block — that is what interleaves text with tool activity.
function appendStatusLine(msg, cls) {
  curSeg = null;
  curBuf = "";
  var d = document.createElement("div");
  d.className = "tline" + (cls ? " " + cls : "");
  d.textContent = "⏺ " + msg;
  analysisAppend(d);
}

function appendDelta(text) {
  if (!curSeg) {
    curSeg = document.createElement("div");
    curSeg.className = "mdseg";
    curBuf = "";
    analysisAppend(curSeg);
  }
  curBuf += text;
  var box = el("analysis").parentElement;
  var stick = nearBottom(box);
  renderMarkdown(curBuf, curSeg);
  if (stick) box.scrollTop = box.scrollHeight;
}

function setState(s) { el("state").textContent = s; }

function refreshButtons() {
  var busy = runningId !== null;
  el("cancel").disabled = !busy;
  document.querySelectorAll("#queue .qrow").forEach(function (row) {
    var isRunning = row.dataset.id === runningId;
    row.classList.toggle("running", isRunning);
    row.querySelector(".qprocess").disabled = busy;
    row.querySelector(".qdelete").disabled = isRunning || row.dataset.deleting === "1";
  });
  setState(busy ? "processing" : "idle");
}

function renderEmpty() {
  if (el("queue").childElementCount === 0) {
    var d = document.createElement("div");
    d.className = "empty";
    d.textContent = "no batches";
    el("queue").appendChild(d);
  }
}

function addQueueRow(id, batch) {
  if (!id || el("queue").querySelector('[data-id="' + CSS.escape(id) + '"]')) return;
  var empty = el("queue").querySelector(".empty");
  if (empty) empty.remove();

  var row = document.createElement("div");
  row.className = "qrow";
  row.dataset.id = id;

  var meta = document.createElement("span");
  meta.className = "qmeta";
  var ts = batch && batch.timestamp ? new Date(batch.timestamp).toLocaleTimeString() : "";
  meta.textContent = ts + " " + (batch && batch.service || "") + " (" + (batch && batch.count || 0) + ")";
  row.appendChild(meta);

  var err = document.createElement("span");
  err.className = "qerr";
  err.textContent = batch && batch.first_error || id;
  row.appendChild(err);

  var proc = document.createElement("button");
  proc.className = "qprocess";
  proc.textContent = "process";
  proc.onclick = function () {
    send({ type: "decide", batch_id: id, action: "process" });
    runningId = id;
    clearAnalysis();
    el("status").textContent = "running";
    refreshButtons();
  };
  row.appendChild(proc);

  var del = document.createElement("button");
  del.className = "qdelete";
  del.textContent = "✕";
  del.onclick = function () {
    del.disabled = true;
    row.dataset.deleting = "1";
    send({ type: "decide", batch_id: id, action: "clear" });
  };
  row.appendChild(del);

  el("queue").appendChild(row);
  refreshButtons();
}

function removeQueueRow(id) {
  var row = el("queue").querySelector('[data-id="' + CSS.escape(id) + '"]');
  if (row) row.remove();
  if (id === runningId) runningId = null;
  renderEmpty();
  refreshButtons();
}

function wipe() {
  el("logs").replaceChildren();
  el("queue").replaceChildren();
  clearAnalysis();
  el("status").textContent = "";
  runningId = null;
  renderEmpty();
  refreshButtons();
}

var noticeTimer = null;
function notice(msg) {
  banner(msg);
  if (noticeTimer) clearTimeout(noticeTimer);
  noticeTimer = setTimeout(function () { banner(""); }, 5000);
}

function handleFrame(f) {
  switch (f.type) {
    case "hello":
      if (f.version !== "2") banner("protocol version " + f.version + " (expected 2)");
      // Fresh attach: server replays the queue next. Old rows are stale.
      el("queue").replaceChildren();
      runningId = null;
      renderEmpty();
      refreshButtons();
      break;
    case "event":
      if (f.event) appendRow(el("logs"), fmtRow(f.event));
      break;
    case "batch_queued":
      addQueueRow(f.batch_id, f.batch);
      break;
    case "batch_removed":
      if (f.reason === "canceled") {
        // Canceled batch stays queued server-side; keep the row and
        // just unstick the buttons.
        runningId = null;
        refreshButtons();
      } else {
        removeQueueRow(f.batch_id);
      }
      break;
    case "notice":
      notice(f.msg || "");
      break;
    case "claude_status":
      var msg = (f.text || "").replace(/^\[[0-9a-f]+\]\s*/, "");
      el("status").textContent = msg;
      // Heartbeats stay in the status bar; everything else (tool use,
      // reasoning iterations, failures) becomes a transcript line.
      if (msg && !/^analysis running,/.test(msg)) {
        var cls = /failed|^error/.test(msg) ? "terr"
          : /^analysis completed/.test(msg) ? "tok" : "";
        appendStatusLine(msg, cls);
      }
      break;
    case "claude_delta":
      appendDelta(f.text || "");
      break;
    case "claude_done":
      el("status").textContent = f.summary ? "done" : "";
      if (f.summary && el("analysis").childElementCount === 0) {
        appendDelta(f.summary);
        curSeg = null;
        curBuf = "";
      }
      break;
    case "error":
      banner(f.msg || "server error");
      // A rejected decide leaves no batch_removed; unstick the buttons.
      runningId = null;
      refreshButtons();
      break;
    case "reset":
      wipe();
      break;
  }
}

el("reset").onclick = function () {
  send({ type: "reset" });
};
el("cancel").onclick = function () {
  if (runningId) send({ type: "decide", batch_id: runningId, action: "cancel" });
};
renderEmpty();

function connect() {
  var proto = location.protocol === "https:" ? "wss://" : "ws://";
  ws = new WebSocket(proto + location.host + MONITOR_PATH);
  ws.onopen = function () {
    retryMs = 1000;
    el("conn").classList.add("up");
    banner("");
  };
  ws.onmessage = function (m) {
    var f;
    try { f = JSON.parse(m.data); } catch (err) { return; }
    handleFrame(f);
  };
  ws.onclose = function () {
    el("conn").classList.remove("up");
    banner("disconnected (another monitor may be attached) - retrying");
    setTimeout(connect, retryMs);
    retryMs = Math.min(retryMs * 2, 30000);
  };
}
connect();
</script>
</body>
</html>
`

// HandleIndex serves the embedded monitor UI. monitorPath is the
// already-defaulted WebSocket path the page should dial.
func HandleIndex(monitorPath string) gin.HandlerFunc {
	page := []byte(strings.ReplaceAll(indexHTML, "{{MONITOR_PATH}}", monitorPath))
	return func(c *gin.Context) {
		c.Data(http.StatusOK, "text/html; charset=utf-8", page)
	}
}
