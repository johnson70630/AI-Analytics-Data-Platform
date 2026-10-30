const elements = {
  approvalPanel: document.querySelector("#approval-panel"),
  approvalSummary: document.querySelector("#approval-summary"),
  approveAction: document.querySelector("#approve-action"),
  backgroundMode: document.querySelector("#background-mode-value"),
  backgroundStatus: document.querySelector("#background-status"),
  chatLog: document.querySelector("#chat-log"),
  clearEvents: document.querySelector("#clear-events"),
  composer: document.querySelector("#composer"),
  copySession: document.querySelector("#copy-session"),
  emptyState: document.querySelector("#empty-state"),
  eventLog: document.querySelector("#event-log"),
  memoryMode: document.querySelector("#memory-mode-value"),
  memoryStatus: document.querySelector("#memory-status"),
  tracingMode: document.querySelector("#tracing-mode-value"),
  tracingStatus: document.querySelector("#tracing-status"),
  modelSelect: document.querySelector("#model-select"),
  newSession: document.querySelector("#new-session"),
  promptInput: document.querySelector("#prompt-input"),
  refreshConfig: document.querySelector("#refresh-config"),
  refreshSession: document.querySelector("#refresh-session"),
  rejectAction: document.querySelector("#reject-action"),
  rejectSession: document.querySelector("#reject-session"),
  resumeSession: document.querySelector("#resume-session"),
  runStatus: document.querySelector("#run-status"),
  sendButton: document.querySelector("#send-button"),
  sessionId: document.querySelector("#session-id"),
  sessionItems: document.querySelector("#session-items"),
  sessionList: document.querySelector("#session-list"),
  sessionPersistenceNote: document.querySelector("#session-persistence-note"),
  sessionMode: document.querySelector("#session-mode-value"),
  sessionStatus: document.querySelector("#session-status"),
  sessionStoreLabel: document.querySelector("#session-store-label"),
  streamingMode: document.querySelector("#streaming-mode-value"),
  streamingStatus: document.querySelector("#streaming-status"),
  viewerValue: document.querySelector("#viewer-value"),
  viewerAvatar: document.querySelector("#viewer-avatar"),
  brandSub: document.querySelector("#brand-sub"),
  sessionSearch: document.querySelector("#session-search-input"),
  eventFilters: document.querySelector("#event-filters"),
  eventDetail: document.querySelector("#event-detail"),
  eventDetailJson: document.querySelector("#event-detail-json"),
  copyEventDetail: document.querySelector("#copy-event-detail"),
  eventViewLogs: document.querySelector("#event-view-logs"),
  eventViewList: document.querySelector("#event-view-list"),
  leftCard: document.querySelector(".left-card"),
  capabilitiesBlock: document.querySelector("#capabilities-block"),
  memoryPane: document.querySelector("#memory-pane"),
  memoryBack: document.querySelector("#memory-back"),
  memoryRefresh: document.querySelector("#memory-refresh"),
  memoryStoreName: document.querySelector("#memory-store-name"),
  memoryStoreResource: document.querySelector("#memory-store-resource"),
  memoryActor: document.querySelector("#memory-actor-input"),
  memorySearch: document.querySelector("#memory-search-input"),
  memoryCount: document.querySelector("#memory-count"),
  memoryEntries: document.querySelector("#memory-entries"),
  memoryModal: document.querySelector("#memory-modal"),
  memoryModalTitle: document.querySelector("#memory-modal-title"),
  memoryModalBody: document.querySelector("#memory-modal-body"),
  memoryModalFoot: document.querySelector("#memory-modal-foot"),
  memoryModalClose: document.querySelector("#memory-modal-close"),
  sidebarToggle: document.querySelector("#sidebar-toggle"),
  evidenceToggle: document.querySelector("#evidence-toggle"),
  chatView: document.querySelector("#chat-view"),
  chatViewTab: document.querySelector("#chat-view-tab"),
  healthView: document.querySelector("#health-review-view"),
  healthViewTab: document.querySelector("#health-view-tab"),
  healthRefresh: document.querySelector("#health-refresh"),
  healthState: document.querySelector("#health-state"),
  pipelineHealthStatus: document.querySelector("#pipeline-health-status"),
  pipelineRunValue: document.querySelector("#pipeline-run-value"),
  pipelineCompletedValue: document.querySelector("#pipeline-completed-value"),
  pipelineTaskValue: document.querySelector("#pipeline-task-value"),
  pipelineFailedValue: document.querySelector("#pipeline-failed-value"),
  monitoringHealthStatus: document.querySelector("#monitoring-health-status"),
  healthPassCount: document.querySelector("#health-pass-count"),
  healthWarnCount: document.querySelector("#health-warn-count"),
  healthFailCount: document.querySelector("#health-fail-count"),
  healthTotalCount: document.querySelector("#health-total-count"),
  healthStaleStatus: document.querySelector("#health-stale-status"),
  monitoringRunValue: document.querySelector("#monitoring-run-value"),
  monitoringReferenceValue: document.querySelector("#monitoring-reference-value"),
  syntheticReferenceValue: document.querySelector("#synthetic-reference-value"),
  pipelineAssociationValue: document.querySelector("#pipeline-association-value"),
  healthGeneratedValue: document.querySelector("#health-generated-value"),
  healthSourceErrors: document.querySelector("#health-source-errors"),
  healthIssueList: document.querySelector("#health-issue-list"),
  runtimeTransport: document.querySelector("#runtime-transport"),
  runtimeSession: document.querySelector("#runtime-session"),
  runtimeModel: document.querySelector("#runtime-model"),
  runtimeSettings: document.querySelector("#runtime-settings"),
  runtimeDrawer: document.querySelector("#runtime-drawer"),
  runtimeDrawerClose: document.querySelector("#runtime-drawer-close"),
  railExpand: document.querySelector("#rail-expand"),
  railNewSession: document.querySelector("#rail-new-session"),
  railRuntime: document.querySelector("#rail-runtime"),
};

// The framework label shown in the header subtitle, next to the host.
const AGENT_FRAMEWORK = "LangGraph";

const SESSION_STORAGE_KEY = "databricks-agentbricks-session-id";
const CONVERSATION_INDEX_STORAGE_KEY = "dataops-conversation-index-v1";
const conversationIndex = window.DataOpsConversationIndex;

if (!conversationIndex) throw new Error("Conversation index helpers failed to load.");

function newSessionId() {
  return crypto.randomUUID();
}

const initialSessionId = localStorage.getItem(SESSION_STORAGE_KEY) || newSessionId();
const initialConversations = conversationIndex.parseConversationIndex(
  localStorage.getItem(CONVERSATION_INDEX_STORAGE_KEY),
  initialSessionId,
);

const state = {
  busy: false,
  config: null,
  draft: null,
  draftText: "",
  events: [],
  eventSeq: 0,
  eventView: "logs",
  eventFilter: "all",
  selectedEventId: null,
  sessionFilter: "",
  sessions: initialConversations,
  unavailableSessions: new Set(),
  sessionEpoch: 0,
  instanceId: null,
  lastAssistantText: "",
  managedSessionId: "",
  model: "",
  mode: "streaming",
  pendingInterrupt: null,
  pendingInterruptSessionId: "",
  sessionId: initialSessionId,
  healthLoaded: false,
  healthLoading: false,
};

function ensureSessionId() {
  if (!state.sessionId) throw new Error("The routing session is not initialized yet.");
  return state.sessionId;
}

function persistConversationIndex() {
  try {
    localStorage.setItem(
      CONVERSATION_INDEX_STORAGE_KEY,
      conversationIndex.serializeConversationIndex(state.sessions),
    );
  } catch (error) {
    console.warn("Conversation metadata could not be persisted.", error);
  }
}

function registerConversation(sessionId, changes = {}) {
  state.sessions = conversationIndex.upsertConversation(state.sessions, sessionId, changes);
  persistConversationIndex();
  renderSessionList();
  return state.sessions.find((item) => item.session_id === sessionId);
}

function setSessionId(value) {
  const nextSessionId = String(value || "");
  if (!nextSessionId) return;
  if (state.sessionId !== nextSessionId) {
    state.managedSessionId = "";
    state.sessionEpoch += 1;
  }
  state.sessionId = nextSessionId;
  localStorage.setItem(SESSION_STORAGE_KEY, state.sessionId);
  elements.sessionId.textContent = state.sessionId;
  registerConversation(state.sessionId);
}

setSessionId(state.sessionId);

function demoUrl(path, sessionId = ensureSessionId()) {
  const url = new URL(path, window.location.origin);
  url.searchParams.set("session_id", sessionId);
  return `${url.pathname}${url.search}`;
}

function setStatus(label, type = "ready") {
  elements.runStatus.textContent = label;
  elements.runStatus.className = `run-status ${type === "ready" ? "" : type}`.trim();
}

// The send button is enabled only when there is text to send and no run is in flight.
function updateSendState() {
  elements.sendButton.disabled = state.busy || !elements.promptInput.value.trim();
}

function setBusy(busy, label = "Working") {
  state.busy = busy;
  elements.chatLog.setAttribute("aria-busy", String(busy));
  updateSendState();
  elements.promptInput.disabled = busy;
  elements.approveAction.disabled = busy;
  elements.rejectAction.disabled = busy;
  elements.newSession.disabled = busy;
  elements.railNewSession.disabled = busy;
  elements.modelSelect.disabled = busy || elements.modelSelect.options.length <= 1;
  elements.refreshSession.disabled = busy || !state.config?.session.history;
  elements.resumeSession.disabled = busy || !state.config?.session.durable;
  elements.rejectSession.disabled = busy || !state.config?.session.durable;
  document.querySelectorAll(".session-open-button").forEach((button) => {
    button.disabled = busy;
  });
  if (busy) setStatus(label, "busy");
  else if (!elements.runStatus.classList.contains("error")) setStatus("Ready");
}

// Human-readable name for each orb color, shown in the expanded capability detail.
const CAPABILITY_SUMMARY = { enabled: "Available", degraded: "Limited", disabled: "Unavailable" };

// Populate a capability's status orb and its click-to-expand detail panel.
// `link` (optional) renders an external link in the detail, e.g. to an MLflow experiment.
function setCapability(element, state, detail, command, link) {
  const key = state === true ? "enabled" : state === "degraded" ? "degraded" : "disabled";
  element.classList.toggle("enabled", key === "enabled");
  element.classList.toggle("degraded", key === "degraded");
  element.classList.toggle("disabled", key === "disabled");
  element.setAttribute("role", "img");
  element.setAttribute("aria-label", `Status: ${CAPABILITY_SUMMARY[key]}`);
  const item = element.closest(".capability-item");
  const panel = item?.querySelector(".capability-detail");
  if (!panel) return;

  panel.className = `capability-detail ${key}`;
  panel.hidden = !item.classList.contains("expanded");
  panel.replaceChildren();

  const status = document.createElement("span");
  status.className = "cap-status";
  status.textContent = CAPABILITY_SUMMARY[key];
  panel.append(status);

  if (detail) {
    const description = document.createElement("span");
    description.textContent = detail;
    panel.append(description);
  }
  if (command) {
    const label = document.createElement("span");
    label.className = "cap-command-label";
    label.textContent = "Connect by running:";
    const row = document.createElement("div");
    row.className = "cap-command";
    const code = document.createElement("code");
    code.textContent = command;
    const copy = document.createElement("button");
    copy.type = "button";
    copy.setAttribute("aria-label", `Copy command: ${command}`);
    copy.innerHTML =
      '<svg class="icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="9" y="9" width="12" height="12" rx="2"/><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/></svg>';
    copy.addEventListener("click", (event) => {
      event.stopPropagation();
      void navigator.clipboard.writeText(command).catch(() => {});
    });
    row.append(code, copy);
    panel.append(label, row);
  }
  if (link) {
    const anchor = document.createElement("a");
    anchor.className = "cap-link";
    anchor.href = link.href;
    anchor.target = "_blank";
    anchor.rel = "noopener noreferrer";
    anchor.append(
      iconSvg(
        "icon",
        '<path d="M15 3h6v6"/><path d="M10 14 21 3"/><path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"/>',
      ),
    );
    const label = document.createElement("span");
    label.textContent = link.label;
    anchor.append(label);
    panel.append(anchor);
  }
}

function formatJson(value) {
  try {
    return JSON.stringify(value, null, 2);
  } catch {
    return String(value);
  }
}

function escapeHtml(text) {
  return text.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}

// Basic JSON syntax highlighting: wraps tokens in <span> classes for coloring.
// The payload is HTML-escaped first, so runtime data can't inject markup.
function highlightJson(value) {
  const json = escapeHtml(formatJson(value));
  return json.replace(
    /("(?:\\.|[^"\\])*"(\s*:)?|\b(?:true|false)\b|\bnull\b|-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)/g,
    (match) => {
      let cls = "json-num";
      if (match.startsWith("&quot;") || match.startsWith('"')) {
        cls = /:\s*$/.test(match) ? "json-key" : "json-str";
      } else if (match === "true" || match === "false") {
        cls = "json-bool";
      } else if (match === "null") {
        cls = "json-null";
      }
      return `<span class="${cls}">${match}</span>`;
    },
  );
}

// Bucket an event type into one of the filter categories used by the list view.
function eventCategory(type) {
  const value = String(type || "").toLowerCase();
  if (value === "error" || value.endsWith(".error")) return "error";
  if (value.startsWith("session")) return "sessions";
  if (value.startsWith("memory")) return "memory";
  if (
    value.startsWith("background") ||
    value.startsWith("model") ||
    value.startsWith("run") ||
    ["response", "delta", "message", "interrupt", "runtime.config"].includes(value)
  ) {
    return "invocation";
  }
  return "system";
}

// A short one-line summary for the list view, derived from the payload.
function eventSummary(type, payload) {
  if (payload && typeof payload === "object") {
    for (const key of ["message", "summary", "detail", "status", "error"]) {
      if (typeof payload[key] === "string" && payload[key]) return payload[key];
    }
  }
  return String(type || "event");
}

function eventRunId(payload) {
  if (payload && typeof payload === "object") {
    return payload.run_id || payload.runId || payload.id || payload.invocation_id || "";
  }
  return "";
}

function addEvent(type, payload) {
  state.events.unshift({
    id: `evt-${(state.eventSeq += 1)}`,
    type,
    category: eventCategory(type),
    summary: eventSummary(type, payload),
    runId: eventRunId(payload),
    payload,
    at: new Date(),
  });
  state.events = state.events.slice(0, 80);
  renderEvents();
}

function eventEmpty() {
  const wrap = document.createElement("div");
  wrap.className = "event-empty";
  wrap.innerHTML =
    '<svg class="icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><path d="M14 2v6h6"/></svg>' +
    "<strong>No events in this session</strong>" +
    "<span>Start an agent run to collect runtime evidence.</span>";
  return wrap;
}

function renderEventDetail() {
  const event = state.events.find((item) => item.id === state.selectedEventId);
  const show = state.eventView === "list" && Boolean(event);
  elements.eventDetail.hidden = !show;
  if (show) elements.eventDetailJson.innerHTML = highlightJson(event.payload);
}

function renderEvents() {
  const listView = state.eventView === "list";
  elements.eventFilters.hidden = !listView;
  elements.eventLog.classList.toggle("list-view", listView);
  elements.eventLog.classList.toggle("logs-view", !listView);
  elements.eventLog.replaceChildren();

  const events = listView
    ? state.events.filter((event) => state.eventFilter === "all" || event.category === state.eventFilter)
    : state.events;

  if (!events.length) {
    elements.eventLog.append(eventEmpty());
    renderEventDetail();
    return;
  }

  if (!listView) {
    for (const event of events) {
      const entry = document.createElement("div");
      entry.className = "event-entry";
      const header = document.createElement("div");
      header.className = "event-entry-header";
      const name = document.createElement("span");
      name.className = `event-type cat-${event.category}`;
      name.textContent = event.type;
      const time = document.createElement("span");
      time.className = "event-time";
      time.textContent = event.at.toLocaleTimeString();
      const body = document.createElement("pre");
      body.innerHTML = highlightJson(event.payload);
      header.append(name, time);
      entry.append(header, body);
      elements.eventLog.append(entry);
    }
    renderEventDetail();
    return;
  }

  for (const event of events) {
    const row = document.createElement("button");
    row.type = "button";
    row.className = `event-row${event.id === state.selectedEventId ? " selected" : ""}`;
    const top = document.createElement("div");
    top.className = "event-row-top";
    const type = document.createElement("span");
    type.className = `event-row-type cat-${event.category}`;
    type.textContent = event.type;
    const time = document.createElement("span");
    time.className = "event-row-time";
    time.textContent = event.at.toLocaleTimeString();
    top.append(type, time);
    const summary = document.createElement("div");
    summary.className = "event-row-summary";
    summary.textContent = event.summary;
    row.append(top, summary);
    if (event.runId) {
      const run = document.createElement("div");
      run.className = "event-row-run";
      run.textContent = event.runId;
      row.append(run);
    }
    row.addEventListener("click", () => {
      state.selectedEventId = event.id;
      renderEvents();
    });
    elements.eventLog.append(row);
  }
  renderEventDetail();
}

function setEventView(view) {
  state.eventView = view;
  elements.eventViewLogs.classList.toggle("active", view === "logs");
  elements.eventViewLogs.setAttribute("aria-pressed", String(view === "logs"));
  elements.eventViewList.classList.toggle("active", view === "list");
  elements.eventViewList.setAttribute("aria-pressed", String(view === "list"));
  renderEvents();
}

function normalizeRole(message) {
  const value = String(message?.role || message?.type || "assistant").toLowerCase();
  if (["human", "user"].includes(value)) return "user";
  if (["tool", "function"].includes(value)) return "tool";
  if (["system", "developer"].includes(value)) return "system";
  return "assistant";
}

function extractText(content) {
  if (content == null) return "";
  if (typeof content === "string") return content;
  if (content?.type) return extractText([content]);
  if (Array.isArray(content)) {
    return content
      .map((part) => {
        if (typeof part === "string") return part;
        // Reasoning is displayed separately; metadata never belongs in the answer.
        if (!["text", "input_text", "output_text", "refusal"].includes(part?.type)) return "";
        if (typeof part.text === "string") return part.text;
        if (typeof part.refusal === "string") return part.refusal;
        return "";
      })
      .filter(Boolean)
      .join("\n");
  }
  return typeof content === "object" ? formatJson(content) : String(content);
}

function extractReasoning(content) {
  return (Array.isArray(content) ? content : [content])
    .filter((part) => ["reasoning", "thinking"].includes(part?.type))
    .map((part) => {
      // Read only public text fields, never signature/encrypted_content or extras.
      const text = part.reasoning ?? part.thinking ?? part.text;
      if (typeof text === "string") return text;
      const parts = part.summary?.length ? part.summary : part.content;
      return (Array.isArray(parts) ? parts : [])
        .filter((item) => ["summary_text", "reasoning_text", "text"].includes(item?.type))
        .map((item) => typeof item.text === "string" ? item.text : "").join("\n");
    }).filter(Boolean).join("\n");
}

function renderReasoning(answer, reasoning, append = false) {
  if (!reasoning) return;
  let details = answer.parentElement.querySelector(".message-reasoning");
  if (!details) {
    details = document.createElement("details");
    details.className = "message-reasoning";
    details.innerHTML = '<summary>Reasoning</summary><div class="message-content"></div>';
    answer.before(details);
  }
  const body = details.lastElementChild;
  renderMessageText(body, (append ? body.dataset.source || "" : "") + reasoning);
}

function hideEmptyState() {
  elements.emptyState.hidden = true;
}

function initials(name) {
  const parts = String(name || "")
    .replace(/@.*/, "")
    .split(/[.\s_-]+/)
    .filter(Boolean);
  if (!parts.length) return "•";
  if (parts.length === 1) return parts[0].slice(0, 2).toUpperCase();
  return (parts[0][0] + parts[parts.length - 1][0]).toUpperCase();
}

function setViewer(name) {
  const label = name || "Local developer";
  elements.viewerValue.textContent = label;
  if (elements.viewerAvatar) elements.viewerAvatar.textContent = initials(label);
}

function formatClock(date = new Date()) {
  return date.toLocaleTimeString([], { hour: "numeric", minute: "2-digit" });
}

function messageName(role, label) {
  if (label) return label;
  if (role === "tool") return "Tool";
  if (role === "system") return "System";
  if (role === "error") return "Error";
  return "Agent";
}

// Hover actions under an agent message. "View trace" is intentionally omitted
// until the demo backend exposes a trace URL (per design feedback).
function buildMessageActions(textEl) {
  const actions = document.createElement("div");
  actions.className = "message-actions";
  const copy = document.createElement("button");
  copy.type = "button";
  copy.className = "message-action";
  copy.innerHTML =
    '<svg class="icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="9" y="9" width="12" height="12" rx="2"/><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/></svg><span>Copy</span>';
  copy.addEventListener("click", async () => {
    try {
      await navigator.clipboard.writeText(textEl.dataset.source ?? textEl.textContent ?? "");
    } catch {
      /* clipboard may be unavailable */
    }
    const span = copy.querySelector("span");
    if (span) {
      span.textContent = "Copied";
      setTimeout(() => {
        span.textContent = "Copy";
      }, 1200);
    }
  });
  actions.append(copy);
  return actions;
}

const markdown = window.markdownit({ html: false }).disable("image");

function renderMessageText(element, content) {
  element.dataset.source = content;
  element.innerHTML = markdown.render(content);
}

function appendMessage(role, content, label, { time } = {}) {
  hideEmptyState();
  const wrapper = document.createElement("article");
  wrapper.className = `message ${role}`;
  const text = document.createElement("div");
  text.className = "message-content";
  if (role === "assistant") renderMessageText(text, content);
  else text.textContent = content;

  if (role === "user") {
    wrapper.append(text);
  } else if (role === "tool") {
    const details = document.createElement("details");
    details.className = "tool-card";
    details.open = content.length < 700;
    const summary = document.createElement("summary");
    summary.textContent = label || "Tool evidence";
    details.append(summary, text);
    wrapper.append(details);
  } else {
    const head = document.createElement("div");
    head.className = "message-head";
    const name = document.createElement("span");
    name.className = "message-name";
    name.textContent = messageName(role, label);
    const stamp = document.createElement("span");
    stamp.className = "message-time";
    stamp.textContent = time || formatClock();
    head.append(name, stamp);
    wrapper.append(head, text);
    if (role === "assistant") wrapper.append(buildMessageActions(text));
  }

  elements.chatLog.append(wrapper);
  elements.chatLog.scrollTop = elements.chatLog.scrollHeight;
  return { wrapper, text };
}

function appendError(error) {
  const message = error instanceof Error ? error.message : String(error);
  appendMessage("error", message, "Request failed");
  setStatus("Error", "error");
  addEvent("error", { message });
}

function startDraft() {
  if (state.draft) return state.draft;
  const draft = appendMessage("assistant", "", "Agent");
  draft.wrapper.classList.add("streaming");
  state.draft = draft;
  state.draftText = "";
  return draft;
}

function appendDelta(content) {
  const text = extractText(content);
  const reasoning = extractReasoning(content);
  if (!text && !reasoning) return;
  const draft = startDraft();
  renderReasoning(draft.text, reasoning, true);
  state.draftText += text;
  state.lastAssistantText = state.draftText;
  renderMessageText(draft.text, state.draftText);
  elements.chatLog.scrollTop = elements.chatLog.scrollHeight;
}

function finishDraft(finalText = "") {
  if (!state.draft) return false;
  if (finalText) {
    state.draftText = finalText;
    state.lastAssistantText = finalText;
    renderMessageText(state.draft.text, finalText);
  }
  state.draft.wrapper.classList.remove("streaming");
  state.draft = null;
  return true;
}

function toolSummary(message) {
  if (message?.name) return `${message.name}\n${extractText(message.content)}`.trim();
  if (Array.isArray(message?.tool_calls) && message.tool_calls.length) {
    return message.tool_calls.map((call) => `${call.name || "tool"}(${formatJson(call.args || {})})`).join("\n");
  }
  return extractText(message?.content) || formatJson(message);
}

function handleAgentMessage(message) {
  const role = normalizeRole(message);
  if (role === "user") return;
  if (role === "assistant") {
    const text = extractText(message?.content);
    const reasoning = extractReasoning(message) || extractReasoning(message?.content);
    if (reasoning) renderReasoning(startDraft().text, reasoning);
    if (!text && !message?.tool_calls?.length) return;
    const finished = finishDraft(text);
    if (text && !finished) {
      state.lastAssistantText = text;
      appendMessage("assistant", text, "Agent");
    }
    if (message?.tool_calls?.length) appendMessage("tool", toolSummary(message), "Tool request");
    return;
  }
  appendMessage(role, toolSummary(message), role === "tool" ? "Tool result" : "System");
}

function interruptSummary(interrupt) {
  const requests = interrupt?.value?.action_requests || [];
  if (!requests.length) return "The agent paused and needs a decision.";
  return requests
    .map((request) => `${request.name || "tool"} ${formatJson(request.args || {})}`)
    .join(" · ");
}

function handleInterrupt(interrupt, sessionId = ensureSessionId()) {
  if (sessionId !== state.sessionId) return;
  finishDraft();
  state.pendingInterrupt = interrupt;
  state.pendingInterruptSessionId = sessionId;
  elements.approvalSummary.textContent = interruptSummary(interrupt);
  elements.approvalPanel.hidden = false;
  appendMessage("system", interruptSummary(interrupt), "Approval required");
}

function handleEvent(event, sessionId = ensureSessionId()) {
  if (sessionId !== state.sessionId) {
    addEvent("session.event.deferred", { session_id: sessionId, event_type: event?.type || "event" });
    return;
  }
  addEvent(event?.type || "event", event);
  if (event?.type === "delta") appendDelta(event.content);
  if (event?.type === "message") handleAgentMessage(event.message);
  if (event?.type === "interrupt") handleInterrupt(event, sessionId);
  if (event?.error) throw new Error(event.error);
}

function handleOutput(output, sessionId = ensureSessionId()) {
  if (sessionId !== state.sessionId) {
    addEvent("session.output.deferred", { session_id: sessionId });
    return;
  }
  for (const item of output || []) {
    if (item?.type === "interrupt") handleInterrupt(item, sessionId);
    else handleAgentMessage(item);
  }
  finishDraft();
}

function routingHeaders(sessionId = state.sessionId) {
  const headers = {};
  // The UI reuses its stable session value for routing affinity, but the header is routing-only.
  if (sessionId) headers["X-Routing-Key"] = sessionId;
  return headers;
}

function invocationHeaders(sessionId = state.sessionId) {
  return { "Content-Type": "application/json", ...routingHeaders(sessionId) };
}

function invocationPayload(payload, transport = {}, sessionId = ensureSessionId()) {
  const input = {
    actor: state.config?.session.actor || sessionId,
    ...payload,
  };
  if (state.model) input.model = state.model;
  return { id: newSessionId(), session_id: sessionId, input, ...transport };
}

function agentResult(result) {
  return result?.output && !Array.isArray(result.output) ? result.output : result;
}

async function jsonResponse(response) {
  const body = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(body.detail || body.error || `Request failed with ${response.status}`);
  return body;
}

function stateMessage(container, message, kind = "empty") {
  container.replaceChildren();
  const item = document.createElement("div");
  item.className = `state-${kind}`;
  item.textContent = message;
  container.append(item);
}

function renderStateItems(container, items, emptyMessage, renderItem) {
  container.replaceChildren();
  if (!items.length) {
    stateMessage(container, emptyMessage);
    return;
  }
  for (const value of items) {
    const item = document.createElement("article");
    item.className = "state-item";
    const rendered = renderItem(value);
    const title = document.createElement("strong");
    title.textContent = rendered.title;
    const content = document.createElement("p");
    content.textContent = rendered.content || "No content returned.";
    const meta = document.createElement("small");
    meta.textContent = rendered.meta || "";
    item.append(title, content, meta);
    container.append(item);
  }
}

function memoryEntries(payload) {
  return payload?.managed_memory_entries || [];
}

function sessionItems(payload) {
  return payload?.session_items || [];
}

function sessions(payload) {
  return payload?.sessions || [];
}

// ---- Memory pane -----------------------------------------------------------

const memoryState = { query: "", searchTimer: null };

function prettyDate(value) {
  if (!value) return "";
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return "";
  return d.toLocaleDateString([], { year: "numeric", month: "short", day: "numeric" });
}

function prettyDateTime(value) {
  if (!value) return "";
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return "";
  return d.toLocaleString([], {
    year: "numeric",
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  });
}

function iconSvg(className, inner) {
  const span = document.createElement("span");
  span.innerHTML = `<svg class="${className}" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">${inner}</svg>`;
  return span.firstChild;
}

// Store name = the trailing segment of "memory-stores/<name>"; resource = the full path.
function memoryStoreName() {
  const store = state.config?.memory.store || "";
  const parts = store.split("/").filter(Boolean);
  return parts.length ? parts[parts.length - 1] : "Memory store";
}

function currentMemoryActor() {
  return (elements.memoryActor.value || "").trim() || state.config?.memory.actor || "";
}

function memoryMessage(text, kind = "empty") {
  elements.memoryEntries.replaceChildren();
  const div = document.createElement("div");
  div.className = `memory-${kind}`;
  div.textContent = text;
  elements.memoryEntries.append(div);
  elements.memoryCount.textContent = "";
}

function renderMemoryCards(entries, query) {
  elements.memoryEntries.replaceChildren();
  elements.memoryCount.textContent = entries.length
    ? query
      ? `${entries.length} matching`
      : `${entries.length} of ${entries.length} entries`
    : "";
  if (!entries.length) {
    memoryMessage(query ? "No entries match your search." : "No memory entries for this actor yet.");
    return;
  }
  for (const entry of entries) {
    const card = document.createElement("button");
    card.type = "button";
    card.className = "memory-entry";
    const top = document.createElement("div");
    top.className = "memory-entry-top";
    const path = document.createElement("span");
    path.className = "memory-entry-path";
    path.textContent = entry.path || entry.name || "Memory entry";
    top.append(path, iconSvg("memory-entry-expand icon", '<path d="M7 7h10v10"/><path d="M7 17 17 7"/>'));
    const preview = document.createElement("p");
    preview.className = "memory-entry-preview";
    preview.textContent = extractText(entry.content) || entry.description || "";
    const foot = document.createElement("div");
    foot.className = "memory-entry-foot";
    const actor = document.createElement("span");
    actor.className = "memory-entry-actor";
    actor.textContent = entry.actor_id || "";
    const time = document.createElement("span");
    time.className = "memory-entry-time";
    time.textContent = prettyDate(entry.update_time || entry.create_time);
    foot.append(actor, time);
    card.append(top, preview, foot);
    card.addEventListener("click", () => openMemoryModal(entry));
    elements.memoryEntries.append(card);
  }
}

async function loadMemory() {
  if (!state.config?.memory.enabled) {
    memoryMessage("Connect a Memory Store to browse entries: agentbricks memory bind <store-name>");
    return;
  }
  const query = memoryState.query.trim();
  const actor = currentMemoryActor();
  memoryMessage(query ? "Searching…" : "Loading memory…", "loading");
  try {
    let entries;
    if (query) {
      const response = await fetch(demoUrl("/api/demo/memory/search"), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ query, limit: 50, actor: actor || undefined }),
      });
      entries = memoryEntries(await jsonResponse(response));
    } else {
      const url = demoUrl("/api/demo/memory/entries") + (actor ? `&actor=${encodeURIComponent(actor)}` : "");
      const response = await fetch(url, { cache: "no-store" });
      entries = memoryEntries(await jsonResponse(response));
    }
    renderMemoryCards(entries, query);
    addEvent(query ? "memory.entries.search" : "memory.entries.list", {
      count: entries.length,
      actor,
      query: query || undefined,
    });
  } catch (error) {
    memoryMessage(error instanceof Error ? error.message : String(error), "error");
    addEvent("memory.error", { message: String(error) });
  }
}

function openMemoryPane() {
  openRuntimeDrawer();
  elements.leftCard.classList.add("memory-open");
  elements.capabilitiesBlock.hidden = true;
  elements.memoryPane.hidden = false;
  elements.memoryStoreName.textContent = memoryStoreName();
  elements.memoryStoreResource.textContent = state.config?.memory.store || "";
  if (!elements.memoryActor.value) elements.memoryActor.value = state.config?.memory.actor || "";
  void loadMemory();
}

function closeMemoryPane() {
  elements.leftCard.classList.remove("memory-open");
  elements.memoryPane.hidden = true;
  elements.capabilitiesBlock.hidden = false;
}

function openRuntimeDrawer() {
  elements.runtimeDrawer.hidden = false;
  elements.runtimeSettings.setAttribute("aria-expanded", "true");
  document.body.classList.add("runtime-open");
  elements.runtimeDrawerClose.focus();
}

function closeRuntimeDrawer({ restoreFocus = true } = {}) {
  elements.runtimeDrawer.hidden = true;
  elements.runtimeSettings.setAttribute("aria-expanded", "false");
  document.body.classList.remove("runtime-open");
  if (restoreFocus) elements.runtimeSettings.focus();
}

function openMemoryModal(entry) {
  elements.memoryModalTitle.textContent = entry.path || entry.name || "Memory entry";
  elements.memoryModalBody.textContent = extractText(entry.content) || entry.description || "(no content)";
  elements.memoryModalFoot.textContent = [entry.actor_id, prettyDateTime(entry.update_time || entry.create_time)]
    .filter(Boolean)
    .join(" · ");
  elements.memoryModal.hidden = false;
}

function closeMemoryModal() {
  elements.memoryModal.hidden = true;
}

function renderSessionItems(items) {
  renderStateItems(elements.sessionItems, items, "No transcript items yet.", (item) => {
    const data = item?.data || {};
    return {
      title: String(data.role || data.type || "item"),
      content: extractText(data.content ?? data),
      meta: [item.item_id, item.create_time, data.transport].filter(Boolean).join(" · "),
    };
  });
}

function relativeTime(value) {
  if (!value) return "";
  const then = new Date(value).getTime();
  if (Number.isNaN(then)) return "";
  const minutes = Math.round((Date.now() - then) / 60000);
  if (minutes < 1) return "Just now";
  if (minutes < 60) return `${minutes} min ago`;
  const hours = Math.round(minutes / 60);
  if (hours < 24) return `${hours} hr ago`;
  const days = Math.round(hours / 24);
  return days === 1 ? "Yesterday" : `${days} days ago`;
}

function renderSessions(items) {
  state.sessions = conversationIndex.mergeBackendSessions(
    state.sessions,
    items || [],
    state.sessionId,
  );
  persistConversationIndex();
  renderSessionList();
}

function renderSessionList() {
  elements.sessionList.replaceChildren();
  if (!state.sessions.length) {
    stateMessage(elements.sessionList, "No sessions yet.");
    return;
  }
  const query = state.sessionFilter.trim().toLowerCase();
  const items = state.sessions.filter(
    (session) =>
      !query ||
      String(session.title || "").toLowerCase().includes(query) ||
      String(session.session_id || "").toLowerCase().includes(query) ||
      String(session.actor_id || "").toLowerCase().includes(query),
  );
  if (!items.length) {
    stateMessage(elements.sessionList, "No sessions match your search.");
    return;
  }
  for (const session of items) {
    const current = session.session_id === state.sessionId;
    const canOpen = !current;
    const unavailable = state.unavailableSessions.has(session.session_id);
    const row = document.createElement(canOpen ? "button" : "div");
    row.className = `session-item${current ? " current" : ""}${canOpen ? " session-open-button" : ""}${unavailable ? " unavailable" : ""}`;
    if (canOpen) {
      row.type = "button";
      row.disabled = state.busy;
      row.addEventListener("click", () => openSession(session.session_id));
    }
    const top = document.createElement("div");
    top.className = "session-item-top";
    const title = document.createElement("span");
    title.className = "session-item-title";
    title.textContent = session.title || conversationIndex.DEFAULT_TITLE;
    const time = document.createElement("span");
    time.className = "session-item-time";
    time.textContent = current ? "Active" : unavailable ? "Unavailable" : relativeTime(session.updated_at);
    top.append(title, time);
    const id = document.createElement("span");
    id.className = "session-item-id";
    id.textContent = session.session_id || "unknown";
    row.append(top, id);
    elements.sessionList.append(row);
  }
}

function renderSessionTranscript(items) {
  state.draft = null;
  state.draftText = "";
  state.lastAssistantText = "";
  elements.chatLog.replaceChildren(elements.emptyState);
  elements.emptyState.hidden = false;

  for (const item of items) {
    const data = item?.data || {};
    const storedRole = String(data.role || data.type || "").toLowerCase();
    const content = extractText(data.content ?? data);
    const reasoning = extractReasoning(data) || extractReasoning(data.content);
    if (!content && !reasoning) continue;
    if (storedRole === "human_decision") {
      appendMessage("system", content, "Human decision");
      continue;
    }
    const role = normalizeRole(data);
    if (role === "assistant") state.lastAssistantText = content;
    const message = appendMessage(
      role,
      role === "tool" ? toolSummary(data) : content,
      role === "user" ? "You" : role === "assistant" ? "Agent" : role === "tool" ? "Tool result" : "System",
    );
    if (role === "assistant") renderReasoning(message.text, reasoning);
  }
}

async function ensureManagedSession(sessionId = ensureSessionId()) {
  if (!state.config?.session.managed) return null;
  if (state.managedSessionId === sessionId) return sessionId;
  if (sessionId === state.sessionId) stateMessage(elements.sessionItems, "Connecting managed session…", "loading");
  const response = await fetch(demoUrl("/api/demo/sessions", sessionId), {
    method: "POST",
    headers: { "Content-Type": "application/json", ...routingHeaders(sessionId) },
    body: "{}",
  });
  const result = await jsonResponse(response);
  if (sessionId === state.sessionId) state.managedSessionId = sessionId;
  addEvent("session.managed", result);
  return sessionId;
}

async function refreshSession({ hydrateChat = false } = {}) {
  if (!state.config?.session.history) {
    stateMessage(elements.sessionItems, "Session history is not available.");
    return { ok: false, items: [] };
  }
  const sessionId = ensureSessionId();
  const sessionEpoch = state.sessionEpoch;
  try {
    if (state.config.session.managed) await ensureManagedSession(sessionId);
    const response = await fetch(demoUrl("/api/demo/session/items", sessionId), {
      cache: "no-store",
      headers: routingHeaders(sessionId),
    });
    const result = await jsonResponse(response);
    const items = sessionItems(result);
    if (sessionId !== state.sessionId || sessionEpoch !== state.sessionEpoch) return { ok: false, stale: true, items: [] };
    renderSessionItems(items);
    if (hydrateChat) {
      renderSessionTranscript(items);
      for (const interrupt of result.interrupts || []) handleInterrupt(interrupt, sessionId);
      const indexed = state.sessions.find((item) => item.session_id === sessionId);
      if (!items.length && indexed?.title !== conversationIndex.DEFAULT_TITLE) {
        state.unavailableSessions.add(sessionId);
        appendError(new Error(
          state.config.session.managed
            ? "No transcript was returned for this conversation. It may be unavailable or expired."
            : "No transcript was returned for this conversation. In-process conversation history does not survive a backend restart.",
        ));
      } else {
        state.unavailableSessions.delete(sessionId);
      }
      renderSessionList();
    }
    addEvent("session.items.list", result);
    return { ok: true, items, result };
  } catch (error) {
    if (sessionId !== state.sessionId || sessionEpoch !== state.sessionEpoch) return { ok: false, stale: true, items: [] };
    state.unavailableSessions.add(sessionId);
    stateMessage(elements.sessionItems, error instanceof Error ? error.message : String(error), "error");
    if (hydrateChat) {
      renderSessionTranscript([]);
      appendError(new Error(`Conversation history is unavailable: ${error instanceof Error ? error.message : String(error)}`));
    }
    renderSessionList();
    addEvent("session.error", { message: String(error) });
    return { ok: false, items: [], error };
  }
}

async function refreshSessions() {
  const sessionId = ensureSessionId();
  if (!state.sessions.length) stateMessage(elements.sessionList, "Loading sessions…", "loading");
  try {
    const response = await fetch(demoUrl("/api/demo/sessions", sessionId), {
      cache: "no-store",
      headers: routingHeaders(sessionId),
    });
    const result = await jsonResponse(response);
    renderSessions(sessions(result));
    addEvent("sessions.list", result);
  } catch (error) {
    renderSessionList();
    addEvent("session.error", { message: String(error) });
  }
}

async function refreshSessionView({ hydrateChat = false } = {}) {
  await refreshSession({ hydrateChat });
  await refreshSessions();
}

async function invokeSync(payload, sessionId = ensureSessionId()) {
  const invocation = invocationPayload(payload, {}, sessionId);
  const response = await fetch("/api/invocations", {
    method: "POST",
    credentials: "same-origin",
    headers: invocationHeaders(sessionId),
    body: JSON.stringify(invocation),
  });
  const result = await jsonResponse(response);
  const output = agentResult(result);
  addEvent("response", result);
  handleOutput(output.output, sessionId);
  return output;
}

function parseSseFrame(frame) {
  const data = frame
    .split("\n")
    .filter((line) => line.startsWith("data:"))
    .map((line) => line.slice(5).trimStart())
    .join("\n");
  if (!data || data === "[DONE]") return null;
  return JSON.parse(data);
}

async function invokeStreaming(payload, sessionId = ensureSessionId()) {
  const invocation = invocationPayload(payload, { stream: true }, sessionId);
  const response = await fetch("/api/invocations", {
    method: "POST",
    credentials: "same-origin",
    headers: invocationHeaders(sessionId),
    body: JSON.stringify(invocation),
  });
  if (!response.ok || !response.body) await jsonResponse(response);
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  let terminal = null;
  while (true) {
    const { value, done } = await reader.read();
    buffer += decoder.decode(value || new Uint8Array(), { stream: !done });
    const frames = buffer.split("\n\n");
    buffer = frames.pop() || "";
    for (const frame of frames) {
      const event = parseSseFrame(frame);
      if (["run.failed", "run.completed"].includes(event?.type)) terminal = event.type;
      if (event) handleEvent(event, sessionId);
    }
    if (done) break;
  }
  if (buffer.trim()) {
    const event = parseSseFrame(buffer);
    if (["run.failed", "run.completed"].includes(event?.type)) terminal = event.type;
    if (event) handleEvent(event, sessionId);
  }
  if (terminal === "run.failed") {
    throw new Error(`Agent invocation ${invocation.id} failed. Check server logs for details.`);
  }
  if (terminal !== "run.completed") {
    throw new Error(`Stream disconnected before invocation ${invocation.id} completed. Check its status before retrying.`);
  }
  if (sessionId === state.sessionId) finishDraft();
  return {
    status: state.pendingInterruptSessionId === sessionId ? "interrupted" : "completed",
  };
}

async function pollBackground(invocationId, sessionId) {
  let delay = 850;
  while (true) {
    await new Promise((resolve) => setTimeout(resolve, delay));
    const response = await fetch(`/api/invocations/${encodeURIComponent(invocationId)}`, {
      cache: "no-store",
      credentials: "same-origin",
      headers: routingHeaders(sessionId),
    });
    if (response.status === 404) {
      throw new Error(`Background invocation ${invocationId} is no longer available. Check server logs and tool side effects before retrying.`);
    }
    const result = await jsonResponse(response);
    addEvent("background.poll", result);
    if (result.status === "completed") {
      const output = agentResult(result);
      handleOutput(output.output, sessionId);
      return output;
    }
    if (result.status === "failed") throw new Error(`Background invocation ${invocationId} failed. ${result.error?.message || result.error || "Check server logs for details."}`);
    if (sessionId === state.sessionId) setStatus(`Background · ${result.status}`, "busy");
    // Reduce polling load for long runs without imposing an execution deadline.
    delay = Math.min(delay * 1.5, 5000);
  }
}

async function invokeBackground(payload, sessionId = ensureSessionId()) {
  const invocation = invocationPayload(payload, { background: true }, sessionId);
  const response = await fetch("/api/invocations", {
    method: "POST",
    credentials: "same-origin",
    headers: invocationHeaders(sessionId),
    body: JSON.stringify(invocation),
  });
  const started = await jsonResponse(response);
  addEvent("background.started", started);
  if (sessionId === state.sessionId) setStatus(`Background · ${started.id}`, "busy");
  return pollBackground(started.id, sessionId);
}

async function dispatch(payload, mode = state.mode, sessionId = ensureSessionId()) {
  state.lastAssistantText = "";
  state.pendingInterrupt = null;
  state.pendingInterruptSessionId = "";
  elements.approvalPanel.hidden = true;
  if (mode === "streaming") return invokeStreaming(payload, sessionId);
  if (mode === "background") return invokeBackground(payload, sessionId);
  return invokeSync(payload, sessionId);
}

async function sendText(text, mode = state.mode) {
  const content = text.trim();
  if (!content || state.busy) return "";
  const sessionId = ensureSessionId();
  const indexed = state.sessions.find((item) => item.session_id === sessionId);
  state.unavailableSessions.delete(sessionId);
  registerConversation(sessionId, {
    title: indexed?.title === conversationIndex.DEFAULT_TITLE
      ? conversationIndex.titleFromQuestion(content)
      : indexed?.title,
    updated_at: new Date().toISOString(),
  });
  appendMessage("user", content, "You");
  setBusy(true, mode === "background" ? "Starting background run" : mode === "streaming" ? "Streaming" : "Running");
  try {
    await dispatch({ messages: [{ role: "user", content }] }, mode, sessionId);
    if (sessionId === state.sessionId) await refreshSessionView();
    return state.lastAssistantText;
  } catch (error) {
    finishDraft();
    appendError(error);
    throw error;
  } finally {
    setBusy(false);
  }
}

async function resume(decision) {
  if (state.busy) return;
  const sessionId = ensureSessionId();
  if (state.pendingInterrupt && state.pendingInterruptSessionId !== sessionId) {
    appendError(new Error("The pending approval belongs to a different conversation. Reopen that conversation before deciding."));
    return;
  }
  if (!state.pendingInterrupt && !state.config?.session.durable) {
    appendError(new Error("No paused run is loaded for the current routing session."));
    return;
  }
  const payload =
    decision === "approve"
      ? { resume: { decisions: [{ type: "approve" }] } }
      : { resume: { decisions: [{ type: "reject", message: "Rejected from the Agent Bricks demo UI." }] } };
  appendMessage("system", decision === "approve" ? "Approved pending tool call." : "Rejected pending tool call.", "Human decision");
  setBusy(true, "Resuming");
  try {
    await dispatch(payload, "streaming", sessionId);
    if (sessionId === state.sessionId) await refreshSessionView();
  } catch (error) {
    appendError(error);
  } finally {
    setBusy(false);
  }
}

function clearChat({ recordEvent = true } = {}) {
  state.pendingInterrupt = null;
  state.pendingInterruptSessionId = "";
  state.draft = null;
  state.draftText = "";
  state.lastAssistantText = "";
  elements.approvalPanel.hidden = true;
  elements.chatLog.replaceChildren(elements.emptyState);
  elements.emptyState.hidden = false;
  elements.promptInput.focus();
  if (recordEvent) addEvent("chat.cleared", { session_id: state.sessionId });
}

function resetSessionState() {
  clearChat({ recordEvent: false });
  renderSessionList();
  stateMessage(elements.sessionItems, "Loading transcript…", "loading");
}

async function createNewSession() {
  if (state.busy) return;
  setBusy(true, "Creating session");
  try {
    const previousSessionId = ensureSessionId();
    setSessionId(newSessionId());
    resetSessionState();
    addEvent("session.new", { session_id: state.sessionId, previous_session_id: previousSessionId });
    if (state.config?.session.managed) await ensureManagedSession();
    await refreshSessionView({ hydrateChat: true });
  } catch (error) {
    appendError(error);
  } finally {
    setBusy(false);
  }
}

async function openSession(sessionId) {
  if (!conversationIndex.canSwitchConversation({
    busy: state.busy,
    currentSessionId: state.sessionId,
    targetSessionId: sessionId,
  })) {
    if (state.busy && sessionId && sessionId !== state.sessionId) {
      addEvent("session.switch.blocked", { session_id: sessionId, reason: "invocation_active" });
    }
    return;
  }
  setBusy(true, "Opening session");
  try {
    const previousSessionId = ensureSessionId();
    let result = { session_id: sessionId, previous_session_id: previousSessionId, managed: false };
    if (state.config?.session.managed) {
      const response = await fetch(
        demoUrl(`/api/demo/sessions/${encodeURIComponent(sessionId)}/open`, previousSessionId),
        {
          method: "POST",
          credentials: "same-origin",
          headers: routingHeaders(previousSessionId),
        },
      );
      result = await jsonResponse(response);
    }
    setSessionId(sessionId);
    resetSessionState();
    addEvent("session.open", result);
    await refreshSessionView({ hydrateChat: true });
  } catch (error) {
    appendError(error);
  } finally {
    setBusy(false);
  }
}

function renderModels(models) {
  const available = models?.available?.length ? models.available : [models?.default].filter(Boolean);
  state.model = available.includes(state.model) ? state.model : models?.default || available[0] || "";
  elements.modelSelect.replaceChildren();
  for (const name of available) {
    const option = document.createElement("option");
    option.value = name;
    option.textContent = name === models?.default ? `${name} (project default)` : name;
    option.selected = name === state.model;
    elements.modelSelect.append(option);
  }
  // A single choice is informational, not a decision to make.
  elements.modelSelect.disabled = state.busy || available.length <= 1;
  elements.runtimeModel.textContent = state.model || "Unavailable";
  sizeModelSelect();
}

// Native selects size to their widest option, which leaves a gap between a short
// selected name and the chevron. Size the control to the selected option instead.
function sizeModelSelect() {
  const select = elements.modelSelect;
  const option = select.selectedOptions && select.selectedOptions[0];
  if (!option) return;
  const probe = document.createElement("span");
  const style = getComputedStyle(select);
  probe.style.cssText = "position:absolute;visibility:hidden;white-space:pre;";
  probe.style.fontFamily = style.fontFamily;
  probe.style.fontSize = style.fontSize;
  probe.style.fontWeight = style.fontWeight;
  probe.style.letterSpacing = style.letterSpacing;
  probe.textContent = option.textContent;
  document.body.append(probe);
  const width = probe.getBoundingClientRect().width;
  probe.remove();
  // selected text + left padding (8) + room for the chevron (~26)
  select.style.width = `${Math.ceil(width) + 34}px`;
}

async function loadModels() {
  const response = await fetch(demoUrl("/api/demo/models"), { cache: "no-store" });
  renderModels(await jsonResponse(response));
}

function healthTone(status) {
  const normalized = String(status || "UNKNOWN").toUpperCase();
  if (normalized === "PASS" || normalized === "CURRENT") return "success";
  if (normalized === "WARN" || normalized === "STALE" || normalized === "PARTIAL") return "warning";
  if (normalized === "FAIL" || normalized === "UNAVAILABLE") return "error";
  if (normalized === "RUNNING") return "info";
  return "neutral";
}

function setHealthBadge(element, status, label = status) {
  const tone = healthTone(status);
  element.className = `health-badge ${tone}`;
  element.textContent = label || "Unknown";
}

function formatHealthTimestamp(value) {
  if (!value) return "Unavailable";
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return String(value);
  return parsed.toLocaleString([], {
    year: "numeric",
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
    timeZoneName: "short",
  });
}

function renderHealthIssues(issues, { unavailable = false } = {}) {
  elements.healthIssueList.replaceChildren();
  if (!issues.length) {
    const empty = document.createElement("div");
    empty.className = unavailable ? "state-empty" : "health-empty-success";
    empty.textContent = unavailable
      ? "Monitoring evidence is unavailable; issue count is unknown."
      : "No non-PASS results were returned by the configured monitoring checks.";
    elements.healthIssueList.append(empty);
    return;
  }

  issues.forEach((issue) => {
    const article = document.createElement("article");
    article.className = "health-issue";
    const top = document.createElement("div");
    top.className = "health-issue-top";
    const identity = document.createElement("strong");
    identity.textContent = issue.metric_name || issue.entity_name || issue.reconciliation_name || issue.category;
    const badge = document.createElement("span");
    setHealthBadge(badge, issue.status);
    top.append(identity, badge);

    const category = document.createElement("span");
    category.className = "health-issue-category";
    category.textContent = String(issue.category || "monitoring").replaceAll("_", " ");
    const evidence = document.createElement("code");
    evidence.textContent = issue.evidence_ref || "Evidence reference unavailable";
    const details = document.createElement("details");
    const summary = document.createElement("summary");
    summary.textContent = "View retrieved check details";
    const pre = document.createElement("pre");
    pre.textContent = JSON.stringify(issue.details || {}, null, 2);
    details.append(summary, pre);
    article.append(top, category, evidence, details);
    elements.healthIssueList.append(article);
  });
}

function renderSourceErrors(errors) {
  elements.healthSourceErrors.replaceChildren();
  elements.healthSourceErrors.hidden = !errors.length;
  errors.forEach((error) => {
    const item = document.createElement("p");
    item.textContent = `${error.source || "source"}: ${error.message || "Evidence unavailable."}`;
    elements.healthSourceErrors.append(item);
  });
}

function renderHealthSummary(summary) {
  const pipeline = summary.pipeline || {};
  const monitoring = summary.monitoring || {};
  const counts = monitoring.checks || {};
  const taskCounts = pipeline.task_counts || {};
  const stale = monitoring.staleness || {};
  const availability = summary.availability || {};

  setHealthBadge(elements.pipelineHealthStatus, pipeline.status);
  setHealthBadge(elements.monitoringHealthStatus, monitoring.status);
  setHealthBadge(elements.healthStaleStatus, stale.status, stale.status === "STALE" ? "Stale evidence" : stale.status);
  elements.pipelineRunValue.textContent = pipeline.run_id ? `#${pipeline.run_id}` : pipeline.found === false ? "No run found" : "Unavailable";
  elements.pipelineCompletedValue.textContent = formatHealthTimestamp(pipeline.end_time);
  elements.pipelineTaskValue.textContent = taskCounts.total == null
    ? "Unavailable"
    : `${taskCounts.passed || 0} passed / ${taskCounts.total} total`;
  elements.pipelineFailedValue.textContent = taskCounts.failed == null ? "Unavailable" : String(taskCounts.failed);
  elements.healthPassCount.textContent = counts.pass ?? "—";
  elements.healthWarnCount.textContent = counts.warn ?? "—";
  elements.healthFailCount.textContent = counts.fail ?? "—";
  elements.healthTotalCount.textContent = counts.total ?? "—";
  elements.monitoringRunValue.textContent = formatHealthTimestamp(monitoring.monitoring_run_at);
  elements.monitoringReferenceValue.textContent = formatHealthTimestamp(monitoring.monitoring_reference_at);
  elements.syntheticReferenceValue.textContent = monitoring.synthetic_reference_date || "Unavailable";
  elements.pipelineAssociationValue.textContent = monitoring.pipeline_association || "UNVERIFIED";
  elements.healthGeneratedValue.textContent = `Generated ${formatHealthTimestamp(summary.generated_at)}`;

  const overall = String(summary.overall_status || "UNKNOWN").toUpperCase();
  elements.healthState.className = `health-state ${healthTone(overall)}`;
  if (overall === "UNAVAILABLE") {
    elements.healthState.textContent = "Health evidence is unavailable. No healthy conclusion can be drawn.";
  } else if (overall === "PARTIAL" || availability.partial) {
    const finding = overall === "FAIL"
      ? " Available evidence includes a failure."
      : overall === "WARN"
        ? " Available evidence includes a warning."
        : "";
    elements.healthState.textContent = `Partial review: one evidence source is unavailable.${finding} Available results are shown without filling the gap.`;
  } else if (stale.status === "STALE") {
    elements.healthState.textContent = "Monitoring evidence is stale. The displayed checks may not represent current conditions.";
  } else {
    elements.healthState.textContent = `Review status: ${overall}. This status is limited to the retrieved execution and monitoring evidence.`;
  }

  renderSourceErrors(summary.source_errors || []);
  renderHealthIssues(monitoring.issues || [], { unavailable: !monitoring.available });
  addEvent("health.summary", summary);
}

async function loadDailyHealth() {
  if (state.healthLoading) return;
  state.healthLoading = true;
  elements.healthRefresh.disabled = true;
  elements.healthState.className = "health-state loading";
  elements.healthState.textContent = "Retrieving Jobs and observability evidence…";
  try {
    const response = await fetch("/api/ui/health-summary", {
      cache: "no-store",
      credentials: "same-origin",
      headers: routingHeaders(),
    });
    if (!response.ok) throw new Error(await response.text() || `Health summary failed (${response.status}).`);
    const summary = await response.json();
    renderHealthSummary(summary);
    state.healthLoaded = true;
  } catch (error) {
    elements.healthState.className = "health-state error";
    elements.healthState.textContent = `Daily Health Review unavailable: ${error.message}`;
    renderSourceErrors([{ source: "dashboard", message: error.message }]);
    renderHealthIssues([], { unavailable: true });
    addEvent("health.error", { message: error.message });
  } finally {
    state.healthLoading = false;
    elements.healthRefresh.disabled = false;
  }
}

function setWorkspaceView(view) {
  const showHealth = view === "health";
  elements.healthView.hidden = !showHealth;
  elements.chatView.hidden = showHealth;
  elements.healthViewTab.classList.toggle("active", showHealth);
  elements.chatViewTab.classList.toggle("active", !showHealth);
  elements.healthViewTab.setAttribute("aria-selected", String(showHealth));
  elements.chatViewTab.setAttribute("aria-selected", String(!showHealth));
  if (showHealth && !state.healthLoaded) void loadDailyHealth();
}

async function loadConfig() {
  try {
    const response = await fetch(demoUrl("/api/ui/config"), { cache: "no-store" });
    const config = await jsonResponse(response);
    state.config = config;
    state.instanceId = config.instance_id;
    renderModels(config.models);
    void loadModels().catch((error) => addEvent("models.error", { message: String(error) }));
    setViewer(config.viewer);
    elements.streamingMode.textContent = config.streaming.mode;
    elements.backgroundMode.textContent = config.background.mode;
    elements.sessionMode.textContent = config.session.mode;
    elements.memoryMode.textContent = config.memory.enabled ? `Managed · actor ${config.memory.actor}` : "Not connected";
    elements.runtimeTransport.textContent = config.streaming.transport;
    elements.runtimeSession.textContent = config.session.managed ? "Managed" : "In process";
    setCapability(
      elements.streamingStatus,
      config.streaming.enabled,
      config.streaming.enabled
        ? `Streaming responses over ${config.streaming.transport} use ${config.streaming.persistent ? "the Runtime Store." : "an in-process Runtime Store."}`
        : "Streaming is disabled for this deployment.",
    );
    setCapability(
      elements.backgroundStatus,
      config.background.enabled,
      config.background.enabled
        ? `Background invocations use ${config.background.persistent ? "the Runtime Store." : "an in-process Runtime Store."}`
        : "Background invocations are disabled.",
    );
    setCapability(
      elements.sessionStatus,
      config.session.managed ? true : config.session.history ? "degraded" : false,
      config.session.managed
        ? `Connected to Session Store "${config.session.store}" for actor ${config.session.actor}. State is durable and shareable.`
        : "Session state is kept in process and resets when the app restarts.",
      config.session.managed ? null : "agentbricks sessions bind <store-name>",
    );
    setCapability(
      elements.memoryStatus,
      config.memory.enabled,
      config.memory.enabled
        ? `Connected to ${config.memory.store} for actor ${config.memory.actor}.`
        : "No long-term memory is connected.",
      config.memory.enabled ? null : "agentbricks memory bind <store-name>",
    );
    elements.tracingMode.textContent = config.tracing?.enabled ? "Connected" : "Not configured";
    setCapability(
      elements.tracingStatus,
      config.tracing?.enabled ? true : "degraded",
      config.tracing?.enabled
        ? "Every run is traced to an MLflow experiment."
        : "Tracing is off. Set an MLflow destination and experiment to record traces.",
      config.tracing?.enabled ? null : "agentbricks tracing bind --experiment-name <path>",
      config.tracing?.enabled && config.tracing?.url
        ? { href: config.tracing.url, label: "View traces in MLflow" }
        : null,
    );
    elements.newSession.disabled = state.busy;
    elements.refreshSession.disabled = state.busy || !config.session.history;
    elements.resumeSession.disabled = state.busy || !config.session.durable;
    elements.rejectSession.disabled = state.busy || !config.session.durable;
    elements.sessionStoreLabel.textContent = config.session.managed
      ? `${config.session.store} · actor ${config.session.actor} · the browser session ID keys transcript and checkpoint state.`
      : config.session.history
        ? "Messages load from the in-process LangGraph checkpoint for the current browser session."
        : "Session history is unavailable.";
    elements.sessionPersistenceNote.textContent = config.session.managed
      ? "Conversation transcripts come from the managed Session Store."
      : "This browser remembers conversation names, but in-process transcripts are lost when the backend restarts.";
    addEvent("runtime.config", config);
    void refreshSessionView({ hydrateChat: true });
    // The memory pane loads lazily when opened; refresh it if it is already open.
    if (!elements.memoryPane.hidden) void loadMemory();
    return config;
  } catch (error) {
    throw error;
  }
}
elements.composer.addEventListener("submit", async (event) => {
  event.preventDefault();
  const text = elements.promptInput.value;
  if (!text.trim()) return;
  elements.promptInput.value = "";
  elements.promptInput.style.height = "auto";
  updateSendState();
  try {
    await sendText(text);
  } catch {
    elements.promptInput.value = text;
    updateSendState();
  }
});

elements.promptInput.addEventListener("input", () => {
  elements.promptInput.style.height = "auto";
  elements.promptInput.style.height = `${Math.min(elements.promptInput.scrollHeight, 180)}px`;
  updateSendState();
});

elements.promptInput.addEventListener("keydown", (event) => {
  if (event.key === "Enter" && !event.shiftKey) {
    event.preventDefault();
    elements.composer.requestSubmit();
  }
});

document.querySelectorAll(".mode-button").forEach((button) => {
  button.addEventListener("click", () => {
    state.mode = button.dataset.mode;
    document.querySelectorAll(".mode-button").forEach((item) => item.classList.toggle("active", item === button));
  });
});

elements.modelSelect.addEventListener("change", () => {
  state.model = elements.modelSelect.value;
  elements.runtimeModel.textContent = state.model || "Unavailable";
  sizeModelSelect();
  addEvent("model.selected", { model: state.model });
});

document.querySelectorAll("[data-prompt]").forEach((button) => {
  button.addEventListener("click", () => {
    elements.promptInput.value = button.dataset.prompt;
    elements.promptInput.dispatchEvent(new Event("input"));
    elements.promptInput.focus();
  });
});

elements.copySession.addEventListener("click", async () => {
  await navigator.clipboard.writeText(ensureSessionId());
  const label = elements.copySession.querySelector("span");
  label.textContent = "Copied";
  setTimeout(() => { label.textContent = "Copy"; }, 1200);
});

elements.refreshConfig.addEventListener("click", () => loadConfig().catch(appendError));
elements.chatViewTab.addEventListener("click", () => setWorkspaceView("chat"));
elements.healthViewTab.addEventListener("click", () => setWorkspaceView("health"));
elements.healthRefresh.addEventListener("click", () => void loadDailyHealth());
elements.newSession.addEventListener("click", createNewSession);
elements.railNewSession.addEventListener("click", createNewSession);
elements.refreshSession.addEventListener("click", () => refreshSessionView({ hydrateChat: true }));
elements.clearEvents.addEventListener("click", () => {
  state.events = [];
  state.selectedEventId = null;
  renderEvents();
});

// Capabilities expand inline on click; Memory instead opens its own pane.
document.querySelectorAll(".capability").forEach((button) => {
  button.addEventListener("click", () => {
    const item = button.closest(".capability-item");
    if (item?.dataset.capability === "memory") {
      openMemoryPane();
      return;
    }
    const panel = item?.querySelector(".capability-detail");
    const expanded = item.classList.toggle("expanded");
    button.setAttribute("aria-expanded", String(expanded));
    if (panel) panel.hidden = !expanded;
  });
});

// Memory pane controls.
elements.memoryBack.addEventListener("click", closeMemoryPane);
elements.memoryRefresh.addEventListener("click", () => loadMemory());
elements.memoryActor.addEventListener("input", () => {
  window.clearTimeout(memoryState.searchTimer);
  memoryState.searchTimer = window.setTimeout(() => void loadMemory(), 300);
});
elements.memorySearch.addEventListener("input", () => {
  memoryState.query = elements.memorySearch.value;
  window.clearTimeout(memoryState.searchTimer);
  memoryState.searchTimer = window.setTimeout(() => void loadMemory(), 300);
});

// Memory modal: close on ×, backdrop click, or Escape.
elements.memoryModalClose.addEventListener("click", closeMemoryModal);
elements.memoryModal.addEventListener("click", (event) => {
  if (event.target === elements.memoryModal) closeMemoryModal();
});
document.addEventListener("keydown", (event) => {
  if (event.key === "Escape" && !elements.memoryModal.hidden) closeMemoryModal();
  else if (event.key === "Escape" && !elements.runtimeDrawer.hidden) closeRuntimeDrawer();
  else if (event.key === "Escape" && document.body.classList.contains("evidence-open")) {
    document.body.classList.remove("evidence-open");
    syncPanelToggles();
    elements.evidenceToggle.focus();
  } else if (event.key === "Escape" && document.body.classList.contains("sidebar-open")) {
    document.body.classList.remove("sidebar-open");
    syncPanelToggles();
    elements.sidebarToggle.focus();
  }
});

// Sessions search.
elements.sessionSearch.addEventListener("input", () => {
  state.sessionFilter = elements.sessionSearch.value;
  renderSessionList();
});

// Events view toggles.
elements.eventViewLogs.addEventListener("click", () => setEventView("logs"));
elements.eventViewList.addEventListener("click", () => setEventView("list"));

// Events filter pills (list view only).
elements.eventFilters.querySelectorAll(".event-filter").forEach((pill) => {
  pill.addEventListener("click", () => {
    state.eventFilter = pill.dataset.filter;
    elements.eventFilters
      .querySelectorAll(".event-filter")
      .forEach((item) => item.classList.toggle("active", item === pill));
    renderEvents();
  });
});

// Copy the selected event payload from the list-view detail pane.
elements.copyEventDetail.addEventListener("click", async () => {
  try {
    await navigator.clipboard.writeText(elements.eventDetailJson.textContent || "");
  } catch {
    /* clipboard may be unavailable */
  }
});

function syncPanelToggles() {
  const drawerNavigation = window.matchMedia("(max-width: 1100px)").matches;
  const sidebarVisible = drawerNavigation
    ? document.body.classList.contains("sidebar-open")
    : !document.body.classList.contains("sidebar-collapsed");
  const evidenceVisible = document.body.classList.contains("evidence-open");
  elements.sidebarToggle.setAttribute("aria-expanded", String(sidebarVisible));
  elements.evidenceToggle.setAttribute("aria-expanded", String(evidenceVisible));
}

elements.sidebarToggle.addEventListener("click", () => {
  if (window.matchMedia("(max-width: 1100px)").matches) {
    document.body.classList.toggle("sidebar-open");
  } else {
    document.body.classList.toggle("sidebar-collapsed");
  }
  syncPanelToggles();
});

elements.evidenceToggle.addEventListener("click", () => {
  document.body.classList.toggle("evidence-open");
  syncPanelToggles();
});

elements.railExpand.addEventListener("click", () => {
  document.body.classList.remove("sidebar-collapsed");
  syncPanelToggles();
  elements.sidebarToggle.focus();
});

elements.runtimeSettings.addEventListener("click", openRuntimeDrawer);
elements.railRuntime.addEventListener("click", openRuntimeDrawer);
elements.runtimeDrawerClose.addEventListener("click", () => closeRuntimeDrawer());

window.addEventListener("resize", syncPanelToggles);
syncPanelToggles();

// Header subtitle: product · framework · host (host is only known client-side).
if (elements.brandSub) elements.brandSub.textContent = `DataOps Copilot · ${AGENT_FRAMEWORK} · ${window.location.host}`;
elements.approveAction.addEventListener("click", () => resume("approve"));
elements.rejectAction.addEventListener("click", () => resume("reject"));
elements.resumeSession.addEventListener("click", () => resume("approve"));
elements.rejectSession.addEventListener("click", () => resume("reject"));

loadConfig().catch((error) => {
  appendError(error);
  elements.streamingMode.textContent = "Unavailable";
  elements.backgroundMode.textContent = "Unavailable";
  elements.sessionMode.textContent = "Unavailable";
  elements.memoryMode.textContent = "Unavailable";
  elements.tracingMode.textContent = "Unavailable";
});
