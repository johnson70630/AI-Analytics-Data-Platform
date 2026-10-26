(function exposeConversationIndex(root, factory) {
  const api = factory();
  if (typeof module === "object" && module.exports) module.exports = api;
  if (root) root.DataOpsConversationIndex = api;
})(typeof globalThis === "object" ? globalThis : this, () => {
  "use strict";

  const DEFAULT_TITLE = "New conversation";
  const MAX_CONVERSATIONS = 100;

  function validIso(value, fallback) {
    if (!value) return fallback;
    const parsed = new Date(value);
    return Number.isNaN(parsed.getTime()) ? fallback : parsed.toISOString();
  }

  function cleanTitle(value) {
    const title = String(value || "").replace(/\s+/g, " ").trim();
    return title || DEFAULT_TITLE;
  }

  function titleFromQuestion(question) {
    const title = cleanTitle(question);
    if (title === DEFAULT_TITLE || title.length <= 64) return title;
    return `${title.slice(0, 63).trimEnd()}…`;
  }

  function normalizeConversation(value, now = new Date().toISOString()) {
    const sessionId = String(value?.session_id || "").trim();
    if (!sessionId) return null;
    const createdAt = validIso(value.created_at, now);
    const updatedAt = validIso(value.updated_at, createdAt);
    return {
      session_id: sessionId,
      title: cleanTitle(value.title),
      created_at: createdAt,
      updated_at: updatedAt,
    };
  }

  function sortConversations(items) {
    return [...items].sort((left, right) => {
      const updated = Date.parse(right.updated_at) - Date.parse(left.updated_at);
      return updated || right.session_id.localeCompare(left.session_id);
    });
  }

  function upsertConversation(items, sessionId, changes = {}, now = new Date().toISOString()) {
    const id = String(sessionId || "").trim();
    if (!id) return sortConversations(items || []);
    const existing = (items || []).find((item) => item.session_id === id);
    const normalized = normalizeConversation(
      {
        session_id: id,
        title: changes.title ?? existing?.title ?? DEFAULT_TITLE,
        created_at: changes.created_at ?? existing?.created_at ?? now,
        updated_at: changes.updated_at ?? existing?.updated_at ?? now,
      },
      now,
    );
    return sortConversations([
      ...(items || []).filter((item) => item.session_id !== id),
      normalized,
    ]).slice(0, MAX_CONVERSATIONS);
  }

  function parseConversationIndex(raw, activeSessionId = "", now = new Date().toISOString()) {
    let values = [];
    try {
      const parsed = typeof raw === "string" ? JSON.parse(raw) : raw;
      values = Array.isArray(parsed) ? parsed : Array.isArray(parsed?.conversations) ? parsed.conversations : [];
    } catch {
      values = [];
    }
    let result = [];
    for (const value of values) {
      const normalized = normalizeConversation(value, now);
      if (!normalized) continue;
      const current = result.find((item) => item.session_id === normalized.session_id);
      const preferred = !current || Date.parse(normalized.updated_at) >= Date.parse(current.updated_at) ? normalized : current;
      result = upsertConversation(result, preferred.session_id, preferred, now);
    }
    if (activeSessionId) result = upsertConversation(result, activeSessionId, {}, now);
    return result;
  }

  function laterIso(left, right, fallback) {
    const leftIso = validIso(left, fallback);
    const rightIso = validIso(right, fallback);
    return Date.parse(rightIso) > Date.parse(leftIso) ? rightIso : leftIso;
  }

  function mergeBackendSessions(localItems, backendItems, activeSessionId = "", now = new Date().toISOString()) {
    let result = parseConversationIndex(localItems, activeSessionId, now);
    for (const backend of backendItems || []) {
      const sessionId = String(backend?.session_id || "").trim();
      if (!sessionId) continue;
      const existing = result.find((item) => item.session_id === sessionId);
      const metadata = backend?.metadata && typeof backend.metadata === "object" ? backend.metadata : {};
      const createdAt = validIso(backend.create_time || backend.created_at, existing?.created_at || now);
      const updatedAt = laterIso(
        existing?.updated_at,
        backend.last_activity_time || backend.updated_at || createdAt,
        createdAt,
      );
      result = upsertConversation(
        result,
        sessionId,
        {
          title: existing && existing.title !== DEFAULT_TITLE ? existing.title : metadata.title,
          created_at: existing?.created_at || createdAt,
          updated_at: updatedAt,
        },
        now,
      );
    }
    return result;
  }

  function serializeConversationIndex(items) {
    const safe = sortConversations(
      (items || []).map((item) => normalizeConversation(item)).filter(Boolean),
    )
      .slice(0, MAX_CONVERSATIONS)
      .map(({ session_id, title, created_at, updated_at }) => ({
        session_id,
        title,
        created_at,
        updated_at,
      }));
    return JSON.stringify(safe);
  }

  function canSwitchConversation({ busy, currentSessionId, targetSessionId }) {
    return !busy && Boolean(targetSessionId) && targetSessionId !== currentSessionId;
  }

  return {
    DEFAULT_TITLE,
    MAX_CONVERSATIONS,
    canSwitchConversation,
    mergeBackendSessions,
    normalizeConversation,
    parseConversationIndex,
    serializeConversationIndex,
    sortConversations,
    titleFromQuestion,
    upsertConversation,
  };
});
