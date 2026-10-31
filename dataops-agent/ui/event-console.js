(function eventConsoleHelpers(root, factory) {
  const api = factory();
  if (typeof module === "object" && module.exports) module.exports = api;
  if (root) root.DataOpsEventConsole = api;
})(typeof globalThis !== "undefined" ? globalThis : this, function createEventConsoleHelpers() {
  "use strict";

  const DEFAULT_EVENT_LIMIT = 80;
  const RAW_POLL_LIMIT = 20;
  const TRANSITION_LIMIT = 16;

  function text(value, fallback = "unknown") {
    if (value == null || value === "") return fallback;
    return String(value);
  }

  function isoTime(value) {
    const date = value instanceof Date ? value : new Date(value);
    return Number.isFinite(date.getTime()) ? date.toISOString() : new Date(0).toISOString();
  }

  function isInvestigationPoll(event) {
    return String(event?.type || "").toLowerCase() === "investigation.poll";
  }

  function pollIdentity(event) {
    return text(event?.runId || event?.payload?.id || event?.payload?.invocation_id, "");
  }

  function sanitizedPoll(event) {
    const error = event?.payload?.error;
    return {
      invocation_id: pollIdentity(event),
      status: text(event?.payload?.status),
      observed_at: isoTime(event?.at),
      error: error && typeof error === "object"
        ? { type: text(error.type, "error"), code: text(error.code, "") || null }
        : error ? { type: "error", code: null } : null,
    };
  }

  function initializePoll(event) {
    const raw = sanitizedPoll(event);
    return {
      ...event,
      payload: raw,
      runId: raw.invocation_id,
      summary: `${raw.status} · 1 poll`,
      polling: {
        count: 1,
        currentStatus: raw.status,
        firstAt: raw.observed_at,
        lastAt: raw.observed_at,
        transitions: [{ status: raw.status, at: raw.observed_at }],
        rawEvents: [raw],
        rawEventsTruncated: false,
      },
    };
  }

  function mergePoll(current, incoming) {
    const raw = sanitizedPoll(incoming);
    const previous = current.polling || initializePoll(current).polling;
    const statusChanged = previous.currentStatus !== raw.status;
    const transitions = statusChanged
      ? [...previous.transitions, { status: raw.status, at: raw.observed_at }].slice(-TRANSITION_LIMIT)
      : [...previous.transitions];
    const rawEvents = [...previous.rawEvents, raw].slice(-RAW_POLL_LIMIT);
    const count = previous.count + 1;
    return {
      event: {
        ...current,
        at: incoming.at,
        payload: raw,
        runId: raw.invocation_id,
        summary: `${raw.status} · ${count} polls`,
        polling: {
          count,
          currentStatus: raw.status,
          firstAt: previous.firstAt,
          lastAt: raw.observed_at,
          transitions,
          rawEvents,
          rawEventsTruncated: previous.rawEventsTruncated || count > RAW_POLL_LIMIT,
        },
      },
      statusChanged,
    };
  }

  function appendEvent(events, event, { limit = DEFAULT_EVENT_LIMIT } = {}) {
    const retained = Array.isArray(events) ? events : [];
    if (isInvestigationPoll(event)) {
      const incoming = initializePoll(event);
      const current = retained[0];
      const distinctError = JSON.stringify(current?.payload?.error || null) !== JSON.stringify(incoming.payload.error || null);
      if (isInvestigationPoll(current) && pollIdentity(current) === pollIdentity(incoming) && !distinctError) {
        const merged = mergePoll(current, incoming);
        return {
          events: [merged.event, ...retained.slice(1)].slice(0, limit),
          merged: true,
          announce: merged.statusChanged,
        };
      }
      return { events: [incoming, ...retained].slice(0, limit), merged: false, announce: true };
    }
    return { events: [event, ...retained].slice(0, limit), merged: false, announce: true };
  }

  function detailPayload(event) {
    if (!event?.polling) return event?.payload;
    return {
      invocation_id: event.runId,
      current_status: event.polling.currentStatus,
      polling_count: event.polling.count,
      first_polled_at: event.polling.firstAt,
      last_polled_at: event.polling.lastAt,
      status_transitions: event.polling.transitions,
      retained_raw_poll_events: event.polling.rawEvents,
      raw_events_truncated: event.polling.rawEventsTruncated,
    };
  }

  return {
    DEFAULT_EVENT_LIMIT,
    RAW_POLL_LIMIT,
    appendEvent,
    detailPayload,
    isInvestigationPoll,
  };
});
