const test = require("node:test");
const assert = require("node:assert/strict");

const eventConsole = require("../ui/event-console.js");

function poll(id, status, at, sequence, extra = {}) {
  return {
    id: `event-${sequence}`,
    type: "investigation.poll",
    category: "system",
    summary: status,
    runId: id,
    payload: { id, status, ...extra },
    at: new Date(at),
  };
}

test("consecutive polls for one invocation consolidate into one visible event", () => {
  let events = [];
  let outcome = eventConsole.appendEvent(events, poll("inv-1", "active", "2026-10-30T10:00:00Z", 1));
  events = outcome.events;
  outcome = eventConsole.appendEvent(events, poll("inv-1", "active", "2026-10-30T10:00:05Z", 2));

  assert.equal(outcome.events.length, 1);
  assert.equal(outcome.events[0].polling.count, 2);
  assert.equal(outcome.events[0].polling.currentStatus, "active");
  assert.equal(outcome.events[0].polling.firstAt, "2026-10-30T10:00:00.000Z");
  assert.equal(outcome.events[0].polling.lastAt, "2026-10-30T10:00:05.000Z");
  assert.equal(outcome.announce, false);
});

test("invocation switching creates a distinct polling entry", () => {
  const first = eventConsole.appendEvent([], poll("inv-1", "queued", "2026-10-30T10:00:00Z", 1));
  const second = eventConsole.appendEvent(first.events, poll("inv-2", "queued", "2026-10-30T10:00:01Z", 2));

  assert.equal(second.events.length, 2);
  assert.deepEqual(second.events.map((event) => event.runId), ["inv-2", "inv-1"]);
});

test("status transitions remain visible inside one invocation entry", () => {
  let events = [];
  const announcements = [];
  for (const [index, status] of ["queued", "active", "active", "completed"].entries()) {
    const outcome = eventConsole.appendEvent(
      events,
      poll("inv-1", status, `2026-10-30T10:00:0${index}Z`, index),
    );
    events = outcome.events;
    announcements.push(outcome.announce);
  }

  assert.equal(events.length, 1);
  assert.equal(events[0].polling.count, 4);
  assert.deepEqual(events[0].polling.transitions.map((item) => item.status), ["queued", "active", "completed"]);
  assert.deepEqual(announcements, [true, true, false, true]);
  assert.equal(events[0].summary, "completed · 4 polls");
});

test("failures and intervening events remain distinct", () => {
  const first = eventConsole.appendEvent([], poll("inv-1", "active", "2026-10-30T10:00:00Z", 1));
  const failure = {
    id: "event-error",
    type: "investigation.error",
    category: "error",
    summary: "Timed out",
    runId: "inv-1",
    payload: { message: "Timed out" },
    at: new Date("2026-10-30T10:00:01Z"),
  };
  const second = eventConsole.appendEvent(first.events, failure);
  const third = eventConsole.appendEvent(second.events, poll("inv-1", "failed", "2026-10-30T10:00:02Z", 3));

  assert.equal(third.events.length, 3);
  assert.equal(third.events[1].type, "investigation.error");
  assert.equal(third.events[2].polling.currentStatus, "active");
});

test("different polling errors remain distinct and sensitive payload fields are not retained", () => {
  let events = eventConsole.appendEvent([], poll("inv-1", "failed", "2026-10-30T10:00:00Z", 1, {
    error: { type: "timeout", code: "DEADLINE", message: "sensitive detail" },
    report: { large: "not retained" },
  })).events;
  events = eventConsole.appendEvent(events, poll("inv-1", "failed", "2026-10-30T10:00:01Z", 2, {
    error: { type: "permission", code: "DENIED", message: "another sensitive detail" },
  })).events;

  assert.equal(events.length, 2);
  assert.deepEqual(events[0].payload.error, { type: "permission", code: "DENIED" });
  assert.equal("report" in events[1].payload, false);
  assert.equal(JSON.stringify(events).includes("sensitive detail"), false);
});

test("event history and retained raw polls remain bounded", () => {
  let events = [];
  for (let index = 0; index < eventConsole.RAW_POLL_LIMIT + 7; index += 1) {
    events = eventConsole.appendEvent(
      events,
      poll("inv-1", "active", new Date(1_793_350_000_000 + index * 1000), index),
      { limit: 5 },
    ).events;
  }
  assert.equal(events[0].polling.rawEvents.length, eventConsole.RAW_POLL_LIMIT);
  assert.equal(events[0].polling.rawEventsTruncated, true);

  for (let index = 0; index < 12; index += 1) {
    events = eventConsole.appendEvent(events, {
      id: `other-${index}`,
      type: "runtime.config",
      category: "system",
      payload: { index },
      at: new Date(),
    }, { limit: 5 }).events;
  }
  assert.equal(events.length, 5);
});

test("details expose bounded raw poll facts without changing the source payload", () => {
  const source = poll("inv-1", "queued", "2026-10-30T10:00:00Z", 1);
  const original = structuredClone(source.payload);
  const event = eventConsole.appendEvent([], source).events[0];
  const details = eventConsole.detailPayload(event);

  assert.equal(details.invocation_id, "inv-1");
  assert.equal(details.polling_count, 1);
  assert.deepEqual(details.retained_raw_poll_events[0], {
    invocation_id: "inv-1",
    status: "queued",
    observed_at: "2026-10-30T10:00:00.000Z",
    error: null,
  });
  assert.deepEqual(source.payload, original);
});
