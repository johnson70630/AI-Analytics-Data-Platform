"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const conversations = require("../ui/conversations.js");

const A = "11111111-1111-4111-8111-111111111111";
const B = "22222222-2222-4222-8222-222222222222";

test("conversation A and B remain indexed and sort by latest activity", () => {
  let index = conversations.parseConversationIndex("[]", A, "2026-10-25T10:00:00.000Z");
  index = conversations.upsertConversation(index, A, {
    title: conversations.titleFromQuestion("Why is the pipeline stale?"),
    updated_at: "2026-10-25T10:01:00.000Z",
  });
  index = conversations.upsertConversation(index, B, {
    created_at: "2026-10-25T10:02:00.000Z",
    updated_at: "2026-10-25T10:02:00.000Z",
  });
  index = conversations.upsertConversation(index, B, {
    title: conversations.titleFromQuestion("Review the latest quality checks"),
    updated_at: "2026-10-25T10:03:00.000Z",
  });

  assert.deepEqual(index.map((item) => item.session_id), [B, A]);
  assert.equal(index.find((item) => item.session_id === A).title, "Why is the pipeline stale?");
  assert.equal(index.find((item) => item.session_id === B).title, "Review the latest quality checks");
});

test("browser refresh restores metadata without transcripts, tool results, or credentials", () => {
  const unsafe = [{
    session_id: A,
    title: "Pipeline evidence",
    created_at: "2026-10-25T10:00:00.000Z",
    updated_at: "2026-10-25T10:01:00.000Z",
    transcript: [{ role: "user", content: "secret" }],
    tool_results: [{ rows: [1, 2, 3] }],
    credential: "do-not-store",
  }];

  const serialized = conversations.serializeConversationIndex(unsafe);
  const restored = conversations.parseConversationIndex(serialized, A, "2026-10-25T10:05:00.000Z");

  assert.equal(restored.length, 1);
  assert.deepEqual(Object.keys(restored[0]), ["session_id", "title", "created_at", "updated_at"]);
  assert.doesNotMatch(serialized, /secret|tool_results|credential|do-not-store/);
});

test("an unavailable local session is retained when the backend returns no index", () => {
  const local = [{
    session_id: A,
    title: "Historical anomaly",
    created_at: "2026-10-25T10:00:00.000Z",
    updated_at: "2026-10-25T10:01:00.000Z",
  }];

  const merged = conversations.mergeBackendSessions(local, [], B, "2026-10-25T10:05:00.000Z");

  assert.deepEqual(new Set(merged.map((item) => item.session_id)), new Set([A, B]));
  assert.equal(merged.find((item) => item.session_id === A).title, "Historical anomaly");
});

test("managed backend metadata merges without removing browser-indexed sessions", () => {
  const local = [{
    session_id: A,
    title: "Pipeline run",
    created_at: "2026-10-25T10:00:00.000Z",
    updated_at: "2026-10-25T10:01:00.000Z",
  }];
  const backend = [{
    session_id: B,
    metadata: { title: "Quality review" },
    create_time: "2026-10-25T10:02:00.000Z",
    last_activity_time: "2026-10-25T10:03:00.000Z",
    actor_id: "must-not-persist",
  }];

  const merged = conversations.mergeBackendSessions(local, backend, A, "2026-10-25T10:05:00.000Z");
  assert.deepEqual(merged.map((item) => item.session_id), [B, A]);
  assert.equal(merged[0].title, "Quality review");
  assert.equal("actor_id" in merged[0], false);
});

test("active invocation blocks session switching", () => {
  assert.equal(conversations.canSwitchConversation({ busy: true, currentSessionId: A, targetSessionId: B }), false);
  assert.equal(conversations.canSwitchConversation({ busy: false, currentSessionId: A, targetSessionId: A }), false);
  assert.equal(conversations.canSwitchConversation({ busy: false, currentSessionId: A, targetSessionId: B }), true);
});

test("titles are derived from the first question and bounded", () => {
  assert.equal(conversations.titleFromQuestion("   What   failed today?  "), "What failed today?");
  const title = conversations.titleFromQuestion("x".repeat(100));
  assert.equal(title.length, 64);
  assert.match(title, /…$/);
});
