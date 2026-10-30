const assert = require("node:assert/strict");
const test = require("node:test");

const workspace = require("../ui/workspace.js");

function tab(active) {
  const classes = new Set(active ? ["active"] : []);
  return {
    attributes: {},
    classList: {
      contains: (name) => classes.has(name),
      toggle(name, enabled) {
        if (enabled) classes.add(name);
        else classes.delete(name);
      },
    },
    setAttribute(name, value) {
      this.attributes[name] = value;
    },
  };
}

function fixture() {
  return {
    elements: {
      chatView: { hidden: false },
      healthView: { hidden: true },
      chatViewTab: tab(true),
      healthViewTab: tab(false),
    },
    root: { dataset: { workspaceView: "chat" } },
  };
}

test("Daily Review deactivates Chat and records the active workspace", () => {
  const { elements, root } = fixture();

  const result = workspace.applyWorkspaceView(elements, "health", root);

  assert.deepEqual(result, { activeView: "health", showHealth: true });
  assert.equal(elements.chatView.hidden, true);
  assert.equal(elements.healthView.hidden, false);
  assert.equal(elements.chatViewTab.classList.contains("active"), false);
  assert.equal(elements.healthViewTab.classList.contains("active"), true);
  assert.equal(elements.chatViewTab.attributes["aria-selected"], "false");
  assert.equal(elements.healthViewTab.attributes["aria-selected"], "true");
  assert.equal(root.dataset.workspaceView, "health");
});

test("switching back restores Chat without changing either workspace state", () => {
  const { elements, root } = fixture();
  elements.chatView.scrollTop = 420;
  elements.healthView.scrollTop = 180;
  workspace.applyWorkspaceView(elements, "health", root);

  const result = workspace.applyWorkspaceView(elements, "chat", root);

  assert.deepEqual(result, { activeView: "chat", showHealth: false });
  assert.equal(elements.chatView.hidden, false);
  assert.equal(elements.healthView.hidden, true);
  assert.equal(elements.chatView.scrollTop, 420);
  assert.equal(elements.healthView.scrollTop, 180);
  assert.equal(root.dataset.workspaceView, "chat");
});

test("unknown workspace values fail closed to Chat", () => {
  const { elements, root } = fixture();

  workspace.applyWorkspaceView(elements, "unsupported", root);

  assert.equal(elements.chatView.hidden, false);
  assert.equal(elements.healthView.hidden, true);
  assert.equal(root.dataset.workspaceView, "chat");
});

test("investigation action requires runtime support and a health snapshot", () => {
  assert.deepEqual(
    workspace.investigationActionState({
      runtimeAvailable: false,
      healthSnapshotKey: "snapshot",
      running: false,
      completedSnapshotKey: "",
    }),
    {
      disabled: true,
      label: "Investigation unavailable",
      explanation: "Background investigation execution is unavailable in this runtime.",
    },
  );
  assert.equal(
    workspace.investigationActionState({
      runtimeAvailable: true,
      healthSnapshotKey: "",
      running: false,
      completedSnapshotKey: "",
    }).label,
    "Refresh health to investigate",
  );
});

test("investigation action enables once and explains running and completed states", () => {
  const ready = workspace.investigationActionState({
    runtimeAvailable: true,
    healthSnapshotKey: "snapshot",
    running: false,
    completedSnapshotKey: "",
  });
  assert.equal(ready.disabled, false);
  assert.equal(ready.label, "Run Full Investigation");

  const running = workspace.investigationActionState({
    runtimeAvailable: true,
    healthSnapshotKey: "snapshot",
    running: true,
    completedSnapshotKey: "",
  });
  assert.equal(running.disabled, true);
  assert.match(running.explanation, /must finish/);

  const complete = workspace.investigationActionState({
    runtimeAvailable: true,
    healthSnapshotKey: "snapshot",
    running: false,
    completedSnapshotKey: "snapshot",
  });
  assert.equal(complete.disabled, true);
  assert.match(complete.explanation, /already been investigated/);
});
