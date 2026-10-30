(function workspaceModule(root, factory) {
  const api = factory();
  if (typeof module === "object" && module.exports) module.exports = api;
  if (root) root.DataOpsWorkspace = api;
})(typeof globalThis !== "undefined" ? globalThis : this, function createWorkspaceModule() {
  function normalizeView(view) {
    return view === "health" ? "health" : "chat";
  }

  function applyWorkspaceView(elements, view, rootElement) {
    const activeView = normalizeView(view);
    const showHealth = activeView === "health";

    elements.healthView.hidden = !showHealth;
    elements.chatView.hidden = showHealth;
    elements.healthViewTab.classList.toggle("active", showHealth);
    elements.chatViewTab.classList.toggle("active", !showHealth);
    elements.healthViewTab.setAttribute("aria-selected", String(showHealth));
    elements.chatViewTab.setAttribute("aria-selected", String(!showHealth));

    if (rootElement) rootElement.dataset.workspaceView = activeView;
    return { activeView, showHealth };
  }

  function investigationActionState({
    runtimeAvailable,
    healthSnapshotKey,
    running,
    completedSnapshotKey,
  }) {
    if (running) {
      return {
        disabled: true,
        label: "Investigation running…",
        explanation: "The current investigation must finish before another can start.",
      };
    }
    if (!runtimeAvailable) {
      return {
        disabled: true,
        label: "Investigation unavailable",
        explanation: "Background investigation execution is unavailable in this runtime.",
      };
    }
    if (!healthSnapshotKey) {
      return {
        disabled: true,
        label: "Refresh health to investigate",
        explanation: "Load Daily Health Review evidence before starting an investigation.",
      };
    }
    if (completedSnapshotKey === healthSnapshotKey) {
      return {
        disabled: true,
        label: "Investigation complete",
        explanation: "This health snapshot has already been investigated.",
      };
    }
    return {
      disabled: false,
      label: "Run Full Investigation",
      explanation: "Run the bounded read-only investigation workflow for this health snapshot.",
    };
  }

  return { applyWorkspaceView, investigationActionState, normalizeView };
});
