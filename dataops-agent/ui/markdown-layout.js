(function markdownLayoutHelpers(root, factory) {
  const api = factory();
  if (typeof module === "object" && module.exports) module.exports = api;
  if (root) root.DataOpsMarkdownLayout = api;
})(typeof globalThis !== "undefined" ? globalThis : this, function createMarkdownLayoutHelpers() {
  "use strict";

  function normalizeCellText(value) {
    return String(value ?? "").replace(/\s+/g, " ").trim();
  }

  function classifyTableColumns(rows) {
    const columnCount = rows.reduce((maximum, row) => Math.max(maximum, row.length), 0);
    return Array.from({ length: columnCount }, (_, index) => {
      const lengths = rows
        .map((row) => normalizeCellText(row[index]).length)
        .filter((length) => length > 0);
      const maximum = lengths.length ? Math.max(...lengths) : 0;
      const average = lengths.length
        ? lengths.reduce((total, length) => total + length, 0) / lengths.length
        : 0;
      if (maximum <= 24) return "compact";
      if (maximum >= 64 || average >= 40) return "wide";
      return "standard";
    });
  }

  function enhanceMarkdownTables(rootElement) {
    if (!rootElement?.querySelectorAll) return;
    rootElement.querySelectorAll("table:not([data-layout-enhanced])").forEach((table, tableIndex) => {
      const rows = [...table.rows].map((row) => [...row.cells].map((cell) => cell.textContent));
      const columnKinds = classifyTableColumns(rows);
      [...table.rows].forEach((row) => {
        [...row.cells].forEach((cell, index) => {
          cell.classList.add(`markdown-table-cell--${columnKinds[index] || "standard"}`);
        });
      });

      const wrapper = table.ownerDocument.createElement("div");
      wrapper.className = "markdown-table-scroll";
      wrapper.tabIndex = 0;
      wrapper.setAttribute("role", "region");
      wrapper.setAttribute("aria-label", `Scrollable Markdown table ${tableIndex + 1}`);
      table.dataset.layoutEnhanced = "true";
      table.parentNode.insertBefore(wrapper, table);
      wrapper.append(table);
    });
  }

  return { classifyTableColumns, enhanceMarkdownTables };
});
