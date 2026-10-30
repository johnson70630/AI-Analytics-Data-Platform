const test = require("node:test");
const assert = require("node:assert/strict");

const markdownLayout = require("../ui/markdown-layout.js");

test("narrow table columns remain compact", () => {
  assert.deepEqual(
    markdownLayout.classifyTableColumns([
      ["Status", "Category"],
      ["PASS", "Quality"],
      ["WARN", "Row counts"],
    ]),
    ["compact", "compact"],
  );
});

test("descriptive columns receive wide layout without widening short labels", () => {
  assert.deepEqual(
    markdownLayout.classifyTableColumns([
      ["Category", "Recommendation"],
      ["Quality", "Compare the observed metric with its previous and rolling seven-day baselines before assigning a cause."],
      ["Freshness", "Review monitoring and reference timestamps separately before interpreting the reported delay."],
    ]),
    ["compact", "wide"],
  );
});

test("unusually long SQL, URL, and code values are classified for bounded wrapping", () => {
  const longSql = "SELECT monitoring_reference_at, freshness_delay_hours FROM ai_analytics.observability.dq_entity_freshness WHERE entity_name = 'model_inferences'";
  const longUrl = "https://example.invalid/observability/investigations/quality/failed_payment_rate?reference=2026-10-29T17%3A05%3A00Z";
  const rows = [["Type", "Value"], ["SQL", longSql], ["URL", longUrl]];

  assert.deepEqual(markdownLayout.classifyTableColumns(rows), ["compact", "wide"]);
  assert.deepEqual(rows[1], ["SQL", longSql]);
});
