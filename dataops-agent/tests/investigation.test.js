const test = require("node:test");
const assert = require("node:assert/strict");

const investigation = require("../ui/investigation.js");

function report(completeness = "complete") {
  return {
    schema_version: "1.1",
    investigation_completeness: completeness,
    source_availability: {
      jobs: { available: true, status: "PASS" },
      observability: { available: completeness === "complete", status: completeness === "complete" ? "WARN" : "UNAVAILABLE" },
      genie: { available: false, status: "not_requested" },
    },
    pipeline_execution_health: { status: "PASS" },
    monitoring_health: { status: completeness === "complete" ? "WARN" : "UNAVAILABLE" },
    detected_issues: [{ source: "observability", status: "WARN" }],
    genie_findings: [],
    source_errors: completeness === "complete" ? [] : [{ source: "observability", message: "Unavailable" }],
    root_cause: { status: "unknown", explanation: "The evidence does not establish a root cause." },
    impact: { status: "unknown", explanation: "The evidence does not quantify impact." },
    recommendations: [{
      issue_id: "observability.quality.payments.failed-payment-rate",
      priority: "P1",
      category: "diagnostic",
      action: "Compare the monitored metric with its retrieved baseline.",
      rationale: "The check returned WARN.",
      evidence_refs: ["detected_issues[0]"],
      requires_human_approval: true,
      verification_steps: ["Record the comparison."],
    }],
    recommendation_summary: { recommendation_count: 1 },
  };
}

test("snapshot context preserves timing while refusing unverified run linkage", () => {
  const context = investigation.snapshotContext({
    generated_at: "2026-10-29T18:00:00Z",
    overall_status: "WARN",
    pipeline: { status: "PASS", run_id: 456, result_state: "SUCCESS" },
    monitoring: {
      status: "WARN",
      monitoring_run_at: "2026-10-29T17:05:00Z",
      monitoring_reference_at: "2026-10-29T16:00:00Z",
      synthetic_reference_date: "2026-10-28",
      pipeline_association_verified: true,
    },
  });

  assert.equal(context.pipeline.run_id, 456);
  assert.equal(context.monitoring.synthetic_reference_date, "2026-10-28");
  assert.equal(context.monitoring.pipeline_association_verified, false);
});

test("duplicate protection blocks running and completed snapshot requests", () => {
  assert.equal(investigation.canStart({ running: false, completedSnapshotKey: "", currentSnapshotKey: "a" }), true);
  assert.equal(investigation.canStart({ running: true, completedSnapshotKey: "", currentSnapshotKey: "a" }), false);
  assert.equal(investigation.canStart({ running: false, completedSnapshotKey: "a", currentSnapshotKey: "a" }), false);
  assert.equal(investigation.canStart({ running: false, completedSnapshotKey: "a", currentSnapshotKey: "b" }), true);
});

test("minimal summary preserves evidence references, approval flags, and unknown assessments", () => {
  const summary = investigation.minimalSummary(report());

  assert.equal(summary.completeness, "complete");
  assert.deepEqual(summary.recommendations[0].evidence_refs, ["detected_issues[0]"]);
  assert.equal(summary.recommendations[0].requires_human_approval, true);
  assert.equal(summary.root_cause.status, "unknown");
  assert.equal(summary.impact.status, "unknown");
});

test("partial and unavailable reports are never labeled successful", () => {
  assert.equal(investigation.reportState(report("partial")), "partial");
  assert.equal(investigation.reportState(report("unavailable")), "failure");
});

test("schema versions other than 1.1 are rejected", () => {
  const invalid = report();
  invalid.schema_version = "1.0";
  assert.throws(() => investigation.validateReport(invalid), /schema must be version 1.1/);
});
