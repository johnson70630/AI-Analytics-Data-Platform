const test = require("node:test");
const assert = require("node:assert/strict");

const investigation = require("../ui/investigation.js");

function report(completeness = "complete") {
  return {
    schema_version: "1.1",
    generated_at: "2026-10-29T18:30:00Z",
    investigation_completeness: completeness,
    source_availability: {
      jobs: { available: true, status: "PASS" },
      observability: { available: completeness === "complete", status: completeness === "complete" ? "WARN" : "UNAVAILABLE" },
      genie: { available: false, status: "not_requested" },
    },
    pipeline_execution_health: { status: "PASS" },
    monitoring_health: {
      status: completeness === "complete" ? "WARN" : "UNAVAILABLE",
      monitoring_execution_timestamps: ["2026-10-29T17:05:00Z"],
      monitoring_reference_timestamps: ["2026-10-29T16:00:00Z"],
      synthetic_activity_dates: { metric_dates: ["2026-10-28"] },
    },
    detected_issues: [{
      source: "observability",
      category: "quality",
      status: "WARN",
      name: "payments.failed_payment_rate",
      details: {
        entity_name: "payments",
        metric_name: "failed_payment_rate",
        metric_rate: 0.12,
        previous_rate: 0.08,
        quality_warning_delta: 0.02,
      },
    }],
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
    evidence: { jobs: { run_id: 456 }, observability: { total_issue_count: 1 }, genie: [] },
    source_references: {
      jobs: { run_id: 456 },
      observability: {
        monitoring_execution_timestamps: ["2026-10-29T17:05:00Z"],
        monitoring_reference_timestamps: ["2026-10-29T16:00:00Z"],
        synthetic_activity_dates: { metric_dates: ["2026-10-28"] },
      },
      genie: [],
    },
    genie_routing: { maximum_investigations: 2, selected_count: 0, decisions: [] },
    workflow: { path: ["collect_jobs", "collect_observability", "construct_report"] },
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

test("recommendation display derives concise action titles without changing report data", () => {
  const value = report();
  const recommendation = value.recommendations[0];
  const original = structuredClone(recommendation);
  const display = investigation.recommendationDisplay(recommendation, value.detected_issues[0]);

  assert.equal(display.title, "Compare quality metric baselines");
  assert.equal(display.summary, "Observed failed_payment_rate: 0.12; previous rate: 0.08; warning delta: 0.02.");
  assert.deepEqual(recommendation, original);
});

test("recommendation display distinguishes advisory remediation, verification, and unavailable evidence", () => {
  const value = report();
  const base = value.recommendations[0];
  assert.deepEqual(
    investigation.recommendationDisplay({ ...base, category: "proposed_remediation" }, value.detected_issues[0]),
    {
      title: "Prepare a reviewed quality correction",
      summary: "Observed failed_payment_rate: 0.12; previous rate: 0.08; warning delta: 0.02.",
    },
  );
  assert.equal(
    investigation.recommendationDisplay({ ...base, category: "verification" }).title,
    "Recheck quality after approval",
  );
  assert.equal(
    investigation.recommendationDisplay({ ...base, issue_id: "source.observability.unavailable" }).title,
    "Restore monitoring evidence",
  );
});

test("recommendation display falls back safely without reflecting untrusted action text", () => {
  const fallback = investigation.recommendationDisplay({
    ...report().recommendations[0],
    issue_id: "unsupported",
    category: "unsupported",
    action: "<img src=x onerror=globalThis.pwned=true>",
  });

  assert.equal(fallback.title, "Investigate cited evidence");
  assert.match(fallback.summary, /Measurements were not available/);
  assert.equal(fallback.title.includes("<img"), false);
  assert.equal(globalThis.pwned, undefined);
});

test("recommendation summaries use complete freshness and volume measurements", () => {
  const base = report().recommendations[0];
  assert.equal(
    investigation.recommendationDisplay(
      { ...base, issue_id: "observability.freshness.messages" },
      { details: { freshness_delay_hours: 26.5, freshness_warning_threshold_hours: 24 } },
    ).summary,
    "Observed freshness delay: 26.5 hours; warning threshold: 24 hours.",
  );
  assert.equal(
    investigation.recommendationDisplay(
      { ...base, issue_id: "observability.row_counts.messages" },
      { details: { row_count: 1200, rolling_7_day_average: 1500 } },
    ).summary,
    "Observed row count: 1,200; rolling 7-day average: 1,500.",
  );
});

test("recommendation summaries handle partial, malformed, and missing measurements neutrally", () => {
  const base = report().recommendations[0];
  assert.equal(
    investigation.recommendationDisplay(
      { ...base, issue_id: "observability.freshness.messages" },
      { details: { freshness_delay_hours: 26.5 } },
    ).summary,
    "Observed freshness delay: 26.5 hours; comparison threshold unavailable.",
  );
  assert.match(
    investigation.recommendationDisplay(
      { ...base, issue_id: "observability.row_counts.messages" },
      { details: { row_count: "not-a-number", rolling_7_day_average: "<script>alert(1)</script>" } },
    ).summary,
    /Measurements were not available/,
  );
  assert.match(investigation.recommendationDisplay(base, null).summary, /Measurements were not available/);
  assert.equal(globalThis.pwned, undefined);
});

test("long structured labels remain data and malformed scalar types are not measurements", () => {
  const base = report().recommendations[0];
  const metric = `metric_${"very_long_".repeat(300)}`;
  const display = investigation.recommendationDisplay(base, {
    details: { metric_name: metric, metric_rate: 0.125, previous_rate: 0.1 },
  });

  assert.match(display.summary, /^Observed metric_/);
  assert.match(display.summary, /0\.125; previous rate: 0\.1\.$/);
  assert.match(
    investigation.recommendationDisplay(base, {
      details: { metric_name: "bad", metric_rate: true, previous_rate: { value: 1 } },
    }).summary,
    /Measurements were not available/,
  );
});

test("minimal summary associates recommendation display evidence without mutating the report", () => {
  const value = report();
  const original = structuredClone(value);
  const recommendation = investigation.minimalSummary(value).recommendations[0];

  assert.equal(recommendation.issue.reference, "detected_issues[0]");
  assert.match(investigation.recommendationDisplay(recommendation, recommendation.issue).summary, /0.12/);
  assert.deepEqual(value, original);
});

test("recommendations match the cited issue when one entity has multiple findings", () => {
  const value = report();
  value.detected_issues = [
    {
      source: "observability",
      category: "quality",
      status: "WARN",
      name: "payments.refund_rate",
      details: { entity_name: "payments", metric_name: "refund_rate", metric_rate: 0.03, previous_rate: 0.02 },
    },
    {
      source: "observability",
      category: "quality",
      status: "WARN",
      name: "payments.failed_payment_rate",
      details: { entity_name: "payments", metric_name: "failed_payment_rate", metric_rate: 0.12, previous_rate: 0.08 },
    },
  ];
  value.recommendations[0].evidence_refs = ["detected_issues[1]"];

  const recommendation = investigation.minimalSummary(value).recommendations[0];
  const display = investigation.recommendationDisplay(recommendation, recommendation.issue);

  assert.equal(recommendation.issue.reference, "detected_issues[1]");
  assert.equal(recommendation.issue.metric, "failed_payment_rate");
  assert.match(display.summary, /failed_payment_rate: 0\.12/);
  assert.equal(display.summary.includes("refund_rate"), false);
});

test("partial and unavailable reports are never labeled successful", () => {
  assert.equal(investigation.reportState(report("partial")), "partial");
  assert.equal(investigation.reportState(report("unavailable")), "failure");
});

test("source failures remain visible and cannot be converted into healthy evidence", () => {
  const value = report("partial");
  const view = investigation.fullViewModel(value);

  assert.equal(view.executive.completeness, "partial");
  assert.equal(view.executive.source_availability.find((source) => source.source === "observability").available, false);
  assert.deepEqual(view.source_errors, [{ source: "observability", message: "Unavailable" }]);
});

test("schema versions other than 1.1 are rejected", () => {
  const invalid = report();
  invalid.schema_version = "1.0";
  assert.throws(() => investigation.validateReport(invalid), /schema must be version 1.1/);
});

test("full view maps issue references and recommendation groups without losing source associations", () => {
  const view = investigation.fullViewModel(report());

  assert.equal(view.issues[0].reference, "detected_issues[0]");
  assert.equal(view.recommendation_groups[0].reference, "detected_issues[0]");
  assert.equal(view.recommendation_groups[0].issue.status, "WARN");
  assert.equal(view.evidence_sections[1].reference, "source_references");
  assert.deepEqual(view.executive.monitoring.synthetic_activity_dates.metric_dates, ["2026-10-28"]);
});

test("malformed recommendation evidence references remain visible as ungrouped guidance", () => {
  const value = report();
  value.recommendations[0].evidence_refs = ["detected_issues[999]", "unsupported.reference"];

  const view = investigation.fullViewModel(value);
  assert.equal(view.recommendation_groups[0].reference, "general");
  assert.equal(view.recommendation_groups[0].issue, null);
  assert.deepEqual(
    view.recommendation_groups[0].recommendations[0].evidence_refs,
    ["detected_issues[999]", "unsupported.reference"],
  );
});

test("full view preserves bounded Genie SQL metadata, truncation, rows, and partial errors", () => {
  const value = report("partial");
  value.genie_findings = [{
    investigation_id: "genie-1",
    question: "Show historical failed payment rates.",
    status: "partial",
    answer: "One bounded result was returned.",
    error: { type: "partial_query_results", message: "One result was truncated." },
    root_cause: value.root_cause,
    impact: value.impact,
    evidence: {
      query_evidence: [{
        attachment_id: "attachment-1",
        statement_id: "statement-1",
        generated_sql: "SELECT metric_date, metric_rate FROM monitored_metrics",
        result: {
          available: true,
          columns: [{ name: "metric_date" }],
          rows: [["2026-10-28"]],
          total_row_count: 145,
          truncated: true,
          error: null,
        },
      }],
    },
  }];

  const query = investigation.fullViewModel(value).genie_findings[0].queries[0];
  assert.equal(query.generated_sql, "SELECT metric_date, metric_rate FROM monitored_metrics");
  assert.equal(query.truncated, true);
  assert.equal(query.total_row_count, 145);
  assert.deepEqual(query.rows, [["2026-10-28"]]);
});

test("large bounded reports preserve all report evidence for internally scrollable rendering", () => {
  const value = report();
  const rows = Array.from({ length: 100 }, (_, index) => [`2026-10-${String((index % 28) + 1).padStart(2, "0")}`, index]);
  value.detected_issues = Array.from({ length: 250 }, (_, index) => ({
    source: "observability",
    category: "quality",
    status: index % 2 ? "WARN" : "FAIL",
    name: `entity_${index}.metric_${index}`,
    details: { entity_name: `entity_${index}`, metric_name: `metric_${index}`, metric_rate: index / 1000 },
  }));
  value.recommendations = value.detected_issues.map((issue, index) => ({
    ...report().recommendations[0],
    issue_id: `issue-${index}`,
    evidence_refs: [`detected_issues[${index}]`],
  }));
  value.genie_findings = [{
    investigation_id: "genie-large",
    question: "Return a bounded history.",
    status: "complete",
    answer: "Bounded history returned.",
    error: null,
    root_cause: value.root_cause,
    impact: value.impact,
    evidence: {
      query_evidence: [{
        generated_sql: "SELECT metric_date, metric_rate FROM bounded_history",
        result: { available: true, columns: ["metric_date", "metric_rate"], rows, total_row_count: 5_000, truncated: true },
      }],
    },
  }];

  const view = investigation.fullViewModel(value);
  assert.equal(view.issues.length, 250);
  assert.equal(view.recommendation_groups.length, 250);
  assert.equal(view.genie_findings[0].queries[0].rows.length, 100);
  assert.equal(view.genie_findings[0].queries[0].total_row_count, 5_000);
  assert.equal(view.genie_findings[0].queries[0].truncated, true);
});

test("invalid or missing evidence fails browser schema validation", () => {
  const invalid = report();
  delete invalid.evidence;
  assert.throws(() => investigation.validateReport(invalid), /field 'evidence' is missing/);
});

test("external text remains inert data in the full view model", () => {
  const value = report();
  value.detected_issues[0].name = '<img src=x onerror="globalThis.pwned=true">';
  value.recommendations[0].action = "<script>globalThis.pwned=true</script>";

  const view = investigation.fullViewModel(value);
  assert.equal(view.issues[0].name, '<img src=x onerror="globalThis.pwned=true">');
  assert.equal(view.recommendation_groups[0].recommendations[0].action, "<script>globalThis.pwned=true</script>");
  assert.equal(globalThis.pwned, undefined);
});

test("recovery state stores only minimal identifiers and validates session and age", () => {
  const raw = investigation.serializeRecoveryState({
    invocationId: "invocation-1",
    sessionId: "session-1",
    snapshotKey: "snapshot-1",
    startedAt: "2026-10-29T18:00:00Z",
  });
  const parsed = investigation.parseRecoveryState(raw, {
    sessionId: "session-1",
    now: Date.parse("2026-10-29T18:30:00Z"),
  });

  assert.deepEqual(Object.keys(parsed).sort(), ["invocation_id", "session_id", "snapshot_key", "started_at", "version"]);
  assert.equal(raw.includes("report"), false);
  assert.equal(investigation.parseRecoveryState(raw, { sessionId: "session-2", now: Date.parse("2026-10-29T18:30:00Z") }), null);
  assert.equal(investigation.parseRecoveryState(raw, { sessionId: "session-1", now: Date.parse("2026-10-31T18:30:00Z") }), null);
  assert.equal(investigation.parseRecoveryState("not-json", { sessionId: "session-1" }), null);
});

test("snapshot identity is stable across dashboard refresh generation times", () => {
  const first = investigation.snapshotContext({
    generated_at: "2026-10-29T18:00:00Z",
    pipeline: { status: "PASS", run_id: 456 },
    monitoring: { status: "WARN", monitoring_run_at: "2026-10-29T17:05:00Z" },
  });
  const second = { ...first, generated_at: "2026-10-29T18:05:00Z" };
  assert.equal(investigation.snapshotKey(first), investigation.snapshotKey(second));
});
