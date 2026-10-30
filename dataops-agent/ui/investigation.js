(function investigationHelpers(root, factory) {
  const api = factory();
  if (typeof module === "object" && module.exports) module.exports = api;
  if (root) root.DataOpsInvestigation = api;
})(typeof globalThis !== "undefined" ? globalThis : this, function createInvestigationHelpers() {
  "use strict";

  function snapshotContext(summary) {
    const pipeline = summary?.pipeline || {};
    const monitoring = summary?.monitoring || {};
    return {
      schema_version: "1.0",
      generated_at: String(summary?.generated_at || ""),
      overall_status: String(summary?.overall_status || "UNKNOWN"),
      pipeline: {
        status: String(pipeline.status || "UNKNOWN"),
        run_id: Number.isInteger(pipeline.run_id) ? pipeline.run_id : null,
        result_state: pipeline.result_state == null ? null : String(pipeline.result_state),
        end_time: pipeline.end_time == null ? null : String(pipeline.end_time),
      },
      monitoring: {
        status: String(monitoring.status || "UNKNOWN"),
        monitoring_run_at: monitoring.monitoring_run_at == null ? null : String(monitoring.monitoring_run_at),
        monitoring_reference_at: monitoring.monitoring_reference_at == null ? null : String(monitoring.monitoring_reference_at),
        synthetic_reference_date: monitoring.synthetic_reference_date == null ? null : String(monitoring.synthetic_reference_date),
        pipeline_association_verified: false,
      },
    };
  }

  function snapshotKey(snapshot) {
    return JSON.stringify(snapshot);
  }

  function canStart({ running, completedSnapshotKey, currentSnapshotKey }) {
    return Boolean(currentSnapshotKey) && !running && completedSnapshotKey !== currentSnapshotKey;
  }

  function validateReport(report) {
    if (!report || typeof report !== "object") throw new Error("Investigation report is missing.");
    if (report.schema_version !== "1.1") throw new Error("Investigation report schema must be version 1.1.");
    if (!["complete", "partial", "unavailable"].includes(report.investigation_completeness)) {
      throw new Error("Investigation completeness is invalid.");
    }
    for (const field of [
      "source_availability",
      "pipeline_execution_health",
      "monitoring_health",
      "root_cause",
      "impact",
      "recommendation_summary",
    ]) {
      if (!report[field] || typeof report[field] !== "object") {
        throw new Error(`Investigation report field '${field}' is missing.`);
      }
    }
    for (const field of ["detected_issues", "genie_findings", "source_errors", "recommendations"]) {
      if (!Array.isArray(report[field])) throw new Error(`Investigation report field '${field}' must be an array.`);
    }
    return report;
  }

  function reportState(report) {
    validateReport(report);
    if (report.investigation_completeness === "complete") return "success";
    if (report.investigation_completeness === "partial") return "partial";
    return "failure";
  }

  function minimalSummary(report) {
    validateReport(report);
    return {
      schema_version: report.schema_version,
      completeness: report.investigation_completeness,
      pipeline_status: String(report.pipeline_execution_health.status || "UNKNOWN"),
      monitoring_status: String(report.monitoring_health.status || "UNKNOWN"),
      issue_count: report.detected_issues.length,
      source_error_count: report.source_errors.length,
      genie_finding_count: report.genie_findings.length,
      root_cause: report.root_cause,
      impact: report.impact,
      recommendations: report.recommendations.slice(0, 3).map((recommendation) => ({
        issue_id: recommendation.issue_id,
        priority: recommendation.priority,
        category: recommendation.category,
        action: recommendation.action,
        rationale: recommendation.rationale,
        evidence_refs: [...(recommendation.evidence_refs || [])],
        requires_human_approval: Boolean(recommendation.requires_human_approval),
        verification_steps: [...(recommendation.verification_steps || [])],
      })),
      recommendation_count: Number(report.recommendation_summary.recommendation_count || 0),
    };
  }

  return { snapshotContext, snapshotKey, canStart, validateReport, reportState, minimalSummary };
});
