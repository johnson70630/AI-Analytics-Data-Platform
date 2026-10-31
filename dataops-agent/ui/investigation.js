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
    return JSON.stringify({
      pipeline: snapshot?.pipeline || null,
      monitoring: snapshot?.monitoring || null,
    });
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
      "evidence",
      "source_references",
      "genie_routing",
      "workflow",
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
    const issues = report.detected_issues.map((issue, index) => ({
      reference: issueReference(index),
      category: issue.category,
      status: String(issue.status || "UNKNOWN"),
      name: issue.name,
      entity: issue.details?.entity_name || null,
      metric: issue.details?.metric_name || null,
      details: issue.details || {},
    }));
    const issueByReference = new Map(issues.map((issue) => [issue.reference, issue]));
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
      recommendations: report.recommendations.slice(0, 3).map((recommendation) => {
        const evidenceRefs = [...(recommendation.evidence_refs || [])];
        const matchedReference = evidenceRefs.find((reference) => issueByReference.has(reference));
        return {
          issue_id: recommendation.issue_id,
          priority: recommendation.priority,
          category: recommendation.category,
          action: recommendation.action,
          rationale: recommendation.rationale,
          evidence_refs: evidenceRefs,
          requires_human_approval: Boolean(recommendation.requires_human_approval),
          verification_steps: [...(recommendation.verification_steps || [])],
          issue: matchedReference ? issueByReference.get(matchedReference) : null,
        };
      }),
      recommendation_count: Number(report.recommendation_summary.recommendation_count || 0),
    };
  }

  function issueReference(index) {
    return `detected_issues[${index}]`;
  }

  function recommendationTopic(recommendation) {
    const issueId = String(recommendation?.issue_id || "").toLowerCase();
    if (issueId.startsWith("source.jobs")) return "jobs_source";
    if (issueId.startsWith("source.observability")) return "monitoring_source";
    if (issueId.startsWith("genie.")) return "genie";
    for (const topic of ["execution", "freshness", "quality", "row_counts", "reconciliation"]) {
      if (issueId.includes(`.${topic}.`)) return topic;
    }
    return "general";
  }

  function numeric(value) {
    if (value == null || value === "") return null;
    if (typeof value === "number") return Number.isFinite(value) ? value : null;
    if (typeof value !== "string" || !/^[+-]?(?:\d+\.?\d*|\.\d+)(?:e[+-]?\d+)?$/i.test(value.trim())) return null;
    const parsed = Number(value.trim());
    return Number.isFinite(parsed) ? parsed : null;
  }

  function displayNumber(value) {
    return new Intl.NumberFormat("en-US", { maximumFractionDigits: 6 }).format(value);
  }

  function recommendationMeasurementSummary(topic, issue) {
    const details = issue?.details && typeof issue.details === "object" ? issue.details : {};
    if (topic === "freshness") {
      const delay = numeric(details.freshness_delay_hours);
      const threshold = numeric(details.freshness_warning_threshold_hours);
      if (delay != null && threshold != null) {
        return `Observed freshness delay: ${displayNumber(delay)} hours; warning threshold: ${displayNumber(threshold)} hours.`;
      }
      if (delay != null) return `Observed freshness delay: ${displayNumber(delay)} hours; comparison threshold unavailable.`;
      if (threshold != null) return `Freshness delay unavailable; warning threshold: ${displayNumber(threshold)} hours.`;
    }
    if (topic === "row_counts") {
      const observed = numeric(details.row_count);
      const rolling = numeric(details.rolling_7_day_average);
      const previous = numeric(details.previous_day_row_count);
      if (observed != null && rolling != null) {
        return `Observed row count: ${displayNumber(observed)}; rolling 7-day average: ${displayNumber(rolling)}.`;
      }
      if (observed != null && previous != null) {
        return `Observed row count: ${displayNumber(observed)}; previous-day count: ${displayNumber(previous)}.`;
      }
      if (observed != null) return `Observed row count: ${displayNumber(observed)}; historical baseline unavailable.`;
    }
    if (topic === "quality") {
      const metric = String(details.metric_name || issue?.metric || issue?.name || "quality metric");
      const observed = numeric(details.metric_rate);
      const previous = numeric(details.previous_rate);
      const rolling = numeric(details.rolling_7_day_rate);
      const threshold = numeric(details.quality_warning_delta);
      const comparisons = [];
      if (previous != null) comparisons.push(`previous rate: ${displayNumber(previous)}`);
      else if (rolling != null) comparisons.push(`rolling 7-day rate: ${displayNumber(rolling)}`);
      if (threshold != null) comparisons.push(`warning delta: ${displayNumber(threshold)}`);
      if (observed != null && comparisons.length) {
        return `Observed ${metric}: ${displayNumber(observed)}; ${comparisons.join("; ")}.`;
      }
      if (observed != null) return `Observed ${metric}: ${displayNumber(observed)}; comparison baseline unavailable.`;
    }
    if (topic === "reconciliation") {
      const source = numeric(details.source_count);
      const target = numeric(details.target_count);
      const difference = numeric(details.difference);
      if (source != null && target != null) {
        const suffix = difference == null ? "" : `; difference: ${displayNumber(difference)}`;
        return `Observed source count: ${displayNumber(source)}; target count: ${displayNumber(target)}${suffix}.`;
      }
    }
    return null;
  }

  function recommendationDisplay(recommendation, issue = null) {
    const category = String(recommendation?.category || "diagnostic");
    const topic = recommendationTopic(recommendation);
    const titles = {
      diagnostic: {
        jobs_source: "Restore Jobs evidence",
        monitoring_source: "Restore monitoring evidence",
        genie: "Review partial Genie evidence",
        execution: "Inspect failed Lakeflow execution",
        freshness: "Validate freshness timing",
        quality: "Compare quality metric baselines",
        row_counts: "Reconcile volume changes",
        reconciliation: "Reconcile layer counts",
        general: "Investigate cited evidence",
      },
      proposed_remediation: {
        execution: "Prepare an operator-reviewed recovery",
        freshness: "Prepare a reviewed freshness correction",
        quality: "Prepare a reviewed quality correction",
        row_counts: "Prepare a reviewed volume correction",
        reconciliation: "Prepare a reviewed reconciliation correction",
        general: "Prepare a reviewed correction",
      },
      verification: {
        execution: "Verify the next Lakeflow run",
        freshness: "Recheck freshness after approval",
        quality: "Recheck quality after approval",
        row_counts: "Recheck volume after approval",
        reconciliation: "Recheck reconciliation after approval",
        general: "Verify the approved change",
      },
    };
    const titleSet = titles[category] || titles.diagnostic;
    const title = titleSet[topic] || titleSet.general || titles.diagnostic.general;

    const measurementSummary = recommendationMeasurementSummary(topic, issue);
    let summary = measurementSummary
      || "Measurements were not available in the cited structured evidence; review the referenced finding before acting.";
    if (topic === "jobs_source" || topic === "monitoring_source") {
      summary = "Restore the cited read-only evidence before interpreting the affected health signal.";
    } else if (topic === "genie") {
      summary = "Treat the historical context as incomplete until its availability and result metadata are verified.";
    }
    return { title, summary };
  }

  function fullViewModel(report) {
    validateReport(report);
    const issues = report.detected_issues.map((issue, index) => ({
      reference: issueReference(index),
      source: issue.source,
      category: issue.category,
      status: String(issue.status || "UNKNOWN"),
      name: issue.name,
      entity: issue.details?.entity_name || null,
      metric: issue.details?.metric_name || null,
      details: issue.details || {},
      source_available: Boolean(report.source_availability?.[issue.source]?.available),
    }));
    const issueByReference = new Map(issues.map((issue) => [issue.reference, issue]));
    const recommendationsByIssue = new Map();
    report.recommendations.forEach((recommendation) => {
      const matchedReference = (recommendation.evidence_refs || []).find((reference) => issueByReference.has(reference));
      const key = matchedReference || "general";
      if (!recommendationsByIssue.has(key)) recommendationsByIssue.set(key, []);
      recommendationsByIssue.get(key).push({ ...recommendation });
    });

    const genieFindings = report.genie_findings.map((finding) => ({
      investigation_id: finding.investigation_id,
      question: finding.question,
      status: finding.status,
      answer: finding.answer,
      error: finding.error,
      root_cause: finding.root_cause,
      impact: finding.impact,
      queries: (finding.evidence?.query_evidence || []).map((query) => ({
        attachment_id: query.attachment_id || null,
        statement_id: query.statement_id || null,
        generated_sql: query.generated_sql || null,
        result_available: query.result?.available === true,
        columns: query.result?.columns ?? null,
        rows: query.result?.rows ?? null,
        total_row_count: query.result?.total_row_count ?? null,
        truncated: query.result?.truncated === true,
        error: query.result?.error || null,
      })),
    }));

    const severity = { fail: 0, warn: 0, unknown: 0 };
    issues.forEach((issue) => {
      const status = issue.status.toUpperCase();
      if (status === "FAIL") severity.fail += 1;
      else if (status === "WARN") severity.warn += 1;
      else severity.unknown += 1;
    });

    return {
      executive: {
        generated_at: report.generated_at,
        completeness: report.investigation_completeness,
        pipeline: report.pipeline_execution_health,
        monitoring: report.monitoring_health,
        issue_count: issues.length,
        severity,
        source_availability: Object.entries(report.source_availability).map(([source, value]) => ({
          source,
          available: Boolean(value.available),
          status: String(value.status || "UNKNOWN"),
        })),
        root_cause: report.root_cause,
        impact: report.impact,
      },
      issues,
      genie_findings: genieFindings,
      recommendation_groups: [...recommendationsByIssue.entries()].map(([reference, recommendations]) => ({
        reference,
        issue: issueByReference.get(reference) || null,
        recommendations,
      })),
      source_errors: report.source_errors,
      evidence_sections: [
        { name: "Collected evidence", reference: "evidence", value: report.evidence },
        { name: "Source references", reference: "source_references", value: report.source_references },
        { name: "Genie routing", reference: "genie_routing", value: report.genie_routing },
        { name: "Workflow path", reference: "workflow", value: report.workflow },
      ],
    };
  }

  function serializeRecoveryState({ invocationId, sessionId, snapshotKey, startedAt }) {
    if (!invocationId || !sessionId || !snapshotKey || !startedAt) throw new Error("Recovery state is incomplete.");
    return JSON.stringify({
      version: 1,
      invocation_id: String(invocationId),
      session_id: String(sessionId),
      snapshot_key: String(snapshotKey),
      started_at: String(startedAt),
    });
  }

  function parseRecoveryState(raw, { sessionId, now = Date.now(), maxAgeMs = 86_400_000 } = {}) {
    if (!raw) return null;
    try {
      const value = JSON.parse(raw);
      if (
        value?.version !== 1
        || typeof value.invocation_id !== "string"
        || typeof value.session_id !== "string"
        || typeof value.snapshot_key !== "string"
        || typeof value.started_at !== "string"
      ) return null;
      if (sessionId && value.session_id !== sessionId) return null;
      const started = Date.parse(value.started_at);
      if (!Number.isFinite(started) || now - started > maxAgeMs || started - now > 60_000) return null;
      return value;
    } catch {
      return null;
    }
  }

  return {
    snapshotContext,
    snapshotKey,
    canStart,
    validateReport,
    reportState,
    minimalSummary,
    recommendationDisplay,
    fullViewModel,
    serializeRecoveryState,
    parseRecoveryState,
  };
});
