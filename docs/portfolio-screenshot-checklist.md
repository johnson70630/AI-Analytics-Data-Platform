# Portfolio Screenshot Checklist

No portfolio screenshots have been fabricated or inferred from API responses. The functional cloud
acceptance validated deployed API behavior and report data; automated tests validated the HTML/CSS
assets and responsive breakpoint contract. Pixel-level browser acceptance remains outstanding.
Capturing the application requires a separately authorized live session because the Databricks App
is stopped and a new investigation would invoke managed model and Genie services.

## Capture standards

- Use the validated `main` release or a later reviewed release; record its commit in the asset log.
- Use a dedicated demo session and redact workspace identifiers, user information, invocation IDs,
  browser account details, and any sensitive evidence before publication.
- Capture actual UI states only. Do not edit values into the interface or imply a historical result
  is live when the current monitoring snapshot differs.
- Keep the pipeline execution result and monitoring snapshot visually separate. Preserve an
  `UNVERIFIED` association unless an authoritative shared key exists.
- Capture desktop and mobile widths from a real browser. Static breakpoint inspection alone does
  not count as visual acceptance.
- Run Full Investigation at most once under the future authorization and retain the same invocation
  through report capture and reload recovery.

## Required screenshots

### 1. Daily Health Review — desktop

- Full Command Center frame with Daily Health Review selected.
- Pipeline status and task summary visible.
- Monitoring check totals and PASS/WARN/FAIL summary visible.
- Monitoring execution timestamp, monitoring reference timestamp, and synthetic activity date
  visible as distinct values.
- Source availability and the pipeline-to-monitoring association label visible.
- Record whether the displayed values match the historical acceptance result: 9 successful tasks
  and 48 checks with four warnings. If current values differ, use the current values and update the
  caption.

### 2. Daily Health Review — mobile

- Approximately 390–430 px viewport with no horizontal page overflow.
- Health cards stacked legibly and Manual Refresh reachable.
- Navigation and evidence drawers closed, then opened in a separate optional capture.

### 3. One-click investigation states

- Ready state with **Run Full Investigation** enabled.
- Running state showing progress without a second active request.
- Completed summary showing schema `1.1`, completeness, source availability, issue count, and
  explicit unknown root cause and impact where unsupported.
- Do not create separate live investigations merely to capture each state; use one invocation and
  record the states during its normal lifecycle.

### 4. Structured report viewer

- Executive summary with pipeline and monitoring health separated.
- Detected issue cards showing affected entity/metric, status, measurement, and evidence reference.
- Recommendation groups showing priority, rationale, evidence, verification steps, and
  human-approval requirement.
- Partial or unavailable indicators if they occur naturally; do not manufacture a failure state in
  the live environment.

### 5. Genie and evidence details

- Expanded bounded Genie finding with generated-SQL metadata and result rows where available.
- Truncation or partial-result label if present in the real report.
- Expanded source evidence showing that references resolve to the corresponding report evidence.
- Redact sensitive identifiers without changing the meaning of the evidence.

### 6. Invocation recovery

- Browser reload while the authorized investigation is still active, if timing permits.
- Same invocation ID recovered through the authenticated status endpoint.
- No second investigation POST. Record this from developer tools or sanitized request evidence,
  rather than exposing the invocation ID publicly.

## Optional supporting assets

- Lakeflow task graph for the accepted 9-task run.
- Unity Catalog schema browser showing Bronze, staging, Silver, Gold, PII, and observability
  boundaries without exposing sensitive rows.
- AI/BI dashboard overview using governed Gold models.
- Side-by-side architecture diagram and Command Center overview for a portfolio cover image.

## Asset log template

| Asset | Release | Capture date | Viewport | Live result represented | Redactions | Reviewed |
|---|---|---|---|---|---|---|
| Daily Health desktop |  |  |  |  |  |  |
| Daily Health mobile |  |  |  |  |  |  |
| Investigation summary |  |  |  |  |  |  |
| Report viewer |  |  |  |  |  |  |
| Genie/evidence details |  |  |  |  |  |  |
