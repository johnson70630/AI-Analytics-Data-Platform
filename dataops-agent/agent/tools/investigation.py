"""Read-only composite tool for the standalone DataOps investigation workflow."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from langchain_core.tools import tool

from agent.investigation import InvestigationReportV1, run_investigation


InvestigationRunner = Callable[[], Awaitable[dict[str, Any]]]


async def fetch_dataops_investigation_report(
    *,
    runner: InvestigationRunner | None = None,
) -> dict[str, Any]:
    """Run and validate the read-only composite investigation workflow."""

    report = await (runner or run_investigation)()
    validated = InvestigationReportV1.model_validate(report)
    return validated.model_dump(mode="json")


@tool(return_direct=True)
async def generate_dataops_investigation_report() -> dict[str, Any]:
    """Generate a structured, read-only DataOps incident investigation report.

    Use this composite tool for full incident investigations or requests for a structured
    investigation report. It collects the latest Lakeflow execution and deterministic monitoring
    evidence in parallel, optionally asks bounded historical Genie questions for supported WARN or
    FAIL anomalies, and reports partial or unavailable evidence explicitly. It cannot inspect
    unsupported logs, run arbitrary SQL, repair incidents, rerun jobs, write data, or backfill data.
    A complete report means its planned evidence retrieval completed; it does not prove root cause,
    impact, recurrence, non-propagation, or overall platform health. When root cause is unknown,
    report it as unknown without adding hypotheses, invented details, or speculative sub-steps.
    """

    return await fetch_dataops_investigation_report()
