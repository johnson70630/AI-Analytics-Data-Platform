"""LangGraph Studio entrypoint for the read-only DataOps investigation graph."""

from __future__ import annotations

import os

# Studio remains local and private by default. An explicit caller setting still takes precedence.
os.environ.setdefault("LANGSMITH_TRACING", "false")

from agent.investigation.workflow import (  # noqa: E402
    InvestigationDependencies,
    build_investigation_graph,
)


def create_studio_graph(dependencies: InvestigationDependencies | None = None):
    """Compile the existing graph factory without executing any evidence collectors."""

    return build_investigation_graph(dependencies)


graph = create_studio_graph()
