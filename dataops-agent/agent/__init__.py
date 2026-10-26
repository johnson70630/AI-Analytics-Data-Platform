from agent.tools.jobs import get_latest_pipeline_run


def all_tools():
    return [
        get_latest_pipeline_run,
    ]