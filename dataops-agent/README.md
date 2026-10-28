# DataOps AI Agent

A read-only DataOps assistant for the AI Analytics Data Platform, implemented with LangGraph and
served by `databricks_agentkit.DurableAgentServer`. It investigates Lakeflow execution and governed
observability evidence without exposing arbitrary SQL, job control, repair, backfill, or alert
delivery capabilities.

During `agentbricks dev`, invocation state and events are process-local. Deployment attaches the
configured Runtime, Session, and Memory stores so the managed application can provide durable
invocations and conversation state according to the Agent Bricks runtime contract.

The generated project separates portable agent execution from the managed HTTP protocol:

```text
client -> runtime/main.py -> runtime/adapter.py -> agent/agent.py:run_agent
```

- `agent/agent.py` owns the framework-native agent, sessions, tools, MCP lifetime, and `run_agent`.
- `runtime/adapter.py` owns the agent-author integration hooks: runtime input/output translation plus
  `invoke` and `recover`.
- `runtime/main.py` constructs the server and registers those hooks.

[AGENTKIT_CONTRACT.md](AGENTKIT_CONTRACT.md) owns runtime integration requirements, while
[AGENTS.md](AGENTS.md) provides the development map.

## Validated capabilities

The automatically discovered LangChain tools are:

- `get_latest_pipeline_run` for narrow Lakeflow execution-health questions.
- `get_latest_observability_summary` for the latest deterministic observability snapshot.
- `ask_genie_observability` for flexible historical observability questions.
- `generate_dataops_investigation_report` for the full V2.2 investigation and recommendation flow.

The V2.3A/B incident lifecycle and Delta outbox are standalone libraries. They are not registered as
agent tools, are not scheduled, and have no live email provider on this deployable branch.

## Run locally

```bash
agentbricks dev
```

The API is available at `http://localhost:8000/api/invocations`, or on the port you pass with
`--app-port`. The `http://localhost:8001` URL that `databricks apps run-local` also prints is its
proxy to the same app. Requests can fail for a few seconds while the server starts; wait for
`Uvicorn running on ...` in the log.

Every request supplies a UUID `id`.
That ID is the invocation identifier and idempotency key. Supply the stable application session as
the top-level `session_id`; agent-specific values live inside the opaque `input` object:

```bash
SESSION_ID=$(uuidgen)
INVOCATION_ID=$(uuidgen)

curl -sS http://localhost:8000/api/invocations \
  -H 'Content-Type: application/json' \
  -d "{\"id\":\"$INVOCATION_ID\",\"session_id\":\"$SESSION_ID\",\"input\":{\"messages\":[{\"role\":\"user\",\"content\":\"Did the latest Lakeflow pipeline succeed?\"}]}}"
```

Reuse `SESSION_ID` for multi-turn conversation state. Generate a new `INVOCATION_ID` for each turn.
Retrying the same request with the same invocation ID returns the persisted result; changing the
request while reusing the ID returns `409`.

## Long-term memory

The Session Store keeps one conversation's state, keyed by the top-level `session_id`. The Memory Store
keeps facts the agent recalls across conversations, keyed by `actor`: `runtime/adapter.py` reads
`input.actor` and passes it to `memory_tools(actor)`. If you leave out `actor`, it defaults to the
`session_id`, so long-term memory won't carry across sessions. For memory that persists, pass a stable
`actor`, such as the end user's ID, from trusted application context. With a request-user
(`auth = "user"`) tool bound, the adapter also namespaces a supplied `actor` to the signed-in user; see
[Request-user authorization](#request-user-authorization).

Memory is off under `agentbricks dev`, so try it against the deployed app: two invocations with
different `session_id`s and the same `actor`.

```bash
ACTOR=user-123

# Conversation 1: the agent saves a fact with its memory tool.
agentbricks --profile <profile> endpoint invoke agent-bricks-<name> --path /api/invocations \
  --json "{\"id\":\"$(uuidgen)\",\"session_id\":\"$(uuidgen)\",\"input\":{\"actor\":\"$ACTOR\",\"messages\":[{\"role\":\"user\",\"content\":\"Remember that I prefer answers as bullet points.\"}]}}"

# Conversation 2: a new session_id, same actor, so the agent can recall it.
agentbricks --profile <profile> endpoint invoke agent-bricks-<name> --path /api/invocations \
  --json "{\"id\":\"$(uuidgen)\",\"session_id\":\"$(uuidgen)\",\"input\":{\"actor\":\"$ACTOR\",\"messages\":[{\"role\":\"user\",\"content\":\"How do I like my answers formatted?\"}]}}"
```

## Invocation modes

- Foreground: omit `background` and `stream`; the response contains the agent result under `output`.
- Foreground streaming: set `stream: true`; the response is SSE backed by persisted events.
- Background: set `background: true`; poll the returned `status_url`.
- Background streaming: set both flags; the `202` response includes `status_url` and `events_url`.

```bash
INVOCATION_ID=$(uuidgen)
curl -sN http://localhost:8000/api/invocations \
  -H 'Content-Type: application/json' \
  -d "{\"id\":\"$INVOCATION_ID\",\"session_id\":\"$SESSION_ID\",\"input\":{\"messages\":[{\"role\":\"user\",\"content\":\"Count to three.\"}]},\"stream\":true}"

INVOCATION_ID=$(uuidgen)
curl -sS http://localhost:8000/api/invocations \
  -H 'Content-Type: application/json' \
  -d "{\"id\":\"$INVOCATION_ID\",\"session_id\":\"$SESSION_ID\",\"input\":{\"messages\":[{\"role\":\"user\",\"content\":\"Summarize durable agents.\"}]},\"background\":true}" | jq
curl -sS "http://localhost:8000/api/invocations/$INVOCATION_ID" | jq
```

SSE records contain events translated by `runtime/adapter.py`: token `delta`s, completed `message`s,
and HITL `interrupt`s. Replay from a cursor with
`GET /api/invocations/{id}/events?after={sequence}`.

## Human approval

The runtime and browser retain support for LangGraph approval interrupts, but no current DataOps
tool performs a write or requires approval. Adding an action tool requires a separate security
review, an explicit `REQUIRE_APPROVAL` entry, and tests covering authorization and replay behavior.

## Crash recovery

Deployment with a Runtime Store supports invocation recovery after worker loss. Graph checkpoint
persistence requires a Session Store separately. Recovery can replay work, so tools must tolerate
repeated side effects. See [Recovery and durability](AGENTKIT_CONTRACT.md#recovery-and-durability).

## Chat app

The browser UI is included by default. It generates a stable application session ID in local
storage, sends it as the top-level `session_id`, and generates a fresh invocation UUID per turn.
Use `agentbricks init --framework langgraph --disable-chat-app` for API-only output.

## Deployment preparation

Local development requires an authenticated Databricks profile plus:

- `DATABRICKS_PIPELINE_JOB_ID` for the production-style Lakeflow Job inspected by the Jobs tool.
- `DATABRICKS_SQL_WAREHOUSE_ID` for fixed observability queries.
- `DATABRICKS_GENIE_AGENT_ID` for the validated observability Genie Agent.

The deployed application uses the model, Memory Store, Session Store, and MLflow tracing bindings
declared in `agent.toml`. Before deployment, confirm the application identity can read the target
Lakeflow Job, use the SQL warehouse, select from `ai_analytics.observability`, and invoke the Genie
Agent. Do not place workspace tokens in source or `app.yaml`.

`langgraph.json`, `agent/investigation/studio.py`, and `langgraph-cli[inmem]` exist only for local
LangGraph Studio development. Studio defaults `LANGSMITH_TRACING=false`; the deployed command in
`app.yaml` does not start LangGraph Studio or require LangSmith cloud tracing.

## Configure and deploy

- Change the model, instructions, tools, graph, and framework-native execution in `agent/agent.py`.
- Change `runtime/adapter.py` only to map a different application input/output contract.
- Add local tools under `agent/tools/`; modules are auto-discovered.
- Add MCP servers in `agent/mcps.py` or with `agentbricks tools add mcp`.
- Bind long-term memory with `agentbricks memory bind <store>`.
- Route the agent's model through a Unity Gateway model service you own with
  `agentbricks experimental models bind`, then search for a better model and prompt on your own
  eval set with `agentbricks experimental models upgrade`. These commands are experimental, so
  their flags may change between releases.

```bash
agentbricks --profile <profile> deploy dataops-agent --source .
```

When deployment provisions a dedicated Runtime Store, only the app-owned
`databricks_agentkit_runtime_<hash>` schema and runtime tables are added. Managed Runtime Store
deployments use their own default schema.

To keep a session on one app replica (sticky routing), set the `X-Routing-Key` request header to your
stable session id on every call. It is a routing hint only: non-blank, no more than 128 UTF-8 bytes,
used verbatim, and it takes precedence over the legacy `__Host-databricks-app-router` cookie. It is not
authentication and is never used as the application's session id. Send that separately as the
top-level `session_id` in the request body.

# Request-user authorization

Declared tools in `agent.toml` select `auth = "user"` or `auth = "app"`; legacy missing auth
continues to use the application/default identity. Deployed user tools require the request resolver
and never fall back to application credentials. Model, memory-service, session-service, and custom
MCP server credentials are unchanged.

`runtime/main.py` derives the invocation policy after `configure()`. The runtime adapter namespaces
public session IDs and actor values for the request owner, then passes only `workspace_client_for`
to the framework-native agent. Internal session keys are never returned to clients.

User-policy invocations use the existing Runtime for synchronous, streaming, and background calls,
including status polling, event replay, and invocation-ID idempotency. The Runtime Store persists no
credential; request-user authentication remains process-local for the active first attempt. A
replacement attempt fails with `MCP_USER_AUTH_RECOVERY_UNSUPPORTED` before agent code runs.
Interactive approval uses a new authenticated invocation and rebuilds tools with that request's
credentials. The agent's existing namespaced memory, conversation store, and checkpointer behavior
is unchanged; OBO does not add another saver.

Existing generated projects must update their copies of `agent/agent.py` and `runtime/adapter.py`
to remove the OBO pause/resume rejection guards. Upgrading the installed package alone does not
rewrite these files. Keep the runtime's automatic-recovery guard.
