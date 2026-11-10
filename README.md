# Palatial evaluation fork of hoplite2api

This fork is an experimental Hoplite cloud-agent bridge for local coding clients. It retains Hoplite's harness. Local tools run in the requesting client, with that client's permissions. It is not a raw model inference endpoint.

Upstream: https://github.com/Jwjfr2/hoplite2api at `abe0f03a34616b5522362eb1351d5282d17a85cd`.

## Current qualification

Live synthetic local-agent workflows passed on `gpt-6.1-sol` and `claude-opus-5-5`: unpredictable file retrieval, actual Python file edits, unittest creation, test execution, and independent test verification. OpenCode also executed its local read tool and returned the file marker through the streamed Chat Completions interface.

These are bounded functional checks, not a guarantee for every model or workload. Existing Palatial production routes were not replaced. Paid customer publication remains gated on the billing and limit qualifications below.

## Changes in this fork

- A separate bridge credential; clients no longer need the upstream account key.
- Optional authenticated-gateway tenant namespaces for active, idle and pending tool state.
- Session/concurrency limits, per-tenant request pacing, body/output bounds, turn deadlines and SSE comment heartbeats.
- Valid pending tool continuations keep their session lease instead of consuming new-prompt RPM slots; otherwise a rate-limit retry could outlast the upstream callback deadline.
- Explicit callback expiry, tool-error propagation and model-selection confirmation.
- Reported thread-usage snapshots with a REST fallback when ACP returns a stale pre-run snapshot. Context occupancy is never relabelled as input tokens.
- Durable, content-free idempotency tombstones when an `Idempotency-Key` is supplied. Repeated claims return 409 rather than rerun a local edit.
- Adapter-validated `response_format: {"type":"json_object"}`. Streaming JSON is buffered until it is valid; this is not native constrained decoding. Invalid output fails without an automatic paid retry.
- Exact model allowlisting; the CLI defaults to the two agentically tested model IDs.

## Run locally

Requires Python 3.12+, uv, a Hoplite member key and a repository-backed Hoplite project.

```sh
uv sync --locked
# Set HOPLITE_API_KEY privately in the process environment.
# Set HOPLITE_BRIDGE_KEY to a different random secret (at least 32 characters).
uv run hoplite2api --project YOUR_PROJECT_ID --port 8091
```

The listener stays on `127.0.0.1`. Authenticate `/v1/models`, `/v1/capabilities` and `/v1/chat/completions` with `Authorization: Bearer <HOPLITE_BRIDGE_KEY>`. Never give the Hoplite account key to customers or commit an environment file.

`HOPLITE_ALLOWED_MODELS` accepts a comma-separated list of exact Hoplite IDs. Enabling a catalog model is not evidence of a successful generation test.

Use OpenAI-compatible Chat Completions in a local client. OpenCode's model ID must be the Hoplite ID, for example `gpt-6.1-sol`; its base URL is `http://127.0.0.1:8091/v1`. Use an isolated client configuration when evaluating. Keep the client's own tool approvals enabled.

## Private gateway mode

`--gateway-mode` additionally requires `HOPLITE_DISPATCH_JOURNAL` and a stable gateway-generated `X-Palatial-Tenant` (32–64 lowercase hexadecimal characters). The bridge authenticates the gateway service key before trusting this header. The public Palatial edge must overwrite any caller-supplied tenant value. Never expose this internal credential to customers.

Supply an `Idempotency-Key` for each provider attempt. A tombstone is recorded before session acquisition and blocks reuse even after a restart. It deliberately fails closed when a previous attempt's outcome is unknown. It does not replay cached responses or undo tools that already ran.

## Important upstream limits

| Feature | Status |
| --- | --- |
| Chat Completions text and local function tools | Functionally tested on the two models above |
| Anthropic Messages / OpenAI Responses | Not implemented in this bridge |
| Images, audio, binary documents | Rejected by this ACP route |
| Local file reading/editing and client-side commands | The client owns execution and permissions |
| Tool callback window | Upstream limit of 60 seconds; 512 KiB result |
| Tool definitions / ACP message | At most 64 tools / 1 MiB |
| Output token limit | Soft prompt guidance; not a hard spending cap |
| Strict JSON schema / sampling controls | Not natively supported |
| Restart during pending tool call | Explicit conflict; no blind replay |
| Cloud harness isolation | Prompt guidance only; not an enforceable raw-model sandbox boundary |

Do not bypass a client's denied command by executing it somewhere else. For local operations taking longer than the callback window, a client needs an explicit asynchronous job/start-and-poll tool design; this bridge cannot extend Hoplite's server deadline.

## Billing qualification

`hoplite_usage` is a cumulative **Hoplite-thread** snapshot, including the harness's internal model calls. It is intentionally not emitted as ordinary OpenAI `usage`. Input can be much larger than the external request. Sandbox cost can arrive after model output ends.

Before admitting paid Palatial traffic, verify checkpoint freshness and categories, avoid charging cumulative counters repeatedly, enforce an upstream spending cap, and reconcile the provider's agent-run scope with Palatial's per-request reservation/settlement model. Do not treat absent or stale counters as zero, use context occupancy as a token receipt, or silently turn thread totals into per-request counts. This evaluation does not change customers' balances or create reservations in Palatial.

## Verification and rollback

```sh
uv run python -m unittest discover -s tests -v
```

The tests use fake upstreams; live checks must be authorized separately and run in disposable folders. See `PALATIAL_INTEGRATION.md` for the phase prompts, release gates and exact rollback prompt. Disable only the experimental supplier/IDs and stop only its recorded process/container; never reset unrelated repository changes or existing production routes.
