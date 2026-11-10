# Qualification record — 2026-11-11

This records bounded tests, not guaranteed success for every request.

## Offline

45 bridge tests pass. They include the upstream suite and new tests for tenant isolation, forged/expired continuations, tool errors, concurrency and idle eviction, payload/output bounds, cancellation, bounded event backlogs, model selection confirmation, stale usage, exact cumulative accounting, JSON-object validation, SSE heartbeats and durable replay prevention. External network access was blocked during these tests.

## Live, disposable local files

| Client/workflow | Sol 6.1 | Opus 5.5 |
| --- | --- | --- |
| Short non-stream response | Passed | Passed |
| Incremental SSE text, stable completion ID and terminal events | Passed | Passed |
| High reasoning effort with validated JSON object | Passed | Passed |
| Native local tool callbacks, unpredictable file marker | Passed | Passed |
| File edits and test creation through a local client loop | Passed | Passed |
| Client-executed Python tests plus independent verification | Passed | Passed |
| Actual OpenCode read/edit/write/patch workflow | Passed | Passed |

The custom client loop completed seven local callbacks in about 41–42 seconds per model. OpenCode-generated code passed 11 independent tests for Sol and eight for Opus. OpenCode itself was instructed not to run commands in that particular file-edit check; command execution was tested separately in the custom client loop. No denied command was rerouted around a client permission check.

The final SSE samples delivered three or four nonempty text chunks for a 60-line response, with first text around 5.7–6.0 seconds. This sample does not establish a general latency or TPS guarantee.

The first sequential-chat trial exposed a quota bug: completed idle sessions consumed all capacity. The fork now evicts completed idle sessions before rejecting a fresh conversation, while preserving pending/active work. The repeated structure suite passed all six cases afterward.

The provider's first ACP usage snapshot was stale (zero counters and a different model). The fork now rejects such snapshots and uses bounded reads of the official persisted-usage endpoint. Actual reported input includes cloud-harness work and must not be treated as only the visible user prompt.

## Not qualified for paid production

- Native Anthropic Messages / Responses protocols in this bridge.
- Images, audio and binary document blocks over this ACP transport.
- Native strict JSON-schema decoding or sampling controls.
- An upstream-enforced output-token ceiling for an ACP prompt.
- Palatial per-request billing for an agent run spanning several HTTP tool continuations, including delayed sandbox costs.
- Customer-scale multi-instance recovery of an in-flight local-tool callback.

Other catalog models have not been tested agentically and are not enabled by the local launcher. Customer balances, reservations, existing Opus/GPT suppliers and personal OpenCode settings were not changed by these direct Hoplite evaluations. The Palatial staging policy blocks unqualified supplier IDs before reservation or dispatch.
