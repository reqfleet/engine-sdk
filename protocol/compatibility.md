# Agent/coordinator compatibility

The Go module version, wire protocol, and `shibuya-v1` metrics profile are separate
contracts. A module version is not a protocol negotiation mechanism. This SDK
revision implements wire protocol **1**; releases through `v0.3.0` use the older,
unversioned subscription and cannot connect to a coordinator requiring v1.

## Subscription admission

After trusted TLS is established, the agent sends its existing topic and API key
plus the SDK-owned `protocol_version`:

```json
{"name":"collection:7","api_key":"REDACTED","protocol_version":1}
```

The coordinator authenticates first. Before any commands it sends exactly one
subscription response. Acceptance is:

```json
{"status":"accepted","protocol_version":1}
```

An unsupported version is rejected, then the connection closes:

```json
{"status":"rejected","error":{"code":"unsupported_protocol_version","message":"agent protocol 2 unsupported; coordinator supports 1","received_version":2,"supported_versions":[1]}}
```

A missing version is reported as zero and rejected; zero does not mean v1.
The agent must receive an explicit acceptance of its own version. Missing or
malformed acknowledgments, an unexpected version, or an ordinary command in
place of the acknowledgment cannot make it ready. There is no automatic downgrade
or multi-version negotiation. Authentication failure does not reveal supported
versions.

The subscription is newline-terminated JSON. Responses and subsequent commands
are consecutive JSON values, not a line protocol. Use one stream decoder for the
response and commands: a response read can buffer the first command. The
coordinator makes the connection eligible for delivery before acknowledging it,
but serializes the acknowledgment ahead of all command writes. Failed admission
removes the subscriber and discards queued delivery.

The SDK allows five seconds for the application handshake after connecting.
The coordinator bounds TLS, subscription reading, and admission/reply writing by
one five-second deadline. Successful admission clears handshake deadlines.
Command delivery retains its existing write deadline and does not gain delivery
acknowledgments, retries, replay, or durable queues.

## Health and failure behavior

`GET /healthz` remains unauthenticated. It returns JSON with `subscribed`,
`protocol_version`, and an optional structured `error`. HTTP 200 means the
subscription was accepted; HTTP 503 means it was not. This is not workload
readiness; use `/progress` for that.

Readiness clears as soon as the command stream ends. Transient transport failures
retain the existing reconnect behavior. An explicit, well-formed unsupported
version rejection is permanent for that agent process: it stops reconnecting,
remains HTTP-accessible but unhealthy, and rejects subsequent work. If work is
already preparing or running, existing cancellation and process cleanup run;
the failure is retained in run status and health. Replacing/restarting the agent
with compatible software is required. Do not fabricate a run ID for a rejection
that occurs before any work has been assigned.

An old coordinator that never acknowledges produces a bounded handshake failure,
not an inferred unsupported-version error: network interruption can also cause a
missing response. It never enables an unversioned fallback.

## Compatibility policy

Wire v1 covers subscriptions, command envelopes, run/artifact assignments,
lifecycle meanings, and the associated agent/coordinator HTTP contracts. It does
not version unrelated control-plane APIs or engine-native settings schemas.

- Breaking required fields, units, partitioning, or lifecycle interpretations
  requires a new wire version.
- An optional addition is compatible only when an older peer can ignore it
  without changing execution correctness. Do not silently require a new optional
  field from peers that still advertise the same version.
- SDK releases may change without changing the wire version. Pre-1.0 Go API
  changes still require review when upgrading the pinned module.
- A reporting-schema change is governed by the metrics profile, not by changing
  the module version or advertising another wire version.

## Packaging, registration, and deployment

The SDK advertises its compiled version; adapters and environment variables do
not override it. Build each agent against a pinned public SDK revision. A future
operator-managed engine registration must declare its supported protocol,
reject unsupported declarations before scheduling, and check the actual agent
against that expectation. Registration metadata cannot replace runtime admission.
Engine registration itself is not implemented by this SDK.

Deploy matching coordinator and engine images together. Quiesce existing work
and recreate coordinator/engine resources through the deployment lifecycle;
existing resources do not become compatible just because image configuration
changes. Older unversioned agents are intentionally rejected, and new agents do
not work against unversioned coordinators. Roll back to a matching image set,
not one side independently. Include private-region coordinators, watchers, and
engine deployments in the coordinated rollout.
