# Engine SDK

The public Go runtime for coordinator-managed Shibuya engine agents. JMeter,
Locust, and Replay use the same supported callback contract available to engine
adapters. The SDK has no dependency on the private Shibuya control-plane module.

```sh
go get github.com/reqfleet/engine-sdk@v0.1.0
```

Go 1.26.9 or later is required.

## Supported packages

- [`agent`](agent/doc.go): runtime/options, per-run contexts, workspaces, commands,
  process supervision, metric events, and the authenticated HTTP handler.
- [`protocol`](protocol): wire messages, explicit run/artifact assignments,
  lifecycle and preparation status, structured errors, and artifact paths.

`internal` packages implement HTTPS artifact downloads, TLS subscriptions, and
resource collection. They are not adapter extension APIs. Storage backends,
engine registration, control-plane reporting, and engine-specific configuration
are not part of this module.

## Adapter contract

Configure `agent.AgentServerOptions` with a `BuildCommand` callback and any needed
preparation, observation, progress, or graceful-stop callbacks. Start the runtime
with `agent.MakeAgentServer(options).Run()`. See the [package contract](agent/doc.go)
for callback lifetime, cancellation, process supervision, and workspace details.

The coordinator supplies an explicit assignment for each run; agents do not
redistribute workload or derive input shards from their identity. Optional
engine-specific progress does not determine lifecycle success. A completed work
observation cannot conceal a failing process exit, and cleanup finishes before a
subsequent run can reuse the workspace.

`EngineMeta` and `CoordinatorRootCAs` provide explicit configuration. The factory
otherwise reads the existing deployment environment: `engine_name`, `offset`,
`coordinator_ip`, `coordinator_ca_cert`, `collection_id`, `plan_id`, `zone`,
`api_key`, and `label_limit`. Workload commands receive assigned `engine_no` and
`run_id`. `AGENT_ROOT` holds `test-conf`, `test-result`, and `run-tmp`; downloaded
inputs remain in `/test-data`. Dependency state survives runs.

`Handler()` exposes `GET /progress`, `GET /prepare-progress`, `GET /metrics`, and
`GET /healthz`. All except health require the configured Bearer token; health
reports coordinator subscription state. Artifact downloads and coordinator
subscriptions require trusted TLS peers; there is no insecure fallback.

## Metrics and compatibility

Use a result-file parser returning `agent.ShibuyaMetric`, or an adapter-normalized
native endpoint explicitly declaring `agent.MetricsSchemaShibuyaV1`. An arbitrary
Prometheus endpoint is not automatically reporting-compatible. The
[metrics contract](agent/metrics.md) defines required families, units, labels,
buckets, and aggregation behavior.

The Go API release `v0.1.0`, metrics profile `shibuya-v1`, and wire behavior are
separate contracts. The existing wire envelope and subscription are unversioned;
there is no protocol negotiation or unsupported-version rejection. This release
does not introduce third-party engine registration or a reference k6/Gatling
integration. Pre-1.0 Go API changes may be incompatible and must be reviewed when
upgrading a pinned module version.

## Verification

```sh
GOWORK=off go test -race -count=1 ./...
GOWORK=off go list -deps -test -json ./...
GOWORK=off go list -m -json all
```

Licensed under [Apache-2.0](LICENSE). [NOTICE](NOTICE) records extraction provenance;
dependency licenses and notices are retained in `THIRD_PARTY_NOTICES`.
