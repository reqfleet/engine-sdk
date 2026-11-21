# Adapter development guide

Use this file when implementing an engine adapter or changing the SDK. Keep the
project introduction, Reqfleet motivation, and basic architecture in
[README.md](README.md); keep implementation workflow and constraints here.
README describes published releases only. Keep unreleased APIs and development
contracts here and in package documentation until they ship.

## Responsibility boundary

Three owners share the work:

- **Reqfleet's control plane** supplies assignments and external services.
- **The SDK runtime** runs inside the engine agent.
- **The agent developer** supplies engine-specific adapter code and its runnable image.

| Concern | Control plane provides or decides | SDK already implements | Agent developer implements |
| --- | --- | --- | --- |
| Work assignment | Run ID, resolved per-agent settings, and per-file partition assignments | Selects the addressed assignment and exposes it as `RunContext` | Translate the assigned settings and partition mode into engine behavior; do not allocate workload |
| Input artifacts | Serves test scripts, data, optional native config, and dependency declarations selected for the run | Downloads and stages files before the relevant callbacks | Declare expected filenames/formats and consume staged paths; generate engine-local files only where needed |
| Dependencies | Supplies the declaration, if any, and authorizes the preparation stage | Stages the declaration, invokes the hook, and exposes preparation status | Install, remove, and reconcile dependencies in `PrepareDependencies`, including cancellation and rollback |
| Lifecycle | Sends prepare/start/stop commands and the stop reason; polls agent status | Coordinator subscription, lifecycle state, command launch, process supervision, and cleanup | Supply `BuildCommand` and any needed preparation, readiness, progress, or graceful-stop hooks |
| Metrics | Owns tier policy, storage, aggregation, and reporting; the collector scrapes the agent | Owns per-run collectors, request-name admission, resource sampling and final-snapshot retention; ingests parsed events or recorder updates, serves native handlers, proxies native endpoints, or explicitly folds URL snapshots | Translate engine output into SDK recording methods, a parser, or a complete native exposition; native exporters own validation, admission and final-output retention |

Agree these integration prerequisites with the control-plane maintainers:
settings schema, artifact/config filenames and rendering, dependency format,
and image launch requirements. The control plane serves selected artifacts,
including user-supplied scripts and data.

Deliver the adapter executable, an image with its required tools, and verification
evidence. Coordinate engine registration, coordinator-side config rendering,
and deployment integration with control-plane maintainers.
Register each engine separately from image publication.
Supply the engine binary and adapter; this repository provides the SDK.

## Read before changing code

- Read [the package contract](agent/doc.go) for callback ownership, cancellation,
  lifecycle outcomes, and workspace reuse.
- Read [the metrics contract](agent/metrics.md) before implementing a parser or
  native metrics endpoint. Keep reporting families, units, labels, histogram
  boundaries, and accumulation semantics in that canonical specification.
- The [LLM streaming contract](agent/metrics.md#llm-streaming-metrics) defines
  `ttft`, `llm_resp` and received-token `tps` semantics for every engine.
  Increment the status counter once per genuine received token, independently of bandwidth and timing.
  Establish token cardinality from token evidence beyond transport chunk boundaries.
  Document source selection and instrumentation in each adapter.
  Verify optional support per engine against its actual image and legacy event behavior.
- Limit adapter imports to the public SDK packages below.

| Package / source | Responsibility |
| --- | --- |
| [`agent/server.go`](agent/server.go) | `AgentServerOptions`, constructors, deployment configuration, metrics admission |
| [`agent/run.go`](agent/run.go), [`agent/command.go`](agent/command.go) | `RunContext`, `Workspace`, `Process`, and the command returned by an adapter |
| [`agent/lifecycle.go`](agent/lifecycle.go) | SDK-owned preparation, lifecycle state, observations, and cancellation |
| [`agent/runtime_unix.go`](agent/runtime_unix.go), [`agent/runtime_unsupported.go`](agent/runtime_unsupported.go) | Unix runtime startup, process supervision and cleanup; explicit execution rejection on non-Unix systems |
| [`agent/metrics_collection.go`](agent/metrics_collection.go) | SDK-owned `CustomIngestMetrics` runner, cancellation, and final post-exit ingestion |
| [`agent/http.go`](agent/http.go), [`agent/metrics.go`](agent/metrics.go), [`agent/resource_metrics.go`](agent/resource_metrics.go), [`agent/metrics_native.go`](agent/metrics_native.go) | Authenticated endpoints, per-run `MetricsRegistry`, SDK resource sampling, native admission and folding |
| [`agent/metrics_recorder.go`](agent/metrics_recorder.go) | Per-run `MetricsRecorder`, the public recording API with measurement presence and the shared result-file/recorder validation and admission policy; see the [recorder contract](agent/metrics.md#run-scoped-recorder) |
| [`protocol`](protocol) | Coordinator messages, explicit run/artifact assignments, lifecycle states, errors, and artifact paths |
| [`metricschema`](metricschema) | Collector-free metric names, reserved request names, request-name admission, ordered Prometheus labels, and caller-owned histogram boundaries |
| [`containerstats`](containerstats) | Raw cgroup CPU/memory readers backed by `internal/containerstats`; no rate calculation or run lifecycle policy |
| [`resources`](resources) | Synchronized `Sampler` for CPU rates, memory bytes and run reset, shared by the runtime and adapter-owned exporters; see the [resource metrics contract](agent/metrics.md#runtime-resource-metrics) |
| [`validation`](validation/doc.go) | Synchronous upload-validation contract, structured diagnostics, and result checks; not part of agent execution |
| [`conformance`](conformance/doc.go) | Optional adapter-testing toolkit: normalized metric assertions and Linux/container-only lifecycle, artifact, authentication and budget scenarios; never imported by the runtime |
| [`examples/jsonvalidator`](examples/jsonvalidator) | Working JSON request-plan validator using only standard-library and public SDK dependencies |
| [`internal`](internal) | SDK-only artifact transport, subscriptions, and cgroup reader implementation |

## Development environment

Use the Go version declared in [go.mod](go.mod) for the checkout you are changing.
Run commands from this repository's root with `GOWORK=off` to check the public
module independently of any surrounding workspace:

```sh
go version
GOWORK=off go mod download
GOWORK=off go doc ./agent AgentServerOptions
GOWORK=off go doc ./agent RunContext
```

Deployed agents require Linux containers with the engine and its dependencies.
Provide writable workspaces, coordinator access, and readable cgroup v1 or v2
CPU/memory files. `Run()` collects container resources.
Verify deployment through a container smoke test.

## Resolve deployment options before construction

Call `agent.LoadAgentServerOptions(base)` before initializing adapter components.
It returns environment-backed `(AgentServerOptions, error)` and preserves `base`.
Loading has no logging, process, server, or workload side effects.
Errors return zero-valued options.
Validate callbacks and metrics integration separately.

| Setting | Environment and precedence |
| --- | --- |
| `LabelsUsageLimit` | Always replaced by lowercase `label_limit`, even when `base` supplies a value. Unset or empty means 10. Nonempty values use Go's `strconv.Atoi`: decimal, optional sign, no whitespace, within the platform `int` range. Negative results are errors; `0`, `+0`, `-0`, and `+1` are valid. |
| `CoordinatorRootCAs` | A non-nil explicit pool wins and skips the environment. Otherwise set `coordinator_ca_cert` to PEM content with at least one parseable certificate. Unset, empty, whitespace-only, and malformed values are errors. |
| `EngineMeta` | A nonzero explicit struct wins as a whole, without validation or filling empty fields. Otherwise load all metadata from the variables below. |
| Other options | Preserved unchanged, including adapter callbacks and metrics options. |

`engine_name` requires exactly five hyphen-separated components.
Its fifth component and the required `offset` must parse with `strconv.Atoi`.
Unset, empty, malformed, or overflowing integers are errors.
`EngineID` is the suffix plus offset, formatted in decimal.
`coordinator_ip`, `collection_id`, `zone`, `plan_id`, and `api_key` are copied as-is.
Unset and empty values both yield an empty string.

These rules preserve empty/default and signed-integer semantics.
Negative label limits fail during loading.
At zero, SDK-managed metrics admit only the overflow label.
Native exporters implement their own configured budget semantics.

Apply explicit overrides **after** loading and before initializing dependent components.
Pass the resolved options to `agent.NewAgentServer`.
It preserves those settings, including a zero label limit.
`agent.MakeAgentServer` loads options itself and logs fatally on errors.
Use it only for unresolved options.

This adapter-owned helper initializes an in-process exporter from the resolved
budget, then constructs the SDK server. It uses `net/http`'s `http.Handler`
and the SDK listener:

```go
func newHandlerAgent(
    base agent.AgentServerOptions,
    newExporter func(labelLimit int) (http.Handler, error),
) (*agent.AgentServer, error) {
    options, err := agent.LoadAgentServerOptions(base)
    if err != nil {
        return nil, err
    }
    exporter, err := newExporter(options.LabelsUsageLimit)
    if err != nil {
        return nil, err
    }
    options.MetricsHandler = exporter
    options.NativeMetricsSchema = agent.MetricsSchemaShibuyaV1
    return agent.NewAgentServer(options), nil
}
```

Supply `BuildCommand` and other adapter hooks in `base`.
Keep `MetricParser` nil, `MetricsURL` and `NativeMetricsOverflowLabel` empty,
and `FoldNativeMetrics` false.
For CSV output, set `base.ResultFile` to `"results.csv"` and write to
`RunContext.ResultFile`. In handler mode, the adapter owns reading that file.

Call the adapter initializer `newExporter` before constructing the server.
It must return a normalized exporter conforming to
[the metrics contract](agent/metrics.md) and enforce the resolved label budget.
Pass resolved `options.EngineMeta` if construction needs identity.
Update run identity from callback `RunContext.RunID`.
The adapter owns CSV parsing, normalization, synchronized run-local state,
exporter workers, and shutdown, including when `Run()` fails.
The SDK owns its listener and server lifecycle.
Preserve completed-run cumulative metrics for final scrapes and honor scrape contexts.
Reuse the SDK-resolved label limit.
Assign an existing normalized handler directly to `options.MetricsHandler`.

For adapter-parsed samples, prefer SDK collectors.
Set `options.CustomIngestMetrics` and record through its supplied `run.Metrics`.
The SDK creates a fresh recorder after successful process launch.
`PrepareDependencies`, `BeforeStart`, and `BuildCommand` receive nil metrics.
Identity, registry, and label budget remain bound to that run.
Leave `MetricParser`, `MetricsHandler`, and native options unset.
Use the run's recorder; the SDK owns registry construction, collectors, and CPU/memory.
Collection runs independently of scrapes, with a final post-exit call before workspace reuse.
Accepting a new run ID through prepare or start immediately clears previous SDK metrics.
Scrapes remain empty until successful launch, including after startup failure.
Rejected, duplicate, and stale triggers preserve existing output.
Use `run.Metrics.Registry()` for gathering and recorder methods for recording.
See [the recording and lifecycle contract](agent/metrics.md#sdk-owned-registry).

## Implement an adapter

### 1. Consume control-plane inputs

Configure `agent.AgentServerOptions`.
The control plane supplies run-specific artifacts and assignments.
Use the SDK-provided `RunContext` and staged downloads in your callbacks.

- `TestFileName`: you choose the local name of the required test artifact. The
  coordinator's `EngineMessage.TestFile` selects the remote artifact; the SDK
  downloads it and supplies the local path as `RunContext.TestFile`.
- `ConfFileName`: declare an optional config filename agreed with the control plane.
  The coordinator serves that artifact.
  The SDK downloads it and supplies `RunContext.ConfigFile`.
- `DependencyFileName`: choose a local declaration filename and implement
  engine-specific installation and reconciliation.
  The coordinator selects the remote declaration.
  The SDK supplies `RunContext.DependencyFile`, or an empty string when absent.
- `ResultFile`: you choose a local output name and configure the engine to write
  to `RunContext.ResultFile`. With `MetricParser`, the SDK ingests its events;
  with `CustomIngestMetrics`, `MetricsHandler`, or `MetricsURL`, it remains ordinary workload output.
  The engine produces this file.

Use filenames for these options. The SDK stages their basenames.
Callback paths are absolute. Resolve workspace paths through `RunContext.Workspace`.

`RunContext.Settings` contains engine-specific JSON resolved for this agent's
`protocol.RunSpec`. The SDK passes it through unchanged.
Decode and translate it to native arguments or local configuration as needed.
Preserve the control plane's workload allocation.

Consume a coordinator-rendered native config when supplied.
If the integration requires local rendering, implement it using the assigned settings.
Agree the engine-specific schema and renderer with the control plane.
Use explicit assignments for VUs and workload size, preserving their scope and values.

The control plane chooses each `RunContext.Artifacts` assignment; you implement
how the load generator consumes it:

| Assignment mode | Adapter behavior |
| --- | --- |
| `protocol.PartitionReplicated` | Consume the delivered file whole. |
| `protocol.PartitionMaterialized` | Consume the delivered partition as-is; do not shard it again. |
| `protocol.PartitionNative` | Configure the engine to apply exactly the supplied `ShardIndex` and `ShardCount`. |

The runtime stages data under `Workspace.Inputs`.
Use each explicit artifact assignment to determine consumption.

### 2. Implement the callbacks

Implement the required hooks and attach them to `AgentServerOptions`.
The SDK invokes them at the lifecycle points below.
Signatures are in [`agent/server.go`](agent/server.go).
`BuildCommand` is required; add other hooks as needed.

| Callback | Agent developer's implementation responsibility |
| --- | --- |
| `PrepareDependencies` | Reconcile engine dependencies with the staged declaration. An empty `DependencyFile` still requires removing dependencies left by an earlier run. Honor cancellation and finish rollback before returning. |
| `BeforeStart` | Prepare engine-specific run files after execution artifacts are staged and before command construction. |
| `BuildCommand` | Return `agent.Command{Command, Args, Dir, Env}` for the primary workload. The SDK owns launch and wait. |
| `Observe` | Return `protocol.ObservationPending`, `ObservationReady`, or `ObservationWorkComplete`. Pending means healthy and awaiting readiness. An error fails the run and initiates shutdown. |
| `CustomIngestMetrics` | Normalize independent engine samples, such as k6 CSV points, into the SDK registry. Called after launch, every 250ms, and after live callbacks/processes finish. The final call has a fresh two-second context. Honor cancellation. Errors fail the run and registry scrapes. The SDK owns scheduling, HTTP, and resource sampling. |
| `Progress` | Return a `protocol.Progress` with a nonempty engine schema `Kind` and valid JSON `Data`. This supplies optional reporting. Lifecycle signals belong in `Observe`. |
| `Stop` | Request graceful engine shutdown using the runtime-owned `Process.PID` or the engine's control API. Honor the supplied deadline and never reap the primary process. |

The handoff between owners is:

1. The coordinator may send a preparation request. The SDK initializes the
   workspace, stages the dependency declaration, and invokes your
   `PrepareDependencies` hook; your hook performs the engine-specific work.
2. The coordinator must wait for preparation to finish before starting a run
   with a dependency declaration. The SDK enforces that barrier and rejects
   changed settings or artifact assignments across it.
3. On the coordinator's start request, the SDK stages the test, optional config,
   and data files. With no dependency declaration, it still invokes your
   dependency reconciliation hook, then `BeforeStart` and `BuildCommand`.
4. The SDK launches the command, streams stdout/stderr, runs observation,
   progress and optional metric-collection loops, and ingests result events when configured.
5. On exit or stop, the SDK terminates remaining descendants, joins callbacks
   and readers, collects final metrics/resources and progress, and only then
   releases the workspace.

Set `Command.Command` to an executable and `Args` to its literal argument vector.
Set `Dir` when the engine needs a working directory.
The SDK merges `Env` entries (`KEY=value`) into the inherited environment.
It then sets authoritative `collection_id`, `plan_id`, `engine_no`, `zone`, and `run_id`.
Preserve those identities and use explicit assignments for workload allocation.

### 3. Preserve callback and process ownership

- Treat `RunContext`, settings, and artifacts as read-only callback-scoped data.
  Cache only adapter-owned summaries keyed by `RunID`.
  Synchronize access from concurrent callbacks.
- Every callback and its subprocesses must honor its context.
  The SDK waits for callbacks even after killing the primary process.
  Ignoring cancellation can block every subsequent run.
- `Observe` and `Progress` use independent loops outside the runtime lock.
  Observation probes have five-second contexts; live progress uses the monitor context.
  Keep readiness and status requests independent of progress work.
- Without `Observe`, successful launch acknowledges readiness.
  A nonzero process exit fails the run, including after work-complete observations.
  A successful short-lived process may finish before the first readiness probe.
- After live monitors stop, `Progress` gets a fresh two-second context.
  Reuse cached summaries/checkpoints for this final call.
- `Stop` gets a fresh context bounded by `StopTimeout` (ten seconds by default).
  Without a hook the SDK sends SIGTERM to the process group; it escalates to
  SIGKILL after the grace period. The runtime alone waits for the primary process.
- The coordinator supplies scheduled completion or cancellation through `protocol.StopReason`.
  The `Stop` callback receives no reason.
- Runs are serialized. Duplicate requests launch once.
  Retired IDs remain recorded for the agent's lifetime.
  Stale stops leave new runs untouched. Preserve these invariants when changing the SDK.

### 4. Choose a metrics integration

Map engine measurements using [agent/metrics.md](agent/metrics.md).
The Reqfleet collector scrapes the SDK's `/metrics` endpoint.
The control plane owns reporting and storage. Choose one mode:

- **Result-file events:** set both `ResultFile` and `MetricParser`; leave
  `MetricsHandler` nil, `MetricsURL` and `NativeMetricsSchema` empty. The SDK tails the output file.
  Each parser call receives a line and returns `agent.ShibuyaMetric` or an error.
  Map request label, status, latency in milliseconds, received bytes into
  `EgressBytes`, and current concurrency into `Threads`. The runtime supplies
  run identity and accumulates metrics; parser errors drop the event. Never
  feed cumulative snapshots into a per-request event parser.
- **SDK recorder updates:** optionally supply `CustomIngestMetrics`.
  Leave parser and native options unset.
  Normalize samples through the callback's SDK-provided `run.Metrics`.
  Each run gets the resolved budget, fresh identity, request names, collectors,
  resource baseline, and collection errors.
  Prelaunch hooks receive nil metrics.
  Successful launch creates and publishes the recorder before postlaunch workers start.
  Completion retains its snapshot until prepare or start accepts a new run ID.
  Acceptance clears prior SDK output, including when startup later fails.
  Rejected, duplicate, and stale triggers preserve output.
  Matching prepare-to-start preserves the state established by prepare.
  In-flight scrapes finish on their captured registry.
  Native exporters own their output lifecycle; native callbacks receive nil `run.Metrics`.
  Recorders ignore sample identity.
  `ObserveLatency`, `RecordStatus`, `ObserveRequest`, and `AddReceivedBytes`
  validate and admit request names. `SetConcurrency` is independent.
  Use `AddAggregateReceivedBytes` for source-specific aggregate bytes.
  Use `RecordSample` for other measurement combinations and `RecordEvent` for complete events.
  The SDK owns collectors, resource sampling, and HTTP exposition.
  The collection callback drains engine output.
  `ResultFile` may name that ordinary output.
- **In-process normalized handler:** set `MetricsHandler` to the adapter's `http.Handler`.
  Set `NativeMetricsSchema: agent.MetricsSchemaShibuyaV1`.
  Leave `MetricParser` nil and `MetricsURL` empty.
  The SDK invokes the handler at the authenticated `/metrics` route.
  It preserves the request context, status, headers, and body, including errors.
  The exporter owns complete normalized exposition and label-budget enforcement.
  Its output passes through unchanged and independently of the SDK registry.
  `ResultFile` may name CSV or other workload output.
  Keep `FoldNativeMetrics` false and `NativeMetricsOverflowLabel` empty.
  Both folding options require `MetricsURL`.
- **Normalized native endpoint:** set an HTTP(S) `MetricsURL` and
  `NativeMetricsSchema: agent.MetricsSchemaShibuyaV1`; leave `MetricParser` and `MetricsHandler` nil.
  Supply a complete normalized exposition with compatible identity labels,
  resource metrics, and cumulative request counters/histograms. By default
  (`FoldNativeMetrics: false`), the SDK streams the upstream response without
  parsing, validation, or request-label admission. The native engine must enforce
  its configured label limit.
  Set `FoldNativeMetrics: true` only as a last-resort fallback when the engine
  cannot enforce that budget. This adds per-scrape buffering, parsing, validation,
  aggregation, and re-encoding of Prometheus text snapshots. The fallback's
  `NativeMetricsOverflowLabel` defaults to `others` and also supports `_other_`.
  `ResultFile` may name ordinary output, which the adapter reads.
- **Registry only:** leave all metrics options unset.
  The SDK serves its registry. Complete request reporting requires ingestion.

Upload validators return optional `validation.Result.LabelCount` values to the control plane.
The callback receives no tier or numeric limit; the control plane owns upload policy.

Configure runtime admission separately through `AgentServerOptions.LabelsUsageLimit`.
Result-file ingestion, recorder request samples, and opt-in SDK folding use a nonnegative budget N.
Each permits N named request labels plus one overflow label per agent/run.
Zero folds all names in these SDK-managed modes.
Default native proxies and in-process handlers leave budget enforcement to the exporter.
See the metrics contract for engine-specific semantics.
Pass original request names to recorder methods for validation and admission.
Preserve aggregate byte attribution through `AddAggregateReceivedBytes`.
Aggregate byte labels remain outside the request-name budget.

SDK native folding retains admitted names across scrapes.
New names are admitted deterministically within each successful scrape.
Rejected scrapes consume no slots.
Folding sums the current cumulative snapshot independently on each scrape.
Admission survives completion and resets at the next successful launch.
Preserve cumulative source series throughout each run.
Keep native endpoints and handlers reachable for final scrapes.
Folding bounds exported request labels.
Manage producer memory, other Prometheus dimensions, and plan-wide cardinality separately.
Verify runtime labels independently of preflight counts.

Verify endpoint conformance before declaring `shibuya-v1`.
Each producer remains responsible for the complete profile.
`FoldNativeMetrics: true` checks safe aggregation and bounds snapshots to 16 MiB
of uncompressed Prometheus text.
Malformed, unsafe, or oversized snapshots return HTTP 502.
Default proxying and handlers pass through source responses, including failures.
Their producers own validation and size limits.
Use [the metrics contract](agent/metrics.md) for format, aggregation, error, and lifecycle rules.
Use public `metricschema` definitions.
Its label-list functions return caller-owned slices.

## Prepare the engine image and workspace

Package the adapter, engine runtime, and callback tools in an engine image.
In the entry point:

1. Configure hooks and filenames.
2. Resolve deployment options with `agent.LoadAgentServerOptions`.
3. Initialize adapter components.
4. Create `agent.NewAgentServer(options)` and call `Run()`.
5. Handle loading and runtime errors.

Agree filesystem and launch requirements with the deployment integration.
Provision the container and register the image through that integration.
The coordinator authorizes workload starts.

Before server construction, set `AGENT_ROOT` to an absolute, writable directory.
Also create writable `/test-data` separately.
Grant the adapter, engine, and dependency-hook user write access to both parents.
The SDK creates run subdirectories.

Install engine and callback tools, then add this setup before the adapter's `ENTRYPOINT`.
The example uses UID/GID 10001.
Match ownership and `USER` to your image:

```dockerfile
USER root
ENV AGENT_ROOT=/var/lib/reqfleet
RUN mkdir -p "$AGENT_ROOT" /test-data \
    && chown 10001:10001 "$AGENT_ROOT" /test-data
USER 10001:10001
```

Keep installed engine files outside per-run directories.
Grant the engine and dependency hooks write access to every path they modify.
Provision mounted `AGENT_ROOT` and `/test-data` directories with matching ownership
and write permissions. Image-layer permissions apply only to image contents.

The current workspace layout is:

| `Workspace` field | Location / lifetime |
| --- | --- |
| `Root`, `State` | `AGENT_ROOT`; reusable engine and dependency state survives runs. |
| `Inputs` | `/test-data`; reset per run, independently of `AGENT_ROOT`. |
| `Config` | `AGENT_ROOT/test-conf`; not cleared wholesale because it also holds dependency reconciliation state. |
| `Results` | `AGENT_ROOT/test-result`; reset per run. |
| `Temp` | `AGENT_ROOT/run-tmp`; reset per run. |

Use isolated containers with dedicated directories for runtime smoke tests.
The SDK clears run inputs, results, and temporary files.
Reconcile adapter-owned persistent state between runs.

## Runtime HTTP endpoints

Use the SDK's coordinator subscription and authenticated HTTP endpoints.
When launched through `Run()`, `Handler()` serves these endpoints on port 8080.
Keep that port on the trusted deployment network:

- `GET /progress` (optionally `?run_id=...`), `GET /prepare-progress`, and
  `GET /metrics` require `Authorization: Bearer <APIKey>`.
- `GET /healthz` is unauthenticated and reports coordinator subscription state:
  200 when subscribed, 503 otherwise. Check workload readiness through `/progress`.

The coordinator consumes preparation/run status.
The Reqfleet collector consumes `/metrics`.
Callbacks supply observations and metric data; the SDK serves and authenticates requests.

The runtime uses `github.com/reqfleet/toolkit/lib/http/route` for route registration,
`http/auth` for bearer authentication, and `http/client` for its three-second
coordinator client. The SDK retains coordinator TLS configuration, endpoint
handlers, metrics proxying, and listener/lifecycle ownership. Missing or malformed
bearer credentials still return 400; mismatched tokens return 403. Only the exact
`/healthz` path bypasses authentication. Request logging is not enabled implicitly.

## Verification and completion

Run the SDK checks from the repository root:

```sh
GOWORK=off go test -race -count=1 ./...
GOWORK=off go list -deps -test -json ./...
GOWORK=off go list -m -json all
```

The optional [`conformance`](conformance/doc.go) package provides the shared adapter harness.
Metrics assertions run in ordinary unit tests.
Runtime helpers and regressions require `-tags=integration`, Linux, and a disposable container.
Run `make test-toolkit` for shared runtime regressions.
It uses private storage and ports, with external networking disabled.
The root harness launches adapters and workloads as UID/GID 65532.

External adapter tests import `github.com/reqfleet/engine-sdk/conformance`.
Supply the real executable, artifacts/settings, controlled target,
completion/reuse fixtures, label-budget fixtures, and independent native-output expectations.
Keep engine plans, parsing, dependency/version policy, and error mapping in the adapter repository.
Build and run both the shared regression binary and adapter integration binary in its conformance image.
See package documentation for extension points and cleanup guarantees.
Verify control-plane deployment compatibility separately.

Existing behavior tests show the repository's conventions:

- [Dependency/start tests](agent/dependency_start_test.go): staged artifacts,
  preparation barriers, cancellation, and serialized runs.
  Fixtures isolate inputs in temporary directories; deployments use `/test-data`.
- [Startup/progress tests](agent/startup_progress_test.go): readiness versus exit,
  progress isolation, final snapshots, and process cleanup.
- [HTTP tests](agent/http_test.go) and [TLS tests](agent/tls_test.go): authentication,
  metrics option admission, proxy behavior, and coordinator trust.
- [Metric producer tests](agent/metrics_producer_test.go): event accumulation,
  identity, and compatible histogram output.
- [Native metrics tests](agent/metrics_native_test.go): numeric budgets, stable
  admission, cumulative folding, malformed input, and final/next-run scrapes.

Build the adapter image and test its real engine in an isolated deployment.
Use the integration's coordinator, artifact service, metrics collector, and configuration.
Verify:

- Assigned settings and artifact partition modes.
- A normal run with reporting-compatible metrics and a nonzero-exit run.
- Preparation cancellation, graceful stop, and a second run after cleanup.
- Dependency removal between runs, where supported.
- Readiness through `/progress`.
- Metric values, units, labels, and buckets against the metrics contract.

Report executed scenarios and unavailable engine/coordinator prerequisites.
Use deployment evidence to establish integration compatibility.

Update affected contracts and behavior tests when behavior changes.
Keep project explanations in README and adapter instructions here.
Review the Go API, `shibuya-v1` profile, and wire behavior as separate contracts.
The wire envelope and subscription are unversioned.
They provide neither negotiation nor unsupported-version rejection.
Review pre-1.0 API changes and coordinator compatibility separately when upgrading a dependency.

Leave `CHANGELOG.md` updates to Release Please's release PRs.

Coordinators **MUST ALWAYS maintain backward compatibility with existing engines**,
as defined by the [coordinator compatibility policy](README.md#coordinator-backward-compatibility).
Verify older and current engine images before releasing coordinator changes.
Cover payload semantics, artifact/settings delivery, preparation, execution,
metrics/reporting, completion, and cancellation.
Preserve existing engine behavior without rebuilds, upgrades, or re-registration.

## Releasing the SDK

The [`release-please` workflow](.github/workflows/release-please.yml) runs on
pushes to `main`; it can also be dispatched manually with `main` selected.
It uses the Go release strategy in
[`release-please-config.json`](release-please-config.json) for the root module
only. Examples are not separate release components.

The workflow uses the built-in `GITHUB_TOKEN`; no additional secret is needed.
Under repository Settings > Actions > General > Workflow permissions, enable
"Allow GitHub Actions to create and approve pull requests". The workflow grants
the token Contents, Issues, and Pull requests write permissions.

PR and tag events created by `GITHUB_TOKEN` do not trigger other workflows.
To run `pr-test`, close and reopen the generated release PR using your own
account. Repeat this after Release Please pushes updates, so checks cover the
latest commit. See the
[action's credentials documentation](https://github.com/googleapis/release-please-action#github-credentials).

Use Conventional Commit messages on `main`, including squash-merge titles.
`fix:` produces a patch release; `feat:` produces a minor release. While the SDK
is pre-1.0, breaking changes (`!` or a `BREAKING CHANGE:` footer) also bump the
minor version. Documentation and chore commits alone do not trigger releases.

Release Please opens or updates a release PR containing `CHANGELOG.md` and the
version manifest. Review the changelog and wait for `pr-test` to pass before
merging. The subsequent push to `main` publishes the GitHub release and its
Go-compatible `vX.Y.Z` tag, without a component prefix.

The [manifest](.release-please-manifest.json) records the **last released** version.
It starts at `0.1.1`; the unreleased validation feature makes the first automated
release `0.2.0`. Let Release Please advance it in release PRs.
Keep subsequent versions driven by Conventional Commits and leave `release-as` unset.

