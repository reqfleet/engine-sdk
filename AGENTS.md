# Adapter development guide

Use this file when implementing an engine adapter or changing the SDK. Keep the
project introduction, Reqfleet motivation, and basic architecture in
[README.md](README.md); keep implementation workflow and constraints here.
README describes published releases only. Keep unreleased APIs and development
contracts here and in package documentation until they ship.

## Responsibility boundary

There are three distinct owners. **Reqfleet's control plane** supplies assignments
and services outside this module. **The SDK runtime** is embedded in the engine
agent, not a separate control-plane service. **You, the agent developer**, supply
the engine-specific adapter code and its runnable image.

| Concern | Control plane provides or decides | SDK already implements | Agent developer implements |
| --- | --- | --- | --- |
| Work assignment | Run ID, resolved per-agent settings, and per-file partition assignments | Selects the addressed assignment and exposes it as `RunContext` | Translate the assigned settings and partition mode into engine behavior; do not allocate workload |
| Input artifacts | Serves test scripts, data, optional native config, and dependency declarations selected for the run | Downloads and stages files before the relevant callbacks | Declare expected filenames/formats and consume staged paths; generate engine-local files only where needed |
| Dependencies | Supplies the declaration, if any, and authorizes the preparation stage | Stages the declaration, invokes the hook, and exposes preparation status | Install, remove, and reconcile dependencies in `PrepareDependencies`, including cancellation and rollback |
| Lifecycle | Sends prepare/start/stop commands and the stop reason; polls agent status | Coordinator subscription, lifecycle state, command launch, process supervision, and cleanup | Supply `BuildCommand` and any needed preparation, readiness, progress, or graceful-stop hooks |
| Metrics | Owns tier policy, storage, aggregation, and reporting; the collector scrapes the agent | Owns canonical per-agent collectors and resource sampling; ingests parsed events, serves direct registry updates or native handlers, proxies native endpoints, or explicitly folds URL snapshots | Translate engine output into SDK recording methods, a parser, or a complete native exposition; validate/admit direct samples and retain final output |

For a new engine, agree the settings schema, artifact/config filenames and
rendering, dependency format, and image launch requirements with the control-plane
maintainers. These are integration prerequisites, not capabilities the SDK
generates automatically. Scripts and data may originate from users; the control
plane makes the selected artifacts available, rather than necessarily authoring
them.

Your deliverable is the adapter executable, the engine image with its required
tools, and evidence that they work with those inputs. Engine registration,
coordinator-side configuration rendering, and deployment integration are separate
control-plane work outside this repository; coordinate those changes rather than
reimplementing the coordinator or metrics collector in the adapter. Publishing an
image does not register an engine. No engine binary or reference k6/Gatling
adapter is supplied here.

## Read before changing code

- Read [the package contract](agent/doc.go) for callback ownership, cancellation,
  lifecycle outcomes, and workspace reuse.
- Read [the metrics contract](agent/metrics.md) before implementing a parser or
  native metrics endpoint. It is the canonical definition of reporting families,
  units, labels, histogram boundaries, and accumulation semantics; do not copy
  those definitions into a second specification.
- Adapter imports are limited to the public SDK packages below. Do not depend on
  SDK internals or the private Shibuya control-plane module.

| Package / source | Responsibility |
| --- | --- |
| [`agent/server.go`](agent/server.go) | `AgentServerOptions`, constructors, deployment configuration, metrics admission |
| [`agent/run.go`](agent/run.go), [`agent/command.go`](agent/command.go) | `RunContext`, `Workspace`, `Process`, and the command returned by an adapter |
| [`agent/lifecycle.go`](agent/lifecycle.go) | SDK-owned preparation, lifecycle state, observations, and cancellation |
| [`agent/runtime_unix.go`](agent/runtime_unix.go), [`agent/runtime_unsupported.go`](agent/runtime_unsupported.go) | Unix runtime startup, process supervision and cleanup; explicit execution rejection on non-Unix systems |
| [`agent/http.go`](agent/http.go), [`agent/metrics.go`](agent/metrics.go), [`agent/resource_metrics.go`](agent/resource_metrics.go), [`agent/metrics_native.go`](agent/metrics_native.go) | Authenticated endpoints, per-agent `MetricsRegistry`, independent recording methods, SDK resource sampling, native admission and folding |
| [`protocol`](protocol) | Coordinator messages, explicit run/artifact assignments, lifecycle states, errors, and artifact paths |
| [`metricschema`](metricschema) | Collector-free metric names, reserved request names, request-name admission, ordered Prometheus labels, and caller-owned histogram boundaries |
| [`containerstats`](containerstats) | Raw cgroup CPU/memory readers backed by `internal/containerstats`; no rate calculation or run lifecycle policy |
| [`resources`](resources) | Synchronized `Sampler` for CPU rates, memory bytes and run reset, shared by the runtime and adapter-owned exporters; see the [resource metrics contract](agent/metrics.md#runtime-resource-metrics) |
| [`validation`](validation/doc.go) | Synchronous upload-validation contract, structured diagnostics, and result checks; not part of agent execution |
| [`examples/jsonvalidator`](examples/jsonvalidator) | Working JSON request-plan validator using only standard-library and public SDK dependencies |
| [`internal`](internal) | SDK-only artifact transport, subscriptions, and cgroup reader implementation |

## Development environment

Use the Go version declared in [go.mod](go.mod) for the checkout you are changing
(currently Go 1.26.9). Run commands from this repository's root with `GOWORK=off`
to check the public module independently of any surrounding workspace:

```sh
go version
GOWORK=off go mod download
GOWORK=off go doc ./agent AgentServerOptions
GOWORK=off go doc ./agent RunContext
```

Deployed agents need a Linux container with the engine executable and its runtime
dependencies, writable workspace paths, access to the coordinator, and readable
cgroup v1 or v2 CPU/memory files. `Run()` collects container resource metrics; a
local build or handler check on another OS is not a deployed-engine smoke test.

## Resolve deployment options before construction

Call `agent.LoadAgentServerOptions(base)` to obtain environment-backed options
before initializing adapter components. It returns `(AgentServerOptions, error)`,
does not mutate `base`, and does not log, exit, construct a server, or start
listeners or workloads. On error, the returned options are zero-valued.
Callback and metrics-integration validation remains separate from environment
loading.

| Setting | Environment and precedence |
| --- | --- |
| `LabelsUsageLimit` | Always replaced by lowercase `label_limit`, even when `base` supplies a value. Unset or empty means 10. Nonempty values use Go's `strconv.Atoi`: decimal, optional sign, no whitespace, within the platform `int` range. Negative results are errors; `0`, `+0`, `-0`, and `+1` are valid. |
| `CoordinatorRootCAs` | A non-nil explicit pool wins and skips the environment. Otherwise `coordinator_ca_cert` must contain at least one parseable PEM certificate, not a filename. Unset, empty, whitespace-only, and malformed values are errors. |
| `EngineMeta` | A nonzero explicit struct wins as a whole, without validation or filling empty fields. Otherwise load all metadata from the variables below. |
| Other options | Preserved unchanged, including adapter callbacks and metrics options. |

Environment-derived metadata uses `engine_name` with exactly five
hyphen-separated components; the fifth component and the required `offset`
must parse with `strconv.Atoi`. Unset, empty, malformed, or overflowing integers
are errors. `EngineID` is the suffix plus offset, formatted in decimal.
`coordinator_ip`, `collection_id`, `zone`, `plan_id`, and `api_key` are copied
as-is, without additional validation; unset and empty both yield an empty string.

These rules retain the SDK's existing empty/default and signed-integer semantics,
rather than adopting an adapter-local strict unsigned parser. Negative label
limits now fail during loading instead of waiting for metrics validation.
Zero is a real budget: SDK-managed metrics admit only the overflow label.
Native exporters must implement their own configured budget semantics.

To make an explicit override, assign it **after** loading, before initializing
dependent components. Pass those same options to `agent.NewAgentServer`, which
does not reread these environment settings or replace a zero label limit.
`agent.MakeAgentServer` uses the same loader, but retains fatal logging on errors;
do not pass already-resolved options through it.

For example, this adapter-owned helper initializes an in-process exporter from
the resolved budget, then constructs the SDK server from the same options.
`http.Handler` is from `net/http`; no second listener or HTTP server is needed:

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

Supply `BuildCommand` and other adapter hooks in `base`, with `MetricParser` nil,
`MetricsURL` and `NativeMetricsOverflowLabel` empty, and `FoldNativeMetrics` false.
Set `base.ResultFile` to `"results.csv"` when the workload still writes CSV, and
configure the engine to write to `RunContext.ResultFile`. The SDK supplies that
path but does not tail the file in handler mode.

`newExporter` is your adapter's initializer, not an SDK callback. It must return
the real normalized exporter conforming to [the metrics contract](agent/metrics.md)
and enforce the resolved label budget. If the exporter needs identity at
construction, also pass the resolved `options.EngineMeta`; update run identity
from callback `RunContext.RunID`. The adapter owns CSV parsing, normalization,
synchronized run-local state, and exporter shutdown, including when `Run()`
returns an error. The SDK owns its listener and server lifecycle, not the
exporter's workers or state. Preserve completed-run cumulative metrics for final
scrapes, and honor the incoming scrape context. No local `label_limit` parser is
needed. An existing normalized `http.Handler` can be assigned directly to
`options.MetricsHandler`; do not wrap it in another server or `MetricsURL`.

For adapters that parse engine samples themselves, prefer SDK-owned collectors:
create `agent.NewMetricsRegistry()`, pass it to the adapter and
`options.MetricsRegistry`, and set `options.CustomIngestMetrics` to the adapter's
ingestion callback. Leave `MetricParser`, `MetricsHandler`, and native options unset.
Use independent recording methods rather than defining collectors or reporting
CPU/memory. Collection runs independently of scrapes, with a final post-exit call
before workspace reuse. Call `registry.Reset()` with parser/admission reset in
`BeforeStart`, under the adapter state lock. See
[the recording and lifecycle contract](agent/metrics.md#sdk-owned-registry).

## Implement an adapter

### 1. Consume control-plane inputs

You configure `agent.AgentServerOptions`; the control plane supplies run-specific
artifact contents and assignments. The SDK constructs `RunContext` and passes it
to your callbacks. Do not construct production run contexts or download the same
artifacts yourself.

- `TestFileName`: you choose the local name of the required test artifact. The
  coordinator's `EngineMessage.TestFile` selects the remote artifact; the SDK
  downloads it and supplies the local path as `RunContext.TestFile`.
- `ConfFileName`: you declare an optional configuration artifact name agreed
  with the control-plane integration. The coordinator must serve that artifact;
  the SDK downloads it and supplies `RunContext.ConfigFile`. Setting this option
  does not generate configuration.
- `DependencyFileName`: you choose the local name for a dependency declaration
  and implement its installer/reconciliation hook. The coordinator selects the
  remote declaration; the SDK supplies `RunContext.DependencyFile`, or an empty
  string if none was supplied. Neither the control plane nor the SDK implements
  the engine-specific installation logic in your container.
- `ResultFile`: you choose a local output name and configure the engine to write
  to `RunContext.ResultFile`. With `MetricParser`, the SDK ingests its events;
  with `CustomIngestMetrics`, `MetricsHandler`, or `MetricsURL`, it remains ordinary workload output.
  This is produced by the engine, not an artifact supplied by the control plane.

Use filenames, not paths, for these options; the SDK uses their basenames for
local staging. Callback paths are absolute. Use `RunContext.Workspace` instead
of reconstructing paths or assuming the process working directory.

`RunContext.Settings` is engine-specific JSON whose values have already been
resolved by the control plane for this agent's `protocol.RunSpec`. The SDK passes
it through without interpreting the engine's schema. You implement decoding
and translation to native arguments or local configuration when needed; do not
redo the control plane's workload allocation.

If the control-plane integration already renders native settings into the
configuration artifact, consume that file. If the agreed integration instead
requires local rendering, implement it in your adapter using only the assigned
settings. The SDK provides neither an engine-specific config renderer nor a
universal settings schema. Do not redistribute VUs, rescale the workload, or
infer a missing assignment from the agent's identity or deployment topology.

The control plane chooses each `RunContext.Artifacts` assignment; you implement
how the load generator consumes it:

| Assignment mode | Adapter behavior |
| --- | --- |
| `protocol.PartitionReplicated` | Consume the delivered file whole. |
| `protocol.PartitionMaterialized` | Consume the delivered partition as-is; do not shard it again. |
| `protocol.PartitionNative` | Configure the engine to apply exactly the supplied `ShardIndex` and `ShardCount`. |

The runtime stages data files under `Workspace.Inputs`. Their download identity
is not a substitute for the explicit artifact assignment.

### 2. Implement the callbacks

You implement these functions and attach them to `AgentServerOptions`; they are
not callbacks supplied by the control plane. The SDK invokes them at the lifecycle
points below. Their exact signatures are in [`agent/server.go`](agent/server.go).
`BuildCommand` is required; add other hooks only when the engine needs them.

| Callback | Agent developer's implementation responsibility |
| --- | --- |
| `PrepareDependencies` | Reconcile engine dependencies with the staged declaration. An empty `DependencyFile` still requires removing dependencies left by an earlier run. Honor cancellation and finish rollback before returning. |
| `BeforeStart` | Prepare engine-specific run files after execution artifacts are staged and before command construction. |
| `BuildCommand` | Return `agent.Command{Command, Args, Dir, Env}` describing the primary workload. Do not start or wait for that process. |
| `Observe` | Return `protocol.ObservationPending`, `ObservationReady`, or `ObservationWorkComplete`. Use pending for a healthy engine that is not ready; an error fails the run and initiates shutdown. |
| `CustomIngestMetrics` | Normalize engine samples that do not fit `MetricParser`'s complete-request event model, such as k6's independent CSV points, into the SDK registry. Called after launch, every 250ms and once after live callbacks/processes finish with a fresh two-second context. Honor cancellation. Errors fail the run and registry scrapes. The SDK owns scheduling, HTTP and resource sampling. |
| `Progress` | Return a `protocol.Progress` with a nonempty engine schema `Kind` and valid JSON `Data`. This is optional reporting, not a lifecycle signal. |
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

`Command.Command` is an executable and `Args` is its argument vector, not a shell
command string. Set `Dir` explicitly when the engine needs a working directory.
The SDK merges `Env` entries (`KEY=value`) into the inherited environment, then
sets the authoritative `collection_id`, `plan_id`, `engine_no`, `zone`, and
`run_id`. Do not override or use those identities to allocate work.

### 3. Preserve callback and process ownership

- Treat `RunContext`, its settings, and artifact map as read-only. Do not retain
  mutable run data after a callback returns. Cache only adapter-owned summaries,
  keyed by `RunID`, and synchronize access from concurrent callbacks.
- Every callback must honor its context, including subprocesses it creates.
  The SDK waits for callbacks to finish; ignoring cancellation can prevent all
  subsequent runs, even after the primary process has been killed.
- `Observe` and `Progress` run on independent loops outside the runtime lock.
  Observation probes have a five-second context; live progress uses the monitor
  context. Do not make readiness or status requests depend on progress work.
- Without `Observe`, successful launch acknowledges readiness. Work completion
  observations do not override a nonzero process exit. A successful short-lived
  process can complete before the first readiness probe.
- After live monitors stop, `Progress` gets a final call with a fresh two-second
  context. Reuse cached summaries/checkpoints rather than restarting canceled work.
- `Stop` gets a fresh context bounded by `StopTimeout` (ten seconds by default).
  Without a hook the SDK sends SIGTERM to the process group; it escalates to
  SIGKILL after the grace period. The runtime alone waits for the primary process.
- The coordinator distinguishes scheduled completion from cancellation through
  `protocol.StopReason`. Do not infer that distinction from elapsed time or VUs;
  the `Stop` callback itself does not receive the reason.
- Runs are serialized. Duplicate run requests do not launch twice, retired IDs
  are remembered for the agent's lifetime, and stale stops cannot stop a new run.
  Preserve these invariants when modifying the SDK.

### 4. Choose a metrics integration

You implement the engine-to-metrics mapping using [agent/metrics.md](agent/metrics.md).
The Reqfleet metrics collector scrapes the SDK's `/metrics` endpoint; the adapter
does not push metrics to the coordinator or implement reporting/storage. Choose
one of these modes:

- **Result-file events:** set both `ResultFile` and `MetricParser`; leave
  `MetricsHandler` nil, `MetricsURL` and `NativeMetricsSchema` empty. The SDK tails the output file.
  Each parser call receives a line and returns `agent.ShibuyaMetric` or an error.
  Map request label, status, latency in milliseconds, received bytes into
  `EgressBytes`, and current concurrency into `Threads`. The runtime supplies
  run identity and accumulates metrics; parser errors drop the event. Never
  feed cumulative snapshots into a per-request event parser.
- **Direct SDK registry updates:** supply a `MetricsRegistry` and optionally
  `CustomIngestMetrics`; leave parser and native options unset. Translate validated,
  admitted samples into `ObserveRequest`, `AddReceivedBytes`, or `SetConcurrency`.
  `RecordEvent` is only for complete request events. The SDK defines collectors,
  runs a background resource sampler, and serves stored values. The collection
  callback owns engine-output draining, not resource sampling or HTTP exposition.
  `ResultFile` may name ordinary output when using the collection callback.
- **In-process normalized handler:** set `MetricsHandler` to the adapter's
  `http.Handler` and `NativeMetricsSchema: agent.MetricsSchemaShibuyaV1`; leave
  `MetricParser` nil and `MetricsURL` empty. The SDK invokes it directly at the
  existing authenticated `/metrics` route with the incoming request context and
  preserves status, headers, and body, including errors. It does not parse,
  aggregate, or merge output with its registry. The exporter supplies the complete
  normalized exposition and enforces its label budget. `ResultFile` may still
  name CSV or other workload output. `FoldNativeMetrics` must be false and
  `NativeMetricsOverflowLabel` empty; both folding options require `MetricsURL`.
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
  `ResultFile` may name ordinary workload output in this mode but is not tailed.
- **Registry only:** leave all metrics options unset. The runtime registry is
  exposed without request-event ingestion; this is not complete request reporting.

For upload preflight, your validator returns the optional
`validation.Result.LabelCount` to the calling control plane through the SDK's
validation contract. The callback receives no tier or numeric label-limit value;
the control plane interprets the returned count and owns policy decisions.

Separately, `AgentServerOptions.LabelsUsageLimit` configures runtime admission in
the agent; it is not a coordinator run assignment or an input to preflight counting.
Result-file ingestion and opt-in SDK folding permit N named request labels plus
one overflow label per agent/run for a nonnegative budget N; zero folds all names
in these SDK-managed modes. Default native proxying and in-process handlers do
not enforce this budget; consult the metrics contract for configuration and
engine-specific limit semantics.
Direct registry recording does not perform admission again: validate source
samples and use `metricschema.AdmitRequestName` with the resolved budget before
recording. Source-specific aggregate byte labels must preserve their attribution.

With SDK native folding enabled, names keep their slots across scrapes, with
deterministic admission of new names within a scrape. Rejected scrapes do not
consume slots. Folding sums each current cumulative snapshot, not deltas or
previously scraped values. Admission survives completion and resets on the next
successful launch. In all native modes, preserve cumulative source series
throughout a run; the final scrape still requires a reachable endpoint or handler. SDK
folding bounds exported request labels, not producer memory, other Prometheus
dimensions, or global plan-wide cardinality. An exact preflight count is not a
guarantee of runtime labels.

An arbitrary Prometheus endpoint is not automatically compatible. Declaring
`shibuya-v1` promises conformance; no mode is a complete profile validator.
Only with `FoldNativeMetrics: true` does the SDK check safe folding and limit
successful snapshots to 16 MiB of uncompressed Prometheus text. In that fallback,
malformed, unsafe, or oversized snapshots return HTTP 502 rather than passing
through. Default native proxying and in-process handlers impose neither this size
bound nor these validation checks. Producer failures are not replaced with empty
or cached metrics.
Use [the metrics contract](agent/metrics.md) for the full format, aggregation,
error, and lifecycle rules. Use `metricschema` definitions rather than private
collectors or copied metric-name strings. Its label-list functions return
caller-owned slices.

## Prepare the engine image and workspace

You package the adapter executable, engine binary/runtime, and any tools required
by your callbacks into an engine image. In the adapter entry point, configure
your hooks and filenames, resolve deployment options with
`agent.LoadAgentServerOptions`, initialize adapter components, and create
`agent.NewAgentServer(options)`. Handle loading errors and the error returned by
`Run()`. Agree the image's filesystem and launch requirements with the deployment
integration; the SDK does not provision a container or register the image.
There is no public standalone start API.

Set `AGENT_ROOT` to an absolute, writable directory before constructing the server.
Also create writable `/test-data`; it is separate from `AGENT_ROOT`. The SDK creates
the run subdirectories, but both parent directories must be writable by the user
running the adapter, engine, and dependency hooks.

After installing the engine and callback tools, add this workspace setup to your
Dockerfile before the adapter's `ENTRYPOINT`. This example runs as UID/GID 10001;
adjust both the ownership and `USER` to match your engine image:

```dockerfile
USER root
ENV AGENT_ROOT=/var/lib/reqfleet
RUN mkdir -p "$AGENT_ROOT" /test-data \
    && chown 10001:10001 "$AGENT_ROOT" /test-data
USER 10001:10001
```

Keep installed engine files outside the per-run directories. Any other paths
written by the engine or dependency hooks must also be writable by that user.
If deployment mounts replace `AGENT_ROOT` or `/test-data`, provision the mounted
directories with matching ownership and write permissions; the image's `chown`
does not set permissions on mounted contents.

The current workspace layout is:

| `Workspace` field | Location / lifetime |
| --- | --- |
| `Root`, `State` | `AGENT_ROOT`; reusable engine and dependency state survives runs. |
| `Inputs` | `/test-data`; reset per run, independently of `AGENT_ROOT`. |
| `Config` | `AGENT_ROOT/test-conf`; not cleared wholesale because it also holds dependency reconciliation state. |
| `Results` | `AGENT_ROOT/test-result`; reset per run. |
| `Temp` | `AGENT_ROOT/run-tmp`; reset per run. |

Use an isolated container for runtime smoke tests: the SDK clears run inputs,
results, and temporary files. Do not point it at directories containing unrelated
data. Adapters must reconcile their own persistent state rather than assuming
all configuration disappears between runs.

## Runtime HTTP endpoints

The SDK opens the coordinator subscription and serves the agent HTTP endpoints;
do not implement a second transport client or status server for those functions.
The runtime's `Handler()` serves these endpoints on HTTP port 8080 when launched
through `Run()`; keep that port on the trusted deployment network:

- `GET /progress` (optionally `?run_id=...`), `GET /prepare-progress`, and
  `GET /metrics` require `Authorization: Bearer <APIKey>`.
- `GET /healthz` is unauthenticated and reports coordinator subscription state:
  200 when subscribed, 503 otherwise. It does not report workload readiness.

The coordinator consumes preparation/run status; the Reqfleet metrics collector
consumes `/metrics`. Your callbacks supply engine observations and metric data,
but the SDK implements these public endpoints and their authentication.

## Verification and completion

Run the SDK checks from the repository root:

```sh
GOWORK=off go test -race -count=1 ./...
GOWORK=off go list -deps -test -json ./...
GOWORK=off go list -m -json all
```

Existing behavior tests show the repository's conventions:

- [Dependency/start tests](agent/dependency_start_test.go): staged artifacts,
  preparation barriers, cancellation, and serialized runs. Their fixture uses
  temporary input directories instead of the deployment's `/test-data`.
- [Startup/progress tests](agent/startup_progress_test.go): readiness versus exit,
  progress isolation, final snapshots, and process cleanup.
- [HTTP tests](agent/http_test.go) and [TLS tests](agent/tls_test.go): authentication,
  metrics option admission, proxy behavior, and coordinator trust.
- [Metric producer tests](agent/metrics_producer_test.go): event accumulation,
  identity, and compatible histogram output.
- [Native metrics tests](agent/metrics_native_test.go): numeric budgets, stable
  admission, cumulative folding, malformed input, and final/next-run scrapes.

For an adapter, build the image you supply and exercise the actual engine with
the control-plane integration's coordinator, artifact service, metrics collector,
and deployment configuration in an isolated environment. Verify the assigned
settings and artifact partition modes, a normal run with reporting-compatible
metrics, nonzero exit,
preparation cancellation, graceful stop, and a second run after cleanup. Include
dependency removal between runs if the engine supports dependencies. Confirm
readiness through `/progress`, not merely a healthy `/healthz` response. Check
metrics values, units, labels, and buckets against the metrics contract, not just
that `/metrics` returns 200. Report which scenarios ran and any unavailable
engine/coordinator prerequisites; SDK tests alone do not prove an integration.

When changing behavior, update the affected contracts and behavior tests; keep
project-level explanations in README and adapter instructions here. The Go API,
`shibuya-v1` metrics profile, and wire behavior are separate contracts. The
existing wire envelope and subscription are unversioned: there is no protocol
negotiation or unsupported-version rejection. Review pre-1.0 API changes when
upgrading a pinned dependency; do not imply that a module version negotiates
coordinator compatibility.

Do not manually update `CHANGELOG.md` for code or documentation changes.
Release Please manages changelog updates in its release PRs.

Coordinators **MUST ALWAYS maintain backward compatibility with existing engines**,
as defined by the [coordinator compatibility policy](README.md#coordinator-backward-compatibility).
Before releasing coordinator changes, verify older engine images as well as
current ones, including payload semantics, artifact/settings delivery, preparation,
execution, metrics/reporting, completion, and cancellation. Do not require an
engine rebuild, upgrade, or re-registration merely because the coordinator changes.

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

The [manifest](.release-please-manifest.json) records the **last released**
version, not the next target. It is initialized to the existing `0.1.1` release;
the unreleased validation feature makes the first automated release `0.2.0`.
Release Please advances the manifest in its release PR. Do not pre-seed it with
`0.2.0` or leave a permanent `release-as` override: subsequent versions should
continue to follow Conventional Commits.

