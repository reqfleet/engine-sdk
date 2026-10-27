# Adapter development guide

Use this file when implementing an engine adapter or changing the SDK. Keep the
project introduction, Reqfleet motivation, and basic architecture in
[README.md](README.md); keep implementation workflow and constraints here.

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
| Metrics | Reqfleet metrics collector scrapes the agent; the control plane owns storage, aggregation, and reporting | Exposes `/metrics`; ingests parsed result events or proxies a declared native endpoint | Map engine output to the reporting contract through a parser or normalized native endpoint |

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
| [`agent/server.go`](agent/server.go) | `AgentServerOptions`, constructors, runtime startup, deployment configuration, metrics admission |
| [`agent/run.go`](agent/run.go), [`agent/command.go`](agent/command.go) | `RunContext`, `Workspace`, `Process`, and the command returned by an adapter |
| [`agent/lifecycle.go`](agent/lifecycle.go) | SDK-owned preparation, process supervision, observations, cancellation, and cleanup |
| [`agent/http.go`](agent/http.go), [`agent/metrics.go`](agent/metrics.go) | Authenticated endpoints, native metrics proxy, result-event ingestion and collectors |
| [`protocol`](protocol) | Coordinator messages, explicit run/artifact assignments, lifecycle states, errors, and artifact paths |
| [`metricschema`](metricschema) | Collector-free metric names, reserved request labels, and ordered label definitions |
| [`validation`](validation/doc.go) | Synchronous upload-validation contract, structured diagnostics, and result checks; not part of agent execution |
| [`examples/jsonvalidator`](examples/jsonvalidator) | Working JSON request-plan validator using only standard-library and public SDK dependencies |
| [`internal`](internal) | SDK-only artifact transport, subscriptions, and container resource collection |

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
- `ResultFile`: you choose a local output name for result-file metrics and
  configure the engine to write to `RunContext.ResultFile`. This is output
  produced by the engine, not an artifact supplied by the control plane.

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
4. The SDK launches the command, streams stdout/stderr, runs observation and
   progress monitors, and ingests result events when configured.
5. On exit or stop, the SDK terminates remaining descendants, joins callbacks
   and readers, captures final progress, and only then releases the workspace.

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
  `MetricsURL` and `NativeMetricsSchema` empty. The SDK tails the output file.
  Each parser call receives a line and returns `agent.ShibuyaMetric` or an error.
  Map request label, status, latency in milliseconds, received bytes into
  `EgressBytes`, and current concurrency into `Threads`. The runtime supplies
  run identity and accumulates metrics; parser errors drop the event. Never
  feed cumulative snapshots into a per-request event parser.
- **Normalized native endpoint:** set an HTTP(S) `MetricsURL` and
  `NativeMetricsSchema: agent.MetricsSchemaShibuyaV1`; leave `MetricParser` nil.
  The SDK proxies the endpoint unchanged, so the adapter must supply the complete
  compatible exposition, including identity labels and resource metrics.
  `ResultFile` may name ordinary workload output in this mode but is not tailed.
- **Registry only:** leave all metrics options unset. The runtime registry is
  exposed without request-event ingestion; this is not complete request reporting.

An arbitrary Prometheus endpoint is not automatically compatible. Declaring
`shibuya-v1` promises conformance; the SDK does not normalize or validate every
scraped sample. Use `metricschema` definitions rather than private collectors or
copied metric-name strings. Its label-list functions return caller-owned slices.

## Prepare the engine image and workspace

You package the adapter executable, engine binary/runtime, and any tools required
by your callbacks into an engine image. In the adapter entry point, configure
your hooks and filenames, create `agent.MakeAgentServer(options)`, call `Run()`,
and handle its returned error. Agree the image's filesystem and launch requirements
with the deployment integration; the SDK does not provision a container or register
the image. There is no public standalone start API.

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

## Releasing the SDK

The [`release-please` workflow](.github/workflows/release-please.yml) runs on
pushes to `main`; it can also be dispatched manually with `main` selected.
It uses the Go release strategy in
[`release-please-config.json`](release-please-config.json) for the root module
only. Examples are not separate release components.

Before enabling the workflow, add the repository Actions secret
`RELEASE_PLEASE_TOKEN`: a fine-grained personal access token with access to this
repository and read/write permissions for Contents, Issues, and Pull requests.
Obtain any required organization approval. The dedicated token lets generated
release PRs trigger the existing `pr-test` workflow; the default `GITHUB_TOKEN`
does not trigger workflows from its PR or tag events. See the
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

