// Derived from reqfleet/shibuya; modified for engine-sdk.
// Package agent runs coordinator-dependent engine adapters.
//
// # Platform support
//
// Deployed agents require Linux containers and cgroup resource metrics. Windows
// clients may import SDK types and use Handler without starting an agent.
// On non-Unix systems, AgentServer.Run returns an error matching
// errors.ErrUnsupported before starting workers, subscriptions, or HTTP services.
// Workload execution requires Unix process-group supervision.
//
// # Coordinator authority
//
// An agent is not a standalone runner. The coordinator authorizes preparation,
// start and stop, and supplies an explicit RunSpec for the addressed agent.
// Settings are already resolved: adapters must not interpret VU scope, redistribute
// load, infer settings from topology or manufacture a missing assignment. Native
// settings may already be rendered into coordinator-provided configuration files.
// Artifact assignments are per file, not derived from the agent's identity.
// Materialized input has already been partitioned; native input requires the
// engine to apply the exact supplied shard. Replicated input is consumed whole.
//
// # Callback order and ownership
//
// PrepareDependencies receives the staged dependency declaration, if any. An
// absent declaration still invokes reconciliation to remove prior dependencies.
// A coordinator preparation barrier may separate this stage from start. Execution
// artifacts are staged before BeforeStart, then BuildCommand returns the primary
// command. No callback launches or reaps the primary workload; the runtime owns it.
// Context paths are absolute. Inputs, results and temporary files are reset per
// run. State and dependency reconciliation files survive. The current runtime
// serializes runs and reuses the existing image paths only after cleanup ends.
//
// Observe and Progress run after launch, on separate polling loops, outside the
// runtime lock. Live Progress uses the monitor's cancellation context, allowing
// summaries to outlast individual observation probes without delaying readiness
// or status requests. Their read-only RunContext remains valid until each callback
// returns. Callbacks must honor context cancellation, including child processes.
// They may cache engine-private summaries by RunID, but must not mutate the
// supplied settings or artifact maps. BuildCommand is required; other hooks are
// optional. Without Observe, a successful launch acknowledges readiness.
//
// # Outcomes and shutdown
//
// Startup acknowledgement and terminal outcome are independent. WorkComplete
// acknowledges that startup succeeded but cannot override a failed process.
// A successful short run completes even if readiness was never polled. A nonzero
// exit after readiness is an execution failure. Optional progress cannot change
// lifecycle state or make the status endpoint unavailable. Progress payloads must
// be valid JSON and identify their engine schema; aggregation belongs to the
// coordinator. After canceling and joining the live monitors, the runtime captures
// a final progress snapshot with a fresh two-second context. Adapters should reuse
// cached summaries/checkpoints rather than restart canceled long-running work.
//
// Stop targets a run and carries the coordinator's reason: scheduled completion
// or cancellation. Preparation cancellation prevents launch. The runtime retains
// ownership while callbacks roll back or terminate child processes. For a running
// workload, Stop receives a fresh context bounded by StopTimeout (ten seconds by
// default). Without Stop the runtime sends SIGTERM to the process group; after the
// grace period it sends SIGKILL. Stop must honor its context and must not wait on
// the primary process. The runtime reaps it, terminates remaining descendants and
// joins all callbacks/readers before releasing the workspace for another run.
//
// Duplicate requests do not execute a run twice. Retired run IDs are remembered
// for the agent process lifetime. A stale stop or callback cannot affect a newer
// run. A new run cannot start while preparation, execution or cleanup is active.
// Run-scoped progress acknowledges a stopped run only after its worker has
// finished cleanup. A stop received before preparation tombstones that run ID.
//
// # Metrics integration
//
// Adapters either map result-file events with MetricParser, explicitly declare
// MetricsSchemaShibuyaV1 for an adapter-normalized native endpoint, or expose the
// registry without request ingestion. A Prometheus URL alone is not a schema
// mapping. See the [metrics contract] for names, units, labels, bucket profiles,
// accumulation semantics, and built-in adapter limitations.
//
// The control plane supplies a numeric LabelsUsageLimit; the SDK has no tier
// policy. Workloads inherit the lowercase label_limit environment variable;
// Command.Env may override it. The SDK does not rewrite it from LabelsUsageLimit.
// Native engines apply this assignment through their own limit configuration.
// Native proxying streams by default without parsing, format negotiation,
// validation, or SDK label admission.
//
// FoldNativeMetrics explicitly enables last-resort SDK folding for native engines
// that cannot enforce the budget. It buffers and parses each Prometheus text
// snapshot (at most 16 MiB), validates safe aggregation, and re-encodes the result.
// Invalid snapshots fail rather than bypassing admission. Admission is stable
// across scrapes; colliding cumulative counters and histograms are aggregated
// within each snapshot, not added again across snapshots. The producer must retain
// cumulative source series for the run. NativeMetricsOverflowLabel defaults to
// "others"; Replay's engine-owned overflow convention is "_other_".
// Replay handles its limits through config.yaml with SDK folding disabled.
// Its native max_labels=0 setting remains unlimited.
//
// With folding enabled, admission survives completion for final scrapes and resets
// on the next successful workload launch; the producer must still be reachable.
// This bounds exported request labels, not producer memory, other dimensions, or
// plan-wide labels across agents. Result-file ingestion and opt-in folding permit
// N named labels plus one overflow label; zero permits only overflow in these
// SDK-managed modes. Optional preflight counts do not guarantee runtime cardinality.
//
// [metrics contract]: https://github.com/reqfleet/engine-sdk/blob/main/agent/metrics.md
package agent
