// Package conformance provides optional, engine-independent adapter contract tests.
// It validates the public SDK protocol and normalized metrics, not control-plane
// registration, deployment configuration, or compatibility with a particular
// Reqfleet deployment. Importing this package does not start listeners, launch
// workloads, or make the importing process a subreaper.
//
// # Metric assertions and independent evidence
//
// AssertMetrics and the expectation/snapshot types are available without build
// tags. They check normalized exposition against measurements supplied by the
// adapter author. The [SDK metrics contract] remains authoritative for names,
// labels, units, histogram boundaries, and accumulation semantics.
//
// Expected values must come from independently parsed engine-native output or
// other independent workload evidence, not from the normalized scrape being
// tested or the production parser/exporter under test. Keep native parsers,
// scripts, error mappings, engine versions, and runtime dependencies in the
// adapter repository. Preserve independently available status counts, latency
// observations, received bytes, and concurrency; do not invent measurements an
// engine does not produce. MetricsSnapshot supports comparisons of repeated and
// retained final scrapes without conflating independently changing resources.
//
// # Runtime extension points
//
// The runtime API is available only with the integration build tag on Linux.
// Contract supplies the real adapter executable, test and data artifacts, a local
// HTTP target, BasicScenarios, optional BudgetScenario cases, and an optional
// Scenarios callback. Fixtures supply engine-specific settings and artifact names.
// BasicScenarios.AssertMetrics supplies the independent native-output oracle and
// returns a validated MetricsSnapshot. It must work for both lifecycle fixtures
// and every budget fixture. Complete must terminate naturally; Reuse and budget
// fixtures must remain active until stopped and generate real HTTP requests.
//
// Run owns authenticated TLS command and artifact peers, request authentication
// checks, lifecycle and cancellation checks, duplicate/stale command handling,
// workspace reuse, final-scrape retention, and optional exact label-budget checks.
// Runtime methods expose commands, progress, scrapes, logs, artifact cancellation,
// and liveness checks for additional engine-specific scenarios. Payload and
// DataArtifactPath construct commands and artifact paths with harness identities.
// Scenarios and runtime calls must be serialized; do not use parallel tests.
// AwaitRequests tolerates transient scrape failures only while waiting for the
// requested run's measurements within Wait. Explicit Scrape calls and final
// metric assertions still require a successful response.
//
// # Container isolation and prerequisites
//
// Never execute runtime integration binaries on the host. Use a disposable Linux
// Docker container with its own network and PID namespaces, no host mounts, no
// published ports, and no external network (docker run --rm --network none).
// Do not use host networking, host PID sharing, or privileged mode. The harness
// requires root inside the container, then launches the adapter as UID/GID 65532.
// Runtime entry points reject non-root callers and environments without
// /.dockerenv; these guards do not establish isolation themselves. The consumer's
// container invocation must enforce the restrictions above.
//
// Provide the real adapter executable, engine and dependencies, /bin/sh for the
// shared regression fixtures, a writable /tmp and /etc/resolv.conf, writable
// /test-data for UID/GID 65532, and readable /proc and cgroup CPU/memory files.
// The adapter must honor AGENT_ROOT for its writable runtime workspace. The
// harness needs permission to bind low ports, change file ownership, switch
// credentials, signal its children, and enable Linux PR_SET_CHILD_SUBREAPER;
// Docker's ordinary root container capabilities suffice without writable cgroups.
// All engine dependencies must be installed before the network-isolated run.
//
// The suite reserves loopback UDP 53 (DNS), TCP 443 (artifacts), TCP 2416
// (commands), TCP 8080 (adapter), TCP 18080 (workload target), and TCP 18081
// (descendant regression). It replaces /etc/resolv.conf with its local failing
// DNS peer and owns /test-data and temporary adapter workspaces. Each scenario
// uses a dedicated supervisor subprocess to reap adopted descendants, including
// separate process groups, without reaping unrelated test children. Only an
// explicit internal supervisor re-exec activates the guarded init path; ordinary
// imports do not change process supervision. Unconfirmed descendant cleanup
// terminates the test process rather than contaminating the next scenario.
//
// # Consumer test wiring
//
// In the consumer's Linux container build stage, compile both the shared SDK
// regressions and the engine-local integration tests, for example:
//
//	go test -c -tags=integration github.com/reqfleet/engine-sdk/conformance
//	mv conformance.test /out/conformance-runtime
//	go test -c -tags=integration -o /out/conformance-engine ./myengine/conformance
//
// The engine-local package imports this package and calls Run with its Contract.
// Importing Run does not include this package's own regression tests in the
// consumer's test binary, so both binaries are required. Copy both into the
// consumer's conformance image and execute them sequentially, as root, only in
// the isolated disposable container described above. Allow sufficient test
// timeouts for natural completion and bounded cleanup. Shared regressions cover
// short-lived completion, unsuccessful completion, preparation cancellation,
// rejected overlaps, stale stops that initiate slow shutdown, transient scrape
// recovery, query-sensitive run IDs, cleanup deadlines, separate process groups,
// and live orphan reaping using a real SDK fixture adapter and child HTTP workloads.
//
// Include this package's untagged metric-validator tests in the consumer's unit
// target (go test github.com/reqfleet/engine-sdk/conformance), and gate consumer
// CI/publication on its container conformance target. A passing fixture suite is
// evidence for the tested adapter/engine/SDK combination, not a deployment
// compatibility guarantee.
//
// [SDK metrics contract]: https://github.com/reqfleet/engine-sdk/blob/main/agent/metrics.md
package conformance
