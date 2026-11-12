# Shibuya metrics profile: `shibuya-v1`

This document defines the `shibuya-v1` metrics reporting contract for engine
adapters using `github.com/reqfleet/engine-sdk/agent`: collection modes, metric names, labels, units, and
histogram boundaries. Adapters declare this profile in their configuration.

The collector-free [`metricschema`](../metricschema) package owns the Go constants
for metric names and reserved request labels, ordered label-list functions, and
canonical histogram boundaries. Returned label and boundary slices are caller-owned;
collectors and event recording remain in `agent`.

## Integration modes

| Mode | `AgentServerOptions` | Ownership |
| --- | --- | --- |
| Result-file events | Nonempty `ResultFile`, non-nil `MetricParser`; leave `MetricsHandler` nil, `MetricsURL` and `NativeMetricsSchema` empty | The parser maps each result row to `agent.ShibuyaMetric`; the runtime ingests events into its registry. |
| Direct SDK registry updates | Optional `MetricsRegistry: agent.NewMetricsRegistry()` and `CustomIngestMetrics`; leave `MetricParser`, `MetricsHandler`, and all native options unset | The adapter validates and admits engine samples, then updates SDK-defined collectors. The SDK schedules ingestion, samples resources, and serves stored values. |
| In-process normalized handler | Non-nil `MetricsHandler`, `NativeMetricsSchema: agent.MetricsSchemaShibuyaV1`, nil `MetricParser`, empty `MetricsURL` | The adapter owns normalization, label limits, and exporter state; the SDK serves the handler directly behind its existing authentication. Folding options are not supported. |
| Normalized native endpoint | An HTTP(S) `MetricsURL` with a host, `NativeMetricsSchema: agent.MetricsSchemaShibuyaV1`, nil `MetricParser` and `MetricsHandler` | The engine enforces its configured label limit and produces the complete normalized exposition; the runtime streams it without parsing. Set `FoldNativeMetrics: true` only for last-resort SDK folding. |
| Registry only | All metrics options zero-valued | Exposes the runtime registry without request-event ingestion. This does **not** imply complete request reporting. |

With `CustomIngestMetrics`, `MetricsHandler`, or `MetricsURL`, `ResultFile` may
name ordinary workload output, such as k6 CSV. The SDK does not tail it or require
a parser. Otherwise, `ResultFile` and `MetricParser` must be paired.
`CustomIngestMetrics`, `MetricsHandler`, `MetricsURL`, and `MetricParser` are
mutually exclusive. An explicit `MetricsRegistry` cannot accompany a native source.
`FoldNativeMetrics: true` and any nonempty `NativeMetricsOverflowLabel` require
`MetricsURL`; they are rejected with a handler, result-file events, or registry only.
For URL-backed metrics, a valid overflow label may be configured with folding
disabled, but has no effect until folding is enabled.

`Run` rejects invalid options before starting background work. Direct use of the
metrics HTTP handler returns 503 for the same invalid configuration. Validation
checks the numeric budget and selected metrics source, schema, and URL. Errors
identify the remedy:

- `metrics label limit must be nonnegative`
- `result-file metrics require both ResultFile and MetricParser`
- `native metrics schema requires MetricsURL or MetricsHandler`
- `native metrics cannot configure both MetricsHandler and MetricsURL`
- `FoldNativeMetrics and NativeMetricsOverflowLabel require MetricsURL`
- `native metrics cannot also configure MetricParser`
- `unsupported native metrics schema %q; require shibuya-v1 from an adapter-normalized endpoint`
- `native metrics overflow label must be others or _other_`
- `native metrics URL must use http or https and include a host`
- `native metrics cannot configure MetricsRegistry or CustomIngestMetrics`
- `CustomIngestMetrics cannot configure MetricParser`

Choose an integration based on both how the engine exposes metrics and the schema
it produces. For example, an engine without native Prometheus export, such as
JMeter writing JTL results, needs a `ResultFile` and a `MetricParser` that converts
each result row into `agent.ShibuyaMetric`. The runtime tails the file and
accumulates those events into the Prometheus metrics required by the control plane.

If an engine exposes Prometheus metrics with a different schema, provide an
adapter-owned endpoint that normalizes them to `shibuya-v1`, or use per-request
result events with a `MetricParser`. The schema declaration is the producer's
promise that its handler or endpoint follows this contract. With
`FoldNativeMetrics: true`, the runtime checks what is needed for safe folding;
none of these modes is a complete profile-conformance validator.

When `MetricsURL` is set, each scrape of the agent's `/metrics` endpoint is
forwarded to that URL. The engine or adapter supplies the identity labels, metric
names, units, histogram buckets, and resource metrics required by this contract.
By default (`FoldNativeMetrics: false`), the ordinary reverse proxy streams the
upstream response without buffering the snapshot, parsing, folding, re-encoding,
or imposing text/identity negotiation. The engine owns budget enforcement and
profile conformance. Metrics-gateway admission by metric name does not establish
semantic conformance.

### SDK-owned registry

`agent.NewMetricsRegistry()` creates the canonical collectors for one agent;
pass it as `AgentServerOptions.MetricsRegistry` when initializing an adapter
that needs to record independent samples. Otherwise, `NewAgentServer` creates
its own instance. `AgentServer.MetricsRegistry()` returns that instance, or nil
in native handler/URL modes. No collectors use process-global state. Do not share
a registry between servers or register duplicate families.

Recording methods take `ShibuyaMetric` values with identity from `RunContext`:

- `ObserveLatency` records only `Latency`, in milliseconds, in the ordinary
  request histogram.
- `RecordStatus` increments only the supplied `Status` counter by one.
- `ObserveRequest` records ordinary latency and one status count only.
- `AddReceivedBytes` adds only `EgressBytes`, under the supplied label.
- `SetConcurrency` sets only `Threads`.
- `RecordEvent` records all four values for a complete per-request event. It
  preserves the parser's case-insensitive `llm_resp` histogram routing;
  `ObserveLatency` and `ObserveRequest` always record ordinary request latency,
  including when the label is `llm_resp`.

Use the operation matching the measurements actually present in the source.
Latency-only and status-only samples must not use `ObserveRequest` or
`RecordEvent`: zero-valued fields do not mean a measurement is absent.
`ObserveLatency` records a supplied zero latency; `RecordStatus` does not observe
any latency. Neither operation changes received bytes or concurrency.

The adapter owns source validation and request-name admission before calling
these methods. Use `LabelIsValid` and `metricschema.AdmitRequestName` with the
resolved label budget. Independent bytes or concurrency samples must not be
passed to `RecordEvent`; doing so invents requests. Preserve aggregate byte
attribution when the source does not identify a request.

Use `CustomIngestMetrics func(context.Context, RunContext) error` when the adapter
must normalize engine output that does not fit `MetricParser`'s complete-request
event model. For example, k6 CSV emits separate request-duration, received-byte
and concurrency samples. Read new output, validate and admit samples, then record
them using the SDK methods. The SDK owns scheduling, HTTP exposition
and CPU/memory reporting; the callback must not duplicate those responsibilities.

The SDK invokes this callback after launch and every 250 milliseconds,
independently of scrapes and the readiness/progress loops.
Honor its context and synchronize adapter state with `Observe`. An uncanceled
collection error fails the run and initiates shutdown. Once accepted, that error
remains a run failure even if the process exits concurrently and final collection
succeeds. The registry also rejects scrapes with HTTP 500 until reset or the next
successful launch, rather than serving partial totals as a successful snapshot.

After process/descendant cleanup, the SDK cancels and joins live callbacks and
readers, then invokes `CustomIngestMetrics` once more with a fresh
`FinalMetricsTimeout` context (two seconds). Drain final output within that
deadline; final errors or an expired deadline fail the run even after a
coordinator-requested stop, including scheduled completion. Live and final
collection calls never overlap, and the workspace is not released until final
collection completes.

Retain completed-run values for final scrapes. When clearing parser/admission
state in `BeforeStart`, call `registry.Reset()` under the adapter's state lock.
Reset clears all series, collection errors, and resource identity/baseline.
The SDK assigns resource identity and clears collection errors on successful
launch. Without explicit Reset, prior-run series remain separately labelled.
Never reset during an active run. Recording and gathering are concurrency-safe,
but a scrape is not a transaction across independent metric updates.

HTTP only gathers the current registry: it does not read files, sample resources,
or invoke callbacks. Before any samples, it may return an empty exposition;
workload readiness belongs to `/progress`, not the metrics response status.

This replaces the process-global `ShibuyaMetric.ToPrometheus()` API with
`registry.RecordEvent(metric)`. JMeter/Locust-style `MetricParser` callbacks
and the `ShibuyaMetric` data fields remain unchanged. Replay-style native
proxying remains separate; a registry cannot be shared with another process.

### In-process exporter ownership

`MetricsHandler` is an `http.Handler` mounted at the SDK's existing authenticated
`GET /metrics` route. Missing, malformed, or incorrect bearer credentials are
rejected before the handler is invoked. The handler receives the incoming request
and response writer directly, including request context values, deadlines, and
cancellation. Its status, headers, and body are served unchanged, including error
responses. There is no additional adapter listener, HTTP proxy hop, snapshot
buffer, parsing, label folding, or merge with the SDK registry.

The adapter must supply the complete normalized exposition, including identity
labels and resource metrics, and enforce its configured label budget. It owns
CSV/result parsing, cumulative metrics, per-run identity/state transitions, and
exporter shutdown. Synchronize updates against concurrent scrapes and honor
request cancellation. A scrape can arrive before a run, during execution, or
after completion; retain the completed run's cumulative snapshot for final
scrapes. The SDK neither resets nor closes the supplied handler and does not
cache responses. `Run()` still owns the HTTP listener and server lifecycle.

See the [in-process exporter example](../AGENTS.md#resolve-deployment-options-before-construction)
for constructing the handler from resolved deployment options without a second
listener.

### Opt-in native folding fallback

Set `FoldNativeMetrics: true` only when the engine cannot enforce its budget.
This adds per-scrape buffering, parsing, validation, aggregation, and re-encoding
cost, plus per-run admission state. Only this fallback requires Prometheus text
exposition (`text/plain; version=0.0.4`): the SDK requests that format with
`Accept-Encoding: identity` and bounds each successful snapshot to
`agent.MaxNativeMetricsBytes` (**16 MiB**).
The following format and validation guarantees apply only with folding enabled.
Compressed responses, malformed
exposition, unsafe request-series aggregation, oversized snapshots, and non-200
2xx responses fail with HTTP 502 and an explanatory error. Upstream non-2xx
status/body responses are preserved; connection failures return 502. Errors
never fall back to an empty registry or a cached successful snapshot.

Request-labelled native metrics must identify the assigned collection, plan,
run, engine, and zone. They must be finite nonnegative counters or classic
histograms, without explicit sample timestamps. Histograms require a sum,
integer count, cumulative ordered buckets, and a `+Inf` bucket equal to the count.
Colliding histograms must have compatible bucket boundaries. Known request
families must have their declared counter/histogram type and request label;
status counters must also carry `status`. Duplicate series, missing or duplicate
histogram components, and numeric aggregation overflow are errors. Histogram
component validation covers ordinary and quoted legacy metric names, including
names inside braces. These checks do not replace the producer's responsibility
for the profile's exact bounds and units. Additional request-labelled
counter/histogram families, such as Replay's
optional schedule-lateness histogram, share the same admission mapping.
Unlabelled resource families are not folded.

## Identity and metric families

Every series carries identity labels
`I = {collection_id, plan_id, run_id, engine_no, zone}`.

Result parsers supply request values. The runtime overwrites `ShibuyaMetric`'s
collection, plan, run, engine, and zone with the assigned run's identity before
recording the event. Direct registry callers populate these fields from their
callback `RunContext`; they do not infer assignments. Native adapters supply the
same identity labels using resolved `EngineMeta` and `RunContext.RunID`, or the
runtime's same-named workload environment variables for an external producer.

| Historical family name | Type | Unit and meaning | Labels |
| --- | --- | --- | --- |
| `shibuya_latency_label_milliseconds` | Classic histogram | Ordinary request latency in milliseconds, with adapter-specific timing semantics below | I + `label`; buckets also have `le` |
| `shibuya_latency_llm_resp_milliseconds` | Optional classic histogram | LLM response duration in milliseconds | I + `label`; buckets also have `le` |
| `shibuya_status_counter` | Counter | Recorded outcomes, grouped by source status | I + `label` + `status` |
| `shibuya_egress_bytes_counter` | Counter | Recorded response/received bytes, despite the historical “egress” name | I + `label` |
| `shibuya_threads_gauge` | Gauge | Current per-engine concurrency | I |
| `shibuya_cpu_gauge` | Gauge | CPU cores consumed | I |
| `shibuya_mem_gauge` | Gauge | Memory bytes | I |

A classic histogram's sample series are its `_bucket`, `_sum`, and `_count`
components. The base family name is not itself a sample series. Preserve these
historical names rather than adding `_total` or changing the byte-counter name.

Use lowercase reserved labels `ttft` and `llm_resp` for optional LLM timing.
The runtime routes `llm_resp` events into the separate wide histogram (its routing
comparison is case-insensitive, but reporting selectors use the lowercase label).
`ttft` uses the ordinary histogram. Omit unavailable optional LLM observations;
do not emit a fabricated zero timing. Status and byte counters still reflect the
events the adapter records; synthetic timing events must not be assumed to be
additional HTTP requests without understanding that adapter's event model.

## Request-label validation

Request labels must contain at most `metricschema.MaxRequestLabelBytes`
(1023 bytes, not characters). Use `agent.ShibuyaMetric.LabelIsValid()` to check
the byte length; adapter-specific requirements, such as a nonempty URL, remain
separate.

Validate the source request label **before admission or metric accumulation**.
Do not truncate oversized labels, merge them into shorter prefixes, or send them
to the overflow label. Invalid labels consume no admission slots. A zero named-label
budget does not bypass validation: only valid labels may fold into overflow.

Rejection scope depends on the integration:

- **Result-file events:** the SDK drops the entire parsed `ShibuyaMetric` without
  updating any collectors. Latency, status, bytes, and concurrency carried by that
  event are all discarded. Parser errors also drop the whole event.
- **Opt-in native folding:** an invalid request label rejects the entire scrape
  with HTTP 502, without consuming admission slots. The SDK does not return a
  partially accepted or cached snapshot.
- **In-process handlers and default native proxying:** the SDK does not validate
  labels or alter the producer's response. The adapter/exporter owns enforcement.
  Adapters that ingest source request events or samples must use the same
  whole-sample rejection rule before updating request metrics. Independently
  reported samples, such as aggregate received bytes or live concurrency, remain
  independent; do not discard them because another request sample was rejected.
- **Direct SDK registry updates:** the adapter validates and admits each source
  sample before recording it. The recording methods do not apply a second
  admission policy; independent byte and concurrency samples remain independent.

These are metrics-reporting rules, not limits on workload HTTP URLs or permission
to execute requests. Rejecting a metric event does not cancel or fail the workload.

## Request-name budget

A **request name** identifies a request or request group, typically by URL or a
custom grouping name. It is emitted as the value of the Prometheus `label`
dimension; it is not a metric-family name or an arbitrary Prometheus label key.
The Go admission API uses request-name terminology without changing that wire key.

Runtime admission is configured by the nonnegative
`AgentServerOptions.LabelsUsageLimit`, not by coordinator run messages or the
upload-preflight callback. `NewAgentServer` uses that option directly, including
zero; `LoadAgentServerOptions` and `MakeAgentServer` resolve it from the lowercase
`label_limit` environment variable (`metricschema.DefaultLabelLimit`, **10**,
when unset or empty). This default counts request names only, excluding overflow.
Adapters can use the loader's resolved budget to initialize native exporters,
then pass the same options to `NewAgentServer` without another environment read.
See the [environment configuration contract](../AGENTS.md#resolve-deployment-options-before-construction)
for validation and explicit-option precedence.
The workload inherits `label_limit` when present in the agent environment,
subject to `Command.Env` overrides. The SDK does not export `LabelsUsageLimit`
or its default back into that environment.
Result-file ingestion and opt-in SDK folding permit **at most N request names
plus one overflow name per agent/run**. In those SDK-managed modes, zero
permits only overflow. Negative budgets are invalid in every mode.
Native engines, in-process exporters, and direct registry callers enforce their
configured limits; recording methods, default proxying, and `MetricsHandler` do
not enforce them. Explicit `FoldNativeMetrics: true` enables SDK enforcement only
for URL-backed native engines as a last-resort fallback.

Adapters can share the SDK's admission policy with
`metricschema.AdmitRequestName(requestName, admitted, limit, overflowName)`.
`metricschema.OverflowRequestName` is the public `others` constant; pass an empty
`overflowName` to select it, or `_other_` when that is the producer's convention.
The configured overflow name is reserved and does not consume a slot.

Create `admitted := make(map[string]struct{})` for each agent/run and use the
resolved `options.LabelsUsageLimit` as `limit`. Validate the source sample first,
then use the returned name for all of its request metrics. Serialize access
alongside collector updates, keep the budget and overflow name fixed, and clear
the map when resetting the run's metrics. Only admitted names are retained; new
map keys are cloned so the admission set does not keep entire CSV records alive.
For an already-admitted name, the helper returns the input string unchanged;
callers retaining that result own its backing-storage lifetime.
The helper does not validate samples or aggregate metrics. Native snapshot
consumers must also preserve all-or-nothing admission on rejected scrapes;
the SDK rolls back tentative new admissions if folding fails.

Replay uses `metrics.max_labels` from its prepared `config.yaml`. Run preparation
preserves that value rather than rewriting it from `label_limit`; no extra command
environment override is added. SDK folding stays disabled for Replay. Positive
budgets use its native limiter and `_other_` overflow; native `max_labels: 0` means
unlimited. Replay's own configuration and environment precedence remains unchanged.

This is not a global plan-wide budget across agents. SDK folding does not cap
producer memory, source-side series, metric-family names, statuses, or other
Prometheus dimensions. Separately, the upload validator returns an optional
preflight count to the calling control plane; no tier or numeric label-limit value
is passed into that callback. The control plane applies its upload policy to the
returned count. These counts provide early feedback only; they do not predict or
bound the labels that runtime code will emit.

Result-file ingestion admits names in event order and folds excess names into
`others`. With native folding enabled, the SDK sorts new names within each successful
scrape before filling remaining slots. Once admitted, a name retains its slot
for that run, even if it disappears from later scrapes. Rotating names cannot
reclaim slots. All native request-labelled families use the same mapping, and a
rejected scrape consumes no admission slots.

For opt-in folding, `AgentServerOptions.NativeMetricsOverflowLabel` defaults to
`others`; use `_other_` for Replay's convention. These are the only supported values. Only the
configured overflow name is exempt from the request-name budget; the other
spelling is an ordinary name. Collisions with an existing overflow series are
aggregated, not emitted as duplicate series.

Native folding sums the **current cumulative snapshot** of counters and
histograms for series that become identical after replacing `label`, preserving
all other label dimensions, including status groups. It sums bytes, histogram
counts, sums, and corresponding cumulative buckets; it never adds a previous
scrape's values again. The SDK retains admitted names, not a history of
overflow-source names or values.


## Histogram compatibility

Construct ordinary histograms with `metricschema.RequestLatencyBuckets()` and
LLM response histograms with `metricschema.LLMResponseLatencyBuckets()`. Both return
fresh, caller-owned slices of finite upper bounds in increasing order, in
**milliseconds**, excluding `+Inf`. Mutating a returned slice does not change
another caller's bounds or future calls.

Pass these bounds directly to Prometheus histogram constructors, which add the
implicit `+Inf` bucket. Exposition validators must append `math.Inf(1)` to check
the complete wire profile. Obtain bounds once when constructing a collector or
validator, not per recorded sample. Do not reconstruct them from
`prometheus.DefBuckets`; the profiles below are the fixed reporting contract.

Every ordinary histogram must expose exactly these cumulative upper bounds in
milliseconds, shared by JMeter, Locust, Replay, and normalized native adapters:

```text
1, 2.5, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, +Inf
```

Ordinary aggregate quantiles sum all buckets by `le` before applying
`histogram_quantile`; there is no bucket-boundary filter. The 1 and 2.5 ms buckets
participate in aggregate percentiles rather than being discarded. Integer
exposition labels (`1`) and Prometheus's normalized float labels (`1.0`)
represent the same numeric boundary. The mean remains exact and count-weighted
because it uses `_sum` divided by `_count`.

Aggregate percentiles require consistent bucket boundaries across all contributing
producers and stored samples. Normalize additional or missing boundaries in the
adapter before declaring this profile.

The separate LLM response histogram has the following finite upper bounds in
milliseconds, followed by `+Inf`; it remains separate from the ordinary profile:

```text
100, 250, 500, 1000, 2500, 5000, 7500, 10000, 15000, 20000,
30000, 45000, 60000, 90000, 120000, 180000, 300000, +Inf
```

Observations must be finite and nonnegative. `_sum` is accumulated milliseconds;
`_count` is the number of observations. Bucket counts are cumulative, monotonic
with increasing `le`, and the `+Inf` count equals `_count`. Do not infer missing
finer buckets, sum counts across `le`, or average engine percentiles. A coarse
summary or precomputed percentile cannot be renamed into a compatible histogram.

## Accumulation, sampling, and reporting

The Shibuya control-plane collection scraper polls each engine's `/metrics`
endpoint every **1 second by default**, in both result-file and native modes.

Counters and histogram components accumulate for one engine/run identity. Each
scrape captures their current cumulative values. Give each run a new run ID and
retain every source series and its cumulative values throughout that run, even
when a request label stops producing events. Do not rotate away source counters,
reset them within a run, or expose per-scrape deltas. Native folding cannot
reconstruct vanished source series or repair resets; the producer owns cumulative
retention so final summary queries can read the totals directly.

Registry mode retains old-run series for the agent process lifetime and rejects
late result-file events after run completion. With SDK folding enabled, native
admission state survives completion so the final upstream snapshot uses the same
mapping, and resets on the next successful workload launch. In all native modes,
the producer must enforce cumulative retention. A final native scrape requires
a reachable endpoint or handler serving the completed run's cumulative snapshot:
the SDK does not retain a cached metrics response. Missing final scrapes or
undrained result rows can affect reported totals.

Apply the selected collection/run/plan/label/zone filters to these formulas:

| Report | Aggregation |
| --- | --- |
| Status totals | `sum by (status) (shibuya_status_counter{...})` |
| Request rate | `sum(rate(shibuya_status_counter{...}[5s]))` |
| Recorded bytes | `sum(shibuya_egress_bytes_counter{...})` |
| Bytes per second | `sum(rate(shibuya_egress_bytes_counter{...}[5s]))` |
| Mean latency | `sum(H_sum{...}) / sum(H_count{...})`; for live windows, divide summed rates of the same components |
| Latency quantile | `histogram_quantile(q, sum by (le) (H_bucket{...}))`; for live windows, sum bucket rates first; all producers of `H` must use the same boundaries |
| Live concurrency | `sum(shibuya_threads_gauge{...})` across selected engines |
| Usage-accounting concurrency | Maximum sampled per-engine concurrency over the run: `max(max_over_time(shibuya_threads_gauge{...}[run_window]))` |

`H` is the ordinary or separate LLM family as appropriate. Quantiles are
histogram-interpolated estimates. Empty or nonfinite optional timing results are
omitted. Existing error charts classify non-2xx/non-3xx statuses as errors.

## Runtime resource metrics

The SDK collects container CPU and memory in a background loop for result-file,
direct-update, and registry-only modes. It samples immediately after launch,
every five seconds, and once after joining live monitors at run termination.
It exposes `shibuya_cpu_gauge` (CPU cores) and `shibuya_mem_gauge` (bytes).
The loop stops before workspace reuse; completed-run values remain available
without further reads. Short runs still get an initial and a final attempt.
Native `MetricsURL` and `MetricsHandler` modes perform no SDK resource reads;
their producers supply the complete exposition without a merge.

The runtime and adapter-owned exporters share a synchronous resource sampler
from `github.com/reqfleet/engine-sdk/resources`. Sampling policy is separate from
`github.com/reqfleet/engine-sdk/containerstats`, which provides raw cgroup readers:

- `containerstats.ReadCPUUsage() (uint64, error)` returns cumulative CPU time in
  **microseconds**, normalized across cgroup v1 and v2. V1's nanosecond counter is
  truncated to whole microseconds.
- `containerstats.ReadMemoryUsage() (uint64, error)` returns current
  cgroup-accounted memory in **bytes**, including cache. It is not process RSS or
  the configured memory limit.

These are container/cgroup measurements, including the SDK and engine processes
sharing the cgroup, not per-PID measurements. The readers require readable Linux
cgroup files at the conventional container mount paths under `/sys/fs/cgroup`;
they do not discover host-side paths or nested process membership. See the
[package documentation](../containerstats/cgroup.go) for the exact v1/v2 paths.
Calls are synchronous and safe to make concurrently, including alongside the SDK
runtime. They start no workers, register no collectors, and require no coordinator.
Missing, unreadable, or malformed files return errors, not successful zero samples.

Use a zero-valued `resources.Sampler` and call `Sample()` to obtain
`(resources.Sample, error)`. A sample contains only CPU cores and memory bytes;
there are no validity flags. The sampler implements
these rules:

- `CPUCores` is the cumulative CPU microsecond delta divided by actual elapsed
  wall-clock microseconds, retaining sub-microsecond elapsed precision. Timestamps
  come from Go's monotonic `time.Now`; the configured polling interval is not the
  denominator. An unchanged successful counter produces a genuine zero rate.
- The first successful read, including a zero CPU counter, establishes a baseline
  and returns `resources.ErrSampleUnavailable` with a zero sample. A decreasing
  counter re-establishes that baseline the same way, without unsigned underflow.
  Both CPU and memory are skipped for that attempt. Nonpositive elapsed time
  returns a different error and does not advance the baseline.
- Sampling stops at the first read error, which identifies the resource and wraps
  the underlying error. Read failures return a zero `Sample` and leave the CPU
  baseline unchanged. Callers must discard the entire sample without publishing
  it. The next complete sample spans from the last successful baseline, not the
  failed attempt.
- A nil error means both `CPUCores` and `MemoryBytes` are reportable, including
  genuine zero usage.
- Call `Reset()` at each run boundary; it discards the CPU baseline. The SDK
  resets automatically on successful launch; adapters using its registry do not
  manage the sampler.

`errors.Is(err, resources.ErrSampleUnavailable)` identifies an expected baseline
attempt, not a zero-usage measurement. Omit both resource series, including
previous values after a counter decrease, without logging a warning. Do not
present stale values as a new sample. Always check the error before updating
gauges. Other sampling errors log a warning and skip the update; last-good
values remain unchanged. They do not fail scrapes or terminate the workload.
The next sampling attempt can recover using the last successful baseline.

`Sample` and `Reset` are synchronized; do not copy a sampler after first use.
The sampler starts no workers and owns no Prometheus collectors. The SDK owns
its background sampling loop and serializes run identity, reset, and publication
against registry reset. Standalone native exporters own those responsibilities when
using the sampler directly; publish available values with the canonical names
and resource labels. SDK collection never adds resource families to a
`MetricsHandler` or `MetricsURL` response.

## Explicit adapter mapping examples

### Per-request events

Suppose a source emits `{name, outcome, duration_seconds, received_bytes,
active_users}` once per completed request. Define the entire mapping:

| Source → target | Type and unit formula | Labels |
| --- | --- | --- |
| `duration_seconds` → `shibuya_latency_label_milliseconds` via `ShibuyaMetric.Latency` | Event observation → classic histogram; `ms = seconds * 1000` | `name` → `label`; runtime supplies I |
| `outcome` → `shibuya_status_counter` via `Status` | One counter increment per event; explicitly map source outcomes to status strings | I + `label` + mapped `status` |
| `received_bytes` → `shibuya_egress_bytes_counter` via `EgressBytes` | Counter increment in bytes, no scaling | I + `label` |
| `active_users` → `shibuya_threads_gauge` via `Threads` | Gauge in concurrent users, no scaling | I |

The parser returns an error for unsupported or incomplete events rather than
inventing status, timing, or byte values. The registry owns cumulative totals,
the shared ordinary bucket profile, and run separation. Optional LLM events are
omitted unless the source measures them; an explicit `llm_resp` observation is in
ms and routes to the wide profile. CPU and memory are supplied separately by the
runtime's resource collector. Never feed a cumulative snapshot into this event
parser as if it were one request.

### Native seconds histogram

An adapter can expose a normalized endpoint for a source classic histogram
`request_duration_seconds` with bounds
`.001,.0025,.005,.01,.025,.05,.1,.25,.5,1,2.5,5,10,+Inf`:

| Source → target | Transformation | Labels |
| --- | --- | --- |
| `request_duration_seconds_bucket` → `shibuya_latency_label_milliseconds_bucket` | Cumulative bucket count unchanged; `le_ms = le_seconds * 1000`, preserving `+Inf` | Source operation → `label`; I from runtime environment; converted `le` |
| `request_duration_seconds_sum` → `shibuya_latency_label_milliseconds_sum` | `sum_ms = sum_seconds * 1000` | I + `label` |
| `request_duration_seconds_count` → `shibuya_latency_label_milliseconds_count` | Observation count unchanged | I + `label` |
| Source outcome counter → `shibuya_status_counter` | Preserve cumulative counts; explicitly map outcomes to status strings | I + `label` + `status` |
| Source received-byte counter → `shibuya_egress_bytes_counter` | Preserve cumulative bytes; do not substitute sent bytes | I + `label` |
| Active-user gauge → `shibuya_threads_gauge` | Preserve per-engine concurrency | I |
| CPU-time delta / wall-time delta → `shibuya_cpu_gauge` | Both deltas in seconds; result in CPU cores | I |
| Source memory gauge → `shibuya_mem_gauge` | Convert source units to bytes, e.g. KiB × 1024 | I |

The source/adapter owns accumulation and resets for each engine/run, and must
switch identity for a new run. A scrape with sum `0.25` seconds and count `1`
becomes sum `250` ms and count `1`; a second scrape without new events stays the
same. Omit unavailable optional LLM metrics. Required ordinary histogram bounds
must all exist after conversion; converting seconds does not manufacture missing
boundaries. Optional LLM histograms require their own wide profile. Missing
required reporting values prevent a claim of complete request reporting. If the
source lacks the histogram information, use per-request result events instead of
renaming coarse buckets, summaries, or percentiles.

## Implementation references

- [Agent options, validation, and ingestion](server.go)
- [Registry, in-process handler, and native reverse proxy](http.go)
- [Native label admission and snapshot folding](metrics_native.go)
- [Public metric event and producer definitions](metrics.go)
- [Producer histogram regression tests](metrics_producer_test.go)
- [Replay histogram implementation](https://github.com/reqfleet/replay/blob/main/internal/metrics/metrics.go)

The SDK Go API release, this metrics profile, and the coordinator wire protocol
are distinct contracts. The existing wire envelope/subscription is unversioned;
it has no protocol-version negotiation or unsupported-version rejection.
Control-plane catalog, PromQL, summary queries, and built-in adapter parsers remain
owned by Shibuya, not the public SDK.
