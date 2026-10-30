# Shibuya metrics profile: `shibuya-v1`

This document defines the `shibuya-v1` metrics reporting contract for engine
adapters using `github.com/reqfleet/engine-sdk/agent`: collection modes, metric names, labels, units, and
histogram boundaries. Adapters declare this profile in their configuration.

The collector-free [`metricschema`](../metricschema) package owns the Go constants
for metric names and reserved request labels, plus ordered label-list functions.
The runtime and reporting clients use these shared definitions. Label lists are
caller-owned; collectors, histogram bounds, and event recording remain in `agent`.

## Integration modes

| Mode | `AgentServerOptions` | Ownership |
| --- | --- | --- |
| Result-file events | Nonempty `ResultFile`, non-nil `MetricParser`; leave `MetricsURL` and `NativeMetricsSchema` empty | The parser maps each result row to `agent.ShibuyaMetric`; the runtime ingests events into its registry. |
| Normalized native endpoint | An HTTP(S) `MetricsURL` with a host, `NativeMetricsSchema: agent.MetricsSchemaShibuyaV1`, nil `MetricParser` | The adapter produces the complete normalized Prometheus exposition; the runtime enforces request-label admission and aggregates folded series. |
| Registry only | All metrics options zero-valued | Exposes the runtime registry without request-event ingestion. This does **not** imply complete request reporting. |

In native mode, `ResultFile` may name ordinary workload output. It is never tailed
for metrics. In registry mode, `ResultFile` and `MetricParser` must be paired.
Setting `MetricsURL` selects native mode; a native endpoint cannot also use a parser.

`Run` rejects invalid options before starting background work. Direct use of the
metrics HTTP handler returns 503 for the same invalid configuration. Validation
checks the numeric budget and selected metrics source, schema, and URL. Errors
identify the remedy:

- `metrics label limit must be nonnegative`
- `result-file metrics require both ResultFile and MetricParser`
- `native metrics schema requires MetricsURL`
- `native metrics cannot also configure MetricParser`
- `unsupported native metrics schema %q; require shibuya-v1 from an adapter-normalized endpoint`
- `native metrics overflow label must be others or _other_`
- `native metrics URL must use http or https and include a host`

Choose an integration based on both how the engine exposes metrics and the schema
it produces. For example, an engine without native Prometheus export, such as
JMeter writing JTL results, needs a `ResultFile` and a `MetricParser` that converts
each result row into `agent.ShibuyaMetric`. The runtime tails the file and
accumulates those events into the Prometheus metrics required by the control plane.

If an engine exposes Prometheus metrics with a different schema, provide an
adapter-owned endpoint that normalizes them to `shibuya-v1`, or use per-request
result events with a `MetricParser`. The schema declaration is the producer's
promise that its endpoint follows this contract. The runtime checks what is
needed for safe label folding; it is not a complete profile-conformance validator.

When `MetricsURL` is set, each scrape of the agent's `/metrics` endpoint is
forwarded to that URL. The engine or adapter supplies the identity labels, metric
names, units, histogram buckets, and resource metrics required by this contract.
The SDK folds excess request labels and returns re-encoded Prometheus text.
Metrics-gateway admission by metric name does not establish semantic conformance.

Native producers must serve Prometheus text exposition (`text/plain; version=0.0.4`).
The SDK requests that format with `Accept-Encoding: identity` and bounds each
successful upstream snapshot to **16 MiB**. Compressed responses, malformed
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
histogram components, and numeric aggregation overflow are errors. These checks
do not replace the producer's responsibility for the profile's exact bounds and
units. Additional request-labelled counter/histogram families, such as Replay's
optional schedule-lateness histogram, share the same admission mapping.
Unlabelled resource families are not folded.

## Identity and metric families

Every series carries identity labels
`I = {collection_id, plan_id, run_id, engine_no, zone}`.

Result parsers supply request values. The runtime overwrites `ShibuyaMetric`'s
collection, plan, run, engine, and zone with the assigned run's identity before
recording the event. Native adapters must supply these labels themselves, using
the runtime's same-named workload environment variables. Parser identity injection
and native proxying are not interchangeable.

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

Request labels must be shorter than 1024 bytes in both ingestion modes. Parser
errors and invalid labels drop the whole result-file event; invalid native
request labels reject the scrape.

Use lowercase reserved labels `ttft` and `llm_resp` for optional LLM timing.
The runtime routes `llm_resp` events into the separate wide histogram (its routing
comparison is case-insensitive, but reporting selectors use the lowercase label).
`ttft` uses the ordinary histogram. Omit unavailable optional LLM observations;
do not emit a fabricated zero timing. Status and byte counters still reflect the
events the adapter records; synthetic timing events must not be assumed to be
additional HTTP requests without understanding that adapter's event model.

## Request-label budget

The control plane supplies a nonnegative numeric `LabelsUsageLimit` to each
agent. The SDK enforces it without interpreting tiers: a budget of **N permits
at most N named request labels plus one overflow label per agent/run**. Zero
permits only the overflow label; negative budgets are invalid configuration.
This is not a global plan-wide budget across agents. It does not cap producer
memory, source-side series, metric-family names, statuses, or other Prometheus
dimensions. A successful exact preflight count describes the analyzed artifact,
not a guarantee of the labels that runtime code will emit.

Result-file ingestion admits names in event order and folds excess names into
`others`. Native ingestion sorts newly encountered names within each successful
scrape before filling remaining slots. Once admitted, a name retains its slot
for that run, even if it disappears from later scrapes. Rotating names cannot
reclaim slots. All native request-labelled families use the same mapping, and a
rejected scrape consumes no admission slots.

Native `AgentServerOptions.NativeMetricsOverflowLabel` defaults to `others`;
Replay sets it to `_other_`. These are the only supported values. Only the
configured overflow name is exempt from the named-label budget; the other
spelling is an ordinary name. Collisions with an existing overflow series are
aggregated, not emitted as duplicate series.

Native folding sums the **current cumulative snapshot** of counters and
histograms for series that become identical after replacing `label`, preserving
all other label dimensions, including status groups. It sums bytes, histogram
counts, sums, and corresponding cumulative buckets; it never adds a previous
scrape's values again. The SDK retains admitted names, not a history of
overflow-source names or values.


## Histogram compatibility

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
late result-file events after run completion. Native admission state survives
completion so the final upstream snapshot uses the same mapping, and resets on
the next successful workload launch. A final native scrape still requires a
reachable producer serving the completed run's cumulative snapshot: the SDK does
not retain a cached metrics response. Missing final scrapes or undrained result
rows can affect reported totals.

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

In addition to the engine's request metrics, `github.com/reqfleet/engine-sdk/agent` automatically collects
runtime CPU and memory usage. In result-file mode, it exposes these as
`shibuya_cpu_gauge` (CPU cores) and `shibuya_mem_gauge` (bytes) alongside the parsed
request metrics. Native endpoints provide these resource metrics in their own
Prometheus exposition.

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
- [Registry and native reverse proxy](http.go)
- [Native label admission and snapshot folding](metrics_native.go)
- [Public metric event and producer definitions](metrics.go)
- [Producer histogram regression tests](metrics_producer_test.go)
- [Replay histogram implementation](https://github.com/reqfleet/replay/blob/main/internal/metrics/metrics.go)

The SDK Go API release, this metrics profile, and the coordinator wire protocol
are distinct contracts. The existing wire envelope/subscription is unversioned;
it has no protocol-version negotiation or unsupported-version rejection.
Control-plane catalog, PromQL, summary queries, and built-in adapter parsers remain
owned by Shibuya, not the public SDK.
