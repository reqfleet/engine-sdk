# Shibuya metrics profile: `shibuya-v1`

This document defines the `shibuya-v1` metrics reporting contract for engine
adapters using `github.com/reqfleet/engine-sdk/agent`: collection modes, metric names, labels, units, and
histogram boundaries. Adapters declare this profile in their configuration.

## Integration modes

| Mode | `AgentServerOptions` | Ownership |
| --- | --- | --- |
| Result-file events | Nonempty `ResultFile`, non-nil `MetricParser`; leave `MetricsURL` and `NativeMetricsSchema` empty | The parser maps each result row to `agent.ShibuyaMetric`; the runtime ingests events into its registry. |
| Normalized native endpoint | An HTTP(S) `MetricsURL` with a host, `NativeMetricsSchema: agent.MetricsSchemaShibuyaV1`, nil `MetricParser` | The adapter produces the complete normalized Prometheus exposition; the runtime proxies it. |
| Registry only | All metrics options zero-valued | Exposes the runtime registry without request-event ingestion. This does **not** imply complete request reporting. |

In native mode, `ResultFile` may name ordinary workload output. It is never tailed
for metrics. In registry mode, `ResultFile` and `MetricParser` must be paired.
Setting `MetricsURL` selects native mode; a native endpoint cannot also use a parser.

`Run` rejects invalid options before starting background work. Direct use of the
metrics HTTP handler returns 503 for the same invalid configuration. Validation
checks source conflicts, then schema, then URL. Errors identify the remedy:

- `result-file metrics require both ResultFile and MetricParser`
- `native metrics schema requires MetricsURL`
- `native metrics cannot also configure MetricParser`
- `unsupported native metrics schema %q; require shibuya-v1 from an adapter-normalized endpoint`
- `native metrics URL must use http or https and include a host`

Choose an integration based on both how the engine exposes metrics and the schema
it produces. For example, an engine without native Prometheus export, such as
JMeter writing JTL results, needs a `ResultFile` and a `MetricParser` that converts
each result row into `agent.ShibuyaMetric`. The runtime tails the file and
accumulates those events into the Prometheus metrics required by the control plane.

If an engine exposes Prometheus metrics with a different schema, provide an
adapter-owned endpoint that normalizes them to `shibuya-v1`, or use per-request
result events with a `MetricParser`. The schema declaration is the producer's
promise that its endpoint follows this contract; the runtime does not validate
every scraped sample.

When `MetricsURL` is set, each scrape of the agent's `/metrics` endpoint is
forwarded to that URL. The agent returns the endpoint's Prometheus exposition
unchanged. The engine or adapter supplies the identity labels, metric names,
units, histogram buckets, and resource metrics required by this contract.
Upstream errors are returned as errors rather than replaced with an empty
registry response. Metrics-gateway admission by metric name does not establish
semantic conformance.

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

Request labels must be shorter than 1024 bytes in the result-file path. Parser
errors and invalid request labels drop the whole event. Runtime cardinality
limits fold excess distinct request labels into `others`; Replay uses `_other_`.
The proxy preserves each producer's label convention.

Use lowercase reserved labels `ttft` and `llm_resp` for optional LLM timing.
The runtime routes `llm_resp` events into the separate wide histogram (its routing
comparison is case-insensitive, but reporting selectors use the lowercase label).
`ttft` uses the ordinary histogram. Omit unavailable optional LLM observations;
do not emit a fabricated zero timing. Status and byte counters still reflect the
events the adapter records; synthetic timing events must not be assumed to be
additional HTTP requests without understanding that adapter's event model.

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
maintain cumulative values throughout the run so final summary queries can read
the totals directly. Registry mode retains old-run series for the agent process
lifetime; a native endpoint's lifetime follows its engine. Missing final scrapes
or undrained result rows can affect reported totals.

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
- [Public metric event and producer definitions](metrics.go)
- [Producer histogram regression tests](metrics_producer_test.go)
- [Replay histogram implementation](https://github.com/reqfleet/replay/blob/main/internal/metrics/metrics.go)

The SDK Go API release, this metrics profile, and the coordinator wire protocol
are distinct contracts. The existing wire envelope/subscription is unversioned;
it has no protocol-version negotiation or unsupported-version rejection.
Control-plane catalog, PromQL, summary queries, and built-in adapter parsers remain
owned by Shibuya, not the public SDK.
