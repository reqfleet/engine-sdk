package agent

import (
	"errors"
	"math"

	"github.com/reqfleet/engine-sdk/metricschema"
)

// RequestSample contains the measurements present in one normalized source request
// sample. Metric supplies the request name and values; its identity is ignored.
// Received bytes here must be attributable to this request.
// Engine-specific normalization and numeric validation remain caller-owned.
// Each Has field selects a measurement, including an explicitly measured zero;
// unselected values are ignored.
type RequestSample struct {
	Metric           ShibuyaMetric
	HasLatency       bool
	HasStatus        bool
	HasReceivedBytes bool
	HasConcurrency   bool
}

// MetricsRecorder owns immutable assigned identity, collectors, and request-name
// admission for one run. The SDK supplies it through RunContext.Metrics after launch.
// Recording is safe for concurrent use, but multi-family updates are not
// transactional with respect to HTTP scrapes. Do not copy a recorder.
//
// A new run gets a new recorder. Source identity is always ignored; retaining an
// old recorder can never attribute its samples to a subsequent run.
type MetricsRecorder struct {
	registry     *MetricsRegistry
	requestNames *RequestsNamesStore
	identity     metricIdentity
}

// NewMetricsRecorder creates independent metrics bound to run's identity and a
// fixed nonnegative request-name budget. It copies only identity strings, not
// settings, artifacts, or another recorder. It starts no workers and reads no
// environment variables. Production adapters use the SDK-supplied RunContext.Metrics;
// this constructor supports standalone recording and adapter tests.
func NewMetricsRecorder(run RunContext, requestNamesLimit int) (*MetricsRecorder, error) {
	names, err := NewRequestsNamesStore(requestNamesLimit)
	if err != nil {
		return nil, err
	}
	return newMetricsRecorder(run, names), nil
}

func newMetricsRecorder(run RunContext, names *RequestsNamesStore) *MetricsRecorder {
	identity := identityFromRun(run)
	return &MetricsRecorder{
		registry:     newMetricsRegistry(identity),
		requestNames: names,
		identity:     identity,
	}
}

// Registry returns this run's collectors for gathering. The registry remains
// bound to the same run for its lifetime and cannot be reset or rebound.
func (r *MetricsRecorder) Registry() *MetricsRegistry {
	return r.registry
}

// RecordSample validates the original request label, admits it once, and records
// only the selected measurements. An invalid label changes neither admission nor
// any measurements, including concurrency carried by this sample. Latency uses
// the ordinary histogram, even for llm_resp. A concurrency-only sample consumes
// no request-name slot. Use SetConcurrency for independent concurrency instead.
func (r *MetricsRecorder) RecordSample(sample RequestSample) error {
	return r.recordSample(sample, false)
}

// ObserveLatency records only metric.Latency, in milliseconds, after request-name
// validation and admission. It uses the ordinary histogram even for llm_resp.
func (r *MetricsRecorder) ObserveLatency(metric ShibuyaMetric) error {
	return r.RecordSample(RequestSample{Metric: metric, HasLatency: true})
}

// ObserveLLMResponse records only an independent LLM response duration in
// milliseconds, including measured zero. Nonfinite or negative values return an
// error without changing admission or metrics. The reserved llm_resp name shares
// request-name admission; overflow remains in the wide LLM response histogram.
// Identity is fixed at construction. No status, bytes, concurrency, or ordinary latency
// measurements are recorded.
func (r *MetricsRecorder) ObserveLLMResponse(latencyMS float64) error {
	if latencyMS < 0 || math.IsNaN(latencyMS) || math.IsInf(latencyMS, 0) {
		return errors.New("LLM response latency must be finite and nonnegative")
	}

	metric := ShibuyaMetric{
		Label:   r.requestNames.Admit(metricschema.LLMResponseLabel),
		Latency: latencyMS,
	}
	r.identity.bind(&metric)
	r.registry.observeLatency(metric, r.registry.labelLLMRespLatencyHistogram)
	return nil
}

// ObserveTTFT records only the measured time to the first generated token, in
// milliseconds, under ttft in the ordinary histogram. Nonfinite or negative
// values return an error before admission; measured zero is valid.
// Identity and request-name admission are shared with all other observations.
func (r *MetricsRecorder) ObserveTTFT(latencyMS float64) error {
	if latencyMS < 0 || math.IsNaN(latencyMS) || math.IsInf(latencyMS, 0) {
		return errors.New("TTFT latency must be finite and nonnegative")
	}
	return r.ObserveLatency(ShibuyaMetric{Label: metricschema.TTFTLabel, Latency: latencyMS})
}

// ObserveInterTokenLatency records only a measured gap between successive
// generated tokens in the same generation, in milliseconds, under tps in the
// ordinary histogram. The first token has no gap. Nonfinite or negative values
// return an error before admission; measured zero is valid. It does not count a
// token. Identity and request-name admission are shared with other observations.
func (r *MetricsRecorder) ObserveInterTokenLatency(latencyMS float64) error {
	if latencyMS < 0 || math.IsNaN(latencyMS) || math.IsInf(latencyMS, 0) {
		return errors.New("inter-token latency must be finite and nonnegative")
	}
	return r.ObserveLatency(ShibuyaMetric{Label: metricschema.TPSLabel, Latency: latencyMS})
}

// RecordReceivedToken counts exactly one genuinely received generated token,
// including the first, under tps with status 200. This denotes successful token
// receipt, not an HTTP response. It records no timing, bytes, or concurrency.
// Identity and request-name admission are shared with all other observations.
func (r *MetricsRecorder) RecordReceivedToken() error {
	return r.RecordStatus(ShibuyaMetric{Label: metricschema.TPSLabel, Status: "200"})
}

// RecordStatus increments only metric.Status's counter after request-name
// validation and admission; it does not fabricate a latency observation.
func (r *MetricsRecorder) RecordStatus(metric ShibuyaMetric) error {
	return r.RecordSample(RequestSample{Metric: metric, HasStatus: true})
}

// ObserveRequest records latency and status together after one request-name
// validation and admission decision. Latency uses the ordinary histogram.
func (r *MetricsRecorder) ObserveRequest(metric ShibuyaMetric) error {
	return r.RecordSample(RequestSample{Metric: metric, HasLatency: true, HasStatus: true})
}

// AddReceivedBytes adds only metric.EgressBytes attributed to this request, after
// request-name validation and admission. Use AddAggregateReceivedBytes for
// independently reported bytes that must retain their aggregate attribution.
func (r *MetricsRecorder) AddReceivedBytes(metric ShibuyaMetric) error {
	return r.RecordSample(RequestSample{Metric: metric, HasReceivedBytes: true})
}

// RecordEvent records a complete event using the same policy as result-file
// ingestion, including case-insensitive llm_resp routing after name admission.
// Use RecordSample when any of latency, status, bytes, or concurrency is absent.
func (r *MetricsRecorder) RecordEvent(metric ShibuyaMetric) error {
	return r.recordSample(completeRequestSample(metric), true)
}

// AddAggregateReceivedBytes records independently measured bytes under the source's
// aggregate label unchanged, without request-name admission or invented requests.
// The label must satisfy LabelIsValid. Use AddReceivedBytes for bytes attributed
// to a request. Keep aggregate labels bounded at the source.
func (r *MetricsRecorder) AddAggregateReceivedBytes(metric ShibuyaMetric) error {
	if _, err := metric.LabelIsValid(); err != nil {
		return err
	}
	r.identity.bind(&metric)
	r.registry.addReceivedBytes(metric)
	return nil
}

// SetConcurrency records an independent concurrency measurement. Metric.Label is
// ignored: concurrency has no request-name dimension and consumes no budget.
func (r *MetricsRecorder) SetConcurrency(metric ShibuyaMetric) {
	r.identity.bind(&metric)
	r.registry.setConcurrency(metric)
}

func completeRequestSample(metric ShibuyaMetric) RequestSample {
	return RequestSample{
		Metric:           metric,
		HasLatency:       true,
		HasStatus:        true,
		HasReceivedBytes: true,
		HasConcurrency:   true,
	}
}

// recordSample is the shared result-file/recorder source-sample boundary.
// Identity and collectors never change; only name admission needs a lock.
// Native folding remains a whole-snapshot operation, not an event producer.
func (r *MetricsRecorder) recordSample(sample RequestSample, eventLatency bool) error {
	registry := r.registry
	metric := sample.Metric
	r.identity.bind(&metric)
	if _, err := metric.LabelIsValid(); err != nil {
		return err
	}
	if sample.HasLatency || sample.HasStatus || sample.HasReceivedBytes {
		metric.Label = r.requestNames.Admit(metric.Label)
	}
	if sample.HasLatency {
		histogram := registry.labelLatencyHistogram
		if eventLatency {
			histogram = registry.latencyHistogramForLabel(metric.Label)
		}
		registry.observeLatency(metric, histogram)
	}
	if sample.HasStatus {
		registry.recordStatus(metric)
	}
	if sample.HasReceivedBytes {
		registry.addReceivedBytes(metric)
	}
	if sample.HasConcurrency {
		registry.setConcurrency(metric)
	}
	return nil
}
