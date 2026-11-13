package agent

import (
	"errors"
	"sync"

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

// MetricsRecorder owns assigned identity and request-name admission for one
// agent/run. Construct it with NewMetricsRecorder; do not copy it. Recording is
// safe for concurrent use, but multi-family updates are not transactional with
// respect to HTTP scrapes.
//
// Call Reset with the assigned RunContext before first ingestion and each new
// run. Source identity is always ignored. Completed-run identity, admission, and
// cumulative metrics remain available until that explicit transition.
type MetricsRecorder struct {
	mu       sync.Mutex
	registry *MetricsRegistry
	admitted map[string]struct{}
	limit    int
	identity metricIdentity
}

// NewMetricsRecorder wraps a registry with a fixed, resolved nonnegative request
// name budget. Pass the same registry to AgentServerOptions.MetricsRegistry.
// It does not read environment variables, reset the registry, or start workers.
// Use one recorder per registry; call Reset before recording the first run.
func NewMetricsRecorder(registry *MetricsRegistry, labelLimit int) (*MetricsRecorder, error) {
	if registry == nil {
		return nil, errors.New("metrics recorder requires a registry")
	}
	if labelLimit < 0 {
		return nil, errors.New("metrics label limit must be nonnegative")
	}
	return &MetricsRecorder{
		registry: registry,
		admitted: make(map[string]struct{}),
		limit:    labelLimit,
	}, nil
}

// Reset clears the previous run and binds the next assigned identity in one
// transition synchronized with recording, resource sampling, and gathering.
// It clears all collectors, request-name admission, collection errors, and the
// resource baseline, without changing the configured label budget.
//
// Callers choose the transition time, after final scrapes and after quiescing
// prior-run ingestion and callbacks. Reset does not cancel work: a late sample
// would be attributed to the newly bound run. It copies only identity strings
// from run, without validating them or retaining settings or artifacts.
func (r *MetricsRecorder) Reset(run RunContext) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.registry.mu.Lock()
	defer r.registry.mu.Unlock()

	r.registry.reset()
	clear(r.admitted)
	r.identity = identityFromRun(run)
	r.registry.resources.identity = r.identity
}

// RecordSample validates the original request label, admits it once, and records
// only the selected measurements. An invalid label changes neither admission nor
// any measurements, including concurrency carried by this sample. Latency uses
// the ordinary histogram, even for llm_resp. A concurrency-only sample consumes
// no request-name slot. Use SetConcurrency for independent concurrency instead.
func (r *MetricsRecorder) RecordSample(sample RequestSample) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return recordRequestSample(r.registry, r.admitted, r.limit, &r.identity, sample, false)
}

// ObserveLatency records only metric.Latency, in milliseconds, after request-name
// validation and admission. It uses the ordinary histogram even for llm_resp.
func (r *MetricsRecorder) ObserveLatency(metric ShibuyaMetric) error {
	return r.RecordSample(RequestSample{Metric: metric, HasLatency: true})
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
	r.mu.Lock()
	defer r.mu.Unlock()
	return recordRequestSample(r.registry, r.admitted, r.limit, &r.identity, completeRequestSample(metric), true)
}

// AddAggregateReceivedBytes records independently measured bytes under the source's
// aggregate label unchanged, without request-name admission or invented requests.
// The label must satisfy LabelIsValid. Use AddReceivedBytes for bytes attributed
// to a request. Keep aggregate labels bounded at the source.
func (r *MetricsRecorder) AddAggregateReceivedBytes(metric ShibuyaMetric) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.registry.mu.RLock()
	defer r.registry.mu.RUnlock()
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
	r.mu.Lock()
	defer r.mu.Unlock()
	r.registry.mu.RLock()
	defer r.registry.mu.RUnlock()
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

// recordRequestSample is the shared result-file/recorder source-sample boundary.
// The caller serializes access to admitted and its lifetime. Hold the registry
// lock across validation, admission, and all updates so Reset cannot split them.
// Native folding remains a whole-snapshot operation, not an event producer.
func recordRequestSample(
	registry *MetricsRegistry,
	admitted map[string]struct{},
	limit int,
	identity *metricIdentity,
	sample RequestSample,
	eventLatency bool,
) error {
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	metric := sample.Metric
	identity.bind(&metric)
	if _, err := metric.LabelIsValid(); err != nil {
		return err
	}
	if sample.HasLatency || sample.HasStatus || sample.HasReceivedBytes {
		metric.Label = metricschema.AdmitRequestName(metric.Label, admitted, limit, "")
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
