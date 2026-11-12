// Derived from reqfleet/shibuya; modified for engine-sdk.
package agent

import (
	"errors"
	"strings"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/reqfleet/engine-sdk/metricschema"
)

type ShibuyaMetric struct {
	Label        string
	Status       string
	Raw          string
	CollectionID string
	PlanID       string
	EngineID     string
	RunID        string
	Zone         string
	Threads      float64
	Latency      float64
	EgressBytes  float64
}

func (sm ShibuyaMetric) LabelLength() int {
	return len(sm.Label)
}

// LabelIsValid checks the request-label byte limit defined by metricschema.
func (sm ShibuyaMetric) LabelIsValid() (bool, error) {
	if sm.LabelLength() > metricschema.MaxRequestLabelBytes {
		return false, errors.New("exceed max label length 1024")
	}
	return true, nil
}

// MetricsRegistry owns the canonical reporting collectors for one agent.
// Construct it with NewMetricsRegistry; the zero value is not usable.
// Callers supply run identity and validate source samples and admit request
// labels before updating metrics. All methods are safe for concurrent use.
type MetricsRegistry struct {
	mu                           sync.RWMutex
	registry                     *prometheus.Registry
	resources                    resourceMetrics
	labelLatencyHistogram        *prometheus.HistogramVec
	labelLLMRespLatencyHistogram *prometheus.HistogramVec
	statusCounter                *prometheus.CounterVec
	threadsGauge                 *prometheus.GaugeVec
	cpuGauge                     *prometheus.GaugeVec
	memGauge                     *prometheus.GaugeVec
	egressCounter                *prometheus.CounterVec
}

// NewMetricsRegistry creates independent canonical collectors without starting
// background work or registering collectors in the default Prometheus registry.
func NewMetricsRegistry() *MetricsRegistry {
	r := &MetricsRegistry{
		registry:  prometheus.NewRegistry(),
		resources: newResourceMetrics(),
		labelLatencyHistogram: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    metricschema.RequestLatency,
			Buckets: metricschema.RequestLatencyBuckets(),
		}, metricschema.LatencyLabels()),
		labelLLMRespLatencyHistogram: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    metricschema.LLMResponseLatency,
			Buckets: metricschema.LLMResponseLatencyBuckets(),
		}, metricschema.LatencyLabels()),
		statusCounter: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: metricschema.StatusCounter,
			Help: "stores count of responses and groups in buckets of response codes",
		}, metricschema.StatusLabels()),
		egressCounter: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: metricschema.EgressBytesCounter,
			Help: "total egress bytes used by the engine",
		}, metricschema.EgressLabels()),
		threadsGauge: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: metricschema.ThreadsGauge,
			Help: "Current number of threads running in JMeter",
		}, metricschema.ThreadsLabels()),
		cpuGauge: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: metricschema.CPUGauge,
			Help: "CPU used by engine",
		}, metricschema.ResourceLabels()),
		memGauge: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: metricschema.MemoryGauge,
			Help: "Memory used by engine",
		}, metricschema.ResourceLabels()),
	}
	r.registry.MustRegister(
		r.labelLatencyHistogram,
		r.labelLLMRespLatencyHistogram,
		r.statusCounter,
		r.threadsGauge,
		r.cpuGauge,
		r.memGauge,
		r.egressCounter,
	)
	return r
}

// ObserveRequest records only the request's latency and status. Every request
// uses the ordinary latency histogram, even when its label is "llm_resp".
func (r *MetricsRegistry) ObserveRequest(metric ShibuyaMetric) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	r.observeRequest(metric, r.labelLatencyHistogram)
}

// AddReceivedBytes adds only the received bytes in metric.EgressBytes.
func (r *MetricsRegistry) AddReceivedBytes(metric ShibuyaMetric) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	r.addReceivedBytes(metric)
}

// SetConcurrency sets only the current concurrency in metric.Threads.
func (r *MetricsRegistry) SetConcurrency(metric ShibuyaMetric) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	r.setConcurrency(metric)
}

// RecordEvent records latency, status, received bytes, and concurrency together
// with respect to Reset. Unlike ObserveRequest, it preserves parser event routing:
// labels equal to "llm_resp" (case-insensitive) use the LLM latency histogram.
// It does not make concurrent scrapes atomic with updates.
func (r *MetricsRegistry) RecordEvent(metric ShibuyaMetric) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	r.observeRequest(metric, r.latencyHistogramForLabel(metric.Label))
	r.addReceivedBytes(metric)
	r.setConcurrency(metric)
}

func (r *MetricsRegistry) observeRequest(sm ShibuyaMetric, histogram *prometheus.HistogramVec) {
	histogram.WithLabelValues(sm.CollectionID, sm.Label, sm.RunID, sm.EngineID, sm.PlanID, sm.Zone).Observe(sm.Latency)
	r.statusCounter.WithLabelValues(sm.CollectionID, sm.PlanID, sm.RunID, sm.EngineID, sm.Label, sm.Zone, sm.Status).Inc()
}

func (r *MetricsRegistry) addReceivedBytes(sm ShibuyaMetric) {
	r.egressCounter.WithLabelValues(sm.CollectionID, sm.PlanID, sm.RunID, sm.EngineID, sm.Label, sm.Zone).Add(sm.EgressBytes)
}

func (r *MetricsRegistry) setConcurrency(sm ShibuyaMetric) {
	r.threadsGauge.WithLabelValues(sm.CollectionID, sm.PlanID, sm.RunID, sm.EngineID, sm.Zone).Set(sm.Threads)
}

func (r *MetricsRegistry) latencyHistogramForLabel(label string) *prometheus.HistogramVec {
	if strings.EqualFold(label, metricschema.LLMResponseLabel) {
		return r.labelLLMRespLatencyHistogram
	}
	return r.labelLatencyHistogram
}

// Reset clears every metric series and the resource run identity and baseline.
func (r *MetricsRegistry) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.labelLatencyHistogram.Reset()
	r.labelLLMRespLatencyHistogram.Reset()
	r.statusCounter.Reset()
	r.threadsGauge.Reset()
	r.cpuGauge.Reset()
	r.memGauge.Reset()
	r.egressCounter.Reset()
	r.resources.reset()
}

// Gather implements prometheus.Gatherer without sampling resource usage.
func (r *MetricsRegistry) Gather() ([]*dto.MetricFamily, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.registry.Gather()
}
