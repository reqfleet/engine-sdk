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
// Use MetricsRecorder to record adapter samples. The registry exposes gathering
// and collector reset, not public recording methods. All methods are concurrency-safe.
type MetricsRegistry struct {
	mu                           sync.RWMutex
	registry                     *prometheus.Registry
	resources                    resourceMetrics
	collectionErr                error
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

// Recording helpers require the caller to hold mu so a sample's updates cannot
// be split by Reset. Prometheus synchronizes individual collector updates.

func (r *MetricsRegistry) observeLatency(sm ShibuyaMetric, histogram *prometheus.HistogramVec) {
	histogram.WithLabelValues(sm.CollectionID, sm.Label, sm.RunID, sm.EngineID, sm.PlanID, sm.Zone).Observe(sm.Latency)
}

func (r *MetricsRegistry) recordStatus(sm ShibuyaMetric) {
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

// Reset clears every metric series, collection failure, and resource baseline.
// It does not change recorder identity or admission; use MetricsRecorder.Reset
// for a recorder run transition, after quiescing previous-run ingestion.
func (r *MetricsRegistry) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reset()
}

// reset requires the exclusive registry lock so recorder state can transition
// with collectors without exposing a partially reset snapshot.
func (r *MetricsRegistry) reset() {
	r.labelLatencyHistogram.Reset()
	r.labelLLMRespLatencyHistogram.Reset()
	r.statusCounter.Reset()
	r.threadsGauge.Reset()
	r.cpuGauge.Reset()
	r.memGauge.Reset()
	r.egressCounter.Reset()
	r.resources.reset()
	r.collectionErr = nil
}

// Gather implements prometheus.Gatherer without collecting new samples. A
// CustomIngestMetrics failure is returned until Reset or the next successful launch.
func (r *MetricsRegistry) Gather() ([]*dto.MetricFamily, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.collectionErr != nil {
		return nil, r.collectionErr
	}
	return r.registry.Gather()
}

func (r *MetricsRegistry) setCollectionError(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.collectionErr = err
}
