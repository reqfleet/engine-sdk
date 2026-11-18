// Derived from reqfleet/shibuya; modified for engine-sdk.
package agent

import (
	"fmt"
	"strings"
	"sync/atomic"

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
		return false, fmt.Errorf("request label exceeds maximum length of %d bytes", metricschema.MaxRequestLabelBytes)
	}
	return true, nil
}

// MetricsRegistry owns the canonical reporting collectors for one run.
// Obtain it from MetricsRecorder.Registry; the zero value is not usable.
// Collectors are never reset or rebound. All methods are concurrency-safe.
type MetricsRegistry struct {
	registry                     *prometheus.Registry
	resources                    resourceMetrics
	collectionErr                atomic.Pointer[metricsCollectionFailure]
	labelLatencyHistogram        *prometheus.HistogramVec
	labelLLMRespLatencyHistogram *prometheus.HistogramVec
	statusCounter                *prometheus.CounterVec
	threadsGauge                 *prometheus.GaugeVec
	cpuGauge                     *prometheus.GaugeVec
	memGauge                     *prometheus.GaugeVec
	egressCounter                *prometheus.CounterVec
}

// newMetricsRegistry creates collectors and a fresh sampler bound to one run.
func newMetricsRegistry(identity metricIdentity) *MetricsRegistry {
	r := &MetricsRegistry{
		registry:  prometheus.NewRegistry(),
		resources: newResourceMetrics(identity),
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

// Prometheus synchronizes individual collector updates. A run's collectors and
// identity are never replaced or reset, so recording needs no registry lock.

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

type metricsCollectionFailure struct {
	err error
}

// Gather implements prometheus.Gatherer without collecting new samples. A
// CustomIngestMetrics failure remains attached to this run's registry.
func (r *MetricsRegistry) Gather() ([]*dto.MetricFamily, error) {
	if failure := r.collectionErr.Load(); failure != nil {
		return nil, failure.err
	}
	return r.registry.Gather()
}

func (r *MetricsRegistry) setCollectionError(err error) {
	r.collectionErr.Store(&metricsCollectionFailure{err: err})
}
