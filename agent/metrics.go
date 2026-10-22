// Derived from reqfleet/shibuya; modified for engine-sdk.
package agent

import (
	"errors"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
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

func convertToMilliBuckets(buckets []float64) []float64 {
	milliBuckets := make([]float64, len(buckets))
	for i, b := range buckets {
		milliBuckets[i] = b * 1000
	}
	return milliBuckets
}

func latencyHistogramForLabel(label string) *prometheus.HistogramVec {
	if strings.EqualFold(label, metricschema.LLMResponseLabel) {
		return labelLLMRespLatencyHistogram
	}
	return labelLatencyHistogram
}

func (sm ShibuyaMetric) ToPrometheus() {
	latencyHistogramForLabel(sm.Label).WithLabelValues(sm.CollectionID, sm.Label, sm.RunID, sm.EngineID, sm.PlanID, sm.Zone).Observe(sm.Latency)
	statusCounter.WithLabelValues(sm.CollectionID, sm.PlanID, sm.RunID, sm.EngineID, sm.Label, sm.Zone, sm.Status).Inc()
	threadsGauge.WithLabelValues(sm.CollectionID, sm.PlanID, sm.RunID, sm.EngineID, sm.Zone).Set(sm.Threads)
	egressCounter.WithLabelValues(sm.CollectionID, sm.PlanID, sm.RunID, sm.EngineID, sm.Label, sm.Zone).Add(sm.EgressBytes)
}

func (sm ShibuyaMetric) LabelLength() int {
	return len(sm.Label)
}

// Cortex has the limit for metric label values for 2048
// We set the limit to half of it. 1024 is already pretty long for any valid label(test) names
func (sm ShibuyaMetric) LabelIsValid() (bool, error) {
	if sm.LabelLength() >= 1024 {
		return false, errors.New("exceed max label length 1024")
	}
	return true, nil
}

var (
	shibuyaBuckets        = append([]float64{1, 2.5}, convertToMilliBuckets(prometheus.DefBuckets)...)
	shibuyaLLMRespBuckets = []float64{100, 250, 500, 1000, 2500, 5000, 7500, 10000, 15000, 20000, 30000, 45000, 60000, 90000, 120000, 180000, 300000}
	labelLatencyHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    metricschema.RequestLatency,
		Buckets: shibuyaBuckets,
	}, metricschema.LatencyLabels())
	labelLLMRespLatencyHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    metricschema.LLMResponseLatency,
		Buckets: shibuyaLLMRespBuckets,
	}, metricschema.LatencyLabels())

	// This is similar to Latency but cannot use histogram here because we need a very accurate count of every status error that occurred.
	// So 200s are different bucket than 201s responses.
	statusCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: metricschema.StatusCounter,
		Help: "stores count of responses and groups in buckets of response codes",
	}, metricschema.StatusLabels())

	egressCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: metricschema.EgressBytesCounter,
		Help: "total egress bytes used by the engine",
	}, metricschema.EgressLabels())

	// Gauge is the most intuitive way to count threads here.
	// We don't care about accuracy and there's no use of rate of threads
	threadsGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: metricschema.ThreadsGauge,
		Help: "Current number of threads running in JMeter",
	}, metricschema.ThreadsLabels())

	cpuGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: metricschema.CPUGauge,
		Help: "CPU used by engine",
	}, metricschema.ResourceLabels())

	memGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: metricschema.MemoryGauge,
		Help: "Memory used by engine",
	}, metricschema.ResourceLabels())
)
