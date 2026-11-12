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

// LabelIsValid checks the request-label byte limit defined by metricschema.
func (sm ShibuyaMetric) LabelIsValid() (bool, error) {
	if sm.LabelLength() > metricschema.MaxRequestLabelBytes {
		return false, errors.New("exceed max label length 1024")
	}
	return true, nil
}

var (
	labelLatencyHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    metricschema.RequestLatency,
		Buckets: metricschema.RequestLatencyBuckets(),
	}, metricschema.LatencyLabels())
	labelLLMRespLatencyHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    metricschema.LLMResponseLatency,
		Buckets: metricschema.LLMResponseLatencyBuckets(),
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
