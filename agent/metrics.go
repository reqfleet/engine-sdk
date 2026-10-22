// Derived from reqfleet/shibuya; modified for engine-sdk.
package agent

import (
	"errors"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
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

const llmRespLabel = "llm_resp"

func convertToMilliBuckets(buckets []float64) []float64 {
	milliBuckets := make([]float64, len(buckets))
	for i, b := range buckets {
		milliBuckets[i] = b * 1000
	}
	return milliBuckets
}

func latencyHistogramForLabel(label string) *prometheus.HistogramVec {
	if strings.EqualFold(label, llmRespLabel) {
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
		Namespace: "shibuya",
		Name:      "latency_label_milliseconds",
		Buckets:   shibuyaBuckets,
	}, []string{"collection_id", "label", "run_id", "engine_no", "plan_id", "zone"})
	labelLLMRespLatencyHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "shibuya",
		Name:      "latency_llm_resp_milliseconds",
		Buckets:   shibuyaLLMRespBuckets,
	}, []string{"collection_id", "label", "run_id", "engine_no", "plan_id", "zone"})

	// This is similar to Latency but cannot use histogram here because we need a very accurate count of every status error that occurred.
	// So 200s are different bucket than 201s responses.
	statusCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "shibuya",
		Name:      "status_counter",
		Help:      "stores count of responses and groups in buckets of response codes",
	}, []string{"collection_id", "plan_id", "run_id", "engine_no", "label", "zone", "status"})

	egressCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "shibuya",
		Name:      "egress_bytes_counter",
		Help:      "total egress bytes used by the engine",
	}, []string{"collection_id", "plan_id", "run_id", "engine_no", "label", "zone"})

	// Gauge is the most intuitive way to count threads here.
	// We don't care about accuracy and there's no use of rate of threads
	threadsGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "shibuya",
		Name:      "threads_gauge",
		Help:      "Current number of threads running in JMeter",
	}, []string{"collection_id", "plan_id", "run_id", "engine_no", "zone"})

	cpuGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "shibuya",
		Name:      "cpu_gauge",
		Help:      "CPU used by engine",
	}, []string{"collection_id", "plan_id", "run_id", "zone", "engine_no"})

	memGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "shibuya",
		Name:      "mem_gauge",
		Help:      "Memory used by engine",
	}, []string{"collection_id", "plan_id", "run_id", "zone", "engine_no"})
)
