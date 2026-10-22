// Derived from reqfleet/shibuya; modified for engine-sdk.
package agent

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShibuyaMetricToPrometheus_UsesLabelSpecificLatencyHistogram(t *testing.T) {
	labelLatencyHistogram.Reset()
	labelLLMRespLatencyHistogram.Reset()

	llmRespMetric := ShibuyaMetric{
		Label:        "llm_resp",
		CollectionID: "collection-1",
		PlanID:       "plan-1",
		RunID:        "run-1",
		EngineID:     "engine-1",
		Zone:         "zone-1",
		Latency:      4500,
	}
	normalMetric := ShibuyaMetric{
		Label:        "req_resp",
		CollectionID: "collection-1",
		PlanID:       "plan-1",
		RunID:        "run-1",
		EngineID:     "engine-1",
		Zone:         "zone-1",
		Latency:      120,
	}

	llmRespMetric.ToPrometheus()
	normalMetric.ToPrometheus()

	assert.Equal(t, uint64(0), histogramSampleCount(t, labelLatencyHistogram, llmRespMetric))
	assert.Equal(t, uint64(1), histogramSampleCount(t, labelLLMRespLatencyHistogram, llmRespMetric))
	assert.Equal(t, uint64(1), histogramSampleCount(t, labelLatencyHistogram, normalMetric))
	assert.Equal(t, uint64(0), histogramSampleCount(t, labelLLMRespLatencyHistogram, normalMetric))
}

func TestLabelLatencyHistogramIncludesLowLatencyBuckets(t *testing.T) {
	labelLatencyHistogram.Reset()

	observer, err := labelLatencyHistogram.GetMetricWithLabelValues(
		"collection-low-latency",
		"req_resp",
		"run-low-latency",
		"engine-low-latency",
		"plan-low-latency",
		"zone-low-latency",
	)
	require.NoError(t, err)

	observer.Observe(1.5)

	metricDTO := &dto.Metric{}
	require.NoError(t, observer.(prometheus.Metric).Write(metricDTO))

	bucketCounts := make(map[float64]uint64, len(metricDTO.GetHistogram().GetBucket()))
	for _, bucket := range metricDTO.GetHistogram().GetBucket() {
		bucketCounts[bucket.GetUpperBound()] = bucket.GetCumulativeCount()
	}

	require.Contains(t, bucketCounts, float64(1))
	require.Contains(t, bucketCounts, float64(2.5))
	assert.Equal(t, uint64(0), bucketCounts[1])
	assert.Equal(t, uint64(1), bucketCounts[2.5])
}

func histogramSampleCount(t *testing.T, histogram *prometheus.HistogramVec, metric ShibuyaMetric) uint64 {
	t.Helper()

	observer, err := histogram.GetMetricWithLabelValues(metric.CollectionID, metric.Label, metric.RunID, metric.EngineID, metric.PlanID, metric.Zone)
	if !assert.NoError(t, err) {
		return 0
	}

	metricDTO := &dto.Metric{}
	if !assert.NoError(t, observer.(prometheus.Metric).Write(metricDTO)) {
		return 0
	}

	return metricDTO.GetHistogram().GetSampleCount()
}
