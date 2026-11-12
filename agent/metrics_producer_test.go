// Derived from reqfleet/shibuya; modified for engine-sdk.
package agent

import (
	"bytes"
	"math"
	"testing"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMetricsRegistryUsesLabelSpecificLatencyHistogram(t *testing.T) {
	for _, test := range []struct {
		label  string
		family string
	}{
		{"llm_resp", "shibuya_latency_llm_resp_milliseconds"},
		{"LLM_RESP", "shibuya_latency_llm_resp_milliseconds"},
		{"req_resp", "shibuya_latency_label_milliseconds"},
		{"ttft", "shibuya_latency_label_milliseconds"},
	} {
		t.Run(test.label, func(t *testing.T) {
			registry := NewMetricsRegistry()
			registry.RecordEvent(ShibuyaMetric{
				Label:        test.label,
				CollectionID: "collection-1",
				PlanID:       "plan-1",
				RunID:        "run-1",
				EngineID:     "engine-1",
				Zone:         "zone-1",
				Latency:      4500,
			})

			families := gatherCanonicalMetrics(t, registry)
			require.Len(t, families, 4)
			require.Contains(t, families, test.family)
			require.Len(t, families[test.family].Metric, 1)
			metric := families[test.family].Metric[0]
			assert.Equal(t, uint64(1), metric.GetHistogram().GetSampleCount())
			assert.Equal(t, float64(4500), metric.GetHistogram().GetSampleSum())
			labels := make(map[string]string, len(metric.Label))
			for _, label := range metric.Label {
				labels[label.GetName()] = label.GetValue()
			}
			assert.Equal(t, map[string]string{
				"collection_id": "collection-1",
				"plan_id":       "plan-1",
				"run_id":        "run-1",
				"engine_no":     "engine-1",
				"zone":          "zone-1",
				"label":         test.label,
			}, labels)
		})
	}
}

func TestLatencyHistogramWireContract(t *testing.T) {
	// Keep these expectations independent of metricschema's construction helpers.
	for _, test := range []struct {
		name         string
		label        string
		bounds       []float64
		observations []float64
	}{
		{
			name:         "shibuya_latency_label_milliseconds",
			label:        "request",
			bounds:       []float64{1, 2.5, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, math.Inf(1)},
			observations: []float64{0, 1, 1.5, 2.5, 10000, 10001},
		},
		{
			name:  "shibuya_latency_llm_resp_milliseconds",
			label: "llm_resp",
			bounds: []float64{
				100, 250, 500, 1000, 2500, 5000, 7500, 10000, 15000, 20000,
				30000, 45000, 60000, 90000, 120000, 180000, 300000, math.Inf(1),
			},
			observations: []float64{0, 100, 7500, 300000, 400000},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			registry := NewMetricsRegistry()
			var sum float64
			for _, observation := range test.observations {
				registry.RecordEvent(ShibuyaMetric{
					CollectionID: "collection",
					PlanID:       "plan",
					RunID:        "run",
					EngineID:     "engine",
					Zone:         "zone",
					Label:        test.label,
					Latency:      observation,
				})
				sum += observation
			}

			families, err := registry.Gather()
			require.NoError(t, err)
			require.Len(t, families, 4)
			var text bytes.Buffer
			for _, family := range families {
				_, err = expfmt.MetricFamilyToText(&text, family)
				require.NoError(t, err)
			}
			parser := expfmt.NewTextParser(model.LegacyValidation)
			scraped, err := parser.TextToMetricFamilies(&text)
			require.NoError(t, err)
			require.Contains(t, scraped, test.name)
			require.Len(t, scraped[test.name].Metric, 1)
			histogram := scraped[test.name].Metric[0].GetHistogram()
			assert.Equal(t, uint64(len(test.observations)), histogram.GetSampleCount())
			assert.Equal(t, sum, histogram.GetSampleSum())
			require.Len(t, histogram.Bucket, len(test.bounds))
			for i, bound := range test.bounds {
				assert.Equal(t, bound, histogram.Bucket[i].GetUpperBound())
				var count uint64
				for _, observation := range test.observations {
					if observation <= bound {
						count++
					}
				}
				assert.Equal(t, count, histogram.Bucket[i].GetCumulativeCount(), "upper bound %g", bound)
			}
		})
	}
}

func TestMetricsRegistryIndependentUpdates(t *testing.T) {
	registry := NewMetricsRegistry()
	metric := ShibuyaMetric{
		CollectionID: "collection",
		PlanID:       "plan",
		RunID:        "run",
		EngineID:     "engine",
		Zone:         "zone",
		Label:        "request",
		Status:       "200",
		Latency:      200,
		EgressBytes:  128,
		Threads:      4,
	}

	registry.AddReceivedBytes(metric)
	families := gatherCanonicalMetrics(t, registry)
	require.Len(t, families, 1)
	require.Contains(t, families, "shibuya_egress_bytes_counter")
	assert.Equal(t, float64(128), families["shibuya_egress_bytes_counter"].Metric[0].GetCounter().GetValue())

	registry.SetConcurrency(metric)
	families = gatherCanonicalMetrics(t, registry)
	require.Len(t, families, 2)
	require.Contains(t, families, "shibuya_threads_gauge")
	assert.Equal(t, float64(4), families["shibuya_threads_gauge"].Metric[0].GetGauge().GetValue())
	assert.Equal(t, float64(128), families["shibuya_egress_bytes_counter"].Metric[0].GetCounter().GetValue())

	metric.Threads = 99
	metric.EgressBytes = 999
	registry.ObserveRequest(metric)
	families = gatherCanonicalMetrics(t, registry)
	require.Len(t, families, 4)
	require.Contains(t, families, "shibuya_status_counter")
	require.Contains(t, families, "shibuya_latency_label_milliseconds")
	assert.Equal(t, float64(1), families["shibuya_status_counter"].Metric[0].GetCounter().GetValue())
	assert.Equal(t, uint64(1), families["shibuya_latency_label_milliseconds"].Metric[0].GetHistogram().GetSampleCount())
	assert.Equal(t, float64(200), families["shibuya_latency_label_milliseconds"].Metric[0].GetHistogram().GetSampleSum())
	assert.Equal(t, float64(4), families["shibuya_threads_gauge"].Metric[0].GetGauge().GetValue())
	assert.Equal(t, float64(128), families["shibuya_egress_bytes_counter"].Metric[0].GetCounter().GetValue())

	metric.Latency = 10
	metric.Threads = 6
	metric.EgressBytes = 64
	registry.RecordEvent(metric)
	families = gatherCanonicalMetrics(t, registry)
	require.Len(t, families, 4)
	assert.Equal(t, float64(2), families["shibuya_status_counter"].Metric[0].GetCounter().GetValue())
	assert.Equal(t, uint64(2), families["shibuya_latency_label_milliseconds"].Metric[0].GetHistogram().GetSampleCount())
	assert.Equal(t, float64(210), families["shibuya_latency_label_milliseconds"].Metric[0].GetHistogram().GetSampleSum())
	assert.Equal(t, float64(6), families["shibuya_threads_gauge"].Metric[0].GetGauge().GetValue())
	assert.Equal(t, float64(192), families["shibuya_egress_bytes_counter"].Metric[0].GetCounter().GetValue())

	t.Run("reserved request label remains ordinary", func(t *testing.T) {
		registry := NewMetricsRegistry()
		metric.Label = "llm_resp"
		registry.ObserveRequest(metric)
		families := gatherCanonicalMetrics(t, registry)
		require.Len(t, families, 2)
		require.Contains(t, families, "shibuya_status_counter")
		require.Contains(t, families, "shibuya_latency_label_milliseconds")
		assert.NotContains(t, families, "shibuya_latency_llm_resp_milliseconds")
		assert.Equal(t, uint64(1), families["shibuya_latency_label_milliseconds"].Metric[0].GetHistogram().GetSampleCount())
		assert.Equal(t, metric.Latency, families["shibuya_latency_label_milliseconds"].Metric[0].GetHistogram().GetSampleSum())
	})
}

func TestMetricsRegistryInstancesAreIndependent(t *testing.T) {
	first := NewMetricsRegistry()
	second := NewMetricsRegistry()
	metric := ShibuyaMetric{
		CollectionID: "collection",
		PlanID:       "plan",
		RunID:        "run",
		EngineID:     "engine",
		Zone:         "zone",
		Label:        "request",
		Status:       "200",
		Latency:      100,
		EgressBytes:  128,
		Threads:      4,
	}

	first.RecordEvent(metric)
	require.Empty(t, gatherCanonicalMetrics(t, second))
	second.RecordEvent(metric)
	second.RecordEvent(metric)
	for _, test := range []struct {
		registry *MetricsRegistry
		count    float64
	}{
		{first, 1},
		{second, 2},
	} {
		families := gatherCanonicalMetrics(t, test.registry)
		require.Len(t, families, 4)
		require.Contains(t, families, "shibuya_status_counter")
		require.Contains(t, families, "shibuya_egress_bytes_counter")
		require.Contains(t, families, "shibuya_latency_label_milliseconds")
		require.Contains(t, families, "shibuya_threads_gauge")
		assert.Equal(t, test.count, families["shibuya_status_counter"].Metric[0].GetCounter().GetValue())
		assert.Equal(t, 128*test.count, families["shibuya_egress_bytes_counter"].Metric[0].GetCounter().GetValue())
		assert.Equal(t, uint64(test.count), families["shibuya_latency_label_milliseconds"].Metric[0].GetHistogram().GetSampleCount())
		assert.Equal(t, 100*test.count, families["shibuya_latency_label_milliseconds"].Metric[0].GetHistogram().GetSampleSum())
		assert.Equal(t, float64(4), families["shibuya_threads_gauge"].Metric[0].GetGauge().GetValue())
	}
	secondBeforeReset := gatherCanonicalMetrics(t, second)
	first.Reset()
	require.Empty(t, gatherCanonicalMetrics(t, first))
	assert.Equal(t, secondBeforeReset, gatherCanonicalMetrics(t, second))
	first.RecordEvent(metric)
	families := gatherCanonicalMetrics(t, first)
	require.Contains(t, families, "shibuya_status_counter")
	assert.Equal(t, float64(1), families["shibuya_status_counter"].Metric[0].GetCounter().GetValue())
	assert.Equal(t, secondBeforeReset, gatherCanonicalMetrics(t, second))
}

func gatherCanonicalMetrics(t *testing.T, registry *MetricsRegistry) map[string]*dto.MetricFamily {
	t.Helper()
	families, err := registry.Gather()
	require.NoError(t, err)
	result := make(map[string]*dto.MetricFamily, len(families))
	for _, family := range families {
		result[family.GetName()] = family
	}
	return result
}
