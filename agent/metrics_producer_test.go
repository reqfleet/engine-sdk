// Derived from reqfleet/shibuya; modified for engine-sdk.
package agent

import (
	"bytes"
	"math"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
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

func TestLatencyHistogramWireContract(t *testing.T) {
	// Keep these expectations independent of metricschema's construction helpers.
	for _, test := range []struct {
		name         string
		histogram    *prometheus.HistogramVec
		bounds       []float64
		observations []float64
	}{
		{
			name:         "shibuya_latency_label_milliseconds",
			histogram:    labelLatencyHistogram,
			bounds:       []float64{1, 2.5, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, math.Inf(1)},
			observations: []float64{0, 1, 1.5, 2.5, 10000, 10001},
		},
		{
			name:      "shibuya_latency_llm_resp_milliseconds",
			histogram: labelLLMRespLatencyHistogram,
			bounds: []float64{
				100, 250, 500, 1000, 2500, 5000, 7500, 10000, 15000, 20000,
				30000, 45000, 60000, 90000, 120000, 180000, 300000, math.Inf(1),
			},
			observations: []float64{0, 100, 7500, 300000, 400000},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			test.histogram.Reset()
			t.Cleanup(test.histogram.Reset)
			registry := prometheus.NewRegistry()
			registry.MustRegister(test.histogram)
			observer := test.histogram.WithLabelValues("collection", "request", "run", "engine", "plan", "zone")
			var sum float64
			for _, observation := range test.observations {
				observer.Observe(observation)
				sum += observation
			}

			families, err := registry.Gather()
			require.NoError(t, err)
			require.Len(t, families, 1)
			var text bytes.Buffer
			_, err = expfmt.MetricFamilyToText(&text, families[0])
			require.NoError(t, err)
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
