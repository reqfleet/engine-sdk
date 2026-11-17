package agent

import (
	"fmt"
	"math"
	"testing"

	"github.com/reqfleet/engine-sdk/metricschema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMetricsRecorderIndependentLLMTimings(t *testing.T) {
	for _, timing := range []struct {
		label  string
		record func(*MetricsRecorder, float64) error
	}{
		{"ttft", (*MetricsRecorder).ObserveTTFT},
		{"tps", (*MetricsRecorder).ObserveInterTokenLatency},
	} {
		for _, budget := range []int{0, 1, 2} {
			t.Run(fmt.Sprintf("%s/budget-%d", timing.label, budget), func(t *testing.T) {
				registry := NewMetricsRegistry()
				recorder, err := NewMetricsRecorder(registry, budget)
				require.NoError(t, err)
				run := resetTestRun("timings")
				recorder.Reset(run)
				require.NoError(t, recorder.RecordStatus(ShibuyaMetric{Label: "http", Status: "500"}))
				before := gatherCanonicalMetrics(t, registry)
				values := []float64{0, 2.5, 10001}
				for _, value := range values {
					require.NoError(t, timing.record(recorder, value))
				}
				after := gatherCanonicalMetrics(t, registry)
				family := after[metricschema.RequestLatency]
				require.NotNil(t, family)
				require.Len(t, family.Metric, 1)
				label := timing.label
				if budget < 2 {
					label = "others"
				}
				assertRecorderMetricLabels(t, family.Metric[0], run, map[string]string{"label": label})
				histogram := family.Metric[0].GetHistogram()
				assert.Equal(t, uint64(3), histogram.GetSampleCount())
				assert.Equal(t, 10003.5, histogram.GetSampleSum())
				bounds := []float64{1, 2.5, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000}
				require.Len(t, histogram.Bucket, len(bounds))
				for i, bound := range bounds {
					assert.Equal(t, bound, histogram.Bucket[i].GetUpperBound())
					count := uint64(2)
					if i == 0 {
						count = 1
					}
					assert.Equal(t, count, histogram.Bucket[i].GetCumulativeCount())
				}
				assert.Equal(t, after, gatherCanonicalMetrics(t, registry), "scrapes must not recount timings")
				delete(after, metricschema.RequestLatency)
				assert.Equal(t, before, after, "timings must not alter status, bytes, concurrency or the wide histogram")

				next := resetTestRun("next")
				recorder.Reset(next)
				require.Empty(t, gatherCanonicalMetrics(t, registry))
				require.NoError(t, timing.record(recorder, 0))
				fresh := gatherCanonicalMetrics(t, registry)
				require.Len(t, fresh, 1)
				if budget > 0 {
					label = timing.label
				}
				metric := fresh[metricschema.RequestLatency].Metric[0]
				assertRecorderMetricLabels(t, metric, next, map[string]string{"label": label})
				assert.Equal(t, uint64(1), metric.GetHistogram().GetSampleCount())
				assert.Zero(t, metric.GetHistogram().GetSampleSum())
			})
		}
		for _, value := range []float64{-1, math.NaN(), math.Inf(1), math.Inf(-1)} {
			t.Run(fmt.Sprintf("%s/invalid-%v", timing.label, value), func(t *testing.T) {
				registry := NewMetricsRegistry()
				recorder, err := NewMetricsRecorder(registry, 1)
				require.NoError(t, err)
				recorder.Reset(resetTestRun("invalid"))
				require.Error(t, timing.record(recorder, value))
				require.Empty(t, gatherCanonicalMetrics(t, registry))
				require.NoError(t, recorder.RecordStatus(ShibuyaMetric{Label: "http", Status: "200"}))
				before := gatherCanonicalMetrics(t, registry)
				assert.Equal(t, map[string]float64{"http/200": 1}, nativeCounters(before[metricschema.StatusCounter]), "invalid timings must not consume admission")
				require.Error(t, timing.record(recorder, value))
				assert.Equal(t, before, gatherCanonicalMetrics(t, registry))
			})
		}
	}
}

func TestMetricsRecorderReceivedTokensShareTimingAdmission(t *testing.T) {
	for _, budget := range []int{0, 1} {
		for _, timingFirst := range []bool{false, true} {
			t.Run(fmt.Sprintf("budget-%d/timing-first-%t", budget, timingFirst), func(t *testing.T) {
				registry := NewMetricsRegistry()
				recorder, err := NewMetricsRecorder(registry, budget)
				require.NoError(t, err)
				run := resetTestRun("tokens")
				recorder.Reset(run)
				if timingFirst {
					require.NoError(t, recorder.ObserveInterTokenLatency(7))
				}
				for range 3 {
					require.NoError(t, recorder.RecordReceivedToken())
				}
				label := "tps"
				if budget == 0 {
					label = "others"
				}
				snapshot := gatherCanonicalMetrics(t, registry)
				assert.Equal(t, map[string]float64{label + "/200": 3}, nativeCounters(snapshot[metricschema.StatusCounter]))
				assertRecorderMetricLabels(t, snapshot[metricschema.StatusCounter].Metric[0], run, map[string]string{"label": label, "status": "200"})
				if timingFirst {
					require.Len(t, snapshot, 2)
				} else {
					require.Len(t, snapshot, 1, "token receipts create only status counts")
					require.NoError(t, recorder.ObserveInterTokenLatency(7))
				}
				require.NoError(t, recorder.RecordStatus(ShibuyaMetric{Label: "http", Status: "503"}))
				after := gatherCanonicalMetrics(t, registry)
				require.Len(t, after, 2)
				assert.Equal(t, map[string]float64{label + "/200": 3, "others/503": 1}, nativeCounters(after[metricschema.StatusCounter]))
				metric := after[metricschema.RequestLatency].Metric[0]
				assertRecorderMetricLabels(t, metric, run, map[string]string{"label": label})
				assert.Equal(t, uint64(1), metric.GetHistogram().GetSampleCount(), "token counts must not create timing samples")
				assert.Equal(t, 7.0, metric.GetHistogram().GetSampleSum())
				assert.Equal(t, after, gatherCanonicalMetrics(t, registry))

				next := resetTestRun("next")
				recorder.Reset(next)
				require.NoError(t, recorder.RecordStatus(ShibuyaMetric{Label: "http", Status: "201"}))
				require.NoError(t, recorder.RecordReceivedToken())
				fresh := gatherCanonicalMetrics(t, registry)
				require.Len(t, fresh, 1)
				httpLabel := "http"
				if budget == 0 {
					httpLabel = "others"
				}
				assert.Equal(t, map[string]float64{httpLabel + "/201": 1, "others/200": 1}, nativeCounters(fresh[metricschema.StatusCounter]))
				for _, metric := range fresh[metricschema.StatusCounter].Metric {
					assertRecorderMetricLabels(t, metric, next, map[string]string{"label": nativeLabelValue(metric, "label"), "status": nativeLabelValue(metric, "status")})
				}
			})
		}
	}
}
