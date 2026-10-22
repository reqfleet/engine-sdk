// Derived from reqfleet/shibuya; modified for engine-sdk.
package agent

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMetricsIngestion(t *testing.T) {
	const runID = "event-ingestion-run"
	meta := EngineMeta{CollectionID: t.Name(), PlanID: "plan", EngineID: "engine", Zone: "zone"}
	identity := prometheus.Labels{"collection_id": meta.CollectionID, "plan_id": meta.PlanID, "run_id": runID, "engine_no": meta.EngineID, "zone": meta.Zone}
	cleanup := func() {
		labelLatencyHistogram.DeletePartialMatch(identity)
		statusCounter.DeletePartialMatch(identity)
		egressCounter.DeletePartialMatch(identity)
		threadsGauge.DeletePartialMatch(identity)
	}
	cleanup()
	t.Cleanup(cleanup)
	as := NewAgentServer(AgentServerOptions{
		EngineMeta: meta, ResultFile: "results.jsonl", LabelsUsageLimit: 10,
		MetricParser: func(line string) (ShibuyaMetric, error) {
			var metric ShibuyaMetric
			err := json.Unmarshal([]byte(line), &metric)
			// Adapter-supplied identity must never override the assigned run.
			metric.CollectionID, metric.PlanID, metric.RunID = "source", "source", "source"
			metric.EngineID, metric.Zone = "source", "source"
			return metric, err
		},
	})
	as.labelsByRun[runID] = make(map[string]struct{})
	as.makePromMetrics(metricEvent{runID: runID, line: `{"Label":"test-label","Status":"200","Latency":250,"EgressBytes":1024,"Threads":10}`})
	gather := func() map[string]*dto.Metric {
		t.Helper()
		families, err := metricsRegistry().Gather()
		require.NoError(t, err)
		metrics := make(map[string]*dto.Metric)
		for _, family := range families {
			for _, metric := range family.Metric {
				labels := make(map[string]string)
				for _, label := range metric.Label {
					labels[label.GetName()] = label.GetValue()
				}
				if labels["collection_id"] != meta.CollectionID {
					continue
				}
				for key, value := range identity {
					assert.Equal(t, value, labels[key])
				}
				if family.GetName() != "shibuya_threads_gauge" {
					assert.Equal(t, "test-label", labels["label"])
				}
				if family.GetName() == "shibuya_status_counter" {
					assert.Equal(t, "200", labels["status"])
				}
				metrics[family.GetName()] = metric
			}
		}
		return metrics
	}
	before := gather()
	require.Len(t, before, 4)
	assert.Equal(t, float64(1), before["shibuya_status_counter"].GetCounter().GetValue())
	assert.Equal(t, uint64(1), before["shibuya_latency_label_milliseconds"].GetHistogram().GetSampleCount())
	assert.Equal(t, float64(250), before["shibuya_latency_label_milliseconds"].GetHistogram().GetSampleSum())
	assert.Equal(t, float64(1024), before["shibuya_egress_bytes_counter"].GetCounter().GetValue())
	assert.Equal(t, float64(10), before["shibuya_threads_gauge"].GetGauge().GetValue())
	as.makePromMetrics(metricEvent{runID: runID, line: "malformed|row"})
	assert.Equal(t, before, gather(), "malformed events must not change any totals")
}

func TestMetricsIngestionLabelLimitsAndStaleRuns(t *testing.T) {
	const runID = "label-limits-run"
	meta := EngineMeta{CollectionID: t.Name(), PlanID: "plan", EngineID: "engine", Zone: "zone"}
	identity := prometheus.Labels{"collection_id": meta.CollectionID}
	t.Cleanup(func() {
		labelLatencyHistogram.DeletePartialMatch(identity)
		statusCounter.DeletePartialMatch(identity)
		egressCounter.DeletePartialMatch(identity)
		threadsGauge.DeletePartialMatch(identity)
	})
	as := NewAgentServer(AgentServerOptions{
		EngineMeta: meta, ResultFile: "results.jsonl", LabelsUsageLimit: 1,
		MetricParser: func(line string) (ShibuyaMetric, error) {
			var metric ShibuyaMetric
			err := json.Unmarshal([]byte(line), &metric)
			return metric, err
		},
	})
	as.labelsByRun[runID] = make(map[string]struct{})
	emit := func(run, label string) {
		t.Helper()
		line, err := json.Marshal(ShibuyaMetric{Label: label, Status: "200", Latency: 250})
		require.NoError(t, err)
		as.makePromMetrics(metricEvent{runID: run, line: string(line)})
	}
	emit(runID, "first")
	emit(runID, "second")
	emit(runID, "first")
	emit(runID, "others")
	emit(runID, strings.Repeat("x", 1024))
	emit("stale-run", "stale")
	as.labelsLock.Lock()
	delete(as.labelsByRun, runID)
	as.labelsLock.Unlock()
	emit(runID, "first")

	families, err := metricsRegistry().Gather()
	require.NoError(t, err)
	counts := make(map[string]float64)
	for _, family := range families {
		if family.GetName() != "shibuya_status_counter" {
			continue
		}
		for _, metric := range family.Metric {
			labels := make(map[string]string)
			for _, label := range metric.Label {
				labels[label.GetName()] = label.GetValue()
			}
			if labels["collection_id"] == meta.CollectionID {
				assert.Equal(t, runID, labels["run_id"])
				counts[labels["label"]] = metric.GetCounter().GetValue()
			}
		}
	}
	assert.Equal(t, map[string]float64{"first": 2, "others": 2}, counts)
}

func TestMetricLabelLengthBoundary(t *testing.T) {
	for _, size := range []int{0, 1023, 1024, 1025} {
		metric := ShibuyaMetric{Label: strings.Repeat("x", size)}
		assert.Equal(t, size, metric.LabelLength())
		valid, err := metric.LabelIsValid()
		assert.Equal(t, size < 1024, valid)
		assert.Equal(t, size >= 1024, err != nil)
	}
}
