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
	startTestMetricsRun(t, as, runID)
	as.makePromMetrics(metricEvent{runID: runID, line: `{"Label":"test-label","Status":"200","Latency":250,"EgressBytes":1024,"Threads":10}`})
	gather := func() map[string]*dto.Metric {
		t.Helper()
		families, err := as.gatherMetrics()
		require.NoError(t, err)
		metrics := make(map[string]*dto.Metric)
		for _, family := range families {
			for _, metric := range family.Metric {
				labels := make(map[string]string)
				for _, label := range metric.Label {
					labels[label.GetName()] = label.GetValue()
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
	as.makePromMetrics(metricEvent{runID: runID, line: `{"Label":"` + strings.Repeat("x", 256) + `","Status":"503","Latency":999,"EgressBytes":999,"Threads":999}`})
	assert.Equal(t, before, gather(), "an invalid label must reject the entire event")
}

func TestMetricsIngestionLabelLimitsAndStaleRuns(t *testing.T) {
	const runID = "label-limits-run"
	meta := EngineMeta{CollectionID: t.Name(), PlanID: "plan", EngineID: "engine", Zone: "zone"}
	as := NewAgentServer(AgentServerOptions{
		EngineMeta: meta, ResultFile: "results.jsonl", LabelsUsageLimit: 1,
		MetricParser: func(line string) (ShibuyaMetric, error) {
			var metric ShibuyaMetric
			err := json.Unmarshal([]byte(line), &metric)
			return metric, err
		},
	})
	firstRun := startTestMetricsRun(t, as, runID)
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
	emit(runID, strings.Repeat("x", 256))
	emit("stale-run", "stale")
	as.finishMetricsRun(runID)
	emit(runID, "first")

	families, err := as.gatherMetrics()
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
	firstSnapshot := gatherResourceSnapshot(t, firstRun.Metrics.Registry())
	next := startTestMetricsRun(t, as, "next")
	emit(runID, "late-old")
	emit("next", "fresh")
	as.finishMetricsRun(runID)
	emit("next", "fresh")
	current := nativeScrape(t, as.metricsHandler())
	assert.Equal(t, map[string]float64{"fresh/200": 2}, nativeCounters(current["shibuya_status_counter"]))
	assert.Equal(t, "next", nativeLabelValue(current["shibuya_status_counter"].Metric[0], "run_id"))
	assert.NotSame(t, firstRun.Metrics, next.Metrics)
	assert.Equal(t, firstSnapshot, gatherResourceSnapshot(t, firstRun.Metrics.Registry()), "stale events cannot alter a completed registry")
}

func TestMetricLabelLengthBoundary(t *testing.T) {
	for _, size := range []int{0, 254, 255, 256, 257, 1023, 1024} {
		metric := ShibuyaMetric{Label: strings.Repeat("x", size)}
		assert.Equal(t, size, metric.LabelLength())
		valid, err := metric.LabelIsValid()
		assert.Equal(t, size <= 255, valid)
		assert.Equal(t, size > 255, err != nil)
	}
}

func TestMetricsIngestionNumericLabelBudget(t *testing.T) {
	for _, test := range []struct {
		name  string
		limit int
		want  map[string]float64
	}{
		{"zero", 0, map[string]float64{"others": 5}},
		{"two", 2, map[string]float64{"a": 2, "b": 1, "others": 2}},
	} {
		t.Run(test.name, func(t *testing.T) {
			meta := EngineMeta{CollectionID: t.Name(), PlanID: "plan", EngineID: "engine", Zone: "zone"}
			as := NewAgentServer(AgentServerOptions{
				EngineMeta: meta, ResultFile: "results", LabelsUsageLimit: test.limit,
				MetricParser: func(label string) (ShibuyaMetric, error) {
					return ShibuyaMetric{Label: label, Status: "200", Latency: 1, EgressBytes: 10}, nil
				},
			})
			startTestMetricsRun(t, as, "run")
			as.makePromMetrics(metricEvent{runID: "run", line: strings.Repeat("x", 256)})
			empty, err := as.gatherMetrics()
			require.NoError(t, err)
			assert.Empty(t, empty, "invalid labels must neither publish metrics nor consume admission")
			for _, label := range []string{"a", "b", "c", "a", "others"} {
				as.makePromMetrics(metricEvent{runID: "run", line: label})
			}
			families, err := as.gatherMetrics()
			require.NoError(t, err)
			counts := make(map[string]float64)
			for _, family := range families {
				if family.GetName() != "shibuya_status_counter" {
					continue
				}
				for _, metric := range family.Metric {
					if nativeLabelValue(metric, "collection_id") == meta.CollectionID {
						counts[nativeLabelValue(metric, "label")] = metric.GetCounter().GetValue()
					}
				}
			}
			assert.Equal(t, test.want, counts)
		})
	}
}

func TestMetricsIngestionIsolatesServersWithIdenticalIdentity(t *testing.T) {
	options := AgentServerOptions{
		EngineMeta:       EngineMeta{CollectionID: "collection", PlanID: "plan", EngineID: "engine", Zone: "zone"},
		ResultFile:       "results",
		LabelsUsageLimit: 1,
		MetricParser: func(string) (ShibuyaMetric, error) {
			return ShibuyaMetric{Label: "checkout", Status: "200", Latency: 250, EgressBytes: 1024, Threads: 7}, nil
		},
	}
	first := NewAgentServer(options)
	second := NewAgentServer(options)
	firstRun := startTestMetricsRun(t, first, "same-run")
	secondRun := startTestMetricsRun(t, second, "same-run")
	assert.NotSame(t, firstRun.Metrics.Registry(), secondRun.Metrics.Registry())
	assert.NotSame(t, firstRun.Metrics.Registry().resources.sampler, secondRun.Metrics.Registry().resources.sampler)
	event := metricEvent{runID: "same-run", line: "event"}
	first.makePromMetrics(event)
	empty, err := second.gatherMetrics()
	require.NoError(t, err)
	assert.Empty(t, empty, "matching identities must not share collectors")
	first.makePromMetrics(event)
	second.makePromMetrics(event)
	for _, test := range []struct {
		server *AgentServer
		count  float64
	}{
		{first, 2},
		{second, 1},
	} {
		families, err := test.server.gatherMetrics()
		require.NoError(t, err)
		require.Len(t, families, 4)
		for _, family := range families {
			require.Len(t, family.Metric, 1)
			metric := family.Metric[0]
			assert.Equal(t, "same-run", nativeLabelValue(metric, "run_id"))
			switch family.GetName() {
			case "shibuya_status_counter":
				assert.Equal(t, test.count, metric.GetCounter().GetValue())
			case "shibuya_egress_bytes_counter":
				assert.Equal(t, test.count*1024, metric.GetCounter().GetValue())
			case "shibuya_latency_label_milliseconds":
				assert.Equal(t, uint64(test.count), metric.GetHistogram().GetSampleCount())
				assert.Equal(t, test.count*250, metric.GetHistogram().GetSampleSum())
			case "shibuya_threads_gauge":
				assert.Equal(t, float64(7), metric.GetGauge().GetValue())
			}
		}
	}
	startTestMetricsRun(t, first, "next-run")
	empty, err = first.gatherMetrics()
	require.NoError(t, err)
	assert.Empty(t, empty)
	retained, err := second.gatherMetrics()
	require.NoError(t, err)
	assert.Len(t, retained, 4, "replacing one server's run must not remove another server's series")
}
