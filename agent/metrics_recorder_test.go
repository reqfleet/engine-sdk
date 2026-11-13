package agent

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"

	dto "github.com/prometheus/client_model/go"
	"github.com/reqfleet/engine-sdk/metricschema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Exercise the public recorder and result-file entry point against the same
// independent expectations, not merely against each other's output.
func TestRequestSamplePolicy(t *testing.T) {
	boundaryName := strings.Repeat("界", 341) // 1023 bytes, not 1023 characters.
	for _, source := range []string{"recorder", "result-file"} {
		for _, budget := range []int{0, 2} {
			t.Run(fmt.Sprintf("%s/budget-%d", source, budget), func(t *testing.T) {
				registry, emit := eventPolicyProducer(t, source, budget)
				invalid := ShibuyaMetric{
					Label: boundaryName + "x", Status: "503", Latency: 999, EgressBytes: 999, Threads: 999,
				}
				emit(invalid)
				require.Empty(t, gatherCanonicalMetrics(t, registry))

				for _, name := range []string{"others", boundaryName, "b", "c", boundaryName} {
					emit(ShibuyaMetric{Label: name, Status: "200", Latency: 2, EgressBytes: 10, Threads: 7})
					before := gatherCanonicalMetrics(t, registry)
					emit(invalid)
					assert.Equal(t, before, gatherCanonicalMetrics(t, registry), "rejection must change no family")
				}
				want := map[string]float64{"others": 5}
				if budget == 2 {
					want = map[string]float64{"others": 2, boundaryName: 2, "b": 1}
				}
				assertCompleteSampleTotals(t, registry, want, 2, 10, 7)
			})
		}
	}
}

func eventPolicyProducer(t *testing.T, source string, budget int) (*MetricsRegistry, func(ShibuyaMetric)) {
	t.Helper()
	if source == "result-file" {
		server := NewAgentServer(AgentServerOptions{
			ResultFile: "results", LabelsUsageLimit: budget,
			MetricParser: func(line string) (ShibuyaMetric, error) {
				var metric ShibuyaMetric
				err := json.Unmarshal([]byte(line), &metric)
				return metric, err
			},
		})
		server.startMetricsRun("run")
		return server.MetricsRegistry(), func(metric ShibuyaMetric) {
			t.Helper()
			line, err := json.Marshal(metric)
			require.NoError(t, err)
			server.makePromMetrics(metricEvent{runID: "run", line: string(line)})
		}
	}
	registry := NewMetricsRegistry()
	recorder, err := NewMetricsRecorder(registry, budget)
	require.NoError(t, err)
	recorder.Reset(RunContext{RunID: "run"})
	return registry, func(metric ShibuyaMetric) {
		t.Helper()
		err := recorder.RecordEvent(metric)
		if len(metric.Label) > metricschema.MaxRequestLabelBytes {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
		}
	}
}

func assertCompleteSampleTotals(t *testing.T, registry *MetricsRegistry, want map[string]float64, latency, bytes, concurrency float64) {
	t.Helper()
	families := gatherCanonicalMetrics(t, registry)
	require.Len(t, families, 4)
	for _, name := range []string{metricschema.RequestLatency, metricschema.StatusCounter, metricschema.EgressBytesCounter} {
		require.Contains(t, families, name)
		counts := make(map[string]float64)
		for _, metric := range families[name].Metric {
			label := nativeLabelValue(metric, "label")
			switch name {
			case metricschema.RequestLatency:
				counts[label] = float64(metric.GetHistogram().GetSampleCount())
				assert.Equal(t, want[label]*latency, metric.GetHistogram().GetSampleSum())
			case metricschema.StatusCounter:
				counts[label] = metric.GetCounter().GetValue()
				assert.Equal(t, "200", nativeLabelValue(metric, "status"))
			case metricschema.EgressBytesCounter:
				counts[label] = metric.GetCounter().GetValue() / bytes
			}
		}
		assert.Equal(t, want, counts, name)
	}
	require.Contains(t, families, metricschema.ThreadsGauge)
	require.Len(t, families[metricschema.ThreadsGauge].Metric, 1)
	threads := families[metricschema.ThreadsGauge].Metric[0]
	assert.Equal(t, concurrency, threads.GetGauge().GetValue())
	for _, label := range threads.Label {
		assert.NotEqual(t, "label", label.GetName(), "concurrency must not gain request attribution")
	}
}

func TestMetricsRecorderMeasurementPresence(t *testing.T) {
	for _, test := range []struct {
		name   string
		sample RequestSample
		want   []string
	}{
		{"latency", RequestSample{HasLatency: true}, []string{metricschema.RequestLatency}},
		{"status", RequestSample{HasStatus: true}, []string{metricschema.StatusCounter}},
		{"bytes", RequestSample{HasReceivedBytes: true}, []string{metricschema.EgressBytesCounter}},
		{"concurrency", RequestSample{HasConcurrency: true}, []string{metricschema.ThreadsGauge}},
		{"partial request", RequestSample{HasLatency: true, HasReceivedBytes: true, HasConcurrency: true},
			[]string{metricschema.RequestLatency, metricschema.EgressBytesCounter, metricschema.ThreadsGauge}},
	} {
		t.Run(test.name, func(t *testing.T) {
			registry := NewMetricsRegistry()
			recorder, err := NewMetricsRecorder(registry, 0)
			require.NoError(t, err)
			recorder.Reset(RunContext{})
			sample := test.sample
			sample.Metric = ShibuyaMetric{Label: "llm_resp", Status: "204", Latency: 0, EgressBytes: 12, Threads: 3}
			require.NoError(t, recorder.RecordSample(sample))
			families := gatherCanonicalMetrics(t, registry)
			require.Len(t, families, len(test.want), "absent measurements must not be fabricated")
			for _, name := range test.want {
				require.Contains(t, families, name)
				require.Len(t, families[name].Metric, 1)
				metric := families[name].Metric[0]
				if name != metricschema.ThreadsGauge {
					assert.Equal(t, "others", nativeLabelValue(metric, "label"))
				}
				switch name {
				case metricschema.RequestLatency:
					assert.Equal(t, uint64(1), metric.GetHistogram().GetSampleCount())
					assert.Zero(t, metric.GetHistogram().GetSampleSum(), "zero latency is an observation")
				case metricschema.StatusCounter:
					assert.Equal(t, float64(1), metric.GetCounter().GetValue())
					assert.Equal(t, "204", nativeLabelValue(metric, "status"))
				case metricschema.EgressBytesCounter:
					assert.Equal(t, float64(12), metric.GetCounter().GetValue())
				case metricschema.ThreadsGauge:
					assert.Equal(t, float64(3), metric.GetGauge().GetValue())
				}
			}
			sample.Metric.Label = strings.Repeat("x", 1024)
			require.Error(t, recorder.RecordSample(sample))
			assert.Equal(t, families, gatherCanonicalMetrics(t, registry))
		})
	}
}

func TestMetricsRecorderNamedMeasurements(t *testing.T) {
	for _, test := range []struct {
		name   string
		record func(*MetricsRecorder, ShibuyaMetric) error
		want   []string
	}{
		{"latency", (*MetricsRecorder).ObserveLatency, []string{metricschema.RequestLatency}},
		{"status", (*MetricsRecorder).RecordStatus, []string{metricschema.StatusCounter}},
		{"request", (*MetricsRecorder).ObserveRequest, []string{metricschema.RequestLatency, metricschema.StatusCounter}},
		{"bytes", (*MetricsRecorder).AddReceivedBytes, []string{metricschema.EgressBytesCounter}},
	} {
		for _, budget := range []int{0, 1} {
			for _, populated := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/budget-%d/populated-%t", test.name, budget, populated), func(t *testing.T) {
					registry := NewMetricsRegistry()
					recorder, err := NewMetricsRecorder(registry, budget)
					require.NoError(t, err)
					recorder.Reset(RunContext{})
					metric := ShibuyaMetric{Label: "llm_resp", Status: "204", Latency: 8, EgressBytes: 12, Threads: 3}
					invalid := metric
					invalid.Label = strings.Repeat("x", 1024)
					require.Error(t, test.record(recorder, invalid))
					require.Empty(t, gatherCanonicalMetrics(t, registry))

					count, latencySum := float64(1), float64(0)
					if populated {
						require.NoError(t, recorder.RecordSample(completeRequestSample(metric)))
						count, latencySum = 2, 8
					}
					before := gatherCanonicalMetrics(t, registry)
					metric.Latency, metric.Threads = 0, 999
					require.NoError(t, test.record(recorder, metric))
					after := gatherCanonicalMetrics(t, registry)
					require.Error(t, test.record(recorder, invalid))
					assert.Equal(t, after, gatherCanonicalMetrics(t, registry), "invalid samples must change no family")

					label := "llm_resp"
					if budget == 0 {
						label = "others"
					}
					for _, family := range test.want {
						require.Contains(t, after, family)
						require.Len(t, after[family].Metric, 1)
						recorded := after[family].Metric[0]
						assert.Equal(t, label, nativeLabelValue(recorded, "label"), "invalid names must not consume slots")
						switch family {
						case metricschema.RequestLatency:
							assert.Equal(t, uint64(count), recorded.GetHistogram().GetSampleCount())
							assert.Equal(t, latencySum, recorded.GetHistogram().GetSampleSum())
						case metricschema.StatusCounter:
							assert.Equal(t, count, recorded.GetCounter().GetValue())
							assert.Equal(t, "204", nativeLabelValue(recorded, "status"))
						case metricschema.EgressBytesCounter:
							assert.Equal(t, count*12, recorded.GetCounter().GetValue())
						}
						delete(before, family)
						delete(after, family)
					}
					assert.Equal(t, before, after, "unselected families must not be created or changed")
				})
			}
		}
	}
}

func TestMetricsRecorderSharedAdmissionAndIndependentSamples(t *testing.T) {
	registry := NewMetricsRegistry()
	recorder, err := NewMetricsRecorder(registry, 1)
	require.NoError(t, err)
	recorder.Reset(RunContext{})
	invalid := ShibuyaMetric{Label: strings.Repeat("x", 1024), Latency: 999, EgressBytes: 999, Threads: 999}
	require.Error(t, recorder.RecordSample(completeRequestSample(invalid)))
	require.Error(t, recorder.AddAggregateReceivedBytes(invalid))

	// Neither independent updates nor request-associated concurrency consume slots.
	require.NoError(t, recorder.RecordSample(RequestSample{
		Metric: ShibuyaMetric{Label: "not-a-request", Threads: 3}, HasConcurrency: true,
	}))
	require.NoError(t, recorder.AddAggregateReceivedBytes(ShibuyaMetric{Label: "aggregate", EgressBytes: 40}))
	invalid.Threads = 8
	recorder.SetConcurrency(invalid) // This independent measurement has no request label.
	require.NoError(t, recorder.ObserveLatency(ShibuyaMetric{Label: "kept", Latency: 5}))
	require.NoError(t, recorder.RecordSample(RequestSample{
		Metric: ShibuyaMetric{Label: "excess", Status: "500", EgressBytes: 20}, HasStatus: true, HasReceivedBytes: true,
	}))
	require.NoError(t, recorder.RecordStatus(ShibuyaMetric{Label: "kept", Status: "200"}))
	require.NoError(t, recorder.AddReceivedBytes(ShibuyaMetric{Label: "bytes-only", EgressBytes: 10}))
	before := gatherCanonicalMetrics(t, registry)
	require.Error(t, recorder.RecordSample(completeRequestSample(invalid)))
	assert.Equal(t, before, gatherCanonicalMetrics(t, registry))

	latency := before[metricschema.RequestLatency]
	require.Len(t, latency.Metric, 1)
	assert.Equal(t, "kept", nativeLabelValue(latency.Metric[0], "label"))
	assert.Equal(t, uint64(1), latency.Metric[0].GetHistogram().GetSampleCount())
	assert.Equal(t, float64(5), latency.Metric[0].GetHistogram().GetSampleSum())
	for name, want := range map[string]map[string]float64{
		metricschema.StatusCounter:      {"kept": 1, "others": 1},
		metricschema.EgressBytesCounter: {"aggregate": 40, "others": 30},
	} {
		got := make(map[string]float64)
		for _, metric := range before[name].Metric {
			got[nativeLabelValue(metric, "label")] = metric.GetCounter().GetValue()
		}
		assert.Equal(t, want, got)
	}
	threads := before[metricschema.ThreadsGauge].Metric[0]
	assert.Equal(t, float64(8), threads.GetGauge().GetValue())
	assert.Len(t, threads.Label, 5)
}

func TestMetricsRecorderInstances(t *testing.T) {
	for _, firstName := range []string{"first-run", "second-run"} {
		registry := NewMetricsRegistry()
		recorder, err := NewMetricsRecorder(registry, 1)
		require.NoError(t, err)
		recorder.Reset(RunContext{RunID: firstName})
		for _, name := range []string{firstName, "excess", firstName} {
			require.NoError(t, recorder.RecordEvent(ShibuyaMetric{
				Label: name, RunID: firstName, Status: "200", Latency: 2, EgressBytes: 10, Threads: 7,
			}))
		}
		assertCompleteSampleTotals(t, registry, map[string]float64{firstName: 2, "others": 1}, 2, 10, 7)
	}
}

func TestMetricsRecorderConcurrentAdmission(t *testing.T) {
	registry := NewMetricsRegistry()
	recorder, err := NewMetricsRecorder(registry, 1)
	require.NoError(t, err)
	recorder.Reset(RunContext{})
	require.NoError(t, recorder.RecordEvent(ShibuyaMetric{Label: "kept", Status: "200", Latency: 2, EgressBytes: 10, Threads: 7}))
	var workers sync.WaitGroup
	for _, label := range []string{"kept", "a", "b", "others"} {
		workers.Go(func() {
			for range 50 {
				metric := ShibuyaMetric{Label: label, Status: "200", Latency: 2, EgressBytes: 10, Threads: 7}
				assert.NoError(t, recorder.ObserveRequest(metric))
				assert.NoError(t, recorder.AddReceivedBytes(metric))
				recorder.SetConcurrency(metric)
				assert.NoError(t, recorder.RecordEvent(metric))
				_, err := registry.Gather()
				assert.NoError(t, err)
			}
		})
	}
	workers.Wait()
	assertCompleteSampleTotals(t, registry, map[string]float64{"kept": 101, "others": 300}, 2, 10, 7)
}

func TestRequestSamplePolicyHistogramRouting(t *testing.T) {
	for _, source := range []string{"recorder", "result-file"} {
		for _, budget := range []int{0, 1} {
			t.Run(fmt.Sprintf("%s/budget-%d", source, budget), func(t *testing.T) {
				registry, emit := eventPolicyProducer(t, source, budget)
				emit(ShibuyaMetric{Label: "LLM_RESP", Latency: 4500})
				family, label := metricschema.LLMResponseLatency, "LLM_RESP"
				if budget == 0 {
					family, label = metricschema.RequestLatency, "others"
				}
				families := gatherCanonicalMetrics(t, registry)
				require.Len(t, families, 4)
				require.Contains(t, families, family)
				metric := families[family].Metric[0]
				assert.Equal(t, label, nativeLabelValue(metric, "label"))
				assert.Equal(t, float64(4500), metric.GetHistogram().GetSampleSum())
				assert.Equal(t, uint64(1), metric.GetHistogram().GetSampleCount())
			})
		}
	}
	registry := NewMetricsRegistry()
	recorder, err := NewMetricsRecorder(registry, 1)
	require.NoError(t, err)
	recorder.Reset(RunContext{})
	require.NoError(t, recorder.RecordSample(RequestSample{
		Metric: ShibuyaMetric{Label: "llm_resp", Latency: 4500}, HasLatency: true,
	}))
	families := gatherCanonicalMetrics(t, registry)
	require.Len(t, families, 1)
	require.Contains(t, families, metricschema.RequestLatency)
	assert.Equal(t, float64(4500), families[metricschema.RequestLatency].Metric[0].GetHistogram().GetSampleSum())
}

func TestNewMetricsRecorderRejectsInvalidConfiguration(t *testing.T) {
	recorder, err := NewMetricsRecorder(nil, 1)
	require.Error(t, err)
	assert.Nil(t, recorder)
	recorder, err = NewMetricsRecorder(NewMetricsRegistry(), -1)
	require.Error(t, err)
	assert.Nil(t, recorder)
}

func TestMetricsRecorderBoundIdentity(t *testing.T) {
	for _, run := range []struct {
		name string
		run  RunContext
	}{
		{
			name: "assigned",
			run: RunContext{
				CollectionID: "assigned-collection", PlanID: "assigned-plan", RunID: "assigned-run",
				AgentID: "assigned-engine", Zone: "assigned-zone",
			},
		},
		{name: "blank", run: RunContext{}},
		{name: "partially blank", run: RunContext{RunID: "assigned-run", AgentID: "assigned-engine"}},
	} {
		for _, entry := range []struct {
			name   string
			label  string
			record func(*MetricsRecorder, ShibuyaMetric) error
			want   []string
		}{
			{
				name: "sample", label: "llm_resp",
				record: func(recorder *MetricsRecorder, metric ShibuyaMetric) error {
					return recorder.RecordSample(RequestSample{
						Metric: metric, HasLatency: true, HasStatus: true,
						HasReceivedBytes: true, HasConcurrency: true,
					})
				},
				want: []string{
					metricschema.RequestLatency, metricschema.StatusCounter,
					metricschema.EgressBytesCounter, metricschema.ThreadsGauge,
				},
			},
			{
				name: "concurrency-only sample", label: "request",
				record: func(recorder *MetricsRecorder, metric ShibuyaMetric) error {
					return recorder.RecordSample(RequestSample{Metric: metric, HasConcurrency: true})
				},
				want: []string{metricschema.ThreadsGauge},
			},
			{
				name: "request event", label: "request", record: (*MetricsRecorder).RecordEvent,
				want: []string{
					metricschema.RequestLatency, metricschema.StatusCounter,
					metricschema.EgressBytesCounter, metricschema.ThreadsGauge,
				},
			},
			{
				name: "LLM event", label: "LLM_RESP", record: (*MetricsRecorder).RecordEvent,
				want: []string{
					metricschema.LLMResponseLatency, metricschema.StatusCounter,
					metricschema.EgressBytesCounter, metricschema.ThreadsGauge,
				},
			},
			{
				name: "TTFT event", label: "ttft", record: (*MetricsRecorder).RecordEvent,
				want: []string{
					metricschema.RequestLatency, metricschema.StatusCounter,
					metricschema.EgressBytesCounter, metricschema.ThreadsGauge,
				},
			},
			{
				name: "latency", label: "llm_resp", record: (*MetricsRecorder).ObserveLatency,
				want: []string{metricschema.RequestLatency},
			},
			{
				name: "status", label: "request", record: (*MetricsRecorder).RecordStatus,
				want: []string{metricschema.StatusCounter},
			},
			{
				name: "request", label: "llm_resp", record: (*MetricsRecorder).ObserveRequest,
				want: []string{metricschema.RequestLatency, metricschema.StatusCounter},
			},
			{
				name: "request bytes", label: "request", record: (*MetricsRecorder).AddReceivedBytes,
				want: []string{metricschema.EgressBytesCounter},
			},
			{
				name: "aggregate bytes", label: "aggregate", record: (*MetricsRecorder).AddAggregateReceivedBytes,
				want: []string{metricschema.EgressBytesCounter},
			},
			{
				name: "concurrency", label: "ignored",
				record: func(recorder *MetricsRecorder, metric ShibuyaMetric) error {
					recorder.SetConcurrency(metric)
					return nil
				},
				want: []string{metricschema.ThreadsGauge},
			},
		} {
			t.Run(run.name+"/"+entry.name, func(t *testing.T) {
				registry := NewMetricsRegistry()
				recorder, err := NewMetricsRecorder(registry, 1)
				require.NoError(t, err)
				recorder.Reset(run.run)
				for _, source := range []ShibuyaMetric{
					{},
					{
						CollectionID: "source-collection", PlanID: "source-plan", RunID: "source-run",
						EngineID: "source-engine", Zone: "source-zone",
					},
				} {
					source.Label, source.Status = entry.label, "201"
					source.Latency, source.EgressBytes, source.Threads = 11, 13, 7
					require.NoError(t, entry.record(recorder, source))
				}

				server := NewAgentServer(AgentServerOptions{MetricsRegistry: registry, LabelsUsageLimit: 1})
				families := nativeScrape(t, server.metricsHandler())
				require.Len(t, families, len(entry.want), "source identity must not create additional families")
				for _, name := range entry.want {
					require.Contains(t, families, name)
					require.Len(t, families[name].Metric, 1, "source identities must coalesce into the assigned series")
					metric := families[name].Metric[0]
					extra := map[string]string{}
					if name != metricschema.ThreadsGauge {
						extra["label"] = entry.label
					}
					switch name {
					case metricschema.RequestLatency, metricschema.LLMResponseLatency:
						assert.Equal(t, uint64(2), metric.GetHistogram().GetSampleCount())
						assert.Equal(t, float64(22), metric.GetHistogram().GetSampleSum())
					case metricschema.StatusCounter:
						extra["status"] = "201"
						assert.Equal(t, float64(2), metric.GetCounter().GetValue())
					case metricschema.EgressBytesCounter:
						assert.Equal(t, float64(26), metric.GetCounter().GetValue())
					case metricschema.ThreadsGauge:
						assert.Equal(t, float64(7), metric.GetGauge().GetValue())
					}
					assertRecorderMetricLabels(t, metric, run.run, extra)
				}
			})
		}
	}
}

func TestRequestSamplePolicyBoundIdentity(t *testing.T) {
	for _, source := range []string{"recorder", "result-file"} {
		t.Run(source, func(t *testing.T) {
			registry, emit := eventPolicyProducer(t, source, 1)
			for _, metric := range []ShibuyaMetric{
				{},
				{
					CollectionID: "source-collection", PlanID: "source-plan", RunID: "source-run",
					EngineID: "source-engine", Zone: "source-zone",
				},
			} {
				metric.Label, metric.Status = "request", "200"
				metric.Latency, metric.EgressBytes, metric.Threads = 2, 10, 7
				emit(metric)
			}
			assertCompleteSampleTotals(t, registry, map[string]float64{"request": 2}, 2, 10, 7)
			for name, family := range gatherCanonicalMetrics(t, registry) {
				require.Len(t, family.Metric, 1)
				extra := map[string]string{}
				if name != metricschema.ThreadsGauge {
					extra["label"] = "request"
				}
				if name == metricschema.StatusCounter {
					extra["status"] = "200"
				}
				assertRecorderMetricLabels(t, family.Metric[0], RunContext{RunID: "run"}, extra)
			}
		})
	}
}

func assertRecorderMetricLabels(t *testing.T, metric *dto.Metric, run RunContext, extra map[string]string) {
	t.Helper()
	want := map[string]string{
		"collection_id": run.CollectionID,
		"plan_id":       run.PlanID,
		"run_id":        run.RunID,
		"engine_no":     run.AgentID,
		"zone":          run.Zone,
	}
	for name, value := range extra {
		want[name] = value
	}
	got := make(map[string]string, len(metric.Label))
	for _, label := range metric.Label {
		got[label.GetName()] = label.GetValue()
	}
	assert.Equal(t, want, got)
}
