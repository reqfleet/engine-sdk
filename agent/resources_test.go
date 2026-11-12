package agent

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/reqfleet/engine-sdk/metricschema"
	"github.com/reqfleet/engine-sdk/protocol"
	"github.com/reqfleet/engine-sdk/resources"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type resourceSampleResult struct {
	sample resources.Sample
	ok     bool
	err    error
}

type fakeResourceSampler struct {
	results []resourceSampleResult
	index   int
	calls   int
	resets  int
}

func (s *fakeResourceSampler) Sample() (resources.Sample, bool, error) {
	s.calls++
	if s.index >= len(s.results) {
		return resources.Sample{}, false, errors.New("unexpected resource sample")
	}
	result := s.results[s.index]
	s.index++
	return result.sample, result.ok, result.err
}

func (s *fakeResourceSampler) Reset() {
	s.index = 0
	s.resets++
}

func TestResourceMetricsOmitUnavailableRatesAndPreserveRunIdentity(t *testing.T) {
	meta := EngineMeta{CollectionID: "collection", PlanID: "plan", Zone: "zone", EngineID: "engine"}
	server := NewAgentServer(AgentServerOptions{EngineMeta: meta})
	registry := server.MetricsRegistry()
	sampler := &fakeResourceSampler{results: []resourceSampleResult{
		{sample: resources.Sample{MemoryBytes: 4096}},
		{sample: resources.Sample{CPUCores: 0.5, MemoryBytes: 8192}, ok: true},
		{}, // A counter decrease invalidates both previously published values.
		{ok: true},
	}}
	registry.resources.sampler = sampler
	handler := server.metricsHandler()
	assert.Empty(t, nativeScrape(t, handler), "before launch there is no resource identity")
	assert.Zero(t, sampler.calls)

	server.startMetricsRun("first")
	assert.Equal(t, 1, sampler.resets)
	families, err := registry.Gather()
	require.NoError(t, err)
	assert.Empty(t, families)
	assert.Zero(t, sampler.calls, "Gather must not sample resources")
	assert.Empty(t, nativeScrape(t, handler), "baseline-only attempts omit both resources")
	all := nativeScrape(t, handler)
	require.Contains(t, all, metricschema.CPUGauge)
	require.Contains(t, all, metricschema.MemoryGauge)
	for name, value := range map[string]float64{metricschema.CPUGauge: 0.5, metricschema.MemoryGauge: 8192} {
		require.Len(t, all[name].Metric, 1)
		metric := all[name].Metric[0]
		assert.Equal(t, value, metric.GetGauge().GetValue())
		labels := make(map[string]string)
		for _, pair := range metric.Label {
			labels[pair.GetName()] = pair.GetValue()
		}
		assert.Equal(t, map[string]string{
			"collection_id": "collection", "plan_id": "plan", "run_id": "first", "zone": "zone", "engine_no": "engine",
		}, labels)
	}
	assert.Empty(t, nativeScrape(t, handler), "a counter decrease removes both stale gauges")
	all = nativeScrape(t, handler)
	require.Contains(t, all, metricschema.CPUGauge)
	require.Contains(t, all, metricschema.MemoryGauge)
	assert.Zero(t, all[metricschema.CPUGauge].Metric[0].GetGauge().GetValue())
	assert.Zero(t, all[metricschema.MemoryGauge].Metric[0].GetGauge().GetValue())

	server.startMetricsRun("second")
	assert.Equal(t, 2, sampler.resets)
	all = nativeScrape(t, handler)
	for _, name := range []string{metricschema.CPUGauge, metricschema.MemoryGauge} {
		require.Len(t, all[name].Metric, 1, "a new baseline must not publish either resource")
		assert.Equal(t, "first", nativeLabelValue(all[name].Metric[0], "run_id"))
	}
	all = nativeScrape(t, handler)
	for _, name := range []string{metricschema.CPUGauge, metricschema.MemoryGauge} {
		require.Len(t, all[name].Metric, 2, "completed-run series remain until explicit reset")
	}

	registry.Reset()
	assert.Equal(t, 3, sampler.resets)
	calls := sampler.calls
	assert.Empty(t, nativeScrape(t, handler))
	assert.Equal(t, calls, sampler.calls, "Reset clears the retained resource identity")
	server.startMetricsRun("third")
	assert.Empty(t, nativeScrape(t, handler), "the next run starts with a fresh baseline")
}

func TestResourceMetricsErrorsFailTheWholeScrape(t *testing.T) {
	for _, published := range []bool{false, true} {
		name := "before first sample"
		if published {
			name = "after successful sample"
		}
		t.Run(name, func(t *testing.T) {
			server := NewAgentServer(AgentServerOptions{})
			registry := server.MetricsRegistry()
			sampler := &fakeResourceSampler{}
			if published {
				sampler.results = append(sampler.results, resourceSampleResult{
					sample: resources.Sample{CPUCores: 0.5, MemoryBytes: 8192}, ok: true,
				})
			}
			sampler.results = append(sampler.results, resourceSampleResult{
				sample: resources.Sample{CPUCores: 99, MemoryBytes: 999}, ok: true,
				err: errors.New("resource read failed"),
			})
			registry.resources.sampler = sampler
			server.startMetricsRun("run")
			registry.RecordEvent(ShibuyaMetric{RunID: "run", Label: "checkout", Status: "200", Latency: 1})
			handler := server.metricsHandler()
			if published {
				require.Contains(t, nativeScrape(t, handler), metricschema.CPUGauge)
			}
			before, err := registry.Gather()
			require.NoError(t, err)
			response := nativeResponse(handler)
			assert.Equal(t, http.StatusInternalServerError, response.Code)
			assert.NotContains(t, response.Body.String(), metricschema.CPUGauge)
			assert.NotContains(t, response.Body.String(), metricschema.MemoryGauge)
			assert.NotContains(t, response.Body.String(), metricschema.StatusCounter, "no partial or fallback exposition")
			after, err := registry.Gather()
			require.NoError(t, err)
			assert.Equal(t, before, after, "failed samples must not partially publish resource values")
		})
	}
}

func TestResourceMetricsRetainCompletedRun(t *testing.T) {
	fixture := newAgentFixture(t, AgentServerOptions{}, nil)
	sampler := &fakeResourceSampler{results: []resourceSampleResult{
		{},
		{sample: resources.Sample{CPUCores: 0.5, MemoryBytes: 8192}, ok: true},
	}}
	fixture.server.MetricsRegistry().resources.sampler = sampler
	fixture.send(runMessage(protocol.VerbStart, "run-1", ""))
	waitRunState(t, fixture, "run-1", protocol.RunRunning)
	fixture.server.stopRun("run-1", protocol.StopCompleted)
	waitRunState(t, fixture, "run-1", protocol.RunCompleted)
	handler := fixture.server.metricsHandler()
	assert.Empty(t, nativeScrape(t, handler))
	all := nativeScrape(t, handler)
	for _, name := range []string{metricschema.CPUGauge, metricschema.MemoryGauge} {
		require.Contains(t, all, name)
		assert.Equal(t, "run-1", nativeLabelValue(all[name].Metric[0], "run_id"), "final scrapes retain completed identity")
	}
}

func TestNativeMetricsNeverSampleSDKResources(t *testing.T) {
	const body = "adapter_ready 1\n"
	exporter := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(body))
	})
	source := httptest.NewServer(exporter)
	defer source.Close()
	for _, test := range []struct {
		name    string
		options AgentServerOptions
	}{
		{"handler", AgentServerOptions{MetricsHandler: exporter, NativeMetricsSchema: MetricsSchemaShibuyaV1}},
		{"proxy", AgentServerOptions{MetricsURL: source.URL, NativeMetricsSchema: MetricsSchemaShibuyaV1}},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := NewAgentServer(test.options)
			require.Nil(t, server.MetricsRegistry())
			// Poison the internal registry to detect accidental sampling or merging
			// even if a native server ever acquires SDK state internally.
			server.metrics = NewMetricsRegistry()
			sampler := &fakeResourceSampler{}
			server.metrics.resources.sampler = sampler
			server.metrics.startResourceRun(EngineMeta{}, "sdk-run")
			server.metrics.RecordEvent(ShibuyaMetric{Label: "sdk-only", Status: "200"})
			server.startMetricsRun("native-run")
			response := nativeResponse(server.metricsHandler())
			assert.Equal(t, http.StatusOK, response.Code)
			assert.Equal(t, body, response.Body.String())
			assert.Zero(t, sampler.calls)
		})
	}
}
