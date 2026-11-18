package agent

import (
	"errors"
	"sync"
	"testing"

	dto "github.com/prometheus/client_model/go"
	"github.com/reqfleet/engine-sdk/metricschema"
	"github.com/reqfleet/engine-sdk/resources"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func resetTestRun(name string) RunContext {
	return RunContext{
		RunID: name, CollectionID: name + "-collection", PlanID: name + "-plan",
		AgentID: name + "-engine", Zone: name + "-zone",
	}
}

func assertResetSnapshotIdentity(t *testing.T, families map[string]*dto.MetricFamily, run RunContext) {
	t.Helper()
	want := map[string]string{
		"run_id": run.RunID, "collection_id": run.CollectionID, "plan_id": run.PlanID,
		"engine_no": run.AgentID, "zone": run.Zone,
	}
	for name, family := range families {
		for _, metric := range family.Metric {
			for label, value := range want {
				assert.Equal(t, value, nativeLabelValue(metric, label), "%s: %s", name, label)
			}
		}
	}
}

func TestMetricsRecorderIndependentRunsRetainEntireSnapshot(t *testing.T) {
	first, next := resetTestRun("first"), resetTestRun("next")
	recorder, err := NewMetricsRecorder(first, 2)
	require.NoError(t, err)
	registry := recorder.Registry()
	sampler := &fakeResourceSampler{results: []resourceSampleResult{
		{err: resources.ErrSampleUnavailable},
		{sample: resources.Sample{CPUCores: 0.5, MemoryBytes: 8192}},
	}}
	registry.resources.sampler = sampler
	for _, label := range []string{"old-request", metricschema.LLMResponseLabel, "old-overflow"} {
		require.NoError(t, recorder.RecordEvent(ShibuyaMetric{
			Label: label, Status: "200", Latency: 30, EgressBytes: 100, Threads: 4,
		}))
	}
	require.NoError(t, recorder.AddAggregateReceivedBytes(ShibuyaMetric{Label: "old-aggregate", EgressBytes: 500}))
	require.NoError(t, registry.sampleResources())
	require.NoError(t, registry.sampleResources())
	retained := gatherResourceSnapshot(t, registry)
	require.Len(t, retained, 7, "both histograms, both counters, concurrency, CPU, and memory are populated")
	assertResetSnapshotIdentity(t, retained, first)
	assert.Equal(t, map[string]float64{
		"old-request/200": 1, "llm_resp/200": 1, "others/200": 1,
	}, nativeCounters(retained[metricschema.StatusCounter]))
	for range 2 {
		assert.Equal(t, retained, gatherResourceSnapshot(t, registry), "final scrapes retain every series")
	}
	assert.Equal(t, 2, sampler.sampleCount(), "gathering retained metrics does not advance the CPU baseline")

	nextRecorder, err := NewMetricsRecorder(next, 2)
	require.NoError(t, err)
	nextRegistry := nextRecorder.Registry()
	require.NotSame(t, registry, nextRegistry)
	assert.Same(t, registry, recorder.Registry(), "a recorder permanently owns its registry")
	nextSampler := &fakeResourceSampler{results: []resourceSampleResult{
		{err: resources.ErrSampleUnavailable},
		{sample: resources.Sample{CPUCores: 0.75, MemoryBytes: 16384}},
	}}
	nextRegistry.resources.sampler = nextSampler
	require.Empty(t, gatherResourceSnapshot(t, nextRegistry))
	require.NoError(t, nextRegistry.sampleResources())
	assert.Empty(t, gatherResourceSnapshot(t, nextRegistry), "the new run cannot reuse the previous resource baseline")
	assert.Equal(t, retained, gatherResourceSnapshot(t, registry))
	for _, label := range []string{"new-request", metricschema.LLMResponseLabel, "new-overflow"} {
		require.NoError(t, nextRecorder.RecordEvent(ShibuyaMetric{
			Label: label, Status: "201", Latency: 7, EgressBytes: 11, Threads: 2,
		}))
	}
	require.NoError(t, nextRecorder.AddAggregateReceivedBytes(ShibuyaMetric{Label: "new-aggregate", EgressBytes: 19}))
	require.NoError(t, nextRegistry.sampleResources())
	fresh := gatherResourceSnapshot(t, nextRegistry)
	require.Len(t, fresh, 7)
	assertResetSnapshotIdentity(t, fresh, next)
	assert.Equal(t, map[string]float64{
		"new-request/201": 1, "llm_resp/201": 1, "others/201": 1,
	}, nativeCounters(fresh[metricschema.StatusCounter]))
	for familyName, count := range map[string]int{metricschema.RequestLatency: 2, metricschema.LLMResponseLatency: 1} {
		require.Contains(t, fresh, familyName)
		require.Len(t, fresh[familyName].Metric, count)
		for _, metric := range fresh[familyName].Metric {
			assert.Equal(t, uint64(1), metric.GetHistogram().GetSampleCount())
			assert.Equal(t, 7.0, metric.GetHistogram().GetSampleSum())
		}
	}
	bytes := make(map[string]float64)
	for _, metric := range fresh[metricschema.EgressBytesCounter].Metric {
		bytes[nativeLabelValue(metric, "label")] = metric.GetCounter().GetValue()
	}
	assert.Equal(t, map[string]float64{"new-request": 11, "llm_resp": 11, "others": 11, "new-aggregate": 19}, bytes)
	for name, value := range map[string]float64{
		metricschema.ThreadsGauge: 2, metricschema.CPUGauge: 0.75, metricschema.MemoryGauge: 16384,
	} {
		require.Contains(t, fresh, name)
		require.Len(t, fresh[name].Metric, 1)
		assert.Equal(t, value, fresh[name].Metric[0].GetGauge().GetValue())
	}
	assert.Equal(t, retained, gatherResourceSnapshot(t, registry), "new-run writes preserve the old final snapshot")
	assert.Equal(t, 2, sampler.sampleCount())
	assert.Equal(t, 2, nextSampler.sampleCount())

	// A retained recorder can still receive stale work, but it cannot write into
	// the next run or reclaim any of that run's independent admission slots.
	require.NoError(t, recorder.RecordEvent(ShibuyaMetric{
		Label: "new-request", Status: "202", Latency: 99, EgressBytes: 123, Threads: 9,
	}))
	require.NoError(t, recorder.ObserveLLMResponse(15000))
	assert.Equal(t, map[string]float64{
		"old-request/200": 1, "llm_resp/200": 1, "others/200": 1, "others/202": 1,
	}, nativeCounters(gatherResourceSnapshot(t, registry)[metricschema.StatusCounter]))
	assert.Equal(t, fresh, gatherResourceSnapshot(t, nextRegistry))
	collectionErr := errors.New("old run collection failed")
	registry.setCollectionError(collectionErr)
	_, err = registry.Gather()
	require.ErrorIs(t, err, collectionErr)
	assert.Equal(t, fresh, gatherResourceSnapshot(t, nextRegistry), "collection errors stay on their owning run")
	_, err = registry.Gather()
	require.ErrorIs(t, err, collectionErr, "gathering another run must not clear the old error")
}

func TestMetricsRecorderInFlightOldSamplingCannotChangeNewRun(t *testing.T) {
	first, next := resetTestRun("first"), resetTestRun("next")
	oldRecorder, err := NewMetricsRecorder(first, 1)
	require.NoError(t, err)
	oldRegistry := oldRecorder.Registry()
	require.NoError(t, oldRecorder.RecordEvent(ShibuyaMetric{
		Label: "old", Status: "200", Latency: 20, EgressBytes: 40, Threads: 3,
	}))

	release := make(chan struct{})
	releaseSample := sync.OnceFunc(func() { close(release) })
	t.Cleanup(releaseSample)
	sampler := &fakeResourceSampler{
		results: []resourceSampleResult{{
			sample: resources.Sample{CPUCores: 0.5, MemoryBytes: 8192}, release: release,
		}},
		entered: make(chan int, 1),
	}
	oldRegistry.resources.sampler = sampler
	sampled := make(chan error, 1)
	go func() { sampled <- oldRegistry.sampleResources() }()
	<-sampler.entered

	// The old sample is deliberately still in flight while the next run records
	// and gathers its own measurements. No reset lock or timing assumption is needed.
	nextRecorder, err := NewMetricsRecorder(next, 1)
	require.NoError(t, err)
	nextRegistry := nextRecorder.Registry()
	nextRegistry.resources.sampler = &fakeResourceSampler{results: []resourceSampleResult{
		{sample: resources.Sample{CPUCores: 0.75, MemoryBytes: 16384}},
	}}
	require.NoError(t, nextRecorder.RecordEvent(ShibuyaMetric{
		Label: "new", Status: "201", Latency: 7, EgressBytes: 13, Threads: 9,
	}))
	require.NoError(t, nextRegistry.sampleResources())
	fresh := gatherResourceSnapshot(t, nextRegistry)
	require.Len(t, fresh, 6)
	assertResetSnapshotIdentity(t, fresh, next)
	releaseSample()
	require.NoError(t, <-sampled)

	var workers sync.WaitGroup
	for range 4 {
		workers.Go(func() {
			for range 25 {
				assert.NoError(t, oldRecorder.RecordEvent(ShibuyaMetric{
					Label: "new", Status: "202", Latency: 2, EgressBytes: 5, Threads: 1,
				}))
				_, err := oldRegistry.Gather()
				assert.NoError(t, err)
				_, err = nextRegistry.Gather()
				assert.NoError(t, err)
			}
		})
	}
	workers.Wait()
	old := gatherResourceSnapshot(t, oldRegistry)
	assertResetSnapshotIdentity(t, old, first)
	assert.Equal(t, map[string]float64{"old/200": 1, "others/202": 100}, nativeCounters(old[metricschema.StatusCounter]))
	assert.Equal(t, 0.5, old[metricschema.CPUGauge].Metric[0].GetGauge().GetValue())
	assert.Equal(t, 8192.0, old[metricschema.MemoryGauge].Metric[0].GetGauge().GetValue())
	assert.Equal(t, fresh, gatherResourceSnapshot(t, nextRegistry))
	assert.Equal(t, map[string]float64{"new/201": 1}, nativeCounters(fresh[metricschema.StatusCounter]))
	assert.Equal(t, uint64(1), fresh[metricschema.RequestLatency].Metric[0].GetHistogram().GetSampleCount())
	assert.Equal(t, 7.0, fresh[metricschema.RequestLatency].Metric[0].GetHistogram().GetSampleSum())
	assert.Equal(t, 13.0, fresh[metricschema.EgressBytesCounter].Metric[0].GetCounter().GetValue())
	for name, value := range map[string]float64{
		metricschema.ThreadsGauge: 9, metricschema.CPUGauge: 0.75, metricschema.MemoryGauge: 16384,
	} {
		assert.Equal(t, value, fresh[name].Metric[0].GetGauge().GetValue())
	}
}
