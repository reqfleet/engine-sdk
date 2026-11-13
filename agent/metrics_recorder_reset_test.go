package agent

import (
	"errors"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
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

func TestMetricsRecorderResetRetainsThenReplacesEntireRun(t *testing.T) {
	registry := NewMetricsRegistry()
	sampler := &fakeResourceSampler{results: []resourceSampleResult{
		{err: resources.ErrSampleUnavailable},
		{sample: resources.Sample{CPUCores: 0.5, MemoryBytes: 8192}},
	}}
	registry.resources.sampler = sampler
	recorder, err := NewMetricsRecorder(registry, 2)
	require.NoError(t, err)
	first, next := resetTestRun("first"), resetTestRun("next")
	recorder.Reset(first)
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
		assert.Equal(t, retained, gatherResourceSnapshot(t, registry), "completion alone does not clear any series")
	}
	assert.Equal(t, 2, sampler.sampleCount(), "gathering retained metrics does not advance the CPU baseline")

	otherRegistry := NewMetricsRegistry()
	otherSampler := &fakeResourceSampler{results: []resourceSampleResult{
		{sample: resources.Sample{CPUCores: 2, MemoryBytes: 1024}},
	}}
	otherRegistry.resources.sampler = otherSampler
	other, err := NewMetricsRecorder(otherRegistry, 1)
	require.NoError(t, err)
	other.Reset(resetTestRun("independent"))
	require.NoError(t, other.RecordEvent(ShibuyaMetric{Label: "kept", Status: "202", Latency: 3, EgressBytes: 9, Threads: 2}))
	require.NoError(t, otherRegistry.sampleResources())
	otherSnapshot := gatherResourceSnapshot(t, otherRegistry)

	collectionErr := errors.New("final collection failed")
	registry.setCollectionError(collectionErr)
	_, err = registry.Gather()
	require.ErrorIs(t, err, collectionErr)
	// The existing sampler seam models baseline-only reads, not a second CPU-rate implementation.
	sampler.results = []resourceSampleResult{
		{err: resources.ErrSampleUnavailable},
		{sample: resources.Sample{CPUCores: 0.75, MemoryBytes: 16384}},
	}
	recorder.Reset(next)
	assert.Empty(t, gatherResourceSnapshot(t, registry), "Reset clears every collector and the collection error")
	assert.Equal(t, 2, sampler.resets)
	assert.Equal(t, 2, sampler.sampleCount(), "Reset does not sample resources")
	assert.Equal(t, otherSnapshot, gatherResourceSnapshot(t, otherRegistry))
	assert.Equal(t, 1, otherSampler.resets)
	assert.Equal(t, 1, otherSampler.sampleCount())

	require.NoError(t, registry.sampleResources())
	assert.Empty(t, gatherResourceSnapshot(t, registry), "the next run cannot publish a rate from the previous baseline")
	for _, label := range []string{"new-request", metricschema.LLMResponseLabel, "new-overflow"} {
		require.NoError(t, recorder.RecordEvent(ShibuyaMetric{
			Label: label, Status: "201", Latency: 7, EgressBytes: 11, Threads: 2,
		}))
	}
	require.NoError(t, recorder.AddAggregateReceivedBytes(ShibuyaMetric{Label: "new-aggregate", EgressBytes: 19}))
	require.NoError(t, registry.sampleResources())
	fresh := gatherResourceSnapshot(t, registry)
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
	assert.Equal(t, otherSnapshot, gatherResourceSnapshot(t, otherRegistry))
	// Its original admission slot is retained too, rather than just its collectors.
	require.NoError(t, other.RecordStatus(ShibuyaMetric{Label: "extra", Status: "203"}))
	assert.Equal(t, map[string]float64{"kept/202": 1, "others/203": 1},
		nativeCounters(gatherResourceSnapshot(t, otherRegistry)[metricschema.StatusCounter]))
}

// These gates stop inside the existing registry sampler and Prometheus gather
// seams. They control the transition without sleeping or depending on CPU reads.
type resetBarrierSampler struct {
	*fakeResourceSampler
	entered chan struct{}
	release <-chan struct{}
}

func (s *resetBarrierSampler) Reset() {
	s.fakeResourceSampler.Reset()
	close(s.entered)
	<-s.release
}

type resetBarrierCollector struct {
	once    sync.Once
	entered chan struct{}
	release <-chan struct{}
}

func (c *resetBarrierCollector) Describe(out chan<- *prometheus.Desc) {
	out <- prometheus.NewDesc("recorder_reset_test_barrier", "Gather synchronization only", nil, nil)
}

func (c *resetBarrierCollector) Collect(chan<- prometheus.Metric) {
	c.once.Do(func() {
		close(c.entered)
		<-c.release
	})
}

func TestMetricsRecorderResetSynchronizesSamplingGatherAndUpdates(t *testing.T) {
	registry := NewMetricsRegistry()
	recorder, err := NewMetricsRecorder(registry, 1)
	require.NoError(t, err)
	first, next := resetTestRun("first"), resetTestRun("next")
	recorder.Reset(first)
	require.NoError(t, recorder.RecordEvent(ShibuyaMetric{Label: "old", Status: "200", Latency: 20, EgressBytes: 40, Threads: 3}))

	sampleRelease, gatherRelease, resetRelease := make(chan struct{}), make(chan struct{}), make(chan struct{})
	releaseSample := sync.OnceFunc(func() { close(sampleRelease) })
	releaseGather := sync.OnceFunc(func() { close(gatherRelease) })
	releaseReset := sync.OnceFunc(func() { close(resetRelease) })
	t.Cleanup(releaseSample)
	t.Cleanup(releaseGather)
	t.Cleanup(releaseReset)
	sampler := &resetBarrierSampler{
		fakeResourceSampler: &fakeResourceSampler{
			results: []resourceSampleResult{{sample: resources.Sample{CPUCores: 0.5, MemoryBytes: 8192}, release: sampleRelease}},
			entered: make(chan int, 2),
		},
		entered: make(chan struct{}), release: resetRelease,
	}
	registry.resources.sampler = sampler
	collector := &resetBarrierCollector{entered: make(chan struct{}), release: gatherRelease}
	registry.registry.MustRegister(collector)

	sampled := make(chan error, 1)
	go func() { sampled <- registry.sampleResources() }()
	<-sampler.fakeResourceSampler.entered
	type gatherResult struct {
		families []*dto.MetricFamily
		err      error
	}
	gathered := make(chan gatherResult, 1)
	go func() {
		families, err := registry.Gather()
		gathered <- gatherResult{families: families, err: err}
	}()
	<-collector.entered
	resetStarted, resetDone := make(chan struct{}), make(chan struct{})
	go func() {
		close(resetStarted)
		recorder.Reset(next)
		close(resetDone)
	}()
	<-resetStarted
	// Old sampling and the old scrape finish before the transition can clear them.
	releaseSample()
	require.NoError(t, <-sampled)
	releaseGather()
	old := <-gathered
	require.NoError(t, old.err)
	oldFamilies := make(map[string]*dto.MetricFamily)
	for _, family := range old.families {
		oldFamilies[family.GetName()] = family
	}
	assertResetSnapshotIdentity(t, oldFamilies, first)
	require.Contains(t, oldFamilies, metricschema.StatusCounter)
	assert.Equal(t, map[string]float64{"old/200": 1}, nativeCounters(oldFamilies[metricschema.StatusCounter]))
	<-sampler.entered // Reset now holds its transition locks inside sampler.Reset.

	operations := []func() error{
		func() error { return recorder.ObserveRequest(ShibuyaMetric{Label: "new", Status: "201", Latency: 7}) },
		func() error {
			return recorder.AddAggregateReceivedBytes(ShibuyaMetric{Label: "aggregate", EgressBytes: 13})
		},
		func() error {
			recorder.SetConcurrency(ShibuyaMetric{Threads: 9})
			return nil
		},
		registry.sampleResources,
	}
	started, finished := make(chan struct{}, len(operations)), make(chan error, len(operations))
	for _, operation := range operations {
		go func() {
			started <- struct{}{}
			finished <- operation()
		}()
	}
	for range operations {
		<-started
	}
	scrapeStarted := make(chan struct{})
	go func() {
		close(scrapeStarted)
		families, err := registry.Gather()
		gathered <- gatherResult{families: families, err: err}
	}()
	<-scrapeStarted
	releaseReset()
	<-resetDone
	for range operations {
		require.NoError(t, <-finished)
	}
	during := <-gathered
	require.NoError(t, during.err)
	concurrentFamilies := make(map[string]*dto.MetricFamily)
	for _, family := range during.families {
		concurrentFamilies[family.GetName()] = family
	}
	// Ordinary updates need not appear atomically across families, but a scrape
	// admitted after Reset cannot include any previous-run identity or values.
	assertResetSnapshotIdentity(t, concurrentFamilies, next)
	fresh := gatherResourceSnapshot(t, registry)
	assertResetSnapshotIdentity(t, fresh, next)
	require.Contains(t, fresh, metricschema.StatusCounter)
	assert.Equal(t, map[string]float64{"new/201": 1}, nativeCounters(fresh[metricschema.StatusCounter]))
	require.Contains(t, fresh, metricschema.RequestLatency)
	require.Len(t, fresh[metricschema.RequestLatency].Metric, 1)
	assert.Equal(t, uint64(1), fresh[metricschema.RequestLatency].Metric[0].GetHistogram().GetSampleCount())
	assert.Equal(t, 7.0, fresh[metricschema.RequestLatency].Metric[0].GetHistogram().GetSampleSum())
	require.Contains(t, fresh, metricschema.EgressBytesCounter)
	require.Len(t, fresh[metricschema.EgressBytesCounter].Metric, 1)
	assert.Equal(t, "aggregate", nativeLabelValue(fresh[metricschema.EgressBytesCounter].Metric[0], "label"))
	assert.Equal(t, 13.0, fresh[metricschema.EgressBytesCounter].Metric[0].GetCounter().GetValue())
	for name, value := range map[string]float64{
		metricschema.ThreadsGauge: 9, metricschema.CPUGauge: 0.5, metricschema.MemoryGauge: 8192,
	} {
		require.Contains(t, fresh, name)
		require.Len(t, fresh[name].Metric, 1)
		assert.Equal(t, value, fresh[name].Metric[0].GetGauge().GetValue())
	}
}
