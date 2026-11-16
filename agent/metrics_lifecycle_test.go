//go:build unix

package agent

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/reqfleet/engine-sdk/metricschema"
	"github.com/reqfleet/engine-sdk/protocol"
	"github.com/reqfleet/engine-sdk/resources"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResultFileMetricsLifecycle(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	missingExecutable := filepath.Join(t.TempDir(), "missing-engine")
	registry := NewMetricsRegistry()
	sampler := &fakeResourceSampler{results: []resourceSampleResult{
		{sample: resources.Sample{CPUCores: 0.5, MemoryBytes: 8192}},
		{sample: resources.Sample{CPUCores: 0.5, MemoryBytes: 8192}},
	}}
	registry.resources.sampler = sampler
	f := newAgentFixture(t, AgentServerOptions{
		ResultFile: "results.txt", LabelsUsageLimit: 1, MetricsRegistry: registry,
		BuildCommand: func(_ context.Context, run RunContext) (Command, error) {
			if run.RunID == "failed" {
				return Command{Command: missingExecutable}, nil
			}
			return Command{Command: "sh", Args: []string{
				"-c", `printf '%s\n' "$2" > "$1"; exec sleep 30`, "sh", run.ResultFile, run.RunID,
			}}, nil
		},
		MetricParser: func(line string) (ShibuyaMetric, error) {
			if line == "first" {
				close(entered)
				<-release
				line = metricschema.LLMResponseLabel
			}
			return ShibuyaMetric{
				Label: line, Status: "200", Latency: 7, EgressBytes: 11, Threads: 2,
				CollectionID: "source", PlanID: "source", RunID: "source", EngineID: "source", Zone: "source",
			}, nil
		},
	}, nil)
	unblock := sync.OnceFunc(func() { close(release) })
	t.Cleanup(unblock)
	handler := f.server.metricsHandler()
	assigned := func(runID string) RunContext {
		return RunContext{RunID: runID, CollectionID: "collection-1", PlanID: "plan-1", AgentID: "0", Zone: "zone-1"}
	}

	f.send(runMessage(protocol.VerbStart, "first", ""))
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("result-file parser did not receive the workload output")
	}
	f.server.stopRun("first", protocol.StopCompleted)
	assert.Equal(t, protocol.RunStopping, readProgressForTest(t, f.server).State)
	require.Error(t, f.server.queueStart(runMessage(protocol.VerbStart, "next", "")), "in-flight parsing retains run ownership")
	unblock()
	waitRunState(t, f, "first", protocol.RunCompleted)
	completed := nativeScrape(t, handler)
	require.Contains(t, completed, metricschema.StatusCounter)
	assert.Equal(t, map[string]float64{"llm_resp/200": 1}, nativeCounters(completed[metricschema.StatusCounter]))
	require.Contains(t, completed, metricschema.LLMResponseLatency)
	assert.Equal(t, uint64(1), completed[metricschema.LLMResponseLatency].Metric[0].GetHistogram().GetSampleCount())
	assert.Equal(t, 7.0, completed[metricschema.LLMResponseLatency].Metric[0].GetHistogram().GetSampleSum())
	assert.NotContains(t, completed, metricschema.RequestLatency)
	require.Contains(t, completed, metricschema.CPUGauge)
	require.Contains(t, completed, metricschema.MemoryGauge)
	assertResetSnapshotIdentity(t, completed, assigned("first"))
	f.server.makePromMetrics(metricEvent{runID: "first", line: "late"})
	assert.Equal(t, completed, nativeScrape(t, handler), "completion retains the final snapshot and rejects late events")

	f.send(runMessage(protocol.VerbStart, "failed", ""))
	status := waitRunState(t, f, "failed", protocol.RunFailed)
	assert.Equal(t, protocol.FailureStartup, status.FailurePhase)
	assert.Equal(t, completed, nativeScrape(t, handler), "a failed launch does not reset completed metrics")

	registry.setCollectionError(errors.New("previous collection failure"))
	sampler.results = []resourceSampleResult{
		{sample: resources.Sample{CPUCores: 0.75, MemoryBytes: 16384}},
		{sample: resources.Sample{CPUCores: 0.75, MemoryBytes: 16384}},
	}
	f.send(runMessage(protocol.VerbStart, "next", ""))
	waitRunState(t, f, "next", protocol.RunRunning)
	require.Eventually(t, func() bool {
		families := nativeScrape(t, handler)
		return families[metricschema.StatusCounter] != nil
	}, 5*time.Second, time.Millisecond)
	f.server.makePromMetrics(metricEvent{runID: "first", line: "stale"})
	f.server.finishResultMetricsRun("first")
	f.server.makePromMetrics(metricEvent{runID: "next", line: "overflow"})
	f.server.stopRun("next", protocol.StopCompleted)
	waitRunState(t, f, "next", protocol.RunCompleted)
	fresh := nativeScrape(t, handler)
	assertResetSnapshotIdentity(t, fresh, assigned("next"))
	assert.NotContains(t, fresh, metricschema.LLMResponseLatency)
	require.Contains(t, fresh, metricschema.StatusCounter)
	assert.Equal(t, map[string]float64{"next/200": 1, "others/200": 1}, nativeCounters(fresh[metricschema.StatusCounter]))
	require.Contains(t, fresh, metricschema.RequestLatency)
	require.Len(t, fresh[metricschema.RequestLatency].Metric, 2)
	for _, metric := range fresh[metricschema.RequestLatency].Metric {
		assert.Equal(t, uint64(1), metric.GetHistogram().GetSampleCount())
		assert.Equal(t, 7.0, metric.GetHistogram().GetSampleSum())
	}
	require.Contains(t, fresh, metricschema.EgressBytesCounter)
	assert.Equal(t, map[string]float64{"next": 11, "others": 11}, nativeCounters(fresh[metricschema.EgressBytesCounter]))
	for name, want := range map[string]float64{
		metricschema.ThreadsGauge: 2, metricschema.CPUGauge: 0.75, metricschema.MemoryGauge: 16384,
	} {
		require.Contains(t, fresh, name)
		require.Len(t, fresh[name].Metric, 1)
		assert.Equal(t, want, fresh[name].Metric[0].GetGauge().GetValue())
	}
	assert.Equal(t, 2, sampler.resets, "each successful launch resets the resource baseline exactly once")
	assert.Equal(t, fresh, nativeScrape(t, handler), "scrapes retain the new run without advancing collection")
}
