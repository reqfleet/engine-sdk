package agent

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/reqfleet/engine-sdk/metricschema"
	"github.com/reqfleet/engine-sdk/resources"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResourceMetricsOmitUnavailableRatesAndPreserveRunIdentity(t *testing.T) {
	cpuGauge.Reset()
	memGauge.Reset()
	t.Cleanup(func() { cpuGauge.Reset(); memGauge.Reset() })
	registry := prometheus.NewRegistry()
	registry.MustRegister(cpuGauge, memGauge)
	server := NewAgentServer(AgentServerOptions{EngineMeta: EngineMeta{
		CollectionID: "collection", PlanID: "plan", Zone: "zone", EngineID: "engine",
	}})
	gather := func() map[string]*dto.MetricFamily {
		t.Helper()
		families, err := registry.Gather()
		require.NoError(t, err)
		result := make(map[string]*dto.MetricFamily, len(families))
		for _, family := range families {
			result[family.GetName()] = family
		}
		return result
	}
	memoryOnly := resources.Sample{MemoryBytes: 8192, MemoryValid: true}
	server.publishResourceMetrics("first", memoryOnly)
	assert.NotContains(t, gather(), metricschema.CPUGauge)
	server.publishResourceMetrics("first", resources.Sample{
		CPUCores: 0.5, CPUValid: true, MemoryBytes: 8192, MemoryValid: true,
	})
	all := gather()
	require.Contains(t, all, metricschema.CPUGauge)
	cpu := all[metricschema.CPUGauge].Metric[0]
	assert.Equal(t, 0.5, cpu.GetGauge().GetValue())
	assert.Equal(t, float64(8192), all[metricschema.MemoryGauge].Metric[0].GetGauge().GetValue())
	labels := map[string]string{}
	for _, pair := range cpu.Label {
		labels[pair.GetName()] = pair.GetValue()
	}
	assert.Equal(t, map[string]string{
		"collection_id": "collection", "plan_id": "plan", "run_id": "first", "zone": "zone", "engine_no": "engine",
	}, labels)

	server.publishResourceMetrics("second", memoryOnly)
	all = gather()
	require.Len(t, all[metricschema.CPUGauge].Metric, 1, "a new run must not inherit a rate")
	require.Len(t, all[metricschema.MemoryGauge].Metric, 2, "retain the completed run's memory")
	server.publishResourceMetrics("first", memoryOnly)
	assert.NotContains(t, gather(), metricschema.CPUGauge, "a counter decrease must remove the stale rate")
	server.publishResourceMetrics("second", resources.Sample{})
	require.Len(t, gather()[metricschema.MemoryGauge].Metric, 1, "failed memory must not retain a stale value")
}
