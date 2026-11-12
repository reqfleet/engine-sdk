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
	server.publishResourceMetrics("first", resources.Sample{}, false)
	assert.Empty(t, gather(), "baseline-only attempts omit both resources")
	server.publishResourceMetrics("first", resources.Sample{
		CPUCores: 0.5, MemoryBytes: 8192,
	}, true)
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

	server.publishResourceMetrics("second", resources.Sample{}, false)
	all = gather()
	require.Len(t, all[metricschema.CPUGauge].Metric, 1, "a new run must not inherit a rate")
	require.Len(t, all[metricschema.MemoryGauge].Metric, 1, "a new baseline must not publish memory")
	server.publishResourceMetrics("first", resources.Sample{}, false)
	assert.Empty(t, gather(), "a counter decrease must remove both stale resource gauges")
	server.publishResourceMetrics("second", resources.Sample{}, true)
	all = gather()
	require.Contains(t, all, metricschema.CPUGauge)
	require.Contains(t, all, metricschema.MemoryGauge)
	assert.Zero(t, all[metricschema.CPUGauge].Metric[0].GetGauge().GetValue())
	assert.Zero(t, all[metricschema.MemoryGauge].Metric[0].GetGauge().GetValue())
}
