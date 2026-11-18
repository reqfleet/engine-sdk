package agent

import "testing"

func newTestMetricsRun(t *testing.T, server *AgentServer, runID string) RunContext {
	t.Helper()
	meta := server.options.EngineMeta
	run := RunContext{
		RunID: runID, CollectionID: meta.CollectionID, PlanID: meta.PlanID,
		AgentID: meta.EngineID, Zone: meta.Zone,
	}
	server.clearMetricsRuns()
	return run
}

func startTestMetricsRun(t *testing.T, server *AgentServer, runID string) RunContext {
	t.Helper()
	run := newTestMetricsRun(t, server, runID)
	server.prepareMetricsForRun(&run)
	return run
}
