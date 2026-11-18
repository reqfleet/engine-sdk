package agent

import dto "github.com/prometheus/client_model/go"

// runMetricsEntry retains the completed snapshot after result-event admission
// stops. Accepting a new run ID removes the entry without resetting its collectors.
type runMetricsEntry struct {
	recorder  *MetricsRecorder
	recording bool // Completion stops admission but retains the entry for final scrapes.
}

func (as *AgentServer) clearMetricsRuns() {
	as.metricsMu.Lock()
	clear(as.metricsByRun)
	as.metricsMu.Unlock()
}

// prepareMetricsForRun initializes metrics after launch, not during dependency preparation.
func (as *AgentServer) prepareMetricsForRun(run *RunContext) {
	// MetricsHandler owns the complete normalized exposition and request-name budget.
	// /metrics invokes it directly, so no SDK recorder or registry is needed.
	if as.options.MetricsHandler != nil {
		return
	}
	// MetricsURL supplies the complete exposition from an HTTP(S) exporter.
	// /metrics proxies it without merging SDK collectors, so no local registry is needed.
	if as.options.MetricsURL != "" {
		if as.options.FoldNativeMetrics {
			// Reset folding's run identity and admitted request names. Scrape-time
			// folding validates each cumulative snapshot and combines excess names
			// into the overflow label to enforce the budget; it does not accumulate
			// values across scrapes. The exporter still owns the counters.
			as.nativeMetricsLock.Lock()
			as.nativeMetricsRun = run.RunID
			as.nativeLabels = make(map[string]struct{})
			as.nativeMetricsLock.Unlock()
		}
		return
	}

	// The request-name budget was validated before accepting this run. No
	// fallible initialization remains after the workload process has started.
	run.Metrics = newMetricsRecorder(*run, newRequestsNamesStore(as.options.LabelsUsageLimit))

	as.metricsMu.Lock()
	as.metricsByRun[run.RunID] = runMetricsEntry{recorder: run.Metrics, recording: true}
	as.metricsMu.Unlock()
}

func (as *AgentServer) finishMetricsRun(runID string) {
	as.metricsMu.Lock()
	if entry, exists := as.metricsByRun[runID]; exists {
		entry.recording = false
		as.metricsByRun[runID] = entry
	}
	as.metricsMu.Unlock()
}

func (as *AgentServer) recordingMetrics(runID string) *MetricsRecorder {
	as.metricsMu.RLock()
	entry := as.metricsByRun[runID]
	as.metricsMu.RUnlock()
	if !entry.recording {
		return nil
	}
	return entry.recorder
}

func (as *AgentServer) gatherMetrics() ([]*dto.MetricFamily, error) {
	var recorder *MetricsRecorder
	as.metricsMu.RLock()
	for _, entry := range as.metricsByRun {
		recorder = entry.recorder
		break
	}
	as.metricsMu.RUnlock()
	if recorder == nil {
		return nil, nil
	}
	// A scrape keeps this run alive even if a new accepted ID removes its entry.
	return recorder.Registry().Gather()
}
