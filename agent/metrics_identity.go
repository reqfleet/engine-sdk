package agent

// metricIdentity contains only assignment strings, never mutable callback data.
type metricIdentity struct {
	collectionID string
	planID       string
	runID        string
	agentID      string
	zone         string
}

func identityFromRun(run RunContext) metricIdentity {
	return metricIdentity{
		collectionID: run.CollectionID,
		planID:       run.PlanID,
		runID:        run.RunID,
		agentID:      run.AgentID,
		zone:         run.Zone,
	}
}

func identityFromMeta(meta EngineMeta, runID string) metricIdentity {
	return metricIdentity{
		collectionID: meta.CollectionID,
		planID:       meta.PlanID,
		runID:        runID,
		agentID:      meta.EngineID,
		zone:         meta.Zone,
	}
}

// bind makes assignment authoritative, including empty assigned fields.
func (identity *metricIdentity) bind(metric *ShibuyaMetric) {
	metric.CollectionID = identity.collectionID
	metric.PlanID = identity.planID
	metric.RunID = identity.runID
	metric.EngineID = identity.agentID
	metric.Zone = identity.zone
}
