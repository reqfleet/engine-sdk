package agent

import (
	"context"
	"errors"
	"time"

	"github.com/reqfleet/engine-sdk/resources"
)

type resourceSampler interface {
	Sample() (resources.Sample, error)
}

type resourceMetrics struct {
	sampler  resourceSampler
	identity metricIdentity
}

func newResourceMetrics(identity metricIdentity) resourceMetrics {
	return resourceMetrics{sampler: &resources.Sampler{}, identity: identity}
}

// sampleResources has one live caller per run; the final call follows worker shutdown.
func (m *MetricsRegistry) sampleResources() error {
	r := &m.resources
	identity := &r.identity
	if identity.runID == "" {
		return nil
	}
	sample, err := r.sampler.Sample()
	if errors.Is(err, resources.ErrSampleUnavailable) {
		m.cpuGauge.DeleteLabelValues(identity.collectionID, identity.planID, identity.runID, identity.zone, identity.agentID)
		m.memGauge.DeleteLabelValues(identity.collectionID, identity.planID, identity.runID, identity.zone, identity.agentID)
		return nil
	}
	if err != nil {
		return err
	}
	m.cpuGauge.WithLabelValues(identity.collectionID, identity.planID, identity.runID, identity.zone, identity.agentID).Set(sample.CPUCores)
	m.memGauge.WithLabelValues(identity.collectionID, identity.planID, identity.runID, identity.zone, identity.agentID).Set(float64(sample.MemoryBytes))
	return nil
}

func (as *AgentServer) sampleOwnMetrics(recorder *MetricsRecorder) {
	if err := recorder.registry.sampleResources(); err != nil {
		as.logger.WithError(err).Warn("Container resource metrics are unavailable")
	}
}

func (as *AgentServer) reportOwnMetrics(ctx context.Context, recorder *MetricsRecorder, interval time.Duration) {
	as.sampleOwnMetrics(recorder)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			as.sampleOwnMetrics(recorder)
		}
	}
}
