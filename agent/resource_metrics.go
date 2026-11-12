package agent

import (
	"context"
	"errors"
	"time"

	"github.com/reqfleet/engine-sdk/resources"
)

type resourceSampler interface {
	Sample() (resources.Sample, error)
	Reset()
}

type resourceMetrics struct {
	sampler resourceSampler
	runID   string
	meta    EngineMeta
}

func newResourceMetrics() resourceMetrics {
	return resourceMetrics{sampler: &resources.Sampler{}}
}

// reset runs under the registry's exclusive lock, after in-flight sampling finishes.
func (r *resourceMetrics) reset() {
	r.runID = ""
	r.meta = EngineMeta{}
	r.sampler.Reset()
}

func (m *MetricsRegistry) startResourceRun(meta EngineMeta, runID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.resources.reset()
	m.resources.meta, m.resources.runID = meta, runID
	m.collectionErr = nil
}

// sampleResources has one live caller per run; the final call follows worker shutdown.
func (m *MetricsRegistry) sampleResources() error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	r := &m.resources
	if r.runID == "" {
		return nil
	}
	sample, err := r.sampler.Sample()
	if errors.Is(err, resources.ErrSampleUnavailable) {
		m.cpuGauge.DeleteLabelValues(r.meta.CollectionID, r.meta.PlanID, r.runID, r.meta.Zone, r.meta.EngineID)
		m.memGauge.DeleteLabelValues(r.meta.CollectionID, r.meta.PlanID, r.runID, r.meta.Zone, r.meta.EngineID)
		return nil
	}
	if err != nil {
		return err
	}
	m.cpuGauge.WithLabelValues(r.meta.CollectionID, r.meta.PlanID, r.runID, r.meta.Zone, r.meta.EngineID).Set(sample.CPUCores)
	m.memGauge.WithLabelValues(r.meta.CollectionID, r.meta.PlanID, r.runID, r.meta.Zone, r.meta.EngineID).Set(float64(sample.MemoryBytes))
	return nil
}

func (as *AgentServer) sampleOwnMetrics() {
	if err := as.metrics.sampleResources(); err != nil {
		as.logger.WithError(err).Warn("Container resource metrics are unavailable")
	}
}

func (as *AgentServer) reportOwnMetrics(ctx context.Context, interval time.Duration) {
	as.sampleOwnMetrics()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			as.sampleOwnMetrics()
		}
	}
}
