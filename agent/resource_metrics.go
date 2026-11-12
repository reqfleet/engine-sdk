package agent

import (
	"net/http"
	"sync"

	"github.com/reqfleet/engine-sdk/resources"
)

type resourceSampler interface {
	Sample() (resources.Sample, bool, error)
	Reset()
}

type resourceMetrics struct {
	mu      sync.Mutex
	sampler resourceSampler
	runID   string
	meta    EngineMeta
}

func newResourceMetrics() resourceMetrics {
	return resourceMetrics{sampler: &resources.Sampler{}}
}

// reset runs under the registry's exclusive lock, after in-flight scrapes finish.
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
}

// publishResourceMetrics runs under the registry lock and resource sampling lock.
func (m *MetricsRegistry) publishResourceMetrics(meta EngineMeta, runID string, sample resources.Sample, ok bool) {
	if !ok {
		m.cpuGauge.DeleteLabelValues(meta.CollectionID, meta.PlanID, runID, meta.Zone, meta.EngineID)
		m.memGauge.DeleteLabelValues(meta.CollectionID, meta.PlanID, runID, meta.Zone, meta.EngineID)
		return
	}
	m.cpuGauge.WithLabelValues(meta.CollectionID, meta.PlanID, runID, meta.Zone, meta.EngineID).Set(sample.CPUCores)
	m.memGauge.WithLabelValues(meta.CollectionID, meta.PlanID, runID, meta.Zone, meta.EngineID).Set(float64(sample.MemoryBytes))
}

// resourceHandler holds the run identity through gathering. The inner handler
// must gather the private Prometheus registry directly, not re-lock via Gather.
func (m *MetricsRegistry) resourceHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		m.mu.RLock()
		defer m.mu.RUnlock()
		if err := r.Context().Err(); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if err := m.sampleResources(); err != nil {
			http.Error(w, "container resources: "+err.Error(), http.StatusInternalServerError)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// sampleResources is called with the registry read lock held. The separate lock
// serializes concurrent scrapes without blocking request-event recording.
func (m *MetricsRegistry) sampleResources() error {
	r := &m.resources
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.runID == "" {
		return nil
	}
	sample, ok, err := r.sampler.Sample()
	if err != nil {
		return err
	}
	m.publishResourceMetrics(r.meta, r.runID, sample, ok)
	return nil
}
