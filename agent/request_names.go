package agent

import (
	"errors"
	"sync"

	"github.com/reqfleet/engine-sdk/metricschema"
)

// RequestsNamesStore owns stable request-name admission for one run. Construct it
// with NewRequestsNamesStore; do not copy it. A new run needs a new store.
// Names beyond the fixed budget use metricschema.OverflowRequestName.
type RequestsNamesStore struct {
	mu    sync.RWMutex
	names map[string]struct{}
	limit int
}

// NewRequestsNamesStore creates an empty store with a nonnegative name budget.
// Zero admits only the reserved overflow name, which never consumes a slot.
func NewRequestsNamesStore(limit int) (*RequestsNamesStore, error) {
	if limit < 0 {
		return nil, errors.New("metrics request-name limit must be nonnegative")
	}
	return newRequestsNamesStore(limit), nil
}

// newRequestsNamesStore requires a validated nonnegative budget.
func newRequestsNamesStore(limit int) *RequestsNamesStore {
	return &RequestsNamesStore{names: make(map[string]struct{}), limit: limit}
}

// Admit returns name if admitted, or the reserved overflow name. It is safe for
// concurrent use. Callers must validate the source name before admission.
// Only admitted names are retained; new keys do not retain source-record buffers.
func (s *RequestsNamesStore) Admit(name string) string {
	if name == metricschema.OverflowRequestName || s.limit == 0 {
		return metricschema.OverflowRequestName
	}
	s.mu.RLock()
	if _, admitted := s.names[name]; admitted {
		s.mu.RUnlock()
		return name
	}
	if len(s.names) >= s.limit {
		s.mu.RUnlock()
		return metricschema.OverflowRequestName
	}
	s.mu.RUnlock()

	s.mu.Lock()
	// Another producer may have admitted this name or consumed the last slot.
	admitted := metricschema.AdmitRequestName(name, s.names, s.limit, "")
	s.mu.Unlock()
	return admitted
}
