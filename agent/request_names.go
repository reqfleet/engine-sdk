package agent

import (
	"errors"
	"sync"

	"github.com/reqfleet/engine-sdk/metricschema"
)

// RequestsNamesStore owns stable request-name admission for one run. Construct it
// with NewRequestsNamesStore; do not copy it. A new run needs a new store.
// NewRequestsNamesStore uses metricschema.OverflowRequestName for overflow.
type RequestsNamesStore struct {
	mu       sync.RWMutex
	names    map[string]struct{}
	limit    int
	overflow string
}

// NewRequestsNamesStore creates an empty store with a nonnegative name budget.
// Zero admits only the reserved overflow name, which never consumes a slot.
func NewRequestsNamesStore(limit int) (*RequestsNamesStore, error) {
	if limit < 0 {
		return nil, errors.New("metrics request-name limit must be nonnegative")
	}
	return newRequestsNamesStore(limit, ""), nil
}

// newRequestsNamesStore requires a validated nonnegative budget.
func newRequestsNamesStore(limit int, overflow string) *RequestsNamesStore {
	if overflow == "" {
		overflow = metricschema.OverflowRequestName
	}
	return &RequestsNamesStore{names: make(map[string]struct{}), limit: limit, overflow: overflow}
}

// Admit returns name if admitted, or the reserved overflow name. It is safe for
// concurrent use. Callers must validate the source name before admission.
// Only admitted names are retained; new keys do not retain source-record buffers.
func (s *RequestsNamesStore) Admit(name string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return metricschema.AdmitRequestName(name, s.names, s.limit, s.overflow)
}

func (s *RequestsNamesStore) contains(name string) bool {
	s.mu.RLock()
	_, admitted := s.names[name]
	s.mu.RUnlock()
	return admitted
}

// rollback removes only a failed native scrape's tentative admissions. The
// caller serializes whole scrapes so no other producer can observe those names.
func (s *RequestsNamesStore) rollback(names []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, name := range names {
		delete(s.names, name)
	}
}
