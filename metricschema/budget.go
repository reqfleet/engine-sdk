package metricschema

import "strings"

// AdmitRequestName returns requestName while it fits the request-name budget, or
// the overflow name otherwise. An empty overflowName selects OverflowRequestName.
// The overflow name is reserved and consumes no slot. Zero permits only overflow.
//
// admitted must be an initialized, caller-owned map for one agent/run, populated
// only by this function. Keep limit (nonnegative) and overflowName fixed for its
// lifetime. Clear the map when resetting the run's metrics. Callers must serialize
// access, validate source names before admission, and apply the returned name
// consistently to every metric belonging to the same sample.
//
// Only admitted names are retained, not overflow sources. New map keys are cloned
// so the set cannot retain a larger CSV record. Already-admitted names are
// returned unchanged; callers retaining the result own its backing-storage lifetime.
// Repeated and overflow names do not allocate. This helper does not validate or
// aggregate samples, select a budget, or provide transactional batch admission.
func AdmitRequestName(requestName string, admitted map[string]struct{}, limit int, overflowName string) string {
	if overflowName == "" {
		overflowName = OverflowRequestName
	}
	if requestName == overflowName {
		return overflowName
	}
	if _, ok := admitted[requestName]; ok {
		return requestName
	}
	if len(admitted) >= limit {
		return overflowName
	}
	// CSV fields can share an entire record's backing allocation. Store only
	// a detached key; admission needs membership, not a duplicate string value.
	requestName = strings.Clone(requestName)
	admitted[requestName] = struct{}{}
	return requestName
}
