package metricschema

import "strings"

// AdmitRequestLabel returns label while it fits the named-label budget, or the
// overflow label otherwise. An empty overflowLabel selects OverflowLabel. The
// overflow name is reserved and never consumes a slot. Zero permits only overflow.
//
// admitted must be an initialized, caller-owned map for one agent/run, populated
// only by this function. Keep limit (nonnegative) and overflowLabel fixed for its
// lifetime. Clear the map when resetting the run's metrics. Callers must serialize
// access, validate source labels before admission, and apply the returned label
// consistently to every metric belonging to the same sample.
//
// Only admitted names are retained, not overflow sources. New map keys are cloned
// so the set cannot retain a larger CSV record. Already-admitted labels are
// returned unchanged; callers retaining the result own its backing-storage lifetime.
// Repeated and overflow labels do not allocate. This helper does not validate or
// aggregate samples, select a budget, or provide transactional batch admission.
func AdmitRequestLabel(label string, admitted map[string]struct{}, limit int, overflowLabel string) string {
	if overflowLabel == "" {
		overflowLabel = OverflowLabel
	}
	if label == overflowLabel {
		return overflowLabel
	}
	if _, ok := admitted[label]; ok {
		return label
	}
	if len(admitted) >= limit {
		return overflowLabel
	}
	// CSV fields can share an entire record's backing allocation. Store only
	// a detached key; admission needs membership, not a duplicate string value.
	label = strings.Clone(label)
	admitted[label] = struct{}{}
	return label
}
