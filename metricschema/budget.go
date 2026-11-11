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
// Only admitted names are retained, not overflow sources. New names are cloned so
// a label sliced from a larger CSV record cannot retain that record. Repeated and
// overflow labels do not allocate. This helper does not validate or aggregate
// samples, select an engine's budget, or provide transactional batch admission.
func AdmitRequestLabel(label string, admitted map[string]string, limit int, overflowLabel string) string {
	if overflowLabel == "" {
		overflowLabel = OverflowLabel
	}
	if label == overflowLabel {
		return overflowLabel
	}
	if existing, ok := admitted[label]; ok {
		return existing
	}
	if len(admitted) >= limit {
		return overflowLabel
	}
	label = strings.Clone(label)
	admitted[label] = label
	return label
}
