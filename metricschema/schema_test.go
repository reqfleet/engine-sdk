package metricschema_test

import (
	"math"
	"slices"
	"testing"

	"github.com/reqfleet/engine-sdk/metricschema"
	"github.com/stretchr/testify/assert"
)

func TestHistogramBucketsAreFiniteAndCallerOwned(t *testing.T) {
	for _, test := range []struct {
		name    string
		buckets func() []float64
	}{
		{name: "request", buckets: metricschema.RequestLatencyBuckets},
		{name: "LLM response", buckets: metricschema.LLMResponseLatencyBuckets},
	} {
		t.Run(test.name, func(t *testing.T) {
			bounds := test.buckets()
			want := slices.Clone(bounds)
			other := test.buckets()
			for i, bound := range bounds {
				assert.False(t, math.IsNaN(bound) || math.IsInf(bound, 0), "bound %d must be finite", i)
				if i > 0 {
					assert.Greater(t, bound, want[i-1], "bounds must be strictly increasing")
				}
				bounds[i] = -1
			}
			assert.Equal(t, want, other, "customizing one collector must not change another's bounds")
			assert.Equal(t, want, test.buckets(), "customizing one collector must not change future bounds")
		})
	}
}
