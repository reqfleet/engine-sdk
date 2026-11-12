package metricschema_test

import (
	"testing"

	"github.com/reqfleet/engine-sdk/metricschema"
	"github.com/stretchr/testify/assert"
)

func TestAdmitRequestName(t *testing.T) {
	for _, test := range []struct {
		name     string
		limit    int
		overflow string
		input    []string
		want     []string
		admitted map[string]struct{}
	}{
		{
			name: "stable names and reserved overflow", limit: 2,
			input:    []string{"others", "z", "a", "b", "z", "b", "a"},
			want:     []string{"others", "z", "a", "others", "z", "others", "a"},
			admitted: map[string]struct{}{"z": {}, "a": {}},
		},
		{
			name: "zero budget", limit: 0,
			input:    []string{"a", "others", "a", "b"},
			want:     []string{"others", "others", "others", "others"},
			admitted: map[string]struct{}{},
		},
		{
			name: "custom overflow reserves only its own spelling", limit: 1, overflow: "_other_",
			input:    []string{"_other_", "others", "a", "others"},
			want:     []string{"_other_", "others", "_other_", "others"},
			admitted: map[string]struct{}{"others": {}},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			admitted := make(map[string]struct{})
			for i, label := range test.input {
				assert.Equal(t, test.want[i], metricschema.AdmitRequestName(label, admitted, test.limit, test.overflow))
			}
			assert.Equal(t, test.admitted, admitted, "retain only the admitted names")
		})
	}
}

func TestAdmitRequestNameRunReset(t *testing.T) {
	admitted := make(map[string]struct{})
	assert.Equal(t, "old", metricschema.AdmitRequestName("old", admitted, 1, ""))
	assert.Equal(t, metricschema.OverflowRequestName, metricschema.AdmitRequestName("new", admitted, 1, ""))
	clear(admitted)
	assert.Equal(t, "new", metricschema.AdmitRequestName("new", admitted, 1, ""))
	assert.Equal(t, metricschema.OverflowRequestName, metricschema.AdmitRequestName("old", admitted, 1, ""))
}
