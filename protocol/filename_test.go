// Derived from reqfleet/shibuya; modified for engine-sdk.
package protocol_test

import (
	"testing"

	"github.com/reqfleet/engine-sdk/protocol"
	"github.com/stretchr/testify/assert"
)

func TestSanitizeFilename(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		want      string
		wantError bool
	}{
		{name: "plain filename", input: "report.csv", want: "report.csv"},
		{name: "trims whitespace", input: "  report.csv  ", want: "report.csv"},
		{name: "strips unix path components", input: "dir/subdir/report.csv", want: "report.csv"},
		{name: "strips windows path components", input: "dir\\subdir\\report.csv", want: "report.csv"},

		{name: "rejects empty", input: "", wantError: true},
		{name: "rejects dot", input: ".", wantError: true},
		{name: "rejects dotdot", input: "..", wantError: true},
		{name: "rejects single slash", input: "/", wantError: true},
		{name: "rejects repeated slashes", input: "///", wantError: true},
		{name: "rejects repeated backslashes", input: "\\\\\\", wantError: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := protocol.SanitizeFilename(tc.input)
			if tc.wantError {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}
