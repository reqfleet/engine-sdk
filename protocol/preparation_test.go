// Derived from reqfleet/shibuya; modified for engine-sdk.
package protocol

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPreparationProgressEffectiveState(t *testing.T) {
	tests := []struct {
		name     string
		runID    string
		progress *PreparationProgress
		want     PreparationState
	}{
		{
			name:     "matching canceled run",
			runID:    "12",
			progress: &PreparationProgress{RunID: "12", Status: PreparationStateCanceled},
			want:     PreparationStateCanceled,
		},
		{
			name:  "matching completed run",
			runID: "12",
			progress: &PreparationProgress{
				RunID:  "12",
				Status: PreparationStateCompleted,
			},
			want: PreparationStateCompleted,
		},
		{
			name:  "matching failed run",
			runID: "12",
			progress: &PreparationProgress{
				RunID:  "12",
				Status: PreparationStateFailed,
			},
			want: PreparationStateFailed,
		},
		{
			name:  "matching pending run",
			runID: "12",
			progress: &PreparationProgress{
				RunID:  "12",
				Status: PreparationStatePending,
			},
			want: PreparationStatePending,
		},
		{
			name:  "mismatched completed run",
			runID: "12",
			progress: &PreparationProgress{
				RunID:  "99",
				Status: PreparationStateCompleted,
			},
			want: PreparationStatePending,
		},
		{
			name:  "empty requested run",
			runID: "",
			progress: &PreparationProgress{
				RunID:  "12",
				Status: PreparationStateCompleted,
			},
			want: PreparationStatePending,
		},
		{
			name:  "empty reported run",
			runID: "12",
			progress: &PreparationProgress{
				Status: PreparationStateCompleted,
			},
			want: PreparationStatePending,
		},
		{
			name:     "nil progress",
			runID:    "12",
			progress: nil,
			want:     PreparationStatePending,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.progress.EffectiveState(tt.runID))
		})
	}
}
