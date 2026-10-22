// Derived from reqfleet/shibuya; modified for engine-sdk.
package protocol

import (
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRunStatusEffectiveStartupState(t *testing.T) {
	for _, test := range []struct {
		name     string
		progress *RunStatus
		want     StartupState
	}{
		{"nil status", nil, StartupStatePending},
		{"started", &RunStatus{RunID: "12", StartupStatus: StartupStateStarted}, StartupStateStarted},
		{"work complete acknowledges startup", &RunStatus{RunID: "12", StartupStatus: StartupStateCompleted}, StartupStateCompleted},
		{"late failure overrides acknowledgement", &RunStatus{RunID: "12", StartupStatus: StartupStateStarted, State: RunFailed}, StartupStateFailed},
		{"canceled startup", &RunStatus{RunID: "12", StartupStatus: StartupStateCanceled}, StartupStateCanceled},
		{"canceled after acknowledgement", &RunStatus{RunID: "12", StartupStatus: StartupStateCompleted, State: RunCanceled}, StartupStateCanceled},
		{"unknown acknowledgement cannot pass", &RunStatus{RunID: "12", StartupStatus: "future", Running: true}, StartupStatePending},
		{"running alone cannot acknowledge", &RunStatus{RunID: "12", Running: true}, StartupStatePending},
		{"wrong run cannot acknowledge", &RunStatus{RunID: "99", StartupStatus: StartupStateCompleted}, StartupStatePending},
		{"missing identity cannot acknowledge", &RunStatus{StartupStatus: StartupStateCompleted}, StartupStatePending},
		{"progress cannot acknowledge", &RunStatus{RunID: "12", Progress: map[string]Progress{"engine": {Kind: "custom", Data: json.RawMessage(`{"done":true}`)}}}, StartupStatePending},
	} {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.want, test.progress.EffectiveStartupState("12"))
		})
	}
}
