// Derived from reqfleet/shibuya; modified for engine-sdk.
package protocol

type StartupFailureKind string

type StartupState string

const (
	StartupStatePending   StartupState = "pending"
	StartupStateStarted   StartupState = "started"
	StartupStateCompleted StartupState = "completed"
	StartupStateFailed    StartupState = "failed"
	StartupStateCanceled  StartupState = "canceled"
)

const (
	StartupFailureDependency StartupFailureKind = "dependency"
)

type RunStatus struct {
	ErrorMessage       string              `json:"error_message,omitempty"`
	RunID              string              `json:"run_id,omitempty"`
	StartupStatus      StartupState        `json:"startup_status,omitempty"`
	StartupFailureKind StartupFailureKind  `json:"startup_failure_kind,omitempty"`
	State              RunState            `json:"state,omitempty"`
	FailurePhase       FailurePhase        `json:"failure_phase,omitempty"`
	StopReason         StopReason          `json:"stop_reason,omitempty"`
	Progress           map[string]Progress `json:"progress,omitempty"`
	ExitCode           int                 `json:"exit_code,omitempty"`
	Running            bool                `json:"running"`
}

func (ps *RunStatus) EffectiveStartupState(runID string) StartupState {
	if ps == nil || runID == "" || ps.RunID != runID {
		return StartupStatePending
	}
	// Startup acknowledgement is latched, but a later terminal failure or cancel
	// must never satisfy a controller still waiting at the startup barrier.
	switch ps.State {
	case RunFailed:
		return StartupStateFailed
	case RunCanceled:
		return StartupStateCanceled
	}
	switch ps.StartupStatus {
	case StartupStateStarted, StartupStateCompleted, StartupStateFailed, StartupStateCanceled:
		return ps.StartupStatus
	default:
		return StartupStatePending
	}
}
