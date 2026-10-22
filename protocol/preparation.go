// Derived from reqfleet/shibuya; modified for engine-sdk.
package protocol

type PreparationState string

const (
	PreparationStatePending   PreparationState = "pending"
	PreparationStateCompleted PreparationState = "completed"
	PreparationStateFailed    PreparationState = "failed"
	PreparationStateCanceled  PreparationState = "canceled"
)

type PreparationProgress struct {
	RunID        string           `json:"run_id,omitempty"`
	Status       PreparationState `json:"status"`
	ErrorMessage string           `json:"error_message,omitempty"`
}

func (pp *PreparationProgress) EffectiveState(runID string) PreparationState {
	if pp == nil || runID == "" || pp.RunID == "" || pp.RunID != runID {
		return PreparationStatePending
	}
	if pp.Status == PreparationStateCompleted || pp.Status == PreparationStateFailed || pp.Status == PreparationStateCanceled {
		return pp.Status
	}
	return PreparationStatePending
}
