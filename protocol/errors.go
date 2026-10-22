// Derived from reqfleet/shibuya; modified for engine-sdk.
package protocol

import "fmt"

type TriggerError struct {
	Message     string             `json:"message"`
	PlanID      string             `json:"plan_id,omitempty"`
	FailureKind StartupFailureKind `json:"failure_kind,omitempty"`
}

func (e *TriggerError) Error() string {
	if e == nil {
		return ""
	}
	if e.FailureKind == StartupFailureDependency && e.PlanID != "" {
		message := fmt.Sprintf("plan %s dependency preparation failed", e.PlanID)
		if e.Message != "" {
			message += ": " + e.Message
		}
		return message
	}
	return e.Message
}
