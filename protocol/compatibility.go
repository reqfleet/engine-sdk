package protocol

import "fmt"

// WireProtocolVersion identifies the agent/coordinator contract, independently
// of the Go module release and metrics profile. Unversioned peers are unsupported.
const WireProtocolVersion = 1

const (
	SubscriptionAccepted       = "accepted"
	SubscriptionRejected       = "rejected"
	UnsupportedProtocolVersion = "unsupported_protocol_version"
)

// ProtocolError describes a permanent incompatibility, not a transport outage.
// It is safe to expose in health and deployment status: it contains no credentials.
type ProtocolError struct {
	Code              string `json:"code"`
	Message           string `json:"message"`
	ReceivedVersion   int    `json:"received_version"`
	SupportedVersions []int  `json:"supported_versions"`
}

func (e *ProtocolError) Error() string {
	return e.Message
}

// UnsupportedProtocol constructs the rejection for a peer's declared version.
// Zero represents a missing/unversioned declaration; it never means version 1.
func UnsupportedProtocol(received int) *ProtocolError {
	return &ProtocolError{
		Code:              UnsupportedProtocolVersion,
		Message:           fmt.Sprintf("agent protocol %d unsupported; coordinator supports %d", received, WireProtocolVersion),
		ReceivedVersion:   received,
		SupportedVersions: []int{WireProtocolVersion},
	}
}

// SubscriptionResponse is the first coordinator JSON value after an authenticated
// subscription. Commands follow only after an accepted response. The connection
// closes after rejection; there is no downgrade or alternate-version negotiation.
type SubscriptionResponse struct {
	Status          string         `json:"status"`
	ProtocolVersion int            `json:"protocol_version,omitempty"`
	Error           *ProtocolError `json:"error,omitempty"`
}

// AgentHealth reports subscription admission, not workload readiness. A permanent
// incompatibility remains observable while the agent is unhealthy.
type AgentHealth struct {
	Subscribed      bool           `json:"subscribed"`
	ProtocolVersion int            `json:"protocol_version"`
	Error           *ProtocolError `json:"error,omitempty"`
}
