//go:build !unix

package agent

import (
	"errors"
	"fmt"
	"runtime"
)

var errUnsupportedPlatform = fmt.Errorf("agent execution on %s: %w; deployed agents require Linux", runtime.GOOS, errors.ErrUnsupported)

// Run rejects execution before starting workers, subscriptions, or HTTP services.
// The package remains importable by clients that only use SDK types and handlers.
func (as *AgentServer) Run() error {
	return errUnsupportedPlatform
}

func (as *AgentServer) runCommand(_ *agentRun) error {
	return errUnsupportedPlatform
}
