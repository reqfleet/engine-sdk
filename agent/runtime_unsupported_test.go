//go:build !unix

package agent

import (
	"context"
	"errors"
	"testing"

	"github.com/reqfleet/engine-sdk/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUnsupportedPlatformRun(t *testing.T) {
	server := NewAgentServer(AgentServerOptions{
		BuildCommand: func(context.Context, RunContext) (Command, error) {
			t.Error("unsupported runtime invoked the command builder")
			return Command{}, nil
		},
	})
	require.ErrorIs(t, server.Run(), errors.ErrUnsupported)
}

func TestUnsupportedPlatformCommand(t *testing.T) {
	f := newAgentFixture(t, AgentServerOptions{
		BuildCommand: func(context.Context, RunContext) (Command, error) {
			t.Error("unsupported runtime invoked the command builder")
			return Command{}, nil
		},
	}, nil)
	for _, runID := range []string{"first", "next"} {
		f.send(runMessage(protocol.VerbStart, runID, ""))
		status := waitRunState(t, f, runID, protocol.RunFailed)
		assert.Equal(t, protocol.StartupStateFailed, status.StartupStatus)
		assert.Equal(t, protocol.FailureStartup, status.FailurePhase)
		assert.Equal(t, -1, status.ExitCode)
		assert.False(t, status.Running)
	}
	require.ErrorIs(t, f.server.runCommand(&agentRun{}), errors.ErrUnsupported)
}
