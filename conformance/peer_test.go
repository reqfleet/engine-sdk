//go:build integration && linux

package conformance

import (
	"testing"

	"github.com/reqfleet/engine-sdk/protocol"
	"github.com/stretchr/testify/require"
)

func TestRuntimeProgressPreservesRunIDs(t *testing.T) {
	runtime := startRuntime(t, fixtureContract(t, "complete"))
	ids := []string{"run+1", "run&1", "run#1", "run%1", "run with spaces", "実行+&%#"}
	for _, id := range ids {
		runtime.Start(t, id, Fixture{File: "complete.txt"})
		status := runtime.Terminal(t, id, protocol.RunCompleted, Wait)
		require.Equal(t, id, status.RunID)
	}

	// A malformed escape used to remove run_id from the query entirely,
	// making a retired-run lookup return the unrelated current run instead.
	runtime.Start(t, "current", Fixture{File: "reuse.txt"})
	runtime.Running(t, "current")
	for _, id := range ids {
		status, err := runtime.Progress(id)
		require.NoError(t, err)
		require.Equal(t, id, status.RunID)
		require.Equal(t, protocol.RunCompleted, status.State)
	}
}
