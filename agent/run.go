// Derived from reqfleet/shibuya; modified for engine-sdk.
package agent

import (
	"encoding/json"

	"github.com/reqfleet/engine-sdk/protocol"
)

// Workspace separates run files from reusable engine/dependency state. A runtime
// executes one run at a time; it may reuse physical paths only after all callbacks,
// processes and cleanup from the preceding run have finished.
type Workspace struct {
	Root    string
	Inputs  string
	Config  string
	Results string
	Temp    string
	State   string
}

// RunContext is a read-only snapshot supplied to each adapter callback. Settings
// and artifact assignments come from the coordinator, without allocation or
// fallback inference in the agent. Paths are resolved locally by the runtime.
// DependencyFile is empty when the coordinator supplies no declaration; dependency
// preparation must still reconcile any installation left by a previous run.
type RunContext struct {
	RunID          string
	CollectionID   string
	PlanID         string
	AgentID        string
	Zone           string
	Workspace      Workspace
	TestFile       string
	ConfigFile     string
	DependencyFile string
	ResultFile     string
	Settings       json.RawMessage
	Artifacts      map[string]protocol.ArtifactAssignment
}

// Process identifies the runtime-owned primary workload. Stop callbacks may
// request shutdown but must not reap it; the runtime owns waiting and escalation.
type Process struct {
	PID int
}
