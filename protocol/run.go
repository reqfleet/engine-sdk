// Derived from reqfleet/shibuya; modified for engine-sdk.
// Package protocol defines engine-neutral contracts between the coordinator and agents.
// Agents require a coordinator; these types do not define standalone execution.
package protocol

import "encoding/json"

// RunSpec is resolved by the coordinator for one agent. Settings are engine-owned
// configuration, not global workload intent. Agents must not allocate or rescale it.
type RunSpec struct {
	Settings  json.RawMessage               `json:"settings,omitempty"`
	Artifacts map[string]ArtifactAssignment `json:"artifacts,omitempty"`
}

// ArtifactAssignment describes how the delivered bytes must be consumed. Agent
// identity is deliberately not a substitute for this file-specific assignment.
type ArtifactAssignment struct {
	Mode       PartitionMode `json:"mode"`
	ShardIndex int           `json:"shard_index"`
	ShardCount int           `json:"shard_count"`
}

type PartitionMode string

const (
	PartitionReplicated   PartitionMode = "replicated"
	PartitionMaterialized PartitionMode = "materialized"
	PartitionNative       PartitionMode = "native"
)

// Observation describes engine work, not a terminal process outcome. WorkComplete
// satisfies the startup barrier; the runtime still waits for process exit and cleanup.
type Observation string

const (
	ObservationPending      Observation = "pending"
	ObservationReady        Observation = "ready"
	ObservationWorkComplete Observation = "work_complete"
)

// Progress is optional engine-specific data. It must never determine generic
// lifecycle correctness. Aggregators must not invent cross-engine arithmetic.
type Progress struct {
	Kind string          `json:"kind"`
	Data json.RawMessage `json:"data"`
}

type RunState string

const (
	RunPreparing RunState = "preparing"
	RunPrepared  RunState = "prepared"
	RunStarting  RunState = "starting"
	RunRunning   RunState = "running"
	RunStopping  RunState = "stopping"
	RunCompleted RunState = "completed"
	RunFailed    RunState = "failed"
	RunCanceled  RunState = "canceled"
)

func (s RunState) Terminal() bool {
	return s == RunCompleted || s == RunFailed || s == RunCanceled
}

type FailurePhase string

const (
	FailurePreparation FailurePhase = "preparation"
	FailureStartup     FailurePhase = "startup"
	FailureExecution   FailurePhase = "execution"
)

// StopReason distinguishes an intentional end of the scheduled workload from an
// abort. The coordinator supplies the reason; agents do not infer it from VU or time.
type StopReason string

const (
	StopCanceled  StopReason = "canceled"
	StopCompleted StopReason = "completed"
)
