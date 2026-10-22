// Derived from reqfleet/shibuya; modified for engine-sdk.
package protocol

import (
	"encoding/json"
	"fmt"
)

const (
	VerbPrepare = "prepare"
	VerbStart   = "start"
	VerbStop    = "stop"
)

type Payload struct {
	Verb        string      `json:"verb"`
	PlanMessage PlanMessage `json:"plan_message"`
}

func (p Payload) ToJSON() ([]byte, error) {
	return json.Marshal(p)
}

func (p Payload) String() string {
	return p.Verb
}

type PlanMessage map[string]*EngineMessage

func (pm PlanMessage) ToJSON() ([]byte, error) {
	return json.Marshal(pm)
}

func (pm PlanMessage) String() string {
	s := ""
	for pid := range pm {
		s = fmt.Sprintf("%s,%s", s, pid)
	}
	return s
}

type EngineMessage struct {
	Verb           string              `json:"verb"`
	RunID          string              `json:"run_id"`
	RunConfigs     map[string]RunSpec  `json:"runs,omitempty"`
	StopReason     StopReason          `json:"stop_reason,omitempty"`
	TestFile       string              `json:"test_file"`
	DependencyFile string              `json:"dependency_file,omitempty"`
	DataFiles      map[string]struct{} `json:"data_files"`
}
