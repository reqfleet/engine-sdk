package validation

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

// Status describes validation, not upload admission or execution capability.
type Status string

const (
	Valid       Status = "valid"
	Invalid     Status = "invalid"
	Unsupported Status = "unsupported"
)

// Artifact is an uploaded file. The caller owns Content; validators must not
// mutate it or retain it after returning.
type Artifact struct {
	Filename string
	Content  []byte
}

// Diagnostic describes an artifact problem. Code and Message must be nonempty.
// Line and Column are one-based when nonzero; zero means unspecified.
type Diagnostic struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Line    int    `json:"line,omitempty"`
	Column  int    `json:"column,omitempty"`
}

// AnalysisStatus describes what an engine can establish about counts of its
// engine-defined entities without executing the uploaded test.
type AnalysisStatus string

const (
	AnalysisExact         AnalysisStatus = "exact"
	AnalysisDynamic       AnalysisStatus = "dynamic"
	AnalysisUnsupported   AnalysisStatus = "unsupported"
	AnalysisNotApplicable AnalysisStatus = "not_applicable"
)

// LabelAnalysis reports counts of engine-defined entities, not admission policy
// or runtime safety. Unit identifies the counted entities, such as "tasks" or
// "request labels"; a static task count is not an emitted-label cardinality.
// Unit is required for AnalysisExact and optional for other statuses.
// Count is meaningful only for AnalysisExact, where zero is an exact count.
// Other statuses require Count to be zero but do not assert a zero count.
type LabelAnalysis struct {
	Status AnalysisStatus `json:"status"`
	Unit   string         `json:"unit"`
	Count  int            `json:"count"`
}

// Check reports whether the analysis obeys the public facts-only contract.
func (a LabelAnalysis) Check() error {
	switch a.Status {
	case AnalysisExact:
		if strings.TrimSpace(a.Unit) == "" {
			return errors.New("exact label analysis must have a nonempty unit")
		}
		if a.Count < 0 {
			return errors.New("exact label analysis must not have a negative count")
		}
	case AnalysisDynamic, AnalysisUnsupported, AnalysisNotApplicable:
		if a.Count != 0 {
			return errors.New("non-exact label analysis must not contain a count")
		}
	default:
		return fmt.Errorf("unknown label analysis status %q", a.Status)
	}
	return nil
}

// Result describes validation and optional preflight label analysis without
// implying admission approval, runtime cardinality safety, or transformed content.
// A nil Analysis means unavailable or not supplied, never an exact zero count.
type Result struct {
	Status      Status         `json:"status"`
	Diagnostics []Diagnostic   `json:"diagnostics,omitempty"`
	Analysis    *LabelAnalysis `json:"analysis,omitempty"`
}

// Validator synchronously parses or compiles an artifact without executing the
// uploaded test. It should honor context cancellation where possible. A returned
// error denotes an operational failure or cancellation, not an invalid artifact.
type Validator func(context.Context, Artifact) (Result, error)

// Check reports whether the result obeys the validation contract. Unsupported is
// a well-formed result, but is not successful validation. A zero Result is invalid.
func (r Result) Check() error {
	if r.Analysis != nil {
		if err := r.Analysis.Check(); err != nil {
			return fmt.Errorf("label analysis: %w", err)
		}
		if r.Status == Invalid || r.Status == Unsupported {
			return errors.New("invalid or unsupported validation must not contain label analysis")
		}
	}
	switch r.Status {
	case Valid:
		if len(r.Diagnostics) != 0 {
			return errors.New("valid result must not contain diagnostics")
		}
	case Invalid:
		if len(r.Diagnostics) == 0 {
			return errors.New("invalid result must contain at least one diagnostic")
		}
	case Unsupported:
	default:
		return fmt.Errorf("unknown validation status %q", r.Status)
	}

	for i, diagnostic := range r.Diagnostics {
		if diagnostic.Code == "" || diagnostic.Message == "" {
			return fmt.Errorf("diagnostic %d must have a nonempty code and message", i)
		}
		if diagnostic.Line < 0 || diagnostic.Column < 0 {
			return fmt.Errorf("diagnostic %d must not have negative coordinates", i)
		}
	}
	return nil
}
