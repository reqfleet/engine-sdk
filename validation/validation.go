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
	Valid   Status = "valid"
	Invalid Status = "invalid"
	Skipped Status = "skipped"
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

// LabelCount reports a best-effort count of engine-defined entities, not runtime
// cardinality safety. Unit identifies the entities, such as "request labels".
type LabelCount struct {
	Count int    `json:"count"`
	Unit  string `json:"unit"`
}

// Check reports whether the count obeys the public contract.
func (c LabelCount) Check() error {
	if c.Count < 0 {
		return errors.New("label count must not be negative")
	}
	if strings.TrimSpace(c.Unit) == "" {
		return errors.New("label count must have a nonempty unit")
	}
	return nil
}

// Result describes optional upload checks, not admission approval, runtime
// cardinality safety, or transformed content. A nil LabelCount means no count was
// supplied; a nonnil count can report zero.
type Result struct {
	Status      Status       `json:"status"`
	Diagnostics []Diagnostic `json:"diagnostics,omitempty"`
	LabelCount  *LabelCount  `json:"label_count,omitempty"`
}

// Options independently disable optional upload checks.
type Options struct {
	SkipPlanFileValidation bool `json:"skip_plan_file_validation"`
	SkipLabelCounting      bool `json:"skip_label_counting"`
}

// Validator checks an artifact without executing the uploaded test. It honors
// both skip options independently and should honor cancellation where possible.
// Adapters may return Skipped or omit LabelCount when a check is unavailable.
// A returned error denotes an operational failure or cancellation, not an invalid
// artifact. No tier or limit policy is supplied to the callback.
type Validator func(context.Context, Artifact, Options) (Result, error)

// Check reports whether the result obeys the validation contract. Valid and
// Skipped may carry a count, but no diagnostics. Invalid requires diagnostics and
// must not carry a count. A zero Result is malformed.
func (r Result) Check() error {
	if r.LabelCount != nil {
		if err := r.LabelCount.Check(); err != nil {
			return fmt.Errorf("label count: %w", err)
		}
		if r.Status == Invalid {
			return errors.New("invalid result must not contain a label count")
		}
	}
	switch r.Status {
	case Valid, Skipped:
		if len(r.Diagnostics) != 0 {
			return fmt.Errorf("%s result must not contain diagnostics", r.Status)
		}
	case Invalid:
		if len(r.Diagnostics) == 0 {
			return errors.New("invalid result must contain at least one diagnostic")
		}
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
