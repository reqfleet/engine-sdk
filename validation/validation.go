package validation

import (
	"context"
	"errors"
	"fmt"
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

// Result describes validation without implying admission approval, a label
// count, runtime cardinality safety, or transformed artifact content.
type Result struct {
	Status      Status       `json:"status"`
	Diagnostics []Diagnostic `json:"diagnostics,omitempty"`
}

// Validator synchronously parses or compiles an artifact without executing the
// uploaded test. It should honor context cancellation where possible. A returned
// error denotes an operational failure or cancellation, not an invalid artifact.
type Validator func(context.Context, Artifact) (Result, error)

// Check reports whether the result obeys the validation contract. Unsupported is
// a well-formed result, but is not successful validation. A zero Result is invalid.
func (r Result) Check() error {
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
