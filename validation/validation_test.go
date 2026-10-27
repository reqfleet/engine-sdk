package validation_test

import (
	"testing"

	"github.com/reqfleet/engine-sdk/validation"
	"github.com/stretchr/testify/require"
)

func TestResultCheck(t *testing.T) {
	tests := []struct {
		name   string
		result validation.Result
		valid  bool
	}{
		{name: "valid", result: validation.Result{Status: validation.Valid}, valid: true},
		{name: "unsupported", result: validation.Result{Status: validation.Unsupported}, valid: true},
		{name: "zero result"},
		{name: "unknown status", result: validation.Result{Status: "skipped"}},
		{name: "invalid without diagnostics", result: validation.Result{Status: validation.Invalid}},
		{
			name: "valid with diagnostics",
			result: validation.Result{Status: validation.Valid, Diagnostics: []validation.Diagnostic{
				{Code: "syntax", Message: "unexpected token"},
			}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.valid {
				require.NoError(t, tt.result.Check())
			} else {
				require.Error(t, tt.result.Check())
			}
		})
	}
}

func TestResultCheckDiagnostics(t *testing.T) {
	tests := []struct {
		name       string
		diagnostic validation.Diagnostic
		valid      bool
	}{
		{name: "unspecified coordinates", diagnostic: validation.Diagnostic{Code: "syntax", Message: "unexpected token"}, valid: true},
		{name: "first position", diagnostic: validation.Diagnostic{Code: "syntax", Message: "unexpected token", Line: 1, Column: 1}, valid: true},
		{name: "later position", diagnostic: validation.Diagnostic{Code: "syntax", Message: "unexpected token", Line: 4, Column: 12}, valid: true},
		{name: "line only", diagnostic: validation.Diagnostic{Code: "syntax", Message: "unexpected token", Line: 1}, valid: true},
		{name: "column only", diagnostic: validation.Diagnostic{Code: "syntax", Message: "unexpected token", Column: 1}, valid: true},
		{name: "missing code", diagnostic: validation.Diagnostic{Message: "unexpected token"}},
		{name: "missing message", diagnostic: validation.Diagnostic{Code: "syntax"}},
		{name: "negative line", diagnostic: validation.Diagnostic{Code: "syntax", Message: "unexpected token", Line: -1}},
		{name: "negative column", diagnostic: validation.Diagnostic{Code: "syntax", Message: "unexpected token", Column: -1}},
	}
	for _, status := range []validation.Status{validation.Invalid, validation.Unsupported} {
		t.Run(string(status), func(t *testing.T) {
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					result := validation.Result{Status: status, Diagnostics: []validation.Diagnostic{
						{Code: "configuration", Message: "first diagnostic"},
						tt.diagnostic,
					}}
					if tt.valid {
						require.NoError(t, result.Check())
					} else {
						require.Error(t, result.Check())
					}
				})
			}
		})
	}
}
