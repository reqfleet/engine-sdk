package validation_test

import (
	"encoding/json"
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
		{name: "skipped", result: validation.Result{Status: validation.Skipped}, valid: true},
		{name: "zero result"},
		{name: "unknown status", result: validation.Result{Status: "other"}},
		{name: "invalid without diagnostics", result: validation.Result{Status: validation.Invalid}},
		{
			name: "valid with diagnostics",
			result: validation.Result{Status: validation.Valid, Diagnostics: []validation.Diagnostic{
				{Code: "syntax", Message: "unexpected token"},
			}},
		},
		{
			name: "skipped with diagnostics",
			result: validation.Result{Status: validation.Skipped, Diagnostics: []validation.Diagnostic{
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
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := validation.Result{Status: validation.Invalid, Diagnostics: []validation.Diagnostic{
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
}

func TestLabelCountCheck(t *testing.T) {
	tests := []struct {
		name  string
		count validation.LabelCount
		valid bool
	}{
		{name: "positive", count: validation.LabelCount{Unit: "tasks", Count: 3}, valid: true},
		{name: "zero", count: validation.LabelCount{Unit: "tasks"}, valid: true},
		{name: "missing unit", count: validation.LabelCount{Count: 1}},
		{name: "blank unit", count: validation.LabelCount{Unit: " \t\n"}},
		{name: "negative count", count: validation.LabelCount{Unit: "tasks", Count: -1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.valid {
				require.NoError(t, tt.count.Check())
			} else {
				require.Error(t, tt.count.Check())
			}
		})
	}
}

func TestResultCheckLabelCount(t *testing.T) {
	for _, status := range []validation.Status{validation.Valid, validation.Skipped} {
		t.Run(string(status), func(t *testing.T) {
			require.NoError(t, (validation.Result{Status: status}).Check())
			for _, count := range []int{0, 3} {
				require.NoError(t, (validation.Result{
					Status: status, LabelCount: &validation.LabelCount{Unit: "tasks", Count: count},
				}).Check())
			}
			require.Error(t, (validation.Result{
				Status: status, LabelCount: &validation.LabelCount{},
			}).Check())
			require.Error(t, (validation.Result{
				Status: status, LabelCount: &validation.LabelCount{Unit: "tasks", Count: -1},
			}).Check())
		})
	}
	count := &validation.LabelCount{Unit: "tasks"}
	require.Error(t, (validation.Result{Status: validation.Invalid, LabelCount: count, Diagnostics: []validation.Diagnostic{
		{Code: "configuration", Message: "invalid configuration"},
	}}).Check())
	require.Error(t, (validation.Result{LabelCount: count}).Check())
}

func TestResultLabelCountJSON(t *testing.T) {
	tests := []struct {
		name     string
		result   validation.Result
		wantJSON string
	}{
		{
			name:     "valid without count",
			result:   validation.Result{Status: validation.Valid},
			wantJSON: `{"status":"valid"}`,
		},
		{
			name:     "skipped without count",
			result:   validation.Result{Status: validation.Skipped},
			wantJSON: `{"status":"skipped"}`,
		},
		{
			name:     "positive count",
			result:   validation.Result{Status: validation.Valid, LabelCount: &validation.LabelCount{Unit: "tasks", Count: 3}},
			wantJSON: `{"status":"valid","label_count":{"unit":"tasks","count":3}}`,
		},
		{
			name:     "zero count",
			result:   validation.Result{Status: validation.Valid, LabelCount: &validation.LabelCount{Unit: "tasks"}},
			wantJSON: `{"status":"valid","label_count":{"unit":"tasks","count":0}}`,
		},
		{
			name:     "skipped with count",
			result:   validation.Result{Status: validation.Skipped, LabelCount: &validation.LabelCount{Unit: "tasks", Count: 2}},
			wantJSON: `{"status":"skipped","label_count":{"unit":"tasks","count":2}}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := json.Marshal(tt.result)
			require.NoError(t, err)
			require.JSONEq(t, tt.wantJSON, string(data))
			var decoded validation.Result
			require.NoError(t, json.Unmarshal(data, &decoded))
			require.NoError(t, decoded.Check())
			require.Equal(t, tt.result, decoded)
		})
	}
}

func TestResultCheckMalformedLabelCountJSON(t *testing.T) {
	for _, content := range []string{
		`{"status":"valid","label_count":{}}`,
		`{"status":"valid","label_count":{"unit":"tasks","count":-1}}`,
		`{"status":"valid","label_count":{"count":1}}`,
		`{"status":"skipped","label_count":{"unit":" ","count":1}}`,
		`{"status":"invalid","diagnostics":[{"code":"syntax","message":"invalid JSON"}],"label_count":{"unit":"tasks","count":1}}`,
		`{"status":"skipped","diagnostics":[{"code":"syntax","message":"invalid JSON"}]}`,
	} {
		t.Run(content, func(t *testing.T) {
			var result validation.Result
			require.NoError(t, json.Unmarshal([]byte(content), &result))
			require.Error(t, result.Check())
		})
	}
}

func TestOptionsJSON(t *testing.T) {
	for _, options := range []validation.Options{
		{},
		{SkipPlanFileValidation: true},
		{SkipLabelCounting: true},
		{SkipPlanFileValidation: true, SkipLabelCounting: true},
	} {
		data, err := json.Marshal(options)
		require.NoError(t, err)
		var fields map[string]bool
		require.NoError(t, json.Unmarshal(data, &fields))
		require.Equal(t, map[string]bool{
			"skip_plan_file_validation": options.SkipPlanFileValidation,
			"skip_label_counting":       options.SkipLabelCounting,
		}, fields)
		var decoded validation.Options
		require.NoError(t, json.Unmarshal(data, &decoded))
		require.Equal(t, options, decoded)
	}
}
