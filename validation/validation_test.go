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

func TestLabelAnalysisCheck(t *testing.T) {
	tests := []struct {
		name     string
		analysis validation.LabelAnalysis
		valid    bool
	}{
		{name: "exact", analysis: validation.LabelAnalysis{Status: validation.AnalysisExact, Unit: "tasks", Count: 3}, valid: true},
		{name: "exact zero", analysis: validation.LabelAnalysis{Status: validation.AnalysisExact, Unit: "tasks"}, valid: true},
		{name: "dynamic", analysis: validation.LabelAnalysis{Status: validation.AnalysisDynamic, Unit: "request labels"}, valid: true},
		{name: "unsupported", analysis: validation.LabelAnalysis{Status: validation.AnalysisUnsupported, Unit: "test labels"}, valid: true},
		{name: "not applicable", analysis: validation.LabelAnalysis{Status: validation.AnalysisNotApplicable, Unit: "request labels"}, valid: true},
		{name: "zero analysis"},
		{name: "unknown status", analysis: validation.LabelAnalysis{Status: "estimated", Unit: "tasks", Count: 1}},
		{name: "missing status", analysis: validation.LabelAnalysis{Unit: "tasks"}},
		{name: "missing unit", analysis: validation.LabelAnalysis{Status: validation.AnalysisExact, Count: 1}},
		{name: "blank unit", analysis: validation.LabelAnalysis{Status: validation.AnalysisExact, Unit: " \t\n"}},
		{name: "negative exact count", analysis: validation.LabelAnalysis{Status: validation.AnalysisExact, Unit: "tasks", Count: -1}},
	}
	for _, status := range []validation.AnalysisStatus{
		validation.AnalysisDynamic, validation.AnalysisUnsupported, validation.AnalysisNotApplicable,
	} {
		t.Run(string(status)+" missing unit", func(t *testing.T) {
			require.NoError(t, (validation.LabelAnalysis{Status: status}).Check())
		})
		for _, count := range []int{-1, 1} {
			t.Run(string(status)+" nonzero count", func(t *testing.T) {
				require.Error(t, (validation.LabelAnalysis{Status: status, Unit: "tasks", Count: count}).Check())
			})
		}
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.valid {
				require.NoError(t, tt.analysis.Check())
			} else {
				require.Error(t, tt.analysis.Check())
			}
		})
	}
}

func TestResultCheckAnalysis(t *testing.T) {
	for _, status := range []validation.AnalysisStatus{
		validation.AnalysisExact, validation.AnalysisDynamic, validation.AnalysisUnsupported, validation.AnalysisNotApplicable,
	} {
		t.Run(string(status), func(t *testing.T) {
			analysis := &validation.LabelAnalysis{Status: status, Unit: "tasks"}
			require.NoError(t, (validation.Result{Status: validation.Valid, Analysis: analysis}).Check())
			require.Error(t, (validation.Result{Status: validation.Invalid, Analysis: analysis, Diagnostics: []validation.Diagnostic{
				{Code: "configuration", Message: "invalid configuration"},
			}}).Check())
			require.Error(t, (validation.Result{Status: validation.Unsupported, Analysis: analysis}).Check())
			// Hosts may separately check analysis when they skip validation, but
			// an empty validation status is not a public validator outcome.
			require.NoError(t, analysis.Check())
			require.Error(t, (validation.Result{Analysis: analysis}).Check())
		})
	}
	require.Error(t, (validation.Result{
		Status: validation.Valid,
		Analysis: &validation.LabelAnalysis{
			Status: validation.AnalysisDynamic, Unit: "tasks", Count: 3,
		},
	}).Check())
	require.Error(t, (validation.Result{
		Status: validation.Valid, Analysis: &validation.LabelAnalysis{},
	}).Check())
}

func TestResultAnalysisJSON(t *testing.T) {
	tests := []struct {
		name     string
		analysis *validation.LabelAnalysis
		wantJSON string
	}{
		{name: "absent", wantJSON: `{"status":"valid"}`},
		{
			name:     "exact",
			analysis: &validation.LabelAnalysis{Status: validation.AnalysisExact, Unit: "tasks", Count: 3},
			wantJSON: `{"status":"valid","analysis":{"status":"exact","unit":"tasks","count":3}}`,
		},
		{
			name:     "exact zero",
			analysis: &validation.LabelAnalysis{Status: validation.AnalysisExact, Unit: "tasks"},
			wantJSON: `{"status":"valid","analysis":{"status":"exact","unit":"tasks","count":0}}`,
		},
		{
			name:     "dynamic",
			analysis: &validation.LabelAnalysis{Status: validation.AnalysisDynamic, Unit: "tasks"},
			wantJSON: `{"status":"valid","analysis":{"status":"dynamic","unit":"tasks","count":0}}`,
		},
		{
			name:     "unsupported",
			analysis: &validation.LabelAnalysis{Status: validation.AnalysisUnsupported, Unit: "tasks"},
			wantJSON: `{"status":"valid","analysis":{"status":"unsupported","unit":"tasks","count":0}}`,
		},
		{
			name:     "not applicable",
			analysis: &validation.LabelAnalysis{Status: validation.AnalysisNotApplicable, Unit: "tasks"},
			wantJSON: `{"status":"valid","analysis":{"status":"not_applicable","unit":"tasks","count":0}}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := validation.Result{Status: validation.Valid, Analysis: tt.analysis}
			data, err := json.Marshal(result)
			require.NoError(t, err)
			require.JSONEq(t, tt.wantJSON, string(data))
			var decoded validation.Result
			require.NoError(t, json.Unmarshal(data, &decoded))
			require.NoError(t, decoded.Check())
			require.Equal(t, result, decoded)
		})
	}
}

func TestResultCheckMalformedAnalysisJSON(t *testing.T) {
	for _, content := range []string{
		`{"status":"valid","analysis":{}}`,
		`{"status":"valid","analysis":{"status":"estimated","unit":"tasks","count":2}}`,
		`{"status":"valid","analysis":{"status":"exact","unit":"tasks","count":-1}}`,
		`{"status":"valid","analysis":{"status":"dynamic","unit":"tasks","count":2}}`,
		`{"status":"valid","analysis":{"status":"unsupported","unit":"tasks","count":-1}}`,
		`{"status":"valid","analysis":{"status":"not_applicable","unit":"tasks","count":1}}`,
		`{"status":"valid","analysis":{"status":"exact","count":1}}`,
		`{"status":"invalid","diagnostics":[{"code":"syntax","message":"invalid JSON"}],"analysis":{"status":"exact","unit":"tasks","count":1}}`,
		`{"status":"unsupported","analysis":{"status":"dynamic","unit":"tasks","count":0}}`,
	} {
		t.Run(content, func(t *testing.T) {
			var result validation.Result
			require.NoError(t, json.Unmarshal([]byte(content), &result))
			require.Error(t, result.Check())
		})
	}
}
