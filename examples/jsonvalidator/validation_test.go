package jsonvalidator_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/reqfleet/engine-sdk/examples/jsonvalidator"
	"github.com/reqfleet/engine-sdk/validation"
	"github.com/stretchr/testify/require"
)

func TestValidate(t *testing.T) {
	tests := []struct {
		name    string
		content string
		code    string
	}{
		{name: "https", content: `{"requests":[{"method":"GET","url":"https://example.com"}]}`},
		{name: "http", content: `{"requests":[{"method":"POST","url":"http://localhost:8080/path?q=value"}]}`},
		{name: "trailing whitespace", content: "{\"requests\":[{\"method\":\"GET\",\"url\":\"https://example.com\"}]}\n\t "},
		{name: "empty input", code: "syntax"},
		{name: "malformed", content: `{"requests":]}`, code: "syntax"},
		{name: "truncated", content: `{"requests":`, code: "syntax"},
		{name: "trailing object", content: `{"requests":[{"method":"GET","url":"https://example.com"}]} {}`, code: "syntax"},
		{name: "trailing scalar", content: `{"requests":[{"method":"GET","url":"https://example.com"}]} true`, code: "syntax"},
		{name: "trailing garbage", content: `{"requests":[{"method":"GET","url":"https://example.com"}]} !`, code: "syntax"},
		{name: "unknown top-level field", content: `{"requests":[{"method":"GET","url":"https://example.com"}],"extra":1}`, code: "configuration"},
		{name: "unknown request field", content: `{"requests":[{"method":"GET","url":"https://example.com","extra":1}]}`, code: "configuration"},
		{name: "wrong request type", content: `{"requests":"GET"}`, code: "configuration"},
		{name: "wrong method type", content: `{"requests":[{"method":1,"url":"https://example.com"}]}`, code: "configuration"},
		{name: "wrong label type", content: `{"requests":[{"method":"GET","url":"https://example.com","label":1}]}`, code: "configuration"},
		{name: "wrong dynamic labels type", content: `{"requests":[{"method":"GET","url":"https://example.com"}],"dynamicLabels":"true"}`, code: "configuration"},
		{name: "dynamic malformed", content: `{"dynamicLabels":true,"requests":]}`, code: "syntax"},
		{name: "dynamic invalid request", content: `{"dynamicLabels":true,"requests":[{"method":"","url":"https://example.com"}]}`, code: "configuration"},
		{name: "missing requests", content: `{}`, code: "configuration"},
		{name: "empty requests", content: `{"requests":[]}`, code: "configuration"},
		{name: "null requests", content: `{"requests":null}`, code: "configuration"},
		{name: "null document", content: `null`, code: "configuration"},
		{name: "array document", content: `[]`, code: "configuration"},
		{name: "empty method", content: `{"requests":[{"method":"","url":"https://example.com"}]}`, code: "configuration"},
		{name: "blank method", content: `{"requests":[{"method":" \t","url":"https://example.com"}]}`, code: "configuration"},
		{name: "missing method", content: `{"requests":[{"url":"https://example.com"}]}`, code: "configuration"},
		{name: "empty url", content: `{"requests":[{"method":"GET","url":""}]}`, code: "configuration"},
		{name: "relative url", content: `{"requests":[{"method":"GET","url":"/path"}]}`, code: "configuration"},
		{name: "unsupported scheme", content: `{"requests":[{"method":"GET","url":"ftp://example.com"}]}`, code: "configuration"},
		{name: "missing host", content: `{"requests":[{"method":"GET","url":"https:///path"}]}`, code: "configuration"},
		{name: "port without hostname", content: `{"requests":[{"method":"GET","url":"https://:443/path"}]}`, code: "configuration"},
		{name: "invalid url escape", content: `{"requests":[{"method":"GET","url":"https://example.com/%zz"}]}`, code: "configuration"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			content := []byte(tt.content)
			result, err := jsonvalidator.Validate(context.Background(), validation.Artifact{Filename: "plan.reqjson", Content: content})
			require.NoError(t, err)
			require.NoError(t, result.Check())
			require.Equal(t, tt.content, string(content), "validator must not mutate caller-owned content")
			if tt.code == "" {
				require.Equal(t, validation.Valid, result.Status)
				require.Empty(t, result.Diagnostics)
				require.Equal(t, &validation.LabelAnalysis{
					Status: validation.AnalysisExact, Unit: "request labels", Count: 1,
				}, result.Analysis)
				return
			}
			require.Equal(t, validation.Invalid, result.Status)
			require.Nil(t, result.Analysis)
			require.Len(t, result.Diagnostics, 1)
			require.Equal(t, tt.code, result.Diagnostics[0].Code)
		})
	}
}

func TestValidateDiagnosticOrder(t *testing.T) {
	result, err := jsonvalidator.Validate(context.Background(), validation.Artifact{
		Content: []byte(`{"requests":[{"method":"","url":"/relative"},{"method":"GET","url":"ftp://example.com"}]}`),
	})
	require.NoError(t, err)
	require.Equal(t, validation.Invalid, result.Status)
	require.Nil(t, result.Analysis)
	require.Equal(t, []validation.Diagnostic{
		{Code: "configuration", Message: "requests[0].method must not be empty"},
		{Code: "configuration", Message: "requests[0].url must have an HTTP(S) scheme and host"},
		{Code: "configuration", Message: "requests[1].url must have an HTTP(S) scheme and host"},
	}, result.Diagnostics)
}

func TestValidateCancellation(t *testing.T) {
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	expired, cancelDeadline := context.WithDeadline(context.Background(), time.Unix(0, 0))
	defer cancelDeadline()
	for _, ctx := range []context.Context{canceled, expired} {
		for _, content := range []string{
			`{"requests":`,
			`{"requests":[{"method":"GET","url":"https://example.com","label":"home"}]}`,
			`{"dynamicLabels":true,"requests":[{"method":"GET","url":"https://example.com"}]}`,
		} {
			// Cancellation remains operational for malformed, static, and
			// dynamically named artifacts; it must not become analysis metadata.
			result, err := jsonvalidator.Validate(ctx, validation.Artifact{Content: []byte(content)})
			require.ErrorIs(t, err, ctx.Err())
			require.Equal(t, validation.Result{}, result)
		}
	}
}

func TestValidateDoesNotSendRequests(t *testing.T) {
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	result, err := jsonvalidator.Validate(context.Background(), validation.Artifact{
		Content: []byte(fmt.Sprintf(`{"requests":[{"method":"GET","url":%q}]}`, server.URL)),
	})
	require.NoError(t, err)
	require.Equal(t, validation.Valid, result.Status)
	require.Zero(t, requests.Load(), "validation must not execute the described requests")
}

func TestValidateLabelAnalysis(t *testing.T) {
	tests := []struct {
		name    string
		content string
		status  validation.AnalysisStatus
		count   int
	}{
		{
			name:    "distinct explicit labels",
			content: `{"requests":[{"method":"GET","url":"https://example.com","label":"home"},{"method":"GET","url":"https://example.com","label":"catalog"}]}`,
			status:  validation.AnalysisExact,
			count:   2,
		},
		{
			name:    "shared explicit label",
			content: `{"requests":[{"method":"GET","url":"https://example.com/a","label":"shared"},{"method":"POST","url":"https://example.com/b","label":"shared"}]}`,
			status:  validation.AnalysisExact,
			count:   1,
		},
		{
			name:    "duplicate default labels",
			content: `{"requests":[{"method":"GET","url":"https://example.com"},{"method":"GET","url":"https://example.com"}]}`,
			status:  validation.AnalysisExact,
			count:   1,
		},
		{
			name:    "method and URL both contribute to default",
			content: `{"requests":[{"method":"GET","url":"https://example.com/a"},{"method":"POST","url":"https://example.com/a"},{"method":"GET","url":"https://example.com/b"}]}`,
			status:  validation.AnalysisExact,
			count:   3,
		},
		{
			name:    "explicit and default labels share namespace",
			content: `{"requests":[{"method":"GET","url":"https://example.com"},{"method":"POST","url":"https://example.com/other","label":"GET https://example.com"}]}`,
			status:  validation.AnalysisExact,
			count:   1,
		},
		{
			name:    "empty label uses default",
			content: `{"requests":[{"method":"GET","url":"https://example.com"},{"method":"GET","url":"https://example.com","label":""}]}`,
			status:  validation.AnalysisExact,
			count:   1,
		},
		{
			name:    "static declaration",
			content: `{"dynamicLabels":false,"requests":[{"method":"GET","url":"https://example.com","label":"home"}]}`,
			status:  validation.AnalysisExact,
			count:   1,
		},
		{
			name:    "dynamic names do not claim a static count",
			content: `{"dynamicLabels":true,"requests":[{"method":"GET","url":"https://example.com","label":"home"},{"method":"POST","url":"https://example.com/a","label":"catalog"}]}`,
			status:  validation.AnalysisDynamic,
		},
		{
			name:    "dynamic names without explicit labels",
			content: `{"dynamicLabels":true,"requests":[{"method":"GET","url":"https://example.com"}]}`,
			status:  validation.AnalysisDynamic,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := jsonvalidator.Validate(context.Background(), validation.Artifact{Content: []byte(tt.content)})
			require.NoError(t, err)
			require.NoError(t, result.Check())
			require.Equal(t, validation.Valid, result.Status)
			require.Empty(t, result.Diagnostics)
			require.Equal(t, &validation.LabelAnalysis{
				Status: tt.status, Unit: "request labels", Count: tt.count,
			}, result.Analysis)
		})
	}
}
