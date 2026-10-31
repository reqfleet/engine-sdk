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

var _ validation.Validator = jsonvalidator.Validate

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
			result, err := jsonvalidator.Validate(context.Background(), validation.Artifact{Filename: "plan.reqjson", Content: content}, validation.Options{})
			require.NoError(t, err)
			require.NoError(t, result.Check())
			require.Equal(t, tt.content, string(content), "validator must not mutate caller-owned content")
			if tt.code == "" {
				require.Equal(t, validation.Valid, result.Status)
				require.Empty(t, result.Diagnostics)
				require.Equal(t, &validation.LabelCount{Unit: "request labels", Count: 1}, result.LabelCount)
				return
			}
			require.Equal(t, validation.Invalid, result.Status)
			require.Nil(t, result.LabelCount)
			require.Len(t, result.Diagnostics, 1)
			require.Equal(t, tt.code, result.Diagnostics[0].Code)
		})
	}
}

func TestValidateDiagnosticOrder(t *testing.T) {
	result, err := jsonvalidator.Validate(context.Background(), validation.Artifact{
		Content: []byte(`{"requests":[{"method":"","url":"/relative"},{"method":"GET","url":"ftp://example.com"}]}`),
	}, validation.Options{})
	require.NoError(t, err)
	require.Equal(t, validation.Invalid, result.Status)
	require.Nil(t, result.LabelCount)
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
		for _, options := range []validation.Options{
			{},
			{SkipPlanFileValidation: true},
			{SkipLabelCounting: true},
			{SkipPlanFileValidation: true, SkipLabelCounting: true},
		} {
			for _, content := range []string{
				`{"requests":`,
				`{"requests":[{"method":"GET","url":"https://example.com","label":"home"}]}`,
				`{"dynamicLabels":true,"requests":[{"method":"GET","url":"https://example.com"}]}`,
			} {
				result, err := jsonvalidator.Validate(ctx, validation.Artifact{Content: []byte(content)}, options)
				require.ErrorIs(t, err, ctx.Err())
				require.Equal(t, validation.Result{}, result)
			}
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
	}, validation.Options{})
	require.NoError(t, err)
	require.Equal(t, validation.Valid, result.Status)
	require.Zero(t, requests.Load(), "validation must not execute the described requests")
}

func TestValidateLabelCount(t *testing.T) {
	tests := []struct {
		name    string
		content string
		count   int
	}{
		{
			name:    "distinct explicit labels",
			content: `{"requests":[{"method":"GET","url":"https://example.com","label":"home"},{"method":"GET","url":"https://example.com","label":"catalog"}]}`,
			count:   2,
		},
		{
			name:    "shared explicit label",
			content: `{"requests":[{"method":"GET","url":"https://example.com/a","label":"shared"},{"method":"POST","url":"https://example.com/b","label":"shared"}]}`,
			count:   1,
		},
		{
			name:    "duplicate default labels",
			content: `{"requests":[{"method":"GET","url":"https://example.com"},{"method":"GET","url":"https://example.com"}]}`,
			count:   1,
		},
		{
			name:    "method and URL both contribute to default",
			content: `{"requests":[{"method":"GET","url":"https://example.com/a"},{"method":"POST","url":"https://example.com/a"},{"method":"GET","url":"https://example.com/b"}]}`,
			count:   3,
		},
		{
			name:    "explicit and default labels share namespace",
			content: `{"requests":[{"method":"GET","url":"https://example.com"},{"method":"POST","url":"https://example.com/other","label":"GET https://example.com"}]}`,
			count:   1,
		},
		{
			name:    "empty label uses default",
			content: `{"requests":[{"method":"GET","url":"https://example.com"},{"method":"GET","url":"https://example.com","label":""}]}`,
			count:   1,
		},
		{
			name:    "static declaration",
			content: `{"dynamicLabels":false,"requests":[{"method":"GET","url":"https://example.com","label":"home"}]}`,
			count:   1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, skipValidation := range []bool{false, true} {
				result, err := jsonvalidator.Validate(context.Background(), validation.Artifact{Content: []byte(tt.content)}, validation.Options{
					SkipPlanFileValidation: skipValidation,
				})
				require.NoError(t, err)
				require.NoError(t, result.Check())
				status := validation.Valid
				if skipValidation {
					status = validation.Skipped
				}
				require.Equal(t, validation.Result{
					Status: status, LabelCount: &validation.LabelCount{Unit: "request labels", Count: tt.count},
				}, result)
			}
		})
	}
}

func TestValidateSkipValidation(t *testing.T) {
	tests := []struct {
		name    string
		content string
		count   *validation.LabelCount
	}{
		{
			name:    "invalid methods and URLs still count",
			content: `{"requests":[{"method":"","url":"/relative","label":"home"},{"method":" ","url":"%zz","label":"home"},{"url":"ftp://example.com","label":"other"}]}`,
			count:   &validation.LabelCount{Unit: "request labels", Count: 2},
		},
		{
			name:    "default labels do not require semantic validity",
			content: `{"requests":[{"method":"","url":"/relative"},{"method":"","url":"/relative"},{"method":"GET","url":"%zz"}]}`,
			count:   &validation.LabelCount{Unit: "request labels", Count: 2},
		},
		{
			name:    "unknown fields do not prevent counting",
			content: `{"extra":true,"requests":[{"method":"GET","url":"/relative","extra":1,"label":"home"}]}`,
			count:   &validation.LabelCount{Unit: "request labels", Count: 1},
		},
		{
			name:    "empty requests report zero",
			content: `{"requests":[]}`,
			count:   &validation.LabelCount{Unit: "request labels"},
		},
		{name: "malformed JSON", content: `{"requests":`},
		{name: "empty input"},
		{name: "wrong request type", content: `{"requests":"GET"}`},
		{name: "wrong label type", content: `{"requests":[{"label":1}]}`},
		{name: "wrong dynamic labels type", content: `{"dynamicLabels":"true","requests":[]}`},
		{name: "trailing document", content: `{"requests":[]} {}`},
		{name: "trailing garbage", content: `{"requests":[]} !`},
		{name: "dynamic invalid request", content: `{"dynamicLabels":true,"requests":[{"method":"","url":"/relative"}]}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			content := []byte(tt.content)
			result, err := jsonvalidator.Validate(context.Background(), validation.Artifact{Content: content}, validation.Options{
				SkipPlanFileValidation: true,
			})
			require.NoError(t, err)
			require.NoError(t, result.Check())
			require.Equal(t, validation.Result{Status: validation.Skipped, LabelCount: tt.count}, result)
			require.Equal(t, tt.content, string(content))
		})
	}
}

func TestValidateSkipCounting(t *testing.T) {
	for _, tt := range []struct {
		name    string
		content string
		status  validation.Status
	}{
		{name: "valid", content: `{"requests":[{"method":"GET","url":"https://example.com"}]}`, status: validation.Valid},
		{name: "syntax error", content: `{"requests":`, status: validation.Invalid},
		{name: "semantic error", content: `{"requests":[{"method":"","url":"/relative"}]}`, status: validation.Invalid},
		{name: "unknown field", content: `{"requests":[{"method":"GET","url":"https://example.com"}],"extra":1}`, status: validation.Invalid},
	} {
		t.Run(tt.name, func(t *testing.T) {
			artifact := validation.Artifact{Content: []byte(tt.content)}
			enabled, err := jsonvalidator.Validate(context.Background(), artifact, validation.Options{})
			require.NoError(t, err)
			result, err := jsonvalidator.Validate(context.Background(), artifact, validation.Options{SkipLabelCounting: true})
			require.NoError(t, err)
			require.NoError(t, result.Check())
			require.Equal(t, tt.status, result.Status)
			require.Equal(t, enabled.Diagnostics, result.Diagnostics)
			require.Nil(t, result.LabelCount)
		})
	}
}

func TestValidateBothSkipped(t *testing.T) {
	for _, content := range []string{
		"",
		`{"requests":`,
		`{"requests":[{"method":"","url":"/relative"}]}`,
		`{"requests":[{"method":"GET","url":"https://example.com"}]}`,
	} {
		result, err := jsonvalidator.Validate(context.Background(), validation.Artifact{Content: []byte(content)}, validation.Options{
			SkipPlanFileValidation: true,
			SkipLabelCounting:      true,
		})
		require.NoError(t, err)
		require.NoError(t, result.Check())
		require.Equal(t, validation.Result{Status: validation.Skipped}, result)
	}
}

func TestValidateDynamicLabelsOmitCount(t *testing.T) {
	for _, content := range []string{
		`{"dynamicLabels":true,"requests":[{"method":"GET","url":"https://example.com","label":"home"},{"method":"POST","url":"https://example.com/a","label":"catalog"}]}`,
		`{"dynamicLabels":true,"requests":[{"method":"GET","url":"https://example.com"}]}`,
	} {
		for _, skipValidation := range []bool{false, true} {
			result, err := jsonvalidator.Validate(context.Background(), validation.Artifact{Content: []byte(content)}, validation.Options{
				SkipPlanFileValidation: skipValidation,
			})
			require.NoError(t, err)
			require.NoError(t, result.Check())
			status := validation.Valid
			if skipValidation {
				status = validation.Skipped
			}
			require.Equal(t, validation.Result{Status: status}, result)
		}
	}
}
