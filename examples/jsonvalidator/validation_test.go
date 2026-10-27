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
				return
			}
			require.Equal(t, validation.Invalid, result.Status)
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
		// Cancellation remains operational even when the artifact is malformed.
		result, err := jsonvalidator.Validate(ctx, validation.Artifact{Content: []byte(`{"requests":`)})
		require.ErrorIs(t, err, ctx.Err())
		require.Equal(t, validation.Result{}, result)
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
