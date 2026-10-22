// Derived from reqfleet/shibuya; modified for engine-sdk.
package coordinator

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/reqfleet/engine-sdk/protocol"
	"github.com/stretchr/testify/require"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

type trackedBody struct {
	io.Reader
	closed bool
}

func (b *trackedBody) Close() error {
	b.closed = true
	return nil
}

func TestFetchFileResponseHandling(t *testing.T) {
	for _, tc := range []struct {
		name       string
		status     int
		body       string
		wantError  bool
		structured bool
	}{
		{name: "success", status: 200, body: "artifact"},
		{name: "non-200 success", status: 201, body: "created"},
		{name: "empty success", status: 204},
		{name: "redirect response", status: 304},
		{name: "bad request", status: 400, body: "invalid request", wantError: true},
		{name: "server failure", status: 503, body: "unavailable", wantError: true},
		{name: "structured dependency failure", status: 422, body: `{"message":"package not found","plan_id":"2","failure_kind":"dependency"}`, wantError: true, structured: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := &trackedBody{Reader: strings.NewReader(tc.body)}
			client := NewClient(&http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				require.Equal(t, "https", req.URL.Scheme)
				require.Equal(t, "coordinator.example", req.URL.Host)
				require.Equal(t, "/coordinator/files/collection/c/plan/p/test.jmx", req.URL.Path)
				require.Equal(t, http.MethodGet, req.Method)
				require.Equal(t, "Bearer test-key", req.Header.Get("Authorization"))
				return &http.Response{StatusCode: tc.status, Body: body, Header: make(http.Header)}, nil
			})})
			data, err := client.FetchFile(context.Background(), ReqOpts{Endpoint: "coordinator.example", APIKey: "test-key"}, "collection/c/plan/p/test.jmx")
			require.True(t, body.closed)
			if !tc.wantError {
				require.NoError(t, err)
				require.Equal(t, tc.body, string(data))
				return
			}
			require.Nil(t, data)
			require.ErrorIs(t, err, ErrNon200Response)
			if tc.structured {
				var triggerErr *protocol.TriggerError
				require.ErrorAs(t, err, &triggerErr)
				require.Equal(t, "2", triggerErr.PlanID)
				require.Equal(t, protocol.StartupFailureDependency, triggerErr.FailureKind)
				require.Equal(t, "package not found", triggerErr.Message)
			}
		})
	}
}

type failingReader struct {
	err error
}

func (r failingReader) Read([]byte) (int, error) { return 0, r.err }

func TestFetchFileClosesBodyOnReadFailure(t *testing.T) {
	readErr := errors.New("read failed")
	for _, status := range []int{200, 400} {
		body := &trackedBody{Reader: failingReader{err: readErr}}
		client := NewClient(&http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: status, Body: body, Header: make(http.Header)}, nil
		})})
		_, err := client.FetchFile(context.Background(), ReqOpts{Endpoint: "coordinator.example"}, "artifact")
		require.ErrorIs(t, err, readErr)
		require.True(t, body.closed)
	}
}

func TestFetchFileCancellation(t *testing.T) {
	started := make(chan struct{})
	canceled := make(chan struct{})
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-r.Context().Done()
		close(canceled)
	}))
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	client := NewClient(server.Client())
	go func() {
		_, err := client.FetchFile(ctx, ReqOpts{Endpoint: strings.TrimPrefix(server.URL, "https://")}, "artifact")
		result <- err
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("artifact request did not start")
	}
	cancel()
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("artifact request did not cancel")
	}
	select {
	case <-canceled:
	case <-time.After(5 * time.Second):
		t.Fatal("server did not observe cancellation")
	}
}
