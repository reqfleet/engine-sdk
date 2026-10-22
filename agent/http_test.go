// Derived from reqfleet/shibuya; modified for engine-sdk.
package agent

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/reqfleet/engine-sdk/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMetricsOptionsAdmission(t *testing.T) {
	parser := func(string) (ShibuyaMetric, error) { return ShibuyaMetric{}, nil }
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer source.Close()
	tests := []struct {
		name          string
		options       AgentServerOptions
		invalid       bool
		resultMetrics bool
	}{
		{name: "registry"},
		{name: "result events", options: AgentServerOptions{ResultFile: "results", MetricParser: parser}, resultMetrics: true},
		{name: "unpaired file", options: AgentServerOptions{ResultFile: "results"}, invalid: true},
		{name: "unpaired parser", options: AgentServerOptions{MetricParser: parser}, invalid: true},
		{name: "schema without URL", options: AgentServerOptions{NativeMetricsSchema: MetricsSchemaShibuyaV1}, invalid: true},
		{name: "native parser conflict", options: AgentServerOptions{MetricsURL: source.URL, MetricParser: parser}, invalid: true},
		{name: "native file and parser conflict", options: AgentServerOptions{ResultFile: "results", MetricParser: parser, NativeMetricsSchema: MetricsSchemaShibuyaV1, MetricsURL: source.URL}, invalid: true},
		{name: "missing schema", options: AgentServerOptions{MetricsURL: source.URL}, invalid: true},
		{name: "unknown schema", options: AgentServerOptions{NativeMetricsSchema: "native-v2", MetricsURL: source.URL}, invalid: true},
		{name: "malformed URL", options: AgentServerOptions{NativeMetricsSchema: MetricsSchemaShibuyaV1, MetricsURL: "://bad"}, invalid: true},
		{name: "missing scheme", options: AgentServerOptions{NativeMetricsSchema: MetricsSchemaShibuyaV1, MetricsURL: "//localhost:9102/metrics"}, invalid: true},
		{name: "missing host", options: AgentServerOptions{NativeMetricsSchema: MetricsSchemaShibuyaV1, MetricsURL: "http:///metrics"}, invalid: true},
		{name: "unsupported scheme", options: AgentServerOptions{NativeMetricsSchema: MetricsSchemaShibuyaV1, MetricsURL: "ftp://localhost/metrics"}, invalid: true},
		{name: "native HTTP", options: AgentServerOptions{NativeMetricsSchema: MetricsSchemaShibuyaV1, MetricsURL: source.URL}},
		{name: "native HTTPS", options: AgentServerOptions{NativeMetricsSchema: MetricsSchemaShibuyaV1, MetricsURL: "https://localhost:9102/metrics"}},
		{name: "native workload output", options: AgentServerOptions{NativeMetricsSchema: MetricsSchemaShibuyaV1, MetricsURL: source.URL, ResultFile: "workload-output"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			as := NewAgentServer(tt.options)
			if !tt.invalid {
				require.NoError(t, as.validateMetricsOptions())
				assert.Equal(t, tt.resultMetrics, as.usesResultFileMetrics())
				return
			}
			// No command builder or coordinator: metrics errors must win before
			// runtime startup can reach any background work.
			require.Error(t, as.Run())
			response := httptest.NewRecorder()
			as.metricsHandler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
			assert.Equal(t, http.StatusServiceUnavailable, response.Code)
		})
	}

	// A declared endpoint's upstream failure must not become an empty registry.
	as := NewAgentServer(AgentServerOptions{MetricsURL: source.URL, NativeMetricsSchema: MetricsSchemaShibuyaV1})
	response := httptest.NewRecorder()
	as.metricsHandler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	assert.Equal(t, http.StatusBadGateway, response.Code)
}

func TestMetricsHandlerUsesRegistry(t *testing.T) {
	cpuGauge.WithLabelValues("collection", "plan", "run", "zone", "engine").Set(7)
	t.Cleanup(func() { cpuGauge.DeleteLabelValues("collection", "plan", "run", "zone", "engine") })
	as := NewAgentServer(AgentServerOptions{})
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	recorder := httptest.NewRecorder()

	as.metricsHandler().ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	body, err := io.ReadAll(recorder.Result().Body)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if !strings.Contains(string(body), "shibuya_cpu_gauge") {
		t.Fatalf("metrics body did not include shibuya_cpu_gauge: %s", string(body))
	}
}

func TestPreparationProgressRequiresAuthentication(t *testing.T) {
	f := newAgentFixture(t, AgentServerOptions{
		PrepareDependencies: func(context.Context, RunContext) error {
			return errors.New("installer failed")
		},
	}, nil)
	f.send(runMessage(protocol.VerbPrepare, "run-1", "requirements.txt"))
	waitRunState(t, f, "run-1", protocol.RunFailed)
	f.server.options.EngineMeta.APIKey = "secret"
	handler := f.server.Handler()
	unauthorized := httptest.NewRecorder()
	handler.ServeHTTP(unauthorized, httptest.NewRequest(http.MethodGet, "/prepare-progress", nil))
	if unauthorized.Code != http.StatusBadRequest {
		t.Fatalf("unauthorized status = %d", unauthorized.Code)
	}
	request := httptest.NewRequest(http.MethodGet, "/prepare-progress", nil)
	request.Header.Set("Authorization", "Bearer secret")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d", response.Code)
	}
	var progress protocol.PreparationProgress
	if err := json.Unmarshal(response.Body.Bytes(), &progress); err != nil {
		t.Fatal(err)
	}
	if progress.RunID != "run-1" || progress.Status != protocol.PreparationStateFailed {
		t.Fatalf("unexpected preparation outcome: %+v", progress)
	}
}

func TestHandlerAuthentication(t *testing.T) {
	as := NewAgentServer(AgentServerOptions{EngineMeta: EngineMeta{APIKey: "secret"}})
	handler := as.Handler()
	for _, tt := range []struct {
		name, token, body string
		status            int
	}{
		{"missing", "", "bearer header is empty\n", http.StatusBadRequest},
		{"wrong case", "bearer secret", "token is invalid\n", http.StatusBadRequest},
		{"missing separator", "Bearer", "token is invalid\n", http.StatusBadRequest},
		{"extra separator", "Bearer  secret", "token is invalid\n", http.StatusBadRequest},
		{"wrong token", "Bearer wrong", "forbidden\n", http.StatusForbidden},
		{"empty token", "Bearer ", "forbidden\n", http.StatusForbidden},
		{"correct", "Bearer secret", "", http.StatusOK},
	} {
		t.Run(tt.name, func(t *testing.T) {
			for _, endpoint := range []string{"/progress", "/prepare-progress", "/metrics"} {
				request := httptest.NewRequest(http.MethodGet, endpoint, nil)
				request.Header.Set("Authorization", tt.token)
				response := httptest.NewRecorder()
				handler.ServeHTTP(response, request)
				assert.Equal(t, tt.status, response.Code, endpoint)
				if tt.body != "" {
					assert.Equal(t, tt.body, response.Body.String(), endpoint)
				}
			}
		})
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	assert.Equal(t, http.StatusServiceUnavailable, response.Code)
	assert.Equal(t, "not subscribed to coordinator\n", response.Body.String())
}

func TestNativeMetricsProxyPreservesTargetAndFailure(t *testing.T) {
	var targetHost string
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/native/metrics", r.URL.Path)
		assert.Equal(t, "profile", r.URL.Query().Get("source"))
		assert.Equal(t, "run", r.URL.Query().Get("run_id"))
		assert.Equal(t, targetHost, r.Host)
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("native metrics unavailable"))
	}))
	defer source.Close()
	targetHost = strings.TrimPrefix(source.URL, "http://")
	as := NewAgentServer(AgentServerOptions{
		EngineMeta:          EngineMeta{APIKey: "secret"},
		MetricsURL:          source.URL + "/native/metrics?source=profile",
		NativeMetricsSchema: MetricsSchemaShibuyaV1,
	})
	handler := as.Handler()
	request := httptest.NewRequest(http.MethodGet, "/metrics?run_id=run", nil)
	request.Header.Set("Authorization", "Bearer secret")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	assert.Equal(t, http.StatusServiceUnavailable, response.Code)
	assert.Equal(t, "native metrics unavailable", response.Body.String())
	source.Close()
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	assert.Equal(t, http.StatusBadGateway, response.Code)
}
