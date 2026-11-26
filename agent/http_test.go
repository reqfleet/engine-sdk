// Derived from reqfleet/shibuya; modified for engine-sdk.
package agent

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/reqfleet/engine-sdk/protocol"
	httpserver "github.com/reqfleet/lib/http/server"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMetricsOptionsAdmission(t *testing.T) {
	parser := func(string) (ShibuyaMetric, error) { return ShibuyaMetric{}, nil }
	collector := func(context.Context, RunContext) error { return nil }
	handlerCalls := 0
	exporter := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		handlerCalls++
		w.WriteHeader(http.StatusBadGateway)
	})
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
		{name: "registry collector", options: AgentServerOptions{CustomIngestMetrics: collector}},
		{name: "collector workload output", options: AgentServerOptions{CustomIngestMetrics: collector, ResultFile: "results.csv"}},
		{name: "negative collector label limit", options: AgentServerOptions{CustomIngestMetrics: collector, LabelsUsageLimit: -1}, invalid: true},
		{name: "collector parser conflict", options: AgentServerOptions{CustomIngestMetrics: collector, ResultFile: "results", MetricParser: parser}, invalid: true},
		{name: "collector handler conflict", options: AgentServerOptions{CustomIngestMetrics: collector, MetricsHandler: exporter, NativeMetricsSchema: MetricsSchemaShibuyaV1}, invalid: true},
		{name: "collector URL conflict", options: AgentServerOptions{CustomIngestMetrics: collector, MetricsURL: source.URL, NativeMetricsSchema: MetricsSchemaShibuyaV1}, invalid: true},
		{name: "collector schema conflict", options: AgentServerOptions{CustomIngestMetrics: collector, NativeMetricsSchema: "native-v2"}, invalid: true},
		{name: "collector folding conflict", options: AgentServerOptions{CustomIngestMetrics: collector, FoldNativeMetrics: true}, invalid: true},
		{name: "collector overflow conflict", options: AgentServerOptions{CustomIngestMetrics: collector, NativeMetricsOverflowLabel: "others"}, invalid: true},
		{name: "result events", options: AgentServerOptions{ResultFile: "results", MetricParser: parser}, resultMetrics: true},
		{name: "negative result label limit", options: AgentServerOptions{ResultFile: "results", MetricParser: parser, LabelsUsageLimit: -1}, invalid: true},
		{name: "negative native label limit", options: AgentServerOptions{MetricsURL: source.URL, NativeMetricsSchema: MetricsSchemaShibuyaV1, LabelsUsageLimit: -1}, invalid: true},
		{name: "unpaired file", options: AgentServerOptions{ResultFile: "results"}, invalid: true},
		{name: "unpaired parser", options: AgentServerOptions{MetricParser: parser}, invalid: true},
		{name: "schema without source", options: AgentServerOptions{NativeMetricsSchema: MetricsSchemaShibuyaV1}, invalid: true},
		{name: "native parser conflict", options: AgentServerOptions{MetricsURL: source.URL, MetricParser: parser}, invalid: true},
		{name: "native file and parser conflict", options: AgentServerOptions{ResultFile: "results", MetricParser: parser, NativeMetricsSchema: MetricsSchemaShibuyaV1, MetricsURL: source.URL}, invalid: true},
		{name: "missing schema", options: AgentServerOptions{MetricsURL: source.URL}, invalid: true},
		{name: "unknown schema", options: AgentServerOptions{NativeMetricsSchema: "native-v2", MetricsURL: source.URL}, invalid: true},
		{name: "unknown overflow label", options: AgentServerOptions{MetricsURL: source.URL, NativeMetricsSchema: MetricsSchemaShibuyaV1, NativeMetricsOverflowLabel: "unbounded"}, invalid: true},
		{name: "malformed URL", options: AgentServerOptions{NativeMetricsSchema: MetricsSchemaShibuyaV1, MetricsURL: "://bad"}, invalid: true},
		{name: "missing scheme", options: AgentServerOptions{NativeMetricsSchema: MetricsSchemaShibuyaV1, MetricsURL: "//localhost:9102/metrics"}, invalid: true},
		{name: "missing host", options: AgentServerOptions{NativeMetricsSchema: MetricsSchemaShibuyaV1, MetricsURL: "http:///metrics"}, invalid: true},
		{name: "unsupported scheme", options: AgentServerOptions{NativeMetricsSchema: MetricsSchemaShibuyaV1, MetricsURL: "ftp://localhost/metrics"}, invalid: true},
		{name: "native HTTP", options: AgentServerOptions{NativeMetricsSchema: MetricsSchemaShibuyaV1, MetricsURL: source.URL}},
		{name: "native HTTPS", options: AgentServerOptions{NativeMetricsSchema: MetricsSchemaShibuyaV1, MetricsURL: "https://localhost:9102/metrics"}},
		{name: "native workload output", options: AgentServerOptions{NativeMetricsSchema: MetricsSchemaShibuyaV1, MetricsURL: source.URL, ResultFile: "workload-output"}},
		{name: "native folding", options: AgentServerOptions{NativeMetricsSchema: MetricsSchemaShibuyaV1, MetricsURL: source.URL, FoldNativeMetrics: true, NativeMetricsOverflowLabel: "_other_"}},
		{name: "native overflow without folding", options: AgentServerOptions{NativeMetricsSchema: MetricsSchemaShibuyaV1, MetricsURL: source.URL, NativeMetricsOverflowLabel: "others"}},
		{name: "registry folding", options: AgentServerOptions{FoldNativeMetrics: true}, invalid: true},
		{name: "registry overflow", options: AgentServerOptions{NativeMetricsOverflowLabel: "others"}, invalid: true},
		{name: "result folding", options: AgentServerOptions{ResultFile: "results", MetricParser: parser, FoldNativeMetrics: true}, invalid: true},
		{name: "result overflow", options: AgentServerOptions{ResultFile: "results", MetricParser: parser, NativeMetricsOverflowLabel: "others"}, invalid: true},
		{name: "handler", options: AgentServerOptions{MetricsHandler: exporter, NativeMetricsSchema: MetricsSchemaShibuyaV1}},
		{name: "handler workload output", options: AgentServerOptions{MetricsHandler: exporter, NativeMetricsSchema: MetricsSchemaShibuyaV1, ResultFile: "results.csv"}},
		{name: "negative handler label limit", options: AgentServerOptions{MetricsHandler: exporter, NativeMetricsSchema: MetricsSchemaShibuyaV1, LabelsUsageLimit: -1}, invalid: true},
		{name: "handler URL conflict", options: AgentServerOptions{MetricsHandler: exporter, MetricsURL: source.URL, NativeMetricsSchema: MetricsSchemaShibuyaV1}, invalid: true},
		{name: "handler parser conflict", options: AgentServerOptions{MetricsHandler: exporter, MetricParser: parser, NativeMetricsSchema: MetricsSchemaShibuyaV1}, invalid: true},
		{name: "handler file and parser conflict", options: AgentServerOptions{MetricsHandler: exporter, ResultFile: "results.csv", MetricParser: parser, NativeMetricsSchema: MetricsSchemaShibuyaV1}, invalid: true},
		{name: "handler missing schema", options: AgentServerOptions{MetricsHandler: exporter}, invalid: true},
		{name: "handler unknown schema", options: AgentServerOptions{MetricsHandler: exporter, NativeMetricsSchema: "native-v2"}, invalid: true},
		{name: "handler folding", options: AgentServerOptions{MetricsHandler: exporter, NativeMetricsSchema: MetricsSchemaShibuyaV1, FoldNativeMetrics: true}, invalid: true},
		{name: "handler overflow", options: AgentServerOptions{MetricsHandler: exporter, NativeMetricsSchema: MetricsSchemaShibuyaV1, NativeMetricsOverflowLabel: "others"}, invalid: true},
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
			assert.Zero(t, handlerCalls, "invalid options must not invoke the exporter")
		})
	}

	// A declared endpoint's upstream failure must not become an empty registry.
	as := NewAgentServer(AgentServerOptions{MetricsURL: source.URL, NativeMetricsSchema: MetricsSchemaShibuyaV1})
	response := httptest.NewRecorder()
	as.metricsHandler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	assert.Equal(t, http.StatusBadGateway, response.Code)
}

func TestMetricsHandlerSelectsPublishedRun(t *testing.T) {
	as := NewAgentServer(AgentServerOptions{
		EngineMeta:       EngineMeta{CollectionID: "collection", PlanID: "plan", EngineID: "engine", Zone: "zone"},
		LabelsUsageLimit: 1,
	})
	handler := as.metricsHandler()
	assert.Empty(t, nativeScrape(t, handler))
	run := newTestMetricsRun(t, as, "run")
	assert.Nil(t, run.Metrics)
	assert.Empty(t, nativeScrape(t, handler), "unlaunched runs are not published")
	as.prepareMetricsForRun(&run)
	require.NoError(t, run.Metrics.RecordEvent(ShibuyaMetric{
		CollectionID: "source", PlanID: "source", RunID: "source", EngineID: "source", Zone: "source",
		Label: "checkout", Status: "200", Latency: 250, EgressBytes: 1024, Threads: 7,
	}))
	families := nativeScrape(t, handler)
	require.Len(t, families, 4)
	assert.Equal(t, map[string]float64{"checkout/200": 1}, nativeCounters(families["shibuya_status_counter"]))
	assert.Equal(t, float64(1024), families["shibuya_egress_bytes_counter"].Metric[0].GetCounter().GetValue())
	assert.Equal(t, float64(7), families["shibuya_threads_gauge"].Metric[0].GetGauge().GetValue())
	histogram := families["shibuya_latency_label_milliseconds"].Metric[0].GetHistogram()
	assert.Equal(t, uint64(1), histogram.GetSampleCount())
	assert.Equal(t, float64(250), histogram.GetSampleSum())
	for name, family := range families {
		require.Len(t, family.Metric, 1)
		extra := map[string]string{}
		if name != "shibuya_threads_gauge" {
			extra["label"] = "checkout"
		}
		if name == "shibuya_status_counter" {
			extra["status"] = "200"
		}
		assertRecorderMetricLabels(t, family.Metric[0], run, extra)
	}

	as.finishMetricsRun(run.RunID)
	assert.Equal(t, families, nativeScrape(t, handler), "completion retains the final snapshot")
	next := newTestMetricsRun(t, as, "next")
	assert.Nil(t, next.Metrics)
	assert.Empty(t, nativeScrape(t, handler), "accepting a new run clears completed output")
	as.prepareMetricsForRun(&next)
	require.NoError(t, next.Metrics.ObserveRequest(ShibuyaMetric{Label: "next-request", Status: "201", Latency: 3}))
	require.NoError(t, run.Metrics.ObserveRequest(ShibuyaMetric{Label: "late-old", Status: "500", Latency: 9}))
	current := nativeScrape(t, handler)
	assert.Equal(t, map[string]float64{"next-request/201": 1}, nativeCounters(current["shibuya_status_counter"]))
	assert.Equal(t, "next", nativeLabelValue(current["shibuya_status_counter"].Metric[0], "run_id"))
	other := NewAgentServer(AgentServerOptions{})
	assert.Empty(t, nativeScrape(t, other.metricsHandler()), "a second server must not expose another run's registry")
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
	calls := 0
	as := NewAgentServer(AgentServerOptions{
		EngineMeta:          EngineMeta{APIKey: "secret"},
		NativeMetricsSchema: MetricsSchemaShibuyaV1,
		MetricsHandler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			calls++
			w.WriteHeader(http.StatusOK)
		}),
	})
	handler := as.Handler()
	for _, tt := range []struct {
		name, token string
		status      int
	}{
		{"missing", "", http.StatusBadRequest},
		{"wrong case", "bearer secret", http.StatusBadRequest},
		{"missing separator", "Bearer", http.StatusBadRequest},
		{"extra separator", "Bearer  secret", http.StatusBadRequest},
		{"wrong token", "Bearer wrong", http.StatusForbidden},
		{"empty token", "Bearer ", http.StatusForbidden},
		{"correct", "Bearer secret", http.StatusOK},
	} {
		t.Run(tt.name, func(t *testing.T) {
			for _, endpoint := range []string{"/progress", "/prepare-progress", "/metrics"} {
				request := httptest.NewRequest(http.MethodGet, endpoint, nil)
				before := calls
				request.Header.Set("Authorization", tt.token)
				response := httptest.NewRecorder()
				handler.ServeHTTP(response, request)
				assert.Equal(t, tt.status, response.Code, endpoint)
				if tt.status == http.StatusOK && endpoint == "/metrics" {
					assert.Equal(t, before+1, calls)
				} else {
					assert.Equal(t, before, calls, "rejected requests and other routes must not invoke the exporter")
				}
			}
		})
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	assert.Equal(t, http.StatusServiceUnavailable, response.Code)

	as.updateSubStatus(true)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	assert.Equal(t, http.StatusOK, response.Code)

	for _, req := range []*http.Request{
		httptest.NewRequest(http.MethodGet, "/healthz/", nil),
		httptest.NewRequest(http.MethodPost, "/healthz", nil),
	} {
		response = httptest.NewRecorder()
		handler.ServeHTTP(response, req)
		assert.Equal(t, http.StatusBadRequest, response.Code, "%s %s must require auth", req.Method, req.URL.Path)
	}
}

func TestHandlerEmptyAPIKeyDeniesProtectedRoutes(t *testing.T) {
	calls := 0
	as := NewAgentServer(AgentServerOptions{
		NativeMetricsSchema: MetricsSchemaShibuyaV1,
		MetricsHandler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			calls++
			w.WriteHeader(http.StatusOK)
		}),
	})
	as.updateSubStatus(true)
	handler := as.Handler()
	for _, endpoint := range []string{"/progress", "/prepare-progress", "/metrics"} {
		for _, authHeader := range []string{"", "Bearer ", "Bearer secret"} {
			request := httptest.NewRequest(http.MethodGet, endpoint, nil)
			request.Header.Set("Authorization", authHeader)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			assert.Equal(t, http.StatusUnauthorized, response.Code, "%s with %q", endpoint, authHeader)
		}
	}
	assert.Zero(t, calls, "empty API key must not invoke protected handlers")

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	assert.Equal(t, http.StatusOK, response.Code)
}

func TestHTTPServerDrainsOnCancellation(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	defer close(release)
	as := NewAgentServer(AgentServerOptions{
		EngineMeta:          EngineMeta{APIKey: "secret"},
		NativeMetricsSchema: MetricsSchemaShibuyaV1,
		MetricsHandler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			close(entered)
			<-release
			_, _ = io.WriteString(w, "drained\n")
		}),
	})
	as.updateSubStatus(true)

	ready := make(chan string, 1)
	shuttingDown := make(chan struct{})
	srv := as.newHTTPServer("127.0.0.1:0")
	srv.BaseContext = func(listener net.Listener) context.Context {
		ready <- listener.Addr().String()
		return context.Background()
	}
	srv.RegisterOnShutdown(func() { close(shuttingDown) })
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- httpserver.StartServer(srv, "", "", ctx) }()

	var addr string
	select {
	case addr = <-ready:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for HTTP server to bind")
	}

	client := &http.Client{Timeout: 5 * time.Second}
	defer client.CloseIdleConnections()

	healthResp, err := client.Get("http://" + addr + "/healthz")
	require.NoError(t, err)
	_ = healthResp.Body.Close()
	assert.Equal(t, http.StatusOK, healthResp.StatusCode)

	type scrapeResult struct {
		status int
		body   string
		err    error
	}
	scraped := make(chan scrapeResult, 1)
	go func() {
		req, reqErr := http.NewRequest(http.MethodGet, "http://"+addr+"/metrics", nil)
		if reqErr != nil {
			scraped <- scrapeResult{err: reqErr}
			return
		}
		req.Header.Set("Authorization", "Bearer secret")
		resp, doErr := client.Do(req)
		if doErr != nil {
			scraped <- scrapeResult{err: doErr}
			return
		}
		defer resp.Body.Close()
		body, readErr := io.ReadAll(resp.Body)
		scraped <- scrapeResult{status: resp.StatusCode, body: string(body), err: readErr}
	}()

	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for in-flight metrics scrape")
	}

	cancel()
	select {
	case <-shuttingDown:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for server shutdown")
	}
	select {
	case err := <-done:
		t.Fatalf("server returned before draining in-flight scrape: %v", err)
	default:
	}

	release <- struct{}{}
	select {
	case res := <-scraped:
		require.NoError(t, res.err)
		assert.Equal(t, http.StatusOK, res.status)
		assert.Equal(t, "drained\n", res.body)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for drained scrape response")
	}
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for HTTP server exit")
	}
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

func TestInProcessMetricsPreservesResponses(t *testing.T) {
	snapshot := nativeSnapshot(t, nativeTestMeta, "run", []nativeObservation{
		{"checkout", "200", 12.5, 1234},
		{"search", "503", 250, 5678},
	})
	for _, tt := range []struct {
		name, contentType, body string
		status                  int
	}{
		{"metrics", "text/plain; version=0.0.4", snapshot, http.StatusOK},
		{"unavailable", "application/json", `{"error":"exporter unavailable"}`, http.StatusServiceUnavailable},
		{"limited", "text/plain", "scrape already running\n", http.StatusTooManyRequests},
	} {
		t.Run(tt.name, func(t *testing.T) {
			as := NewAgentServer(AgentServerOptions{
				EngineMeta:          EngineMeta{APIKey: "secret"},
				NativeMetricsSchema: MetricsSchemaShibuyaV1,
				LabelsUsageLimit:    0, // The adapter, not the SDK, owns admission.
				MetricsHandler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					w.Header().Set("Content-Type", tt.contentType)
					w.Header().Set("Retry-After", "2")
					w.Header().Add("X-Exporter", "first")
					w.Header().Add("X-Exporter", "second")
					w.WriteHeader(tt.status)
					_, _ = io.WriteString(w, tt.body)
				}),
			})
			request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
			request.Header.Set("Authorization", "Bearer secret")
			response := httptest.NewRecorder()
			as.Handler().ServeHTTP(response, request)
			assert.Equal(t, tt.status, response.Code)
			assert.Equal(t, tt.contentType, response.Header().Get("Content-Type"))
			assert.Equal(t, "2", response.Header().Get("Retry-After"))
			assert.Equal(t, []string{"first", "second"}, response.Header().Values("X-Exporter"))
			assert.Equal(t, tt.body, response.Body.String())
		})
	}
}

func TestInProcessMetricsUsesRequestContext(t *testing.T) {
	type contextKey struct{}
	ctx, cancel := context.WithTimeout(context.WithValue(context.Background(), contextKey{}, "scrape"), 5*time.Second)
	defer cancel()
	entered := make(chan context.Context, 1)
	done := make(chan struct{})
	as := NewAgentServer(AgentServerOptions{
		EngineMeta:          EngineMeta{APIKey: "secret"},
		NativeMetricsSchema: MetricsSchemaShibuyaV1,
		MetricsHandler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			entered <- r.Context()
			<-r.Context().Done()
			http.Error(w, "scrape canceled", http.StatusRequestTimeout)
		}),
	})
	request := httptest.NewRequest(http.MethodGet, "/metrics", nil).WithContext(ctx)
	request.Header.Set("Authorization", "Bearer secret")
	response := httptest.NewRecorder()
	handler := as.Handler()
	go func() {
		defer close(done)
		handler.ServeHTTP(response, request)
	}()
	select {
	case actual := <-entered:
		assert.Same(t, ctx, actual)
		assert.Equal(t, "scrape", actual.Value(contextKey{}))
	case <-ctx.Done():
		t.Fatal("exporter did not receive the scrape")
	}
	cancel()
	select {
	case <-done:
		assert.Equal(t, http.StatusRequestTimeout, response.Code)
	case <-time.After(5 * time.Second):
		t.Fatal("scrape did not stop on request cancellation")
	}
}

func TestInProcessMetricsWithWorkloadOutput(t *testing.T) {
	var resultPath string
	f := newAgentFixture(t, AgentServerOptions{
		ResultFile:          "results.csv",
		NativeMetricsSchema: MetricsSchemaShibuyaV1,
		MetricsHandler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = io.WriteString(w, "adapter_ready 1\n")
		}),
		BuildCommand: func(_ context.Context, run RunContext) (Command, error) {
			resultPath = run.ResultFile
			return Command{Command: "sh", Args: []string{"-c", `printf 'checkout,200,12.5\n' > "$1"`, "engine", run.ResultFile}}, nil
		},
	}, nil)
	f.send(runMessage(protocol.VerbStart, "run-1", ""))
	waitRunState(t, f, "run-1", protocol.RunCompleted)
	content, err := os.ReadFile(resultPath)
	require.NoError(t, err)
	assert.Equal(t, "checkout,200,12.5\n", string(content))
	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	request.Header.Set("Authorization", "Bearer secret")
	response := httptest.NewRecorder()
	f.server.Handler().ServeHTTP(response, request)
	assert.Equal(t, http.StatusOK, response.Code)
	assert.Equal(t, "adapter_ready 1\n", response.Body.String())
}

func TestMetricsHandlerInFlightGatherRetainsOriginalRun(t *testing.T) {
	server := NewAgentServer(AgentServerOptions{LabelsUsageLimit: 1})
	handler := server.metricsHandler()
	first := startTestMetricsRun(t, server, "first")
	require.NoError(t, first.Metrics.ObserveRequest(ShibuyaMetric{Label: "first-request", Status: "200", Latency: 1}))
	entered, release := make(chan struct{}), make(chan struct{})
	releaseGather := sync.OnceFunc(func() { close(release) })
	t.Cleanup(releaseGather)
	require.NoError(t, first.Metrics.Registry().registry.Register(prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{Name: "test_gather_barrier", Help: "Synchronizes a run transition with an active gather."},
		func() float64 {
			close(entered)
			<-release
			return 1
		},
	)))
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() { done <- nativeResponse(handler) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("HTTP gather did not enter the original registry")
	}
	next := newTestMetricsRun(t, server, "next")
	assert.Empty(t, nativeScrape(t, handler), "new scrapes are empty while the captured registry finishes")
	server.prepareMetricsForRun(&next)
	require.NoError(t, next.Metrics.ObserveRequest(ShibuyaMetric{Label: "next-request", Status: "201", Latency: 2}))
	current := nativeScrape(t, handler)
	assert.Equal(t, map[string]float64{"next-request/201": 1}, nativeCounters(current["shibuya_status_counter"]))
	releaseGather()
	select {
	case response := <-done:
		assert.Equal(t, http.StatusOK, response.Code)
		assert.Contains(t, response.Body.String(), `run_id="first"`)
		assert.NotContains(t, response.Body.String(), `run_id="next"`)
		assert.Contains(t, response.Body.String(), `label="first-request"`)
		assert.NotContains(t, response.Body.String(), `label="next-request"`)
	case <-time.After(5 * time.Second):
		t.Fatal("HTTP gather did not finish after release")
	}
}
