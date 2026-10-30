package agent

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	"github.com/reqfleet/engine-sdk/metricschema"
	"github.com/reqfleet/engine-sdk/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type nativeObservation struct {
	label, status  string
	latency, bytes float64
}

var nativeTestMeta = EngineMeta{CollectionID: "collection", PlanID: "plan", EngineID: "engine", Zone: "zone"}

// Use real Prometheus collectors to supply the normalized cumulative producer
// contract; assertions below inspect the exposition consumed by a scraper.
func nativeSnapshot(t *testing.T, meta EngineMeta, runID string, observations []nativeObservation) string {
	t.Helper()
	registry := prometheus.NewRegistry()
	latency := prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: metricschema.RequestLatency, Buckets: shibuyaBuckets}, metricschema.LatencyLabels())
	status := prometheus.NewCounterVec(prometheus.CounterOpts{Name: metricschema.StatusCounter}, metricschema.StatusLabels())
	egress := prometheus.NewCounterVec(prometheus.CounterOpts{Name: metricschema.EgressBytesCounter}, metricschema.EgressLabels())
	threads := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: metricschema.ThreadsGauge}, metricschema.ThreadsLabels())
	lateness := prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "shibuya_schedule_lateness_seconds", Buckets: prometheus.DefBuckets}, metricschema.LatencyLabels())
	resource := prometheus.NewGauge(prometheus.GaugeOpts{Name: "adapter_resource_bytes"})
	registry.MustRegister(latency, status, egress, threads, lateness, resource)
	for _, observation := range observations {
		latency.WithLabelValues(meta.CollectionID, observation.label, runID, meta.EngineID, meta.PlanID, meta.Zone).Observe(observation.latency)
		lateness.WithLabelValues(meta.CollectionID, observation.label, runID, meta.EngineID, meta.PlanID, meta.Zone).Observe(observation.latency / 1000)
		status.WithLabelValues(meta.CollectionID, meta.PlanID, runID, meta.EngineID, observation.label, meta.Zone, observation.status).Inc()
		egress.WithLabelValues(meta.CollectionID, meta.PlanID, runID, meta.EngineID, observation.label, meta.Zone).Add(observation.bytes)
	}
	threads.WithLabelValues(meta.CollectionID, meta.PlanID, runID, meta.EngineID, meta.Zone).Set(7)
	resource.Set(1234)
	families, err := registry.Gather()
	require.NoError(t, err)
	var text bytes.Buffer
	for _, family := range families {
		_, err := expfmt.MetricFamilyToText(&text, family)
		require.NoError(t, err)
	}
	return text.String()
}

func nativeSource(t *testing.T, initial string) (*httptest.Server, func(string)) {
	t.Helper()
	var current atomic.Value
	current.Store(initial)
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "text/plain; version=0.0.4", r.Header.Get("Accept"))
		assert.Equal(t, "identity", r.Header.Get("Accept-Encoding"))
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = fmt.Fprint(w, current.Load().(string))
	}))
	t.Cleanup(source.Close)
	return source, func(text string) { current.Store(text) }
}

func nativeResponse(handler http.Handler) *httptest.ResponseRecorder {
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	return response
}

func nativeScrape(t *testing.T, handler http.Handler) map[string]*dto.MetricFamily {
	t.Helper()
	response := nativeResponse(handler)
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	parser := expfmt.NewTextParser(model.LegacyValidation)
	families, err := parser.TextToMetricFamilies(response.Body)
	require.NoError(t, err)
	return families
}

func nativeLabelValue(metric *dto.Metric, name string) string {
	for _, label := range metric.Label {
		if label.GetName() == name {
			return label.GetValue()
		}
	}
	return ""
}

func nativeCounters(family *dto.MetricFamily) map[string]float64 {
	values := make(map[string]float64)
	for _, metric := range family.Metric {
		key := nativeLabelValue(metric, "label")
		if status := nativeLabelValue(metric, "status"); status != "" {
			key += "/" + status
		}
		values[key] = metric.GetCounter().GetValue()
	}
	return values
}

func TestNativeMetricsNumericLabelBudget(t *testing.T) {
	for _, test := range []struct {
		limit int
		want  map[string]float64
	}{
		{0, map[string]float64{"others/200": 3}},
		{1, map[string]float64{"a/200": 1, "others/200": 2}},
		{2, map[string]float64{"a/200": 1, "b/200": 1, "others/200": 1}},
	} {
		t.Run(fmt.Sprintf("limit-%d", test.limit), func(t *testing.T) {
			text := nativeSnapshot(t, nativeTestMeta, "run", []nativeObservation{{"b", "200", 3, 20}, {"a", "200", 1, 10}, {"others", "200", 5, 30}})
			source, _ := nativeSource(t, text)
			as := NewAgentServer(AgentServerOptions{EngineMeta: nativeTestMeta, MetricsURL: source.URL, NativeMetricsSchema: MetricsSchemaShibuyaV1, LabelsUsageLimit: test.limit})
			as.startMetricsRun("run")
			handler := as.metricsHandler()
			for range 2 {
				families := nativeScrape(t, handler)
				assert.Equal(t, test.want, nativeCounters(families[metricschema.StatusCounter]))
				assert.Equal(t, float64(7), families[metricschema.ThreadsGauge].Metric[0].GetGauge().GetValue())
				assert.Equal(t, float64(1234), families["adapter_resource_bytes"].Metric[0].GetGauge().GetValue())
			}
		})
	}
}

func TestNativeMetricsStableAdmissionAndCumulativeAggregation(t *testing.T) {
	observations := []nativeObservation{{"z", "200", 1, 10}}
	source, publish := nativeSource(t, nativeSnapshot(t, nativeTestMeta, "run", observations))
	as := NewAgentServer(AgentServerOptions{EngineMeta: nativeTestMeta, MetricsURL: source.URL, NativeMetricsSchema: MetricsSchemaShibuyaV1, LabelsUsageLimit: 1})
	as.startMetricsRun("run")
	handler := as.metricsHandler()
	assert.Equal(t, map[string]float64{"z/200": 1}, nativeCounters(nativeScrape(t, handler)[metricschema.StatusCounter]))
	observations = append(observations, nativeObservation{"a", "200", 2, 20}, nativeObservation{"b", "500", 10, 30}, nativeObservation{"others", "200", 100, 40})
	publish(nativeSnapshot(t, nativeTestMeta, "run", observations))
	for range 2 {
		families := nativeScrape(t, handler)
		assert.Equal(t, map[string]float64{"z/200": 1, "others/200": 2, "others/500": 1}, nativeCounters(families[metricschema.StatusCounter]))
		assert.Equal(t, map[string]float64{"z": 10, "others": 90}, nativeCounters(families[metricschema.EgressBytesCounter]))
		for _, familyName := range []string{metricschema.RequestLatency, "shibuya_schedule_lateness_seconds"} {
			require.Len(t, families[familyName].Metric, 2)
			for _, metric := range families[familyName].Metric {
				if nativeLabelValue(metric, "label") != "others" {
					continue
				}
				histogram := metric.GetHistogram()
				assert.Equal(t, uint64(3), histogram.GetSampleCount())
				factor := 1.0
				if familyName != metricschema.RequestLatency {
					factor = 1000
				}
				assert.InDelta(t, 112/factor, histogram.GetSampleSum(), 1e-12)
				for _, bucket := range histogram.Bucket {
					var want uint64
					for _, latency := range []float64{2, 10, 100} {
						if latency/factor <= bucket.GetUpperBound() {
							want++
						}
					}
					assert.Equal(t, want, bucket.GetCumulativeCount(), "bucket %v", bucket.GetUpperBound())
				}
			}
		}
	}
	// New labels never replace the initially admitted name, even if they sort
	// before it. Each source scrape is a cumulative snapshot, not an event batch.
	for i := range 20 {
		observations = append(observations, nativeObservation{fmt.Sprintf("new-%02d", i), "200", 1, 1})
		publish(nativeSnapshot(t, nativeTestMeta, "run", observations))
		assert.Equal(t, map[string]float64{"z/200": 1, "others/200": float64(i + 3), "others/500": 1}, nativeCounters(nativeScrape(t, handler)[metricschema.StatusCounter]))
	}
}

func TestNativeMetricsReplayOverflowConvention(t *testing.T) {
	source, publish := nativeSource(t, nativeSnapshot(t, nativeTestMeta, "run", []nativeObservation{{"a", "200", 1, 1}}))
	as := NewAgentServer(AgentServerOptions{EngineMeta: nativeTestMeta, MetricsURL: source.URL, NativeMetricsSchema: MetricsSchemaShibuyaV1, NativeMetricsOverflowLabel: "_other_", LabelsUsageLimit: 1})
	as.startMetricsRun("run")
	handler := as.metricsHandler()
	assert.Equal(t, map[string]float64{"a/200": 1}, nativeCounters(nativeScrape(t, handler)[metricschema.StatusCounter]))
	publish(nativeSnapshot(t, nativeTestMeta, "run", []nativeObservation{{"a", "200", 1, 1}, {"_other_", "200", 1, 1}, {"b", "200", 1, 1}, {"others", "200", 1, 1}}))
	assert.Equal(t, map[string]float64{"a/200": 1, "_other_/200": 3}, nativeCounters(nativeScrape(t, handler)[metricschema.StatusCounter]))
}

func TestNativeMetricsFinishAndNextRun(t *testing.T) {
	meta := EngineMeta{CollectionID: "collection-1", PlanID: "plan-1", EngineID: "0", Zone: "zone-1"}
	source, publish := nativeSource(t, nativeSnapshot(t, meta, "run-1", []nativeObservation{{"a", "200", 1, 1}}))
	fixture := newAgentFixture(t, AgentServerOptions{MetricsURL: source.URL, NativeMetricsSchema: MetricsSchemaShibuyaV1, LabelsUsageLimit: 1}, nil)
	handler := fixture.server.metricsHandler()
	fixture.send(runMessage(protocol.VerbStart, "run-1", ""))
	waitRunState(t, fixture, "run-1", protocol.RunRunning)
	assert.Equal(t, map[string]float64{"a/200": 1}, nativeCounters(nativeScrape(t, handler)[metricschema.StatusCounter]))
	fixture.server.stopRun("run-1", protocol.StopCompleted)
	waitRunState(t, fixture, "run-1", protocol.RunCompleted)
	publish(nativeSnapshot(t, meta, "run-1", []nativeObservation{{"a", "200", 1, 1}, {"b", "200", 1, 1}}))
	assert.Equal(t, map[string]float64{"a/200": 1, "others/200": 1}, nativeCounters(nativeScrape(t, handler)[metricschema.StatusCounter]), "final scrape retains the run's admission")
	fixture.send(runMessage(protocol.VerbStart, "run-2", ""))
	waitRunState(t, fixture, "run-2", protocol.RunRunning)
	require.Equal(t, http.StatusBadGateway, nativeResponse(handler).Code, "stale upstream identity must not consume the new run's slots")
	publish(nativeSnapshot(t, meta, "run-2", []nativeObservation{{"b", "200", 1, 1}, {"c", "200", 1, 1}}))
	assert.Equal(t, map[string]float64{"b/200": 1, "others/200": 1}, nativeCounters(nativeScrape(t, handler)[metricschema.StatusCounter]))
}

func TestNativeMetricsRejectsMalformedScrapesWithoutConsumingBudget(t *testing.T) {
	identity := `collection_id="collection",plan_id="plan",run_id="run",engine_no="engine",zone="zone"`
	counter := "# TYPE shibuya_status_counter counter\nshibuya_status_counter{" + identity + `,label="a",status="200"} 1` + "\n"
	histogram := nativeSnapshot(t, nativeTestMeta, "run", []nativeObservation{{"a", "200", 1, 1}, {"b", "200", 1, 1}, {"c", "200", 1, 1}})
	withoutSum := histogram
	for _, line := range strings.Split(histogram, "\n") {
		if strings.HasPrefix(line, metricschema.RequestLatency+"_sum") {
			withoutSum = strings.Replace(withoutSum, line+"\n", "", 1)
			break
		}
	}
	incompatible := histogram
	for _, line := range strings.Split(histogram, "\n") {
		if strings.HasPrefix(line, metricschema.RequestLatency+"_bucket") && strings.Contains(line, `label="b"`) && strings.Contains(line, `le="2.5"`) {
			incompatible = strings.Replace(incompatible, line, strings.Replace(line, `le="2.5"`, `le="3"`, 1), 1)
			break
		}
	}
	for name, invalid := range map[string]string{
		"syntax":                        "not valid prometheus\n",
		"missing type":                  strings.TrimPrefix(counter, "# TYPE shibuya_status_counter counter\n"),
		"missing label":                 strings.Replace(counter, `,label="a"`, "", 1),
		"missing identity":              strings.Replace(counter, `run_id="run",`, "", 1),
		"wrong identity":                strings.Replace(counter, `run_id="run"`, `run_id="old"`, 1),
		"negative counter":              strings.Replace(counter, "} 1", "} -1", 1),
		"nonfinite counter":             strings.Replace(counter, "} 1", "} NaN", 1),
		"duplicate counter":             counter + strings.Split(counter, "\n")[1] + "\n",
		"labelled gauge":                strings.ReplaceAll(strings.Replace(counter, " counter\n", " gauge\n", 1), "shibuya_status_counter", "custom_gauge"),
		"missing histogram sum":         withoutSum,
		"fractional histogram count":    strings.Replace(histogram, `le="1"} 1`, `le="1"} 0.5`, 1),
		"incompatible histogram bounds": incompatible,
		"overlong label":                strings.Replace(counter, `label="a"`, `label="`+strings.Repeat("a", 1024)+`"`, 1),
	} {
		t.Run(name, func(t *testing.T) {
			source, publish := nativeSource(t, invalid)
			as := NewAgentServer(AgentServerOptions{EngineMeta: nativeTestMeta, MetricsURL: source.URL, NativeMetricsSchema: MetricsSchemaShibuyaV1, LabelsUsageLimit: 1})
			as.startMetricsRun("run")
			handler := as.metricsHandler()
			response := nativeResponse(handler)
			require.Equal(t, http.StatusBadGateway, response.Code, response.Body.String())
			require.NotEmpty(t, response.Body.String())
			publish(nativeSnapshot(t, nativeTestMeta, "run", []nativeObservation{{"z", "200", 1, 1}}))
			assert.Equal(t, map[string]float64{"z/200": 1}, nativeCounters(nativeScrape(t, handler)[metricschema.StatusCounter]))
		})
	}
}

func TestNativeMetricsInputBound(t *testing.T) {
	source, _ := nativeSource(t, "# "+strings.Repeat("x", maxNativeMetricsBytes)+"\n")
	as := NewAgentServer(AgentServerOptions{MetricsURL: source.URL, NativeMetricsSchema: MetricsSchemaShibuyaV1})
	response := nativeResponse(as.metricsHandler())
	assert.Equal(t, http.StatusBadGateway, response.Code)
	assert.NotEmpty(t, response.Body.String())
}

func TestNativeMetricsAdmissionAcrossFamiliesAndScrapeOrder(t *testing.T) {
	identity := `collection_id="collection",plan_id="plan",run_id="run",engine_no="engine",zone="zone"`
	counter := func(label string) string {
		return "shibuya_status_counter{" + identity + `,label="` + label + `",status="200"} 1` + "\n"
	}
	atLimit := "# TYPE shibuya_status_counter counter\n" + counter("b") + counter("a")
	for _, initial := range []string{atLimit, "# TYPE shibuya_status_counter counter\n" + counter("a") + counter("b")} {
		source, publish := nativeSource(t, initial)
		as := NewAgentServer(AgentServerOptions{EngineMeta: nativeTestMeta, MetricsURL: source.URL, NativeMetricsSchema: MetricsSchemaShibuyaV1, LabelsUsageLimit: 2})
		as.startMetricsRun("run")
		handler := as.metricsHandler()
		assert.Equal(t, map[string]float64{"a/200": 1, "b/200": 1}, nativeCounters(nativeScrape(t, handler)[metricschema.StatusCounter]))
		publish(atLimit + counter("c") + "# TYPE shibuya_egress_bytes_counter counter\nshibuya_egress_bytes_counter{" + identity + `,label="d"} 12` + "\n")
		families := nativeScrape(t, handler)
		assert.Equal(t, map[string]float64{"a/200": 1, "b/200": 1, "others/200": 1}, nativeCounters(families[metricschema.StatusCounter]))
		assert.Equal(t, map[string]float64{"others": 12}, nativeCounters(families[metricschema.EgressBytesCounter]))
	}
	// The lexicographically first new name wins even when it occurs in a
	// different family; map iteration and sample order must not pick the winner.
	text := "# TYPE shibuya_status_counter counter\n" + counter("z") + "# TYPE shibuya_egress_bytes_counter counter\nshibuya_egress_bytes_counter{" + identity + `,label="a"} 12` + "\n"
	source, _ := nativeSource(t, text)
	as := NewAgentServer(AgentServerOptions{EngineMeta: nativeTestMeta, MetricsURL: source.URL, NativeMetricsSchema: MetricsSchemaShibuyaV1, LabelsUsageLimit: 1})
	as.startMetricsRun("run")
	families := nativeScrape(t, as.metricsHandler())
	assert.Equal(t, map[string]float64{"others/200": 1}, nativeCounters(families[metricschema.StatusCounter]))
	assert.Equal(t, map[string]float64{"a": 12}, nativeCounters(families[metricschema.EgressBytesCounter]))
}
