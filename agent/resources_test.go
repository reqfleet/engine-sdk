package agent

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/reqfleet/engine-sdk/metricschema"
	"github.com/reqfleet/engine-sdk/protocol"
	"github.com/reqfleet/engine-sdk/resources"
	log "github.com/sirupsen/logrus"
	logtest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type resourceSampleResult struct {
	sample  resources.Sample
	err     error
	release <-chan struct{}
}

type fakeResourceSampler struct {
	mu      sync.Mutex
	results []resourceSampleResult
	index   int
	calls   int
	entered chan int
}

func (s *fakeResourceSampler) Sample() (resources.Sample, error) {
	s.mu.Lock()
	s.calls++
	call := s.calls
	result := resourceSampleResult{err: errors.New("unexpected resource sample")}
	if s.index < len(s.results) {
		result = s.results[s.index]
		s.index++
	}
	s.mu.Unlock()
	if s.entered != nil {
		s.entered <- call
	}
	if result.release != nil {
		<-result.release
	}
	return result.sample, result.err
}

func (s *fakeResourceSampler) sampleCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func gatherResourceSnapshot(t *testing.T, registry *MetricsRegistry) map[string]*dto.MetricFamily {
	t.Helper()
	families, err := registry.Gather()
	require.NoError(t, err)
	all := make(map[string]*dto.MetricFamily, len(families))
	for _, family := range families {
		all[family.GetName()] = family
	}
	return all
}

func TestResourceMetricsOmitUnavailableRatesAndPreserveRunIdentity(t *testing.T) {
	meta := EngineMeta{CollectionID: "collection", PlanID: "plan", Zone: "zone", EngineID: "engine"}
	server := NewAgentServer(AgentServerOptions{EngineMeta: meta})
	run := newTestMetricsRun(t, server, "first")
	assert.Nil(t, run.Metrics)
	handler := server.metricsHandler()
	assert.Empty(t, nativeScrape(t, handler), "before launch there is no published resource identity")
	server.prepareMetricsForRun(&run)
	registry := run.Metrics.Registry()
	sampler := &fakeResourceSampler{results: []resourceSampleResult{
		{err: resources.ErrSampleUnavailable},
		{sample: resources.Sample{CPUCores: 0.5, MemoryBytes: 8192}},
		{err: resources.ErrSampleUnavailable}, // A counter decrease invalidates both published values.
		{},
	}}
	registry.resources.sampler = sampler
	assert.Empty(t, gatherResourceSnapshot(t, registry))
	assert.Empty(t, nativeScrape(t, handler))
	assert.Zero(t, sampler.sampleCount(), "launch, Gather, and HTTP must not sample resources")
	server.sampleOwnMetrics(run.Metrics)
	assert.Empty(t, nativeScrape(t, handler), "baseline-only attempts omit both resources")
	server.sampleOwnMetrics(run.Metrics)
	all := nativeScrape(t, handler)
	for name, value := range map[string]float64{metricschema.CPUGauge: 0.5, metricschema.MemoryGauge: 8192} {
		require.Contains(t, all, name)
		require.Len(t, all[name].Metric, 1)
		metric := all[name].Metric[0]
		assert.Equal(t, value, metric.GetGauge().GetValue())
		labels := make(map[string]string)
		for _, pair := range metric.Label {
			labels[pair.GetName()] = pair.GetValue()
		}
		assert.Equal(t, map[string]string{
			"collection_id": "collection", "plan_id": "plan", "run_id": "first", "zone": "zone", "engine_no": "engine",
		}, labels)
	}
	server.sampleOwnMetrics(run.Metrics)
	assert.Empty(t, nativeScrape(t, handler), "a counter decrease removes both stale gauges")
	server.sampleOwnMetrics(run.Metrics)
	all = nativeScrape(t, handler)
	require.Contains(t, all, metricschema.CPUGauge)
	require.Contains(t, all, metricschema.MemoryGauge)
	assert.Zero(t, all[metricschema.CPUGauge].Metric[0].GetGauge().GetValue())
	assert.Zero(t, all[metricschema.MemoryGauge].Metric[0].GetGauge().GetValue())

	retained := gatherResourceSnapshot(t, registry)
	next := newTestMetricsRun(t, server, "second")
	assert.Nil(t, next.Metrics)
	assert.Empty(t, nativeScrape(t, handler), "accepting a new run clears completed resources")
	server.prepareMetricsForRun(&next)
	assert.NotSame(t, registry.resources.sampler, next.Metrics.Registry().resources.sampler)
	nextSampler := &fakeResourceSampler{results: []resourceSampleResult{
		{err: resources.ErrSampleUnavailable},
		{sample: resources.Sample{CPUCores: 0.75, MemoryBytes: 16384}},
	}}
	next.Metrics.Registry().resources.sampler = nextSampler
	assert.Empty(t, nativeScrape(t, handler), "a new run does not expose completed-run resources")
	server.sampleOwnMetrics(next.Metrics)
	assert.Empty(t, nativeScrape(t, handler), "a new baseline must not publish either resource")
	server.sampleOwnMetrics(next.Metrics)
	all = nativeScrape(t, handler)
	for name, value := range map[string]float64{metricschema.CPUGauge: 0.75, metricschema.MemoryGauge: 16384} {
		require.Len(t, all[name].Metric, 1)
		assert.Equal(t, "second", nativeLabelValue(all[name].Metric[0], "run_id"))
		assert.Equal(t, value, all[name].Metric[0].GetGauge().GetValue())
	}
	assert.Equal(t, retained, gatherResourceSnapshot(t, registry), "replacement leaves the old registry intact")
	assert.Equal(t, 4, sampler.sampleCount(), "a new run never samples the previous sampler")
}

func TestResourceMetricsSampleWithoutScrapesAndStopOnCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		server := NewAgentServer(AgentServerOptions{})
		run := startTestMetricsRun(t, server, "run")
		registry := run.Metrics.Registry()
		sampler := &fakeResourceSampler{results: []resourceSampleResult{
			{err: resources.ErrSampleUnavailable},
			{sample: resources.Sample{CPUCores: 0.5, MemoryBytes: 8192}},
			{sample: resources.Sample{CPUCores: 99, MemoryBytes: 999}},
		}}
		registry.resources.sampler = sampler
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan struct{})
		go func() {
			defer close(done)
			server.reportOwnMetrics(ctx, run.Metrics, time.Second)
		}()
		synctest.Wait()
		assert.Equal(t, 1, sampler.sampleCount(), "the baseline is collected immediately")
		assert.Empty(t, gatherResourceSnapshot(t, registry))

		<-time.After(time.Second)
		synctest.Wait()
		before := gatherResourceSnapshot(t, registry)
		require.Contains(t, before, metricschema.CPUGauge)
		require.Contains(t, before, metricschema.MemoryGauge)
		assert.Equal(t, 0.5, before[metricschema.CPUGauge].Metric[0].GetGauge().GetValue())
		assert.Equal(t, float64(8192), before[metricschema.MemoryGauge].Metric[0].GetGauge().GetValue())
		cancel()
		<-done
		<-time.After(3 * time.Second)
		synctest.Wait()
		assert.Equal(t, before, gatherResourceSnapshot(t, registry), "idle time must not replace completed values")
		assert.Equal(t, 2, sampler.sampleCount(), "cancellation stops resource reads")
	})
}

func TestResourceMetricsErrorsSkipUpdatesAndRecover(t *testing.T) {
	for _, published := range []bool{false, true} {
		name := "before first sample"
		if published {
			name = "after successful sample"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				logger, hook := logtest.NewNullLogger()
				server := NewAgentServer(AgentServerOptions{Logger: log.NewEntry(logger), LabelsUsageLimit: 1})
				run := startTestMetricsRun(t, server, "run")
				recorder := run.Metrics
				registry := recorder.Registry()
				sampleErr := errors.New("resource read failed")
				sampler := &fakeResourceSampler{results: []resourceSampleResult{
					{sample: resources.Sample{CPUCores: 0.5, MemoryBytes: 8192}},
					{sample: resources.Sample{CPUCores: 99, MemoryBytes: 999}, err: sampleErr},
					{sample: resources.Sample{CPUCores: 0.75, MemoryBytes: 16384}},
				}}
				if !published {
					sampler.results[0] = resourceSampleResult{err: resources.ErrSampleUnavailable}
				}
				registry.resources.sampler = sampler
				require.NoError(t, recorder.RecordEvent(ShibuyaMetric{RunID: "run", Label: "checkout", Status: "200", Latency: 1}))
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				done := make(chan struct{})
				go func() {
					defer close(done)
					server.reportOwnMetrics(ctx, run.Metrics, time.Second)
				}()
				synctest.Wait()
				before := gatherResourceSnapshot(t, registry)
				<-time.After(time.Second)
				synctest.Wait()
				assert.Equal(t, before, gatherResourceSnapshot(t, registry), "failed samples leave every metric unchanged")
				entries := hook.AllEntries()
				require.Len(t, entries, 1)
				assert.Equal(t, log.WarnLevel, entries[0].Level)
				assert.Equal(t, sampleErr, entries[0].Data[log.ErrorKey])
				scraped := nativeScrape(t, server.metricsHandler())
				require.Contains(t, scraped, metricschema.StatusCounter, "resource errors do not fail HTTP or request metrics")
				assert.Equal(t, before[metricschema.CPUGauge], scraped[metricschema.CPUGauge])
				assert.Equal(t, before[metricschema.MemoryGauge], scraped[metricschema.MemoryGauge])

				<-time.After(time.Second)
				synctest.Wait()
				after := gatherResourceSnapshot(t, registry)
				require.Contains(t, after, metricschema.CPUGauge)
				require.Contains(t, after, metricschema.MemoryGauge)
				assert.Equal(t, 0.75, after[metricschema.CPUGauge].Metric[0].GetGauge().GetValue())
				assert.Equal(t, float64(16384), after[metricschema.MemoryGauge].Metric[0].GetGauge().GetValue())
				cancel()
				<-done
				assert.Equal(t, 3, sampler.sampleCount(), "scrapes do not trigger recovery reads")
			})
		})
	}
}

func TestResourceMetricsCancellationJoinsInFlightSample(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		server := NewAgentServer(AgentServerOptions{})
		run := startTestMetricsRun(t, server, "run")
		registry := run.Metrics.Registry()
		release := make(chan struct{})
		sampler := &fakeResourceSampler{
			results: []resourceSampleResult{{sample: resources.Sample{CPUCores: 0.5, MemoryBytes: 8192}, release: release}},
			entered: make(chan int, 1),
		}
		registry.resources.sampler = sampler
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan struct{})
		go func() {
			defer close(done)
			server.reportOwnMetrics(ctx, run.Metrics, time.Second)
		}()
		<-sampler.entered
		<-time.After(3 * time.Second)
		synctest.Wait()
		assert.Equal(t, 1, sampler.sampleCount(), "ticks cannot overlap an unfinished sample")
		assert.Empty(t, gatherResourceSnapshot(t, registry))
		cancel()
		synctest.Wait()
		select {
		case <-done:
			t.Error("the worker returned before its in-flight sample finished")
		default:
		}
		close(release)
		<-done
		all := gatherResourceSnapshot(t, registry)
		require.Contains(t, all, metricschema.CPUGauge)
		require.Contains(t, all, metricschema.MemoryGauge)
		assert.Equal(t, 0.5, all[metricschema.CPUGauge].Metric[0].GetGauge().GetValue())
		assert.Equal(t, float64(8192), all[metricschema.MemoryGauge].Metric[0].GetGauge().GetValue())
		assert.Equal(t, 1, sampler.sampleCount(), "cancellation prevents another live sample")
	})
}

func TestResourceMetricsCollectionFailuresStayOnTheirRun(t *testing.T) {
	server := NewAgentServer(AgentServerOptions{})
	run := startTestMetricsRun(t, server, "first")
	registry := run.Metrics.Registry()
	collectionErr := errors.New("previous collection failed")
	registry.setCollectionError(collectionErr)
	_, err := server.gatherMetrics()
	require.ErrorIs(t, err, collectionErr)
	next := startTestMetricsRun(t, server, "next")
	assert.NotSame(t, registry, next.Metrics.Registry())
	assert.NotSame(t, registry.resources.sampler, next.Metrics.Registry().resources.sampler)
	current, err := server.gatherMetrics()
	require.NoError(t, err)
	assert.Empty(t, current)
	_, err = registry.Gather()
	require.ErrorIs(t, err, collectionErr, "publishing another run must not clear the original error")
}

func TestResourceMetricsRetainFinalSample(t *testing.T) {
	for _, failed := range []bool{false, true} {
		name := "final sample"
		if failed {
			name = "failed final sample"
		}
		t.Run(name, func(t *testing.T) {
			release := make(chan struct{})
			releaseSample := sync.OnceFunc(func() { close(release) })
			defer releaseSample()
			want := resources.Sample{CPUCores: 0.5, MemoryBytes: 8192}
			sampler := &fakeResourceSampler{
				results: []resourceSampleResult{{err: resources.ErrSampleUnavailable, release: release}, {sample: want}},
				entered: make(chan int, 4),
			}
			if failed {
				sampler.results[0].sample, sampler.results[0].err = want, nil
				sampler.results[1] = resourceSampleResult{err: errors.New("final resource read failed")}
			}
			server := NewAgentServer(AgentServerOptions{})
			run := startTestMetricsRun(t, server, "run-1")
			run.Metrics.Registry().resources.sampler = sampler
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan struct{})
			go func() {
				defer close(done)
				server.reportOwnMetrics(ctx, run.Metrics, time.Hour)
			}()
			select {
			case call := <-sampler.entered:
				require.Equal(t, 1, call)
			case <-time.After(5 * time.Second):
				t.Fatal("the resource worker did not start sampling")
			}
			cancel()
			releaseSample()
			<-done
			server.sampleOwnMetrics(run.Metrics)
			server.finishMetricsRun(run.RunID)
			assert.Equal(t, gatherResourceSnapshot(t, run.Metrics.Registry()), nativeScrape(t, server.metricsHandler()))
			assert.Equal(t, 2, sampler.sampleCount(), "one worker sample and one explicit final sample")
			handler := server.metricsHandler()
			all := nativeScrape(t, handler)
			for name, value := range map[string]float64{metricschema.CPUGauge: want.CPUCores, metricschema.MemoryGauge: float64(want.MemoryBytes)} {
				require.Contains(t, all, name)
				require.Len(t, all[name].Metric, 1)
				assert.Equal(t, value, all[name].Metric[0].GetGauge().GetValue())
				assert.Equal(t, "run-1", nativeLabelValue(all[name].Metric[0], "run_id"))
			}
			assert.Equal(t, all, nativeScrape(t, handler), "scrapes retain the final sampled snapshot")
			assert.Equal(t, 2, sampler.sampleCount(), "scrapes do not read resources")
		})
	}
}

func TestNativeMetricsNeverSampleSDKResources(t *testing.T) {
	const body = "adapter_ready 1\n"
	exporter := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(body))
	})
	source := httptest.NewServer(exporter)
	defer source.Close()
	for _, test := range []struct {
		name    string
		options AgentServerOptions
	}{
		{"handler", AgentServerOptions{MetricsHandler: exporter, NativeMetricsSchema: MetricsSchemaShibuyaV1}},
		{"proxy", AgentServerOptions{MetricsURL: source.URL, NativeMetricsSchema: MetricsSchemaShibuyaV1}},
	} {
		t.Run(test.name, func(t *testing.T) {
			runs := make(chan RunContext, 1)
			test.options.Observe = func(_ context.Context, run RunContext) (protocol.Observation, error) {
				select {
				case runs <- run:
				default:
				}
				return protocol.ObservationReady, nil
			}
			fixture := newAgentFixture(t, test.options, nil)
			handler := fixture.server.metricsHandler()
			fixture.send(runMessage(protocol.VerbStart, "native-run", ""))
			waitRunState(t, fixture, "native-run", protocol.RunRunning)
			assert.Nil(t, (<-runs).Metrics)
			response := nativeResponse(handler)
			assert.Equal(t, http.StatusOK, response.Code)
			assert.Equal(t, body, response.Body.String(), "running native output is not merged with SDK resources")
			fixture.server.stopRun("native-run", protocol.StopCompleted)
			waitRunState(t, fixture, "native-run", protocol.RunCompleted)
			response = nativeResponse(handler)
			assert.Equal(t, http.StatusOK, response.Code)
			assert.Equal(t, body, response.Body.String(), "completed native output is retained unchanged")
		})
	}
}

func TestResourceMetricsAreIsolatedBetweenAgents(t *testing.T) {
	first := NewAgentServer(AgentServerOptions{})
	second := NewAgentServer(AgentServerOptions{})
	firstRun := startTestMetricsRun(t, first, "shared-run")
	secondRun := startTestMetricsRun(t, second, "shared-run")
	firstRun.Metrics.Registry().resources.sampler = &fakeResourceSampler{results: []resourceSampleResult{
		{sample: resources.Sample{CPUCores: 0.5, MemoryBytes: 8192}},
	}}
	secondRun.Metrics.Registry().resources.sampler = &fakeResourceSampler{results: []resourceSampleResult{
		{sample: resources.Sample{CPUCores: 0.75, MemoryBytes: 16384}},
	}}
	first.sampleOwnMetrics(firstRun.Metrics)
	assert.Empty(t, gatherResourceSnapshot(t, secondRun.Metrics.Registry()))
	second.sampleOwnMetrics(secondRun.Metrics)
	firstSnapshot := gatherResourceSnapshot(t, firstRun.Metrics.Registry())
	secondSnapshot := gatherResourceSnapshot(t, secondRun.Metrics.Registry())
	for name, values := range map[string][2]float64{
		metricschema.CPUGauge:    {0.5, 0.75},
		metricschema.MemoryGauge: {8192, 16384},
	} {
		require.Contains(t, firstSnapshot, name)
		require.Contains(t, secondSnapshot, name)
		assert.Equal(t, values[0], firstSnapshot[name].Metric[0].GetGauge().GetValue())
		assert.Equal(t, values[1], secondSnapshot[name].Metric[0].GetGauge().GetValue())
	}
	next := startTestMetricsRun(t, first, "next")
	assert.Empty(t, gatherResourceSnapshot(t, next.Metrics.Registry()))
	assert.Equal(t, firstSnapshot, gatherResourceSnapshot(t, firstRun.Metrics.Registry()))
	assert.Equal(t, secondSnapshot, gatherResourceSnapshot(t, secondRun.Metrics.Registry()))
}

func TestResourceMetricsInFlightSampleStaysOnOriginalRun(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		server := NewAgentServer(AgentServerOptions{})
		first := startTestMetricsRun(t, server, "first")
		release := make(chan struct{})
		sampler := &fakeResourceSampler{
			results: []resourceSampleResult{{sample: resources.Sample{CPUCores: 0.5, MemoryBytes: 8192}, release: release}},
			entered: make(chan int, 1),
		}
		first.Metrics.Registry().resources.sampler = sampler
		done := make(chan struct{})
		go func() {
			defer close(done)
			server.sampleOwnMetrics(first.Metrics)
		}()
		<-sampler.entered
		next := startTestMetricsRun(t, server, "next")
		nextSampler := &fakeResourceSampler{results: []resourceSampleResult{
			{sample: resources.Sample{CPUCores: 0.75, MemoryBytes: 16384}},
		}}
		next.Metrics.Registry().resources.sampler = nextSampler
		server.sampleOwnMetrics(next.Metrics)
		before := nativeScrape(t, server.metricsHandler())
		close(release)
		<-done
		assert.Equal(t, before, nativeScrape(t, server.metricsHandler()), "an old in-flight sample cannot overwrite the new run")
		firstSnapshot := gatherResourceSnapshot(t, first.Metrics.Registry())
		for name, value := range map[string]float64{metricschema.CPUGauge: 0.5, metricschema.MemoryGauge: 8192} {
			require.Contains(t, firstSnapshot, name)
			require.Len(t, firstSnapshot[name].Metric, 1)
			assert.Equal(t, value, firstSnapshot[name].Metric[0].GetGauge().GetValue())
			assert.Equal(t, "first", nativeLabelValue(firstSnapshot[name].Metric[0], "run_id"))
		}
		assert.Equal(t, 1, sampler.sampleCount())
		assert.Equal(t, 1, nextSampler.sampleCount())
	})
}
