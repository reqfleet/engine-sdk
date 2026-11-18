//go:build unix

package agent

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/reqfleet/engine-sdk/metricschema"
	"github.com/reqfleet/engine-sdk/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMetricsCollectionJoinsAndDrainsFinalOutputBeforeRelease(t *testing.T) {
	startedRuns := make(chan RunContext, 2)
	entered, canceled := make(chan struct{}), make(chan struct{})
	liveRelease, finalEntered, finalRelease := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	nextRecorded := false // Live and final ingestion are serialized.
	marker := filepath.Join(t.TempDir(), "exit")
	f := newAgentFixture(t, AgentServerOptions{
		LabelsUsageLimit: 1,
		ResultFile:       "results.csv",
		BuildCommand: func(_ context.Context, run RunContext) (Command, error) {
			if run.RunID == "next" {
				return Command{Command: "true"}, nil
			}
			return Command{Command: "sh", Args: []string{
				"-c", `while [ ! -f "$1" ]; do sleep 0.01; done; printf '12.5' > "$2"`, "sh", marker, run.ResultFile,
			}}, nil
		},
		CustomIngestMetrics: func(ctx context.Context, run RunContext) error {
			if run.Metrics == nil {
				return errors.New("post-launch collection has no recorder")
			}
			if run.RunID == "next" {
				if nextRecorded {
					return nil
				}
				nextRecorded = true
				startedRuns <- run
				return run.Metrics.ObserveRequest(ShibuyaMetric{Label: "next-request", Status: "201", Latency: 7})
			}
			if calls.Add(1) == 1 {
				startedRuns <- run
				close(entered)
				<-ctx.Done()
				close(canceled)
				<-liveRelease
				return ctx.Err()
			}
			if ctx.Err() != nil {
				return errors.New("final collection inherited cancellation")
			}
			if _, ok := ctx.Deadline(); !ok {
				return errors.New("final collection has no deadline")
			}
			close(finalEntered)
			<-finalRelease
			data, err := os.ReadFile(run.ResultFile)
			if err != nil {
				return err
			}
			latency, err := strconv.ParseFloat(string(data), 64)
			if err != nil {
				return err
			}
			return run.Metrics.ObserveRequest(ShibuyaMetric{RunID: run.RunID, Label: "last", Status: "200", Latency: latency})
		},
	}, nil)
	releaseLive := sync.OnceFunc(func() { close(liveRelease) })
	releaseFinal := sync.OnceFunc(func() { close(finalRelease) })
	t.Cleanup(releaseLive)
	t.Cleanup(releaseFinal)
	await := func(ch <-chan struct{}) {
		t.Helper()
		select {
		case <-ch:
		case <-time.After(5 * time.Second):
			t.Fatal("collection did not reach the expected lifecycle boundary")
		}
	}
	f.send(runMessage(protocol.VerbStart, "first", ""))
	await(entered)
	require.NoError(t, os.WriteFile(marker, nil, 0o600))
	await(canceled)
	assert.False(t, readProgressForTest(t, f.server).State.Terminal())
	require.Error(t, f.server.queueStart(runMessage(protocol.VerbStart, "next", "")))
	releaseLive()
	await(finalEntered)
	assert.False(t, readProgressForTest(t, f.server).State.Terminal())
	require.Error(t, f.server.queueStart(runMessage(protocol.VerbStart, "next", "")))
	releaseFinal()
	waitRunState(t, f, "first", protocol.RunCompleted)
	firstRun := <-startedRuns
	firstSnapshot := gatherResourceSnapshot(t, firstRun.Metrics.Registry())
	for range 2 {
		families := nativeScrape(t, f.server.metricsHandler())
		require.Contains(t, families, metricschema.RequestLatency)
		hist := families[metricschema.RequestLatency].Metric[0].GetHistogram()
		assert.Equal(t, uint64(1), hist.GetSampleCount())
		assert.Equal(t, 12.5, hist.GetSampleSum())
		assert.Equal(t, "first", nativeLabelValue(families[metricschema.RequestLatency].Metric[0], "run_id"))
	}
	assert.Equal(t, int32(2), calls.Load(), "scraping must not collect or duplicate the final row")
	f.send(runMessage(protocol.VerbStart, "next", ""))
	waitRunState(t, f, "next", protocol.RunCompleted)
	nextRun := <-startedRuns
	assert.NotSame(t, firstRun.Metrics, nextRun.Metrics)
	assert.NotSame(t, firstRun.Metrics.Registry(), nextRun.Metrics.Registry())
	assert.NotSame(t, firstRun.Metrics.Registry().resources.sampler, nextRun.Metrics.Registry().resources.sampler)
	assert.Equal(t, firstSnapshot, gatherResourceSnapshot(t, firstRun.Metrics.Registry()))
	families := nativeScrape(t, f.server.metricsHandler())
	require.Contains(t, families, metricschema.RequestLatency)
	require.Len(t, families[metricschema.RequestLatency].Metric, 1)
	metric := families[metricschema.RequestLatency].Metric[0]
	assert.Equal(t, "next", nativeLabelValue(metric, "run_id"))
	assert.Equal(t, "next-request", nativeLabelValue(metric, "label"), "the new recorder has an independent name budget")
	assert.Equal(t, uint64(1), metric.GetHistogram().GetSampleCount())
	assert.Equal(t, 7.0, metric.GetHistogram().GetSampleSum())
	assert.Equal(t, map[string]float64{"next-request/201": 1}, nativeCounters(families[metricschema.StatusCounter]))
}

func TestMetricsCollectionFailureFailsRunAndRecoversOnNextRun(t *testing.T) {
	for _, test := range []struct {
		name     string
		final    bool
		stop     bool
		deadline bool
	}{
		{name: "live"},
		{name: "final", final: true},
		{name: "scheduled final", final: true, stop: true},
		{name: "scheduled deadline", final: true, stop: true, deadline: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			startedRuns := make(chan RunContext, 2)
			failure := errors.New("invalid engine output")
			if test.deadline {
				failure = context.DeadlineExceeded
			}
			liveEntered := make(chan struct{})
			recorded := false // Live and final collection must be serialized.
			collectedRunID := ""
			f := newAgentFixture(t, AgentServerOptions{
				LabelsUsageLimit: 1,
				BuildCommand: func(_ context.Context, run RunContext) (Command, error) {
					if run.RunID == "bad" && (!test.final || test.stop) {
						return Command{Command: "sleep", Args: []string{"30"}}, nil
					}
					return Command{Command: "true"}, nil
				},
				CustomIngestMetrics: func(ctx context.Context, run RunContext) error {
					if run.Metrics == nil {
						return errors.New("post-launch collection has no recorder")
					}
					if collectedRunID != run.RunID {
						collectedRunID = run.RunID
						startedRuns <- run
						recorded = false
					}
					if run.RunID == "bad" {
						if _, final := ctx.Deadline(); test.final && !final {
							if test.stop {
								close(liveEntered)
								<-ctx.Done()
								return ctx.Err()
							}
							return nil
						}
						if err := run.Metrics.ObserveRequest(ShibuyaMetric{RunID: run.RunID, Label: "partial", Status: "200", Latency: 1}); err != nil {
							return err
						}
						if test.deadline {
							<-ctx.Done()
							return nil
						}
						return failure
					}
					if !recorded {
						if err := run.Metrics.ObserveRequest(ShibuyaMetric{RunID: run.RunID, Label: "complete", Status: "201", Latency: 5}); err != nil {
							return err
						}
						recorded = true
					}
					return nil
				},
			}, nil)
			f.send(runMessage(protocol.VerbStart, "bad", ""))
			if test.stop {
				select {
				case <-liveEntered:
				case <-time.After(5 * time.Second):
					t.Fatal("live collection did not start")
				}
				f.server.stopRun("bad", protocol.StopCompleted)
			}
			status := waitRunState(t, f, "bad", protocol.RunFailed)
			assert.Equal(t, protocol.FailureExecution, status.FailurePhase)
			if test.stop {
				assert.Equal(t, protocol.StopCompleted, status.StopReason)
			}
			badRun := <-startedRuns
			_, err := badRun.Metrics.Registry().Gather()
			require.ErrorIs(t, err, failure)
			response := nativeResponse(f.server.metricsHandler())
			assert.Equal(t, http.StatusInternalServerError, response.Code)
			assert.NotContains(t, response.Body.String(), metricschema.StatusCounter, "failed collection must not expose partial totals")
			f.send(runMessage(protocol.VerbStart, "good", ""))
			waitRunState(t, f, "good", protocol.RunCompleted)
			goodRun := <-startedRuns
			assert.NotSame(t, badRun.Metrics, goodRun.Metrics)
			_, err = badRun.Metrics.Registry().Gather()
			require.ErrorIs(t, err, failure, "a later launch must not erase the previous run's error")
			families := nativeScrape(t, f.server.metricsHandler())
			require.Contains(t, families, metricschema.StatusCounter)
			assert.Equal(t, map[string]float64{"complete/201": 1}, nativeCounters(families[metricschema.StatusCounter]))
			assert.Equal(t, "good", nativeLabelValue(families[metricschema.StatusCounter].Metric[0], "run_id"))
		})
	}
}

func TestMetricsCollectionFailureSurvivesConcurrentProcessExit(t *testing.T) {
	failure := errors.New("invalid engine output")
	server := NewAgentServer(AgentServerOptions{
		CustomIngestMetrics: func(ctx context.Context, _ RunContext) error {
			if _, final := ctx.Deadline(); final {
				return nil
			}
			return failure
		},
	})
	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	run := &agentRun{
		context:  startTestMetricsRun(t, server, "run"),
		ctx:      runCtx,
		cancel:   cancel,
		busy:     true,
		launched: true,
		status: protocol.RunStatus{
			RunID: "run", State: protocol.RunRunning, StartupStatus: protocol.StartupStateStarted,
		},
	}
	server.runtime.run = run
	monitorCtx, stopMonitor := context.WithCancel(runCtx)
	defer stopMonitor()
	done := make(chan struct{})
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("live collection did not stop")
		}
	})
	func() {
		server.stateLock.Lock()
		defer server.stateLock.Unlock()
		go func() {
			defer close(done)
			server.customIngesterRunner(monitorCtx, run)
		}()
		// Hold the lifecycle lock until the live error is accepted, then model
		// process exit before the collector can record its lifecycle failure.
		require.Eventually(t, func() bool {
			_, err := run.context.Metrics.Registry().Gather()
			return errors.Is(err, failure)
		}, 5*time.Second, time.Millisecond)
		run.exited = true
		stopMonitor()
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("live collection did not finish after process exit")
	}
	require.NoError(t, server.collectFinalMetrics(run))
	server.stateLock.Lock()
	server.finishRunLocked(run, nil, protocol.FailureExecution, 0)
	server.stateLock.Unlock()

	status := readProgressForTest(t, server)
	assert.Equal(t, protocol.RunFailed, status.State)
	assert.Equal(t, protocol.FailureExecution, status.FailurePhase)
	_, err := run.context.Metrics.Registry().Gather()
	require.ErrorIs(t, err, failure)
	assert.Equal(t, http.StatusInternalServerError, nativeResponse(server.metricsHandler()).Code)
}

func TestMetricsPublicationFollowsLaunchNotPreparationOrReadiness(t *testing.T) {
	prepared := make(chan RunContext, 2)
	started := make(chan RunContext, 2)
	built := make(chan RunContext, 2)
	collected := make(chan RunContext, 2)
	observed := make(chan RunContext, 1)
	release := make(chan struct{})
	releasePreparation := sync.OnceFunc(func() { close(release) })
	recorded := make(map[string]bool) // Live and final collection are serialized.
	f := newAgentFixture(t, AgentServerOptions{
		LabelsUsageLimit: 1,
		ResultFile:       "results.csv",
		PrepareDependencies: func(ctx context.Context, run RunContext) error {
			prepared <- run
			if run.RunID == "next" {
				select {
				case <-release:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return nil
		},
		BeforeStart: func(_ context.Context, run RunContext) error {
			started <- run
			return nil
		},
		BuildCommand: func(_ context.Context, run RunContext) (Command, error) {
			built <- run
			script := `printf '3' > "$1"`
			if run.RunID == "next" {
				script = `printf '7' > "$1"; exec sleep 30`
			}
			return Command{Command: "sh", Args: []string{"-c", script, "sh", run.ResultFile}}, nil
		},
		CustomIngestMetrics: func(_ context.Context, run RunContext) error {
			if run.Metrics == nil {
				return errors.New("post-launch collection has no recorder")
			}
			if recorded[run.RunID] {
				return nil
			}
			data, err := os.ReadFile(run.ResultFile)
			if errors.Is(err, os.ErrNotExist) || err == nil && len(data) == 0 {
				return nil
			}
			if err != nil {
				return err
			}
			latency, err := strconv.ParseFloat(string(data), 64)
			if err != nil {
				return err
			}
			if err := run.Metrics.ObserveRequest(ShibuyaMetric{
				Label: run.RunID + "-request", Status: "200", Latency: latency,
			}); err != nil {
				return err
			}
			recorded[run.RunID] = true
			collected <- run
			return nil
		},
		Observe: func(_ context.Context, run RunContext) (protocol.Observation, error) {
			if run.RunID == "next" {
				select {
				case observed <- run:
				default:
				}
			}
			return protocol.ObservationPending, nil
		},
	}, nil)
	t.Cleanup(releasePreparation)
	awaitRun := func(ch <-chan RunContext) RunContext {
		t.Helper()
		select {
		case run := <-ch:
			return run
		case <-time.After(5 * time.Second):
			t.Fatal("callback did not reach the expected lifecycle boundary")
			return RunContext{}
		}
	}
	handler := f.server.metricsHandler()
	require.NoError(t, f.server.queuePrepare(runMessage(protocol.VerbPrepare, "first", "requirements.txt")))
	waitRunState(t, f, "first", protocol.RunPrepared)
	assert.Nil(t, awaitRun(prepared).Metrics)
	assert.Empty(t, nativeScrape(t, handler), "preparation has no workload metrics")
	require.NoError(t, f.server.queueStart(runMessage(protocol.VerbStart, "first", "requirements.txt")))
	waitRunState(t, f, "first", protocol.RunCompleted)
	assert.Nil(t, awaitRun(started).Metrics)
	assert.Nil(t, awaitRun(built).Metrics)
	first := awaitRun(collected)
	require.NotNil(t, first.Metrics)
	retained := nativeScrape(t, handler)
	retainedRegistry := gatherResourceSnapshot(t, first.Metrics.Registry())
	require.Contains(t, retained, metricschema.StatusCounter)
	assert.Equal(t, map[string]float64{"first-request/200": 1}, nativeCounters(retained[metricschema.StatusCounter]))
	assert.Equal(t, 3.0, retained[metricschema.RequestLatency].Metric[0].GetHistogram().GetSampleSum())

	require.NoError(t, f.server.queueStart(runMessage(protocol.VerbStart, "first", "requirements.txt")))
	require.NoError(t, f.server.queuePrepare(runMessage(protocol.VerbPrepare, "first", "requirements.txt")))
	assert.Equal(t, retained, nativeScrape(t, handler), "duplicate completed requests preserve output")
	require.Error(t, f.server.queueStart(runMessage(protocol.VerbStart, "", "")))
	require.Error(t, f.server.queueStart(runMessage(protocol.VerbStart, "unprepared", "requirements.txt")))
	invalid := runMessage(protocol.VerbStart, "invalid", "")
	invalid.RunConfigs = nil
	require.Error(t, f.server.queueStart(invalid))
	assert.Equal(t, retained, nativeScrape(t, handler), "rejected requests preserve output")
	f.server.stopRun("stale", protocol.StopCanceled)
	require.NoError(t, f.server.queueStart(runMessage(protocol.VerbStart, "stale", "")))
	require.NoError(t, f.server.queuePrepare(runMessage(protocol.VerbPrepare, "stale", "")))
	assert.Equal(t, retained, nativeScrape(t, handler), "retired requests preserve output")

	require.NoError(t, f.server.queuePrepare(runMessage(protocol.VerbPrepare, "next", "requirements.txt")))
	assert.Empty(t, nativeScrape(t, handler), "acceptance clears completed output before preparation finishes")
	assert.Nil(t, awaitRun(prepared).Metrics)
	require.Error(t, f.server.queueStart(runMessage(protocol.VerbStart, "next", "requirements.txt")))
	releasePreparation()
	waitRunState(t, f, "next", protocol.RunPrepared)
	assert.Empty(t, nativeScrape(t, handler))
	require.NoError(t, f.server.queueStart(runMessage(protocol.VerbStart, "next", "requirements.txt")))
	assert.Nil(t, awaitRun(started).Metrics)
	assert.Nil(t, awaitRun(built).Metrics)
	next := awaitRun(collected)
	require.NotNil(t, next.Metrics)
	assert.Same(t, next.Metrics, awaitRun(observed).Metrics)
	assert.NotSame(t, first.Metrics, next.Metrics)
	assert.NotSame(t, first.Metrics.Registry(), next.Metrics.Registry())
	assert.Equal(t, protocol.StartupStatePending, readProgressForTest(t, f.server).StartupStatus)
	current := nativeScrape(t, handler)
	require.Contains(t, current, metricschema.StatusCounter)
	assert.Equal(t, map[string]float64{"next-request/200": 1}, nativeCounters(current[metricschema.StatusCounter]),
		"the new run has an independent request-name budget")
	assert.Equal(t, 7.0, current[metricschema.RequestLatency].Metric[0].GetHistogram().GetSampleSum())
	assert.Equal(t, "next", nativeLabelValue(current[metricschema.StatusCounter].Metric[0], "run_id"),
		"publication must not wait for readiness")
	require.NoError(t, f.server.queueStart(runMessage(protocol.VerbStart, "next", "requirements.txt")))
	require.NoError(t, f.server.queuePrepare(runMessage(protocol.VerbPrepare, "next", "requirements.txt")))
	require.Error(t, f.server.queueStart(runMessage(protocol.VerbStart, "busy", "")))
	require.NoError(t, f.server.queueStart(runMessage(protocol.VerbStart, "first", "")))
	f.server.stopRun("first", protocol.StopCanceled)
	assert.Equal(t, current[metricschema.StatusCounter], nativeScrape(t, handler)[metricschema.StatusCounter],
		"duplicate, rejected, and stale requests preserve the running output")
	assert.Equal(t, retainedRegistry, gatherResourceSnapshot(t, first.Metrics.Registry()))
	f.server.stopRun("next", protocol.StopCompleted)
	waitRunState(t, f, "next", protocol.RunCompleted)
	assert.Equal(t, current[metricschema.StatusCounter], nativeScrape(t, handler)[metricschema.StatusCounter])
}

func TestMetricsStartupFailureClearsCompletedRun(t *testing.T) {
	for _, phase := range []string{"prepare", "before", "build", "launch"} {
		t.Run(phase, func(t *testing.T) {
			recorded := false
			f := newAgentFixture(t, AgentServerOptions{
				LabelsUsageLimit: 1,
				ResultFile:       "results.csv",
				PrepareDependencies: func(_ context.Context, run RunContext) error {
					assert.Nil(t, run.Metrics, "preparation must not have a workload recorder")
					if run.RunID == "failed" && phase == "prepare" {
						return errors.New("preparation failed")
					}
					return nil
				},
				BeforeStart: func(_ context.Context, run RunContext) error {
					assert.Nil(t, run.Metrics, "prelaunch setup must not have a workload recorder")
					if run.RunID == "failed" && phase == "before" {
						return errors.New("before start failed")
					}
					return nil
				},
				BuildCommand: func(_ context.Context, run RunContext) (Command, error) {
					assert.Nil(t, run.Metrics, "command construction must not have a workload recorder")
					if run.RunID == "failed" {
						if phase == "build" {
							return Command{}, errors.New("command construction failed")
						}
						return Command{Command: filepath.Join(run.Workspace.Root, "missing-executable")}, nil
					}
					return Command{Command: "sh", Args: []string{
						"-c", `printf '12.5' > "$1"`, "sh", run.ResultFile,
					}}, nil
				},
				CustomIngestMetrics: func(ctx context.Context, run RunContext) error {
					if run.RunID != "completed" || run.Metrics == nil {
						return errors.New("collection ran without a launched workload")
					}
					if _, final := ctx.Deadline(); !final || recorded {
						return nil
					}
					data, err := os.ReadFile(run.ResultFile)
					if err != nil {
						return err
					}
					latency, err := strconv.ParseFloat(string(data), 64)
					if err != nil {
						return err
					}
					recorded = true
					return run.Metrics.ObserveRequest(ShibuyaMetric{Label: "completed", Status: "200", Latency: latency})
				},
			}, nil)
			require.NoError(t, f.server.queueStart(runMessage(protocol.VerbStart, "completed", "")))
			waitRunState(t, f, "completed", protocol.RunCompleted)
			require.Contains(t, nativeScrape(t, f.server.metricsHandler()), metricschema.StatusCounter)
			if phase == "prepare" {
				require.NoError(t, f.server.queuePrepare(runMessage(protocol.VerbPrepare, "failed", "requirements.txt")))
			} else {
				require.NoError(t, f.server.queueStart(runMessage(protocol.VerbStart, "failed", "")))
			}
			assert.Empty(t, nativeScrape(t, f.server.metricsHandler()), "accepting a new ID clears the old snapshot")
			waitRunState(t, f, "failed", protocol.RunFailed)
			assert.Empty(t, nativeScrape(t, f.server.metricsHandler()), "startup failure must leave no SDK metrics")
		})
	}
}

func TestFinalMetricsCollectionTargetsExactRunAfterReplacement(t *testing.T) {
	for _, fail := range []bool{false, true} {
		name := "success"
		if fail {
			name = "failure"
		}
		t.Run(name, func(t *testing.T) {
			failure := errors.New("old final collection failed")
			entered, release := make(chan RunContext, 1), make(chan struct{})
			releaseFinal := sync.OnceFunc(func() { close(release) })
			t.Cleanup(releaseFinal)
			server := NewAgentServer(AgentServerOptions{
				LabelsUsageLimit: 1,
				CustomIngestMetrics: func(ctx context.Context, run RunContext) error {
					entered <- run
					<-release
					if err := ctx.Err(); err != nil {
						return err
					}
					if err := run.Metrics.ObserveRequest(ShibuyaMetric{Label: "final-old", Status: "200", Latency: 3}); err != nil {
						return err
					}
					if fail {
						return failure
					}
					return nil
				},
			})
			old := startTestMetricsRun(t, server, "old")
			done := make(chan error, 1)
			go func() { done <- server.collectFinalMetrics(&agentRun{context: old}) }()
			select {
			case collected := <-entered:
				assert.Same(t, old.Metrics, collected.Metrics)
			case <-time.After(5 * time.Second):
				t.Fatal("final collection did not start")
			}
			next := startTestMetricsRun(t, server, "next")
			require.NoError(t, next.Metrics.ObserveRequest(ShibuyaMetric{Label: "next", Status: "201", Latency: 7}))
			before := nativeScrape(t, server.metricsHandler())
			releaseFinal()
			select {
			case err := <-done:
				if fail {
					require.ErrorIs(t, err, failure)
				} else {
					require.NoError(t, err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("final collection did not finish")
			}
			assert.Equal(t, before, nativeScrape(t, server.metricsHandler()), "late collection never changes the published run")
			if fail {
				_, err := old.Metrics.Registry().Gather()
				require.ErrorIs(t, err, failure)
			} else {
				oldSnapshot := gatherResourceSnapshot(t, old.Metrics.Registry())
				assert.Equal(t, map[string]float64{"final-old/200": 1}, nativeCounters(oldSnapshot[metricschema.StatusCounter]))
			}
		})
	}
}

func TestLiveMetricsCollectionFailureTargetsOriginalRun(t *testing.T) {
	failure := errors.New("old live collection failed")
	entered, release := make(chan struct{}), make(chan struct{})
	releaseCollection := sync.OnceFunc(func() { close(release) })
	t.Cleanup(releaseCollection)
	server := NewAgentServer(AgentServerOptions{
		LabelsUsageLimit: 1,
		CustomIngestMetrics: func(context.Context, RunContext) error {
			close(entered)
			<-release
			return failure
		},
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	run := &agentRun{
		context: startTestMetricsRun(t, server, "old"),
		ctx:     ctx, cancel: cancel,
		status: protocol.RunStatus{RunID: "old", State: protocol.RunRunning, StartupStatus: protocol.StartupStateStarted},
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		server.customIngesterRunner(ctx, run)
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("live collection did not start")
	}
	next := startTestMetricsRun(t, server, "next")
	require.NoError(t, next.Metrics.ObserveRequest(ShibuyaMetric{Label: "next", Status: "201", Latency: 7}))
	before := nativeScrape(t, server.metricsHandler())
	releaseCollection()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("live collection did not finish")
	}
	_, err := run.context.Metrics.Registry().Gather()
	require.ErrorIs(t, err, failure)
	assert.ErrorIs(t, run.failure, failure)
	assert.Equal(t, before, nativeScrape(t, server.metricsHandler()), "late live errors cannot poison the new registry")
}
