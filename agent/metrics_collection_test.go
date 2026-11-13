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
	registry := NewMetricsRegistry()
	recorder, err := NewMetricsRecorder(registry, 1)
	require.NoError(t, err)
	entered, canceled := make(chan struct{}), make(chan struct{})
	liveRelease, finalEntered, finalRelease := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	marker := filepath.Join(t.TempDir(), "exit")
	f := newAgentFixture(t, AgentServerOptions{
		MetricsRegistry:  registry,
		LabelsUsageLimit: 1,
		ResultFile:       "results.csv",
		BuildCommand: func(_ context.Context, run RunContext) (Command, error) {
			return Command{Command: "sh", Args: []string{
				"-c", `while [ ! -f "$1" ]; do sleep 0.01; done; printf '12.5' > "$2"`, "sh", marker, run.ResultFile,
			}}, nil
		},
		CustomIngestMetrics: func(ctx context.Context, run RunContext) error {
			if calls.Add(1) == 1 {
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
			return recorder.ObserveRequest(ShibuyaMetric{RunID: run.RunID, Label: "last", Status: "200", Latency: latency})
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
	for range 2 {
		families := nativeScrape(t, f.server.metricsHandler())
		require.Contains(t, families, metricschema.RequestLatency)
		hist := families[metricschema.RequestLatency].Metric[0].GetHistogram()
		assert.Equal(t, uint64(1), hist.GetSampleCount())
		assert.Equal(t, 12.5, hist.GetSampleSum())
		assert.Equal(t, "first", nativeLabelValue(families[metricschema.RequestLatency].Metric[0], "run_id"))
	}
	assert.Equal(t, int32(2), calls.Load(), "scraping must not collect or duplicate the final row")
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
			registry := NewMetricsRegistry()
			var recorder *MetricsRecorder
			failure := errors.New("invalid engine output")
			if test.deadline {
				failure = context.DeadlineExceeded
			}
			liveEntered := make(chan struct{})
			recorded := false // Live and final collection must be serialized.
			f := newAgentFixture(t, AgentServerOptions{
				MetricsRegistry:  registry,
				LabelsUsageLimit: 1,
				BeforeStart: func(context.Context, RunContext) error {
					registry.Reset()
					recorded = false
					var err error
					recorder, err = NewMetricsRecorder(registry, 1)
					return err
				},
				BuildCommand: func(_ context.Context, run RunContext) (Command, error) {
					if run.RunID == "bad" && (!test.final || test.stop) {
						return Command{Command: "sleep", Args: []string{"30"}}, nil
					}
					return Command{Command: "true"}, nil
				},
				CustomIngestMetrics: func(ctx context.Context, run RunContext) error {
					if run.RunID == "bad" {
						if _, final := ctx.Deadline(); test.final && !final {
							if test.stop {
								close(liveEntered)
								<-ctx.Done()
								return ctx.Err()
							}
							return nil
						}
						if err := recorder.ObserveRequest(ShibuyaMetric{RunID: run.RunID, Label: "partial", Status: "200", Latency: 1}); err != nil {
							return err
						}
						if test.deadline {
							<-ctx.Done()
							return nil
						}
						return failure
					}
					if !recorded {
						if err := recorder.ObserveRequest(ShibuyaMetric{RunID: run.RunID, Label: "complete", Status: "201", Latency: 5}); err != nil {
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
			_, err := registry.Gather()
			require.ErrorIs(t, err, failure)
			response := nativeResponse(f.server.metricsHandler())
			assert.Equal(t, http.StatusInternalServerError, response.Code)
			assert.NotContains(t, response.Body.String(), metricschema.StatusCounter, "failed collection must not expose partial totals")
			f.send(runMessage(protocol.VerbStart, "good", ""))
			waitRunState(t, f, "good", protocol.RunCompleted)
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
		context:  RunContext{RunID: "run"},
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
			_, err := server.metrics.Gather()
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
	_, err := server.metrics.Gather()
	require.ErrorIs(t, err, failure)
	assert.Equal(t, http.StatusInternalServerError, nativeResponse(server.metricsHandler()).Code)
}
