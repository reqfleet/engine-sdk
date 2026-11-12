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
	entered, canceled := make(chan struct{}), make(chan struct{})
	liveRelease, finalEntered, finalRelease := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	marker := filepath.Join(t.TempDir(), "exit")
	f := newAgentFixture(t, AgentServerOptions{
		MetricsRegistry: registry,
		ResultFile:      "results.csv",
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
			registry.ObserveRequest(ShibuyaMetric{RunID: run.RunID, Label: "last", Status: "200", Latency: latency})
			return nil
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
	for _, finalFailure := range []bool{false, true} {
		name := "live"
		if finalFailure {
			name = "final"
		}
		t.Run(name, func(t *testing.T) {
			registry := NewMetricsRegistry()
			failure := errors.New("invalid engine output")
			recorded := false // Live and final collection must be serialized.
			f := newAgentFixture(t, AgentServerOptions{
				MetricsRegistry: registry,
				BeforeStart: func(context.Context, RunContext) error {
					registry.Reset()
					recorded = false
					return nil
				},
				BuildCommand: func(_ context.Context, run RunContext) (Command, error) {
					if run.RunID == "bad" && !finalFailure {
						return Command{Command: "sleep", Args: []string{"30"}}, nil
					}
					return Command{Command: "true"}, nil
				},
				CustomIngestMetrics: func(ctx context.Context, run RunContext) error {
					if run.RunID == "bad" {
						if _, final := ctx.Deadline(); finalFailure && !final {
							return nil
						}
						registry.ObserveRequest(ShibuyaMetric{RunID: run.RunID, Label: "partial", Status: "200", Latency: 1})
						return failure
					}
					if !recorded {
						registry.ObserveRequest(ShibuyaMetric{RunID: run.RunID, Label: "complete", Status: "201", Latency: 5})
						recorded = true
					}
					return nil
				},
			}, nil)
			f.send(runMessage(protocol.VerbStart, "bad", ""))
			waitRunState(t, f, "bad", protocol.RunFailed)
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
