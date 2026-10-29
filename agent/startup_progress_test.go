//go:build unix

// Derived from reqfleet/shibuya; modified for engine-sdk.
package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"github.com/reqfleet/engine-sdk/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShortRunCompletesWithoutReadinessOrResultFile(t *testing.T) {
	f := newAgentFixture(t, AgentServerOptions{
		BuildCommand: func(context.Context, RunContext) (Command, error) { return Command{Command: "true"}, nil },
		Observe: func(ctx context.Context, _ RunContext) (protocol.Observation, error) {
			<-ctx.Done()
			return protocol.ObservationPending, ctx.Err()
		},
		ResultFile: "never-created.csv",
		MetricParser: func(string) (ShibuyaMetric, error) {
			return ShibuyaMetric{}, errors.New("unexpected result")
		},
	}, nil)
	f.send(runMessage(protocol.VerbStart, "short", ""))
	status := waitRunState(t, f, "short", protocol.RunCompleted)
	assert.Equal(t, protocol.StartupStateCompleted, status.StartupStatus)
	assert.Zero(t, status.ExitCode)
	assert.False(t, status.Running)
}

func TestFailureAfterReadinessPreservesStartupAndReportsOutcome(t *testing.T) {
	release := filepath.Join(t.TempDir(), "fail")
	f := newAgentFixture(t, AgentServerOptions{BuildCommand: func(context.Context, RunContext) (Command, error) {
		return Command{Command: "sh", Args: []string{"-c", `while [ ! -f "$RELEASE" ]; do sleep 0.01; done; exit 7`}, Env: []string{"RELEASE=" + release}}, nil
	}}, nil)
	f.send(runMessage(protocol.VerbStart, "run-1", ""))
	status := waitRunState(t, f, "run-1", protocol.RunRunning)
	assert.Equal(t, protocol.StartupStateStarted, status.StartupStatus)
	require.NoError(t, os.WriteFile(release, nil, 0600))
	status = waitRunState(t, f, "run-1", protocol.RunFailed)
	assert.Equal(t, protocol.StartupStateStarted, status.StartupStatus)
	assert.Equal(t, protocol.FailureExecution, status.FailurePhase)
	assert.Equal(t, 7, status.ExitCode)
	assert.NotEmpty(t, status.ErrorMessage)
}

func TestReadinessDoesNotFollowProcessLiveness(t *testing.T) {
	var fail atomic.Bool
	f := newAgentFixture(t, AgentServerOptions{Observe: func(context.Context, RunContext) (protocol.Observation, error) {
		if fail.Load() {
			return "", errors.New("engine rejected configuration")
		}
		return protocol.ObservationPending, nil
	}}, nil)
	f.send(runMessage(protocol.VerbStart, "run-1", ""))
	require.Eventually(t, func() bool { return readProgressForTest(t, f.server).Running }, time.Second, time.Millisecond)
	status := readProgressForTest(t, f.server)
	assert.Equal(t, protocol.StartupStatePending, status.StartupStatus)
	assert.Equal(t, protocol.RunStarting, status.State)
	fail.Store(true)
	status = waitRunState(t, f, "run-1", protocol.RunFailed)
	assert.Equal(t, protocol.FailureStartup, status.FailurePhase)
	assert.Equal(t, protocol.StartupStateFailed, status.StartupStatus)
}

func TestWorkCompleteCannotHideProcessFailure(t *testing.T) {
	release := filepath.Join(t.TempDir(), "exit")
	f := newAgentFixture(t, AgentServerOptions{
		Observe: func(context.Context, RunContext) (protocol.Observation, error) {
			return protocol.ObservationWorkComplete, nil
		},
		BuildCommand: func(context.Context, RunContext) (Command, error) {
			return Command{Command: "sh", Args: []string{"-c", `while [ ! -f "$RELEASE" ]; do sleep 0.01; done; exit 9`}, Env: []string{"RELEASE=" + release}}, nil
		},
	}, nil)
	f.send(runMessage(protocol.VerbStart, "run-1", ""))
	require.Eventually(t, func() bool {
		return readProgressForTest(t, f.server).StartupStatus == protocol.StartupStateCompleted
	}, time.Second, time.Millisecond)
	require.NoError(t, os.WriteFile(release, nil, 0600))
	status := waitRunState(t, f, "run-1", protocol.RunFailed)
	assert.Equal(t, 9, status.ExitCode)
	assert.Equal(t, protocol.FailureExecution, status.FailurePhase)
}

func TestOptionalProgressCannotBreakLifecycleReporting(t *testing.T) {
	for _, invalid := range []bool{false, true} {
		t.Run(fmt.Sprint(invalid), func(t *testing.T) {
			entered := make(chan struct{}, 1)
			f := newAgentFixture(t, AgentServerOptions{Progress: func(context.Context, RunContext) (protocol.Progress, error) {
				select {
				case entered <- struct{}{}:
				default:
				}
				if invalid {
					return protocol.Progress{Kind: "custom", Data: json.RawMessage(`not JSON`)}, nil
				}
				return protocol.Progress{}, errors.New("optional report unavailable")
			}}, nil)
			f.send(runMessage(protocol.VerbStart, "run-1", ""))
			<-entered
			status := waitRunState(t, f, "run-1", protocol.RunRunning)
			assert.Empty(t, status.Progress)
			f.server.stopRun("run-1", protocol.StopCompleted)
			status = waitRunState(t, f, "run-1", protocol.RunCompleted)
			assert.Equal(t, protocol.StopCompleted, status.StopReason)
		})
	}
}

func TestLiveProgressCanOutlastProbeDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		run := &agentRun{context: RunContext{RunID: "slow", AgentID: "0"}}
		server := NewAgentServer(AgentServerOptions{
			Progress: func(ctx context.Context, _ RunContext) (protocol.Progress, error) {
				select {
				case <-ctx.Done():
					return protocol.Progress{}, ctx.Err()
				case <-time.After(3 * time.Second):
					return protocol.Progress{Kind: "slow", Data: json.RawMessage(`{"total":42}`)}, nil
				}
			},
		})
		server.runtime.run = run
		done := make(chan struct{})
		go func() {
			defer close(done)
			server.reportProgress(ctx, run)
		}()
		time.Sleep(3 * time.Second)
		synctest.Wait()
		cancel()
		<-done
		require.Equal(t, protocol.Progress{Kind: "slow", Data: json.RawMessage(`{"total":42}`)}, run.status.Progress["0"])
	})
}

func TestProcessTerminationJoinsProgressBeforeNextRun(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		t.Run(fmt.Sprint(canceled), func(t *testing.T) {
			entered, cleaning, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var calls atomic.Int32
			var cleaned atomic.Bool
			marker := filepath.Join(t.TempDir(), "exit")
			f := newAgentFixture(t, AgentServerOptions{
				BuildCommand: func(_ context.Context, run RunContext) (Command, error) {
					if run.RunID == "next" || canceled {
						return Command{Command: "sleep", Args: []string{"30"}}, nil
					}
					return Command{Command: "sh", Args: []string{
						"-c", `while [ ! -f "$1" ]; do sleep 0.01; done`, "sh", marker,
					}}, nil
				},
				BeforeStart: func(_ context.Context, run RunContext) error {
					if run.RunID == "next" && !cleaned.Load() {
						return errors.New("previous progress callback still owns the workspace")
					}
					return nil
				},
				Progress: func(ctx context.Context, run RunContext) (protocol.Progress, error) {
					if run.RunID == "first" && calls.Add(1) == 1 {
						close(entered)
						<-ctx.Done()
						close(cleaning)
						<-release
						cleaned.Store(true)
						return protocol.Progress{}, ctx.Err()
					}
					return protocol.Progress{Kind: "cached", Data: json.RawMessage(`{"total":42}`)}, nil
				},
			}, nil)
			unblock := sync.OnceFunc(func() { close(release) })
			t.Cleanup(unblock)
			f.send(runMessage(protocol.VerbStart, "first", ""))
			<-entered
			if canceled {
				f.server.stopRun("first", protocol.StopCanceled)
			} else {
				require.NoError(t, os.WriteFile(marker, nil, 0o600))
			}
			select {
			case <-cleaning:
			case <-time.After(5 * time.Second):
				t.Fatal("progress callback was not canceled after process termination")
			}
			status := readProgressForTest(t, f.server)
			require.False(t, status.State.Terminal())
			require.Error(t, f.server.queueStart(runMessage(protocol.VerbStart, "next", "")))
			unblock()
			state := protocol.RunCompleted
			if canceled {
				state = protocol.RunCanceled
			}
			status = waitRunState(t, f, "first", state)
			require.Equal(t, "cached", status.Progress["0"].Kind)
			f.send(runMessage(protocol.VerbStart, "next", ""))
			waitRunState(t, f, "next", protocol.RunRunning)
		})
	}
}

func TestStopBoundsGraceAndTerminatesDescendants(t *testing.T) {
	childPath := filepath.Join(t.TempDir(), "child")
	var freshContext atomic.Bool
	f := newAgentFixture(t, AgentServerOptions{
		BuildCommand: func(context.Context, RunContext) (Command, error) {
			return Command{Command: "sh", Args: []string{"-c", `trap '' TERM; sleep 30 & echo $! > "$CHILD"; wait`}, Env: []string{"CHILD=" + childPath}}, nil
		},
		Stop: func(ctx context.Context, _ RunContext, _ Process) error {
			freshContext.Store(ctx.Err() == nil)
			<-ctx.Done()
			return ctx.Err()
		},
		StopTimeout: 100 * time.Millisecond,
	}, nil)
	f.send(runMessage(protocol.VerbStart, "run-1", ""))
	waitRunState(t, f, "run-1", protocol.RunRunning)
	var child int
	require.Eventually(t, func() bool {
		content, err := os.ReadFile(childPath)
		if err != nil {
			return false
		}
		child, err = strconv.Atoi(strings.TrimSpace(string(content)))
		return err == nil
	}, time.Second, time.Millisecond)
	f.server.stopRun("run-1", protocol.StopCanceled)
	status := waitRunState(t, f, "run-1", protocol.RunCanceled)
	assert.True(t, freshContext.Load())
	assert.False(t, status.Running)
	require.Eventually(t, func() bool {
		if errors.Is(syscall.Kill(child, 0), syscall.ESRCH) {
			return true
		}
		// An adopted zombie has terminated and cannot retain workspace access.
		stat, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", child))
		return err == nil && strings.Contains(string(stat), ") Z ")
	}, time.Second, time.Millisecond)
}

func TestCancellationBeforeLaunchAndContextIdentity(t *testing.T) {
	for _, phase := range []string{"prepare", "build"} {
		t.Run(phase, func(t *testing.T) {
			entered := make(chan struct{})
			marker := filepath.Join(t.TempDir(), "started")
			block := func(ctx context.Context, _ RunContext) error { close(entered); <-ctx.Done(); return ctx.Err() }
			options := AgentServerOptions{BuildCommand: func(context.Context, RunContext) (Command, error) {
				return Command{Command: "touch", Args: []string{marker}}, nil
			}}
			if phase == "prepare" {
				options.BeforeStart = block
			} else {
				options.BuildCommand = func(ctx context.Context, run RunContext) (Command, error) { return Command{}, block(ctx, run) }
			}
			f := newAgentFixture(t, options, nil)
			f.send(runMessage(protocol.VerbStart, "run-1", ""))
			<-entered
			f.server.stopRun("other-run", protocol.StopCanceled)
			assert.Equal(t, "run-1", readProgressForTest(t, f.server).RunID)
			f.server.stopRun("run-1", protocol.StopCanceled)
			waitRunState(t, f, "run-1", protocol.RunCanceled)
			_, err := os.Stat(marker)
			assert.ErrorIs(t, err, os.ErrNotExist)
		})
	}
}

func TestCoordinatorAssignmentIsRequiredAndNeverRescaled(t *testing.T) {
	f := newAgentFixture(t, AgentServerOptions{
		BuildCommand: func(_ context.Context, run RunContext) (Command, error) {
			var settings struct {
				Concurrency string `json:"concurrency"`
			}
			if err := json.Unmarshal(run.Settings, &settings); err != nil {
				return Command{}, err
			}
			return Command{Command: "sh", Dir: run.Workspace.Inputs, Args: []string{"-c", `i=0; while [ "$i" -lt "$USERS" ]; do echo "$run_id"; i=$((i+1)); done > "$OUTPUT"`}, Env: []string{"USERS=" + settings.Concurrency, "OUTPUT=" + filepath.Join(run.Workspace.Results, "work"), "run_id=adapter-must-not-override"}}, nil
		},
	}, nil)
	missing := runMessage(protocol.VerbStart, "missing", "")
	delete(missing.RunConfigs, "0")
	assert.Error(t, f.server.queueStart(missing))
	f.send(runMessage(protocol.VerbStart, "run-1", ""))
	waitRunState(t, f, "run-1", protocol.RunCompleted)
	content, err := os.ReadFile(filepath.Join(f.root, "test-result", "work"))
	require.NoError(t, err)
	assert.Equal(t, strings.Repeat("run-1\n", 10), string(content))
}

func TestSequentialRunsClearArtifactsButRetainDependencyState(t *testing.T) {
	f := newAgentFixture(t, AgentServerOptions{
		BeforeStart: func(_ context.Context, run RunContext) error {
			cache := filepath.Join(run.Workspace.State, "dependency-cache")
			if run.RunID == "first" {
				return os.WriteFile(cache, []byte("reusable"), 0600)
			}
			if _, err := os.Stat(filepath.Join(run.Workspace.Results, "old-checkpoint")); !errors.Is(err, os.ErrNotExist) {
				return errors.New("stale checkpoint")
			}
			content, err := os.ReadFile(cache)
			if err != nil {
				return err
			}
			if string(content) != "reusable" {
				return errors.New("lost dependency state")
			}
			return nil
		},
		BuildCommand: func(_ context.Context, run RunContext) (Command, error) {
			return Command{Command: "touch", Args: []string{filepath.Join(run.Workspace.Results, "old-checkpoint")}}, nil
		},
	}, nil)
	f.send(runMessage(protocol.VerbStart, "first", ""))
	waitRunState(t, f, "first", protocol.RunCompleted)
	f.send(runMessage(protocol.VerbStart, "second", ""))
	waitRunState(t, f, "second", protocol.RunCompleted)
	f.server.stopRun("first", protocol.StopCanceled)
	assert.Equal(t, protocol.RunCompleted, readProgressForTest(t, f.server).State)
}
