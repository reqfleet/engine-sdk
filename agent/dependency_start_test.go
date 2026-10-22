// Derived from reqfleet/shibuya; modified for engine-sdk.
package agent

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/reqfleet/engine-sdk/protocol"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type agentFixture struct {
	server   *AgentServer
	messages chan protocol.Message
	root     string
}

func newAgentFixture(t *testing.T, options AgentServerOptions, handler http.Handler) *agentFixture {
	t.Helper()
	if handler == nil {
		handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if strings.HasSuffix(r.URL.Path, "/missing.txt") {
				http.NotFound(w, r)
				return
			}
			_, _ = w.Write([]byte("coordinator artifact\n"))
		})
	}
	files := httptest.NewTLSServer(handler)
	t.Cleanup(files.Close)
	options.CoordinatorRootCAs = x509.NewCertPool()
	options.CoordinatorRootCAs.AddCert(files.Certificate())
	root := t.TempDir()
	logger := log.New()
	logger.SetOutput(io.Discard)
	options.Logger = log.NewEntry(logger)
	options.EngineMeta = EngineMeta{
		CoordinatorIP: files.Listener.Addr().String(), CollectionID: "collection-1",
		PlanID: "plan-1", EngineID: "0", APIKey: "secret", Zone: "zone-1",
	}
	options.TestFileName = "plan.txt"
	options.DependencyFileName = "requirements.txt"
	if options.BuildCommand == nil {
		options.BuildCommand = func(context.Context, RunContext) (Command, error) {
			return Command{Command: "sleep", Args: []string{"30"}}, nil
		}
	}
	if options.StopTimeout == 0 {
		options.StopTimeout = 100 * time.Millisecond
	}
	server := NewAgentServer(options)
	server.angentDir = AgentDir{dir: root, testFilesDir: filepath.Join(root, "test-data")}
	workerCtx, stopWorker := context.WithCancel(context.Background())
	workerDone := make(chan struct{})
	go func() { defer close(workerDone); server.runRequestWorker(workerCtx) }()
	messages := make(chan protocol.Message, 16)
	listenerDone := make(chan struct{})
	go func() { defer close(listenerDone); server.listenToCoordinator(messages) }()
	t.Cleanup(func() {
		close(messages)
		<-listenerDone
		server.stateLock.RLock()
		runID := server.currentRunIDLocked()
		server.stateLock.RUnlock()
		if runID != "" {
			server.stopRun(runID, protocol.StopCanceled)
			require.Eventually(t, func() bool { return server.getProgressStatus().State.Terminal() }, 5*time.Second, time.Millisecond)
		}
		stopWorker()
		select {
		case <-workerDone:
		case <-time.After(5 * time.Second):
			t.Error("request worker did not stop")
		}
	})
	return &agentFixture{server: server, messages: messages, root: root}
}

func runMessage(verb, runID, dependency string) *protocol.EngineMessage {
	return &protocol.EngineMessage{
		Verb: verb, RunID: runID, TestFile: "plan.txt", DependencyFile: dependency,
		DataFiles: map[string]struct{}{},
		RunConfigs: map[string]protocol.RunSpec{"0": {
			Settings:  json.RawMessage(`{"concurrency":"10"}`),
			Artifacts: map[string]protocol.ArtifactAssignment{"data.csv": {Mode: protocol.PartitionMaterialized, ShardIndex: 2, ShardCount: 4}},
		}},
	}
}

func (f *agentFixture) send(message *protocol.EngineMessage) {
	f.messages <- &protocol.Payload{Verb: message.Verb, PlanMessage: protocol.PlanMessage{"plan-1": message}}
}

func readProgressForTest(t *testing.T, server *AgentServer) protocol.RunStatus {
	t.Helper()
	response := httptest.NewRecorder()
	server.handleProcessCheck(response, httptest.NewRequest(http.MethodGet, "/progress", nil))
	require.Equal(t, http.StatusOK, response.Code)
	var status protocol.RunStatus
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &status))
	return status
}

func waitRunState(t *testing.T, f *agentFixture, runID string, state protocol.RunState) protocol.RunStatus {
	t.Helper()
	require.Eventually(t, func() bool {
		status := readProgressForTest(t, f.server)
		return status.RunID == runID && status.State == state
	}, 5*time.Second, 5*time.Millisecond)
	return readProgressForTest(t, f.server)
}

func TestPreparationBarrierAndDuplicateRequests(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var installs atomic.Int32
	var starts atomic.Int32
	f := newAgentFixture(t, AgentServerOptions{
		PrepareDependencies: func(ctx context.Context, run RunContext) error {
			installs.Add(1)
			content, err := os.ReadFile(run.DependencyFile)
			if err != nil {
				return err
			}
			if string(content) != "coordinator artifact\n" {
				return errors.New("wrong artifact")
			}
			close(entered)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-release:
				return nil
			}
		},
		BuildCommand: func(context.Context, RunContext) (Command, error) {
			starts.Add(1)
			return Command{Command: "true"}, nil
		},
	}, nil)
	f.send(runMessage(protocol.VerbPrepare, "run-1", "requirements.txt"))
	<-entered
	assert.Equal(t, protocol.RunPreparing, readProgressForTest(t, f.server).State)
	assert.False(t, readProgressForTest(t, f.server).Running)
	assert.Error(t, f.server.queueStart(runMessage(protocol.VerbStart, "run-1", "requirements.txt")))
	assert.Zero(t, starts.Load())
	f.send(runMessage(protocol.VerbPrepare, "run-1", "requirements.txt"))
	close(release)
	waitRunState(t, f, "run-1", protocol.RunPrepared)
	assert.Equal(t, protocol.PreparationStateCompleted, f.server.getPreparationProgress().Status)
	f.send(runMessage(protocol.VerbStart, "run-1", "requirements.txt"))
	waitRunState(t, f, "run-1", protocol.RunCompleted)
	require.NoError(t, f.server.queueStart(runMessage(protocol.VerbStart, "run-1", "requirements.txt")))
	assert.Equal(t, int32(1), installs.Load())
	assert.Equal(t, int32(1), starts.Load())
}

func TestMissingDeclarationReconcilesBeforeLaunch(t *testing.T) {
	var starts atomic.Int32
	f := newAgentFixture(t, AgentServerOptions{
		PrepareDependencies: func(_ context.Context, run RunContext) error {
			if run.DependencyFile != "" {
				return errors.New("unexpected dependency")
			}
			if _, err := os.Stat(filepath.Join(run.Workspace.Config, "requirements.txt")); !errors.Is(err, os.ErrNotExist) {
				return errors.New("stale declaration")
			}
			return errors.New("reconciliation failed")
		},
		BuildCommand: func(context.Context, RunContext) (Command, error) {
			starts.Add(1)
			return Command{Command: "true"}, nil
		},
	}, nil)
	require.NoError(t, os.MkdirAll(filepath.Join(f.root, "test-conf"), 0700))
	require.NoError(t, os.WriteFile(filepath.Join(f.root, "test-conf", "requirements.txt"), []byte("stale"), 0600))
	f.send(runMessage(protocol.VerbStart, "run-1", ""))
	status := waitRunState(t, f, "run-1", protocol.RunFailed)
	assert.Equal(t, protocol.FailurePreparation, status.FailurePhase)
	assert.Equal(t, protocol.StartupFailureDependency, status.StartupFailureKind)
	assert.Zero(t, starts.Load())
}

func TestCancellationWaitsForPreparationRollback(t *testing.T) {
	entered, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	f := newAgentFixture(t, AgentServerOptions{PrepareDependencies: func(ctx context.Context, run RunContext) error {
		if run.RunID != "run-1" {
			return nil
		}
		close(entered)
		<-ctx.Done()
		close(canceled)
		<-release // bounded rollback simulated independently of the canceled context
		return ctx.Err()
	}}, nil)
	f.send(runMessage(protocol.VerbPrepare, "run-1", "requirements.txt"))
	<-entered
	f.server.stopRun("run-1", protocol.StopCanceled)
	<-canceled
	assert.Error(t, f.server.queuePrepare(runMessage(protocol.VerbPrepare, "run-2", "requirements.txt")))
	assert.Equal(t, protocol.RunStopping, readProgressForTest(t, f.server).State)
	assert.False(t, readRunProgressForTest(t, f.server, "run-1").State.Terminal())
	close(release)
	status := waitRunState(t, f, "run-1", protocol.RunCanceled)
	assert.Equal(t, protocol.StartupStateCanceled, status.StartupStatus)
	assert.Equal(t, protocol.PreparationStateCanceled, f.server.getPreparationProgress().Status)
	f.send(runMessage(protocol.VerbPrepare, "run-2", "requirements.txt"))
	waitRunState(t, f, "run-2", protocol.RunPrepared)
	f.server.stopRun("run-1", protocol.StopCanceled)
	assert.Equal(t, protocol.RunPrepared, readProgressForTest(t, f.server).State)
	require.NoError(t, f.server.queueStart(runMessage(protocol.VerbStart, "run-1", "requirements.txt")))
	assert.Equal(t, "run-2", readProgressForTest(t, f.server).RunID)
	assert.Equal(t, protocol.RunCanceled, readRunProgressForTest(t, f.server, "run-1").State)
}

func readRunProgressForTest(t *testing.T, server *AgentServer, runID string) protocol.RunStatus {
	t.Helper()
	response := httptest.NewRecorder()
	server.handleProcessCheck(response, httptest.NewRequest(http.MethodGet, "/progress?run_id="+runID, nil))
	require.Equal(t, http.StatusOK, response.Code)
	var status protocol.RunStatus
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &status))
	return status
}

func TestStopBeforePrepareAcknowledgesOnlyRequestedRun(t *testing.T) {
	f := newAgentFixture(t, AgentServerOptions{
		PrepareDependencies: func(context.Context, RunContext) error { return nil },
	}, nil)
	assert.False(t, readRunProgressForTest(t, f.server, "run-1").State.Terminal())
	f.server.stopRun("run-1", protocol.StopCanceled)
	stopped := readRunProgressForTest(t, f.server, "run-1")
	assert.Equal(t, "run-1", stopped.RunID)
	assert.Equal(t, protocol.RunCanceled, stopped.State)
	require.NoError(t, f.server.queuePrepare(runMessage(protocol.VerbPrepare, "run-1", "requirements.txt")))
	assert.Empty(t, readProgressForTest(t, f.server).RunID)
	f.send(runMessage(protocol.VerbPrepare, "run-2", "requirements.txt"))
	waitRunState(t, f, "run-2", protocol.RunPrepared)
	f.server.stopRun("run-1", protocol.StopCanceled)
	f.server.stopRun("unknown", protocol.StopCanceled)
	assert.Equal(t, protocol.RunCanceled, readRunProgressForTest(t, f.server, "unknown").State)
	assert.Equal(t, "run-2", f.server.getPreparationProgress().RunID)
	assert.Equal(t, protocol.RunPrepared, readProgressForTest(t, f.server).State)
}

func TestRunScopedProgressPreservesFailureAfterReplacement(t *testing.T) {
	f := newAgentFixture(t, AgentServerOptions{
		PrepareDependencies: func(_ context.Context, run RunContext) error {
			if run.RunID == "run-1" {
				return errors.New("dependency install failed")
			}
			return nil
		},
	}, nil)
	f.send(runMessage(protocol.VerbPrepare, "run-1", "requirements.txt"))
	waitRunState(t, f, "run-1", protocol.RunFailed)
	assert.Equal(t, "dependency install failed", readRunProgressForTest(t, f.server, "run-1").ErrorMessage)
	f.send(runMessage(protocol.VerbPrepare, "run-2", "requirements.txt"))
	waitRunState(t, f, "run-2", protocol.RunPrepared)
	f.server.stopRun("run-1", protocol.StopCanceled)
	assert.Equal(t, protocol.RunFailed, readRunProgressForTest(t, f.server, "run-1").State)
	assert.Equal(t, protocol.RunPrepared, readProgressForTest(t, f.server).State)
}

func TestCanceledArtifactDownloadDoesNotInstall(t *testing.T) {
	started, canceled := make(chan struct{}), make(chan struct{})
	var installs atomic.Int32
	f := newAgentFixture(t, AgentServerOptions{PrepareDependencies: func(context.Context, RunContext) error { installs.Add(1); return nil }}, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-r.Context().Done()
		close(canceled)
	}))
	f.send(runMessage(protocol.VerbPrepare, "run-1", "requirements.txt"))
	<-started
	f.server.stopRun("run-1", protocol.StopCanceled)
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("coordinator download was not canceled")
	}
	waitRunState(t, f, "run-1", protocol.RunCanceled)
	assert.Zero(t, installs.Load())
	_, err := os.Stat(filepath.Join(f.root, "test-conf", "requirements.txt"))
	assert.ErrorIs(t, err, os.ErrNotExist)
}

func TestStartRequiresMatchingPreparedAssignment(t *testing.T) {
	f := newAgentFixture(t, AgentServerOptions{PrepareDependencies: func(context.Context, RunContext) error { return nil }}, nil)
	assert.Error(t, f.server.queueStart(runMessage(protocol.VerbStart, "run-1", "requirements.txt")))
	f.send(runMessage(protocol.VerbPrepare, "run-1", "requirements.txt"))
	waitRunState(t, f, "run-1", protocol.RunPrepared)
	assert.Error(t, f.server.queueStart(runMessage(protocol.VerbStart, "run-1", "")))
	assert.Error(t, f.server.queueStart(runMessage(protocol.VerbStart, "run-2", "requirements.txt")))
	changed := runMessage(protocol.VerbStart, "run-1", "requirements.txt")
	changed.RunConfigs["0"] = protocol.RunSpec{Settings: json.RawMessage(`{"concurrency":"999"}`)}
	assert.Error(t, f.server.queueStart(changed))
	f.server.stopRun("run-1", protocol.StopCanceled)
	waitRunState(t, f, "run-1", protocol.RunCanceled)
	require.NoError(t, f.server.queueStart(runMessage(protocol.VerbStart, "run-1", "requirements.txt")))
	assert.False(t, readProgressForTest(t, f.server).Running)
}

func TestPreparationFailuresAreTerminalAndIdempotent(t *testing.T) {
	var installs atomic.Int32
	f := newAgentFixture(t, AgentServerOptions{PrepareDependencies: func(context.Context, RunContext) error { installs.Add(1); return errors.New("installer failed") }}, nil)
	f.send(runMessage(protocol.VerbPrepare, "run-1", "missing.txt"))
	status := waitRunState(t, f, "run-1", protocol.RunFailed)
	assert.Equal(t, protocol.FailurePreparation, status.FailurePhase)
	assert.Zero(t, installs.Load())
	f.send(runMessage(protocol.VerbPrepare, "run-2", "requirements.txt"))
	waitRunState(t, f, "run-2", protocol.RunFailed)
	require.NoError(t, f.server.queuePrepare(runMessage(protocol.VerbPrepare, "run-2", "requirements.txt")))
	assert.Equal(t, int32(1), installs.Load())
}

func TestPreparationWithoutConfiguredHookFailsBeforeStart(t *testing.T) {
	f := newAgentFixture(t, AgentServerOptions{}, nil)
	f.send(runMessage(protocol.VerbPrepare, "run-1", "requirements.txt"))
	status := waitRunState(t, f, "run-1", protocol.RunFailed)
	assert.Equal(t, protocol.StartupFailureDependency, status.StartupFailureKind)
	assert.False(t, status.Running)
}
