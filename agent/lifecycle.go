// Derived from reqfleet/shibuya; modified for engine-sdk.
package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"time"

	"github.com/reqfleet/engine-sdk/protocol"
)

// Shared fields are protected by stateLock. The request worker owns preparation,
// launch, waiting and cleanup. A stopped run remains busy until that work returns.
type agentRun struct {
	context      RunContext
	message      *protocol.EngineMessage
	ctx          context.Context
	cancel       context.CancelFunc
	process      *os.Process
	busy         bool
	initialized  bool
	launched     bool
	exited       bool
	stopReason   protocol.StopReason
	failure      error
	failurePhase protocol.FailurePhase
	status       protocol.RunStatus
	preparation  protocol.PreparationProgress
}

func (as *AgentServer) makeRunContext(message *protocol.EngineMessage) (RunContext, error) {
	meta := as.options.EngineMeta
	spec, ok := message.RunConfigs[meta.EngineID]
	if !ok {
		return RunContext{}, fmt.Errorf("coordinator assignment is missing for agent %s", meta.EngineID)
	}
	if as.angentDir.dir == "" {
		return RunContext{}, fmt.Errorf("agent workspace root is not configured")
	}
	root, err := filepath.Abs(as.angentDir.dir)
	if err != nil {
		return RunContext{}, err
	}
	inputs, err := filepath.Abs(string(as.angentDir.TestFilesDir()))
	if err != nil {
		return RunContext{}, err
	}
	workspace := Workspace{
		Root: root, Inputs: inputs, Config: filepath.Join(root, "test-conf"),
		Results: filepath.Join(root, "test-result"), Temp: filepath.Join(root, "run-tmp"), State: root,
	}
	run := RunContext{
		RunID: message.RunID, CollectionID: meta.CollectionID, PlanID: meta.PlanID,
		AgentID: meta.EngineID, Zone: meta.Zone, Workspace: workspace,
		Settings: bytes.Clone(spec.Settings), Artifacts: maps.Clone(spec.Artifacts),
	}
	if as.options.TestFileName != "" {
		run.TestFile = filepath.Join(workspace.Inputs, filepath.Base(as.options.TestFileName))
	}
	if as.options.ConfFileName != "" {
		run.ConfigFile = filepath.Join(workspace.Config, filepath.Base(as.options.ConfFileName))
	}
	if message.DependencyFile != "" && as.options.DependencyFileName != "" {
		run.DependencyFile = filepath.Join(workspace.Config, filepath.Base(as.options.DependencyFileName))
	}
	if as.options.ResultFile != "" {
		run.ResultFile = filepath.Join(workspace.Results, filepath.Base(as.options.ResultFile))
	}
	return run, nil
}

func (as *AgentServer) queuePrepare(message *protocol.EngineMessage) error {
	return as.queueRequest(message, protocol.VerbPrepare)
}

func (as *AgentServer) queueStart(message *protocol.EngineMessage) error {
	return as.queueRequest(message, protocol.VerbStart)
}

func (as *AgentServer) queueRequest(message *protocol.EngineMessage, verb string) error {
	as.stateLock.Lock()
	defer as.stateLock.Unlock()
	if message == nil || message.RunID == "" {
		return fmt.Errorf("run ID is missing")
	}
	if _, retired := as.runtime.retired[message.RunID]; retired {
		return nil
	}
	current := as.runtime.run
	if current != nil {
		if current.context.RunID == message.RunID {
			if verb == protocol.VerbStart && current.status.State == protocol.RunPreparing {
				return fmt.Errorf("run %s is still preparing", message.RunID)
			}
			if current.busy || verb == protocol.VerbPrepare || current.status.State.Terminal() {
				return nil
			}
		} else if current.busy || !current.status.State.Terminal() {
			return fmt.Errorf("run %s is still active", current.context.RunID)
		}
	}
	runContext, err := as.makeRunContext(message)
	if err != nil {
		return err
	}
	if verb == protocol.VerbStart {
		prepared := current != nil && current.context.RunID == message.RunID &&
			current.preparation.Status == protocol.PreparationStateCompleted
		if message.DependencyFile != "" && !prepared {
			return fmt.Errorf("run %s dependency preparation is not completed", message.RunID)
		}
		if prepared && (current.message.DependencyFile != message.DependencyFile ||
			!bytes.Equal(current.context.Settings, runContext.Settings) ||
			!maps.Equal(current.context.Artifacts, runContext.Artifacts)) {
			return fmt.Errorf("run %s coordinator assignment changed after preparation", message.RunID)
		}
	}
	if current == nil || current.context.RunID != message.RunID {
		current = &agentRun{
			context: runContext,
			status:  protocol.RunStatus{RunID: message.RunID, StartupStatus: protocol.StartupStatePending},
		}
		as.runtime.run = current
	}
	messageCopy := *message
	messageCopy.Verb = verb
	messageCopy.DataFiles = maps.Clone(message.DataFiles)
	current.message = &messageCopy
	current.context = runContext
	current.ctx, current.cancel = context.WithCancel(context.Background())
	current.busy = true
	if verb == protocol.VerbPrepare {
		current.status.State = protocol.RunPreparing
		current.preparation = protocol.PreparationProgress{RunID: message.RunID, Status: protocol.PreparationStatePending}
	} else {
		current.status.State = protocol.RunStarting
	}
	// At most one request is outstanding. Admission happens under the same lock,
	// so this buffered send never prevents a stop from reaching an active hook.
	as.requests <- agentRequest{verb: verb, run: current}
	return nil
}

func (as *AgentServer) currentRunIDLocked() string {
	if as.runtime.run == nil {
		return ""
	}
	return as.runtime.run.context.RunID
}

func (as *AgentServer) stopRun(runID string, reason protocol.StopReason) {
	if runID == "" {
		as.logger.Error("Ignoring stop without run ID")
		return
	}
	if reason == "" {
		reason = protocol.StopCanceled
	}
	if reason != protocol.StopCanceled && reason != protocol.StopCompleted {
		as.logger.Errorf("Ignoring invalid stop reason %q", reason)
		return
	}
	as.stateLock.Lock()
	defer as.stateLock.Unlock()
	if _, retired := as.runtime.retired[runID]; !retired {
		as.runtime.retired[runID] = protocol.RunCanceled
	}
	run := as.runtime.run
	if run == nil || run.context.RunID != runID || run.status.State.Terminal() || run.exited || run.stopReason != "" {
		return
	}
	run.stopReason = reason
	run.status.StopReason = reason
	run.status.State = protocol.RunStopping
	run.cancel()
	if !run.busy {
		as.finishRunLocked(run, context.Canceled, protocol.FailurePreparation, -1)
	}
}

func (as *AgentServer) getActiveRunID() string {
	as.stateLock.RLock()
	defer as.stateLock.RUnlock()
	if as.runtime.run == nil || as.runtime.run.process == nil {
		return ""
	}
	return as.runtime.run.context.RunID
}

func (as *AgentServer) getProgressStatus() *protocol.RunStatus {
	return as.getRunProgressStatus("")
}

func (as *AgentServer) getRunProgressStatus(runID string) *protocol.RunStatus {
	as.stateLock.RLock()
	defer as.stateLock.RUnlock()
	if runID != "" && (as.runtime.run == nil || as.runtime.run.context.RunID != runID) {
		if state, retired := as.runtime.retired[runID]; retired {
			return &protocol.RunStatus{RunID: runID, State: state}
		}
		return &protocol.RunStatus{RunID: runID, StartupStatus: protocol.StartupStatePending}
	}
	if as.runtime.run == nil {
		return &protocol.RunStatus{}
	}
	run := as.runtime.run
	status := run.status
	status.Running = run.process != nil && !status.State.Terminal()
	return &status
}

func (as *AgentServer) getPreparationProgress() *protocol.PreparationProgress {
	as.stateLock.RLock()
	defer as.stateLock.RUnlock()
	status := protocol.PreparationProgress{Status: protocol.PreparationStatePending}
	if as.runtime.run != nil && as.runtime.run.preparation.RunID != "" {
		status = as.runtime.run.preparation
	}
	return &status
}

func (as *AgentServer) runRequestWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case request := <-as.requests:
			run := request.run
			stop := context.AfterFunc(ctx, run.cancel)
			if request.verb == protocol.VerbPrepare {
				err := as.initializeWorkspace(run)
				if err == nil {
					err = as.prepareDependencies(run)
				}
				as.stateLock.Lock()
				if err == nil {
					err = run.ctx.Err()
				}
				if err != nil {
					as.finishRunLocked(run, err, protocol.FailurePreparation, -1)
				} else {
					run.busy = false
					run.status.State = protocol.RunPrepared
					run.preparation.Status = protocol.PreparationStateCompleted
				}
				run.cancel()
				as.stateLock.Unlock()
			} else {
				err := as.handleStart(run)
				if err != nil {
					phase := protocol.FailureStartup
					var dependency *startupFailure
					if errors.As(err, &dependency) {
						phase = protocol.FailurePreparation
					}
					as.stateLock.Lock()
					as.finishRunLocked(run, err, phase, -1)
					as.stateLock.Unlock()
				}
			}
			stop()
		}
	}
}

func (as *AgentServer) initializeWorkspace(run *agentRun) error {
	if err := run.ctx.Err(); err != nil {
		return err
	}
	if run.initialized {
		return nil
	}
	workspace := run.context.Workspace
	for _, dir := range []string{workspace.Inputs, workspace.Config, workspace.Results, workspace.Temp} {
		if err := os.MkdirAll(dir, FILEMODE); err != nil {
			return err
		}
	}
	// Config also contains persistent dependency reconciliation state. Only run
	// inputs, outputs and temporary files are disposable wholesale.
	for _, dir := range []string{workspace.Inputs, workspace.Results, workspace.Temp} {
		if err := TestFilesDirectory(dir).reset(); err != nil {
			return err
		}
	}
	run.initialized = true
	return nil
}

func (as *AgentServer) prepareDependencies(run *agentRun) error {
	if err := run.ctx.Err(); err != nil {
		return newDependencyStartupFailure(err)
	}
	if run.message.DependencyFile != "" && (as.options.DependencyFileName == "" || as.options.PrepareDependencies == nil) {
		return newDependencyStartupFailure(fmt.Errorf("dependency preparation is not configured"))
	}
	if as.options.DependencyFileName != "" {
		path := filepath.Join(run.context.Workspace.Config, filepath.Base(as.options.DependencyFileName))
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return newDependencyStartupFailure(err)
		}
		if run.message.DependencyFile != "" {
			remote, err := protocol.DependencyFilePath(run.context.CollectionID, run.context.PlanID, run.message.DependencyFile)
			if err == nil {
				err = as.fetchRunFile(run, remote, path)
			}
			if err != nil {
				return newDependencyStartupFailure(err)
			}
		}
	}
	if err := run.ctx.Err(); err != nil {
		return newDependencyStartupFailure(err)
	}
	if as.options.PrepareDependencies != nil {
		if err := as.options.PrepareDependencies(run.ctx, run.context); err != nil {
			return newDependencyStartupFailure(err)
		}
	}
	return run.ctx.Err()
}

func (as *AgentServer) fetchRunFile(run *agentRun, remote, local string) error {
	content, err := as.cdrclient.FetchFile(run.ctx, as.reqOpts, remote)
	if err != nil {
		return err
	}
	if err := run.ctx.Err(); err != nil {
		return err
	}
	return os.WriteFile(local, content, FILEMODE)
}

func (as *AgentServer) handleStart(run *agentRun) error {
	if err := as.initializeWorkspace(run); err != nil {
		return err
	}
	remote, err := protocol.TestFilePath(run.context.CollectionID, run.context.PlanID, run.message.TestFile)
	if err != nil {
		return err
	}
	if err := as.fetchRunFile(run, remote, run.context.TestFile); err != nil {
		return err
	}
	if run.context.ConfigFile != "" {
		remote, err := protocol.TestFilePath(run.context.CollectionID, run.context.PlanID, as.options.ConfFileName)
		if err != nil {
			return err
		}
		if err := as.fetchRunFile(run, remote, run.context.ConfigFile); err != nil {
			return err
		}
	}
	for name := range run.message.DataFiles {
		remote, err := protocol.EngineDataPath(run.context.CollectionID, run.context.PlanID, name, run.context.AgentID)
		if err != nil {
			return err
		}
		if err := as.fetchRunFile(run, remote, filepath.Join(run.context.Workspace.Inputs, filepath.Base(name))); err != nil {
			return err
		}
	}
	if run.message.DependencyFile == "" {
		if err := as.prepareDependencies(run); err != nil {
			return err
		}
	}
	if err := run.ctx.Err(); err != nil {
		return err
	}
	if as.options.BeforeStart != nil {
		if err := as.options.BeforeStart(run.ctx, run.context); err != nil {
			return err
		}
	}
	return as.runCommand(run)
}

func (as *AgentServer) observeRun(ctx context.Context, run *agentRun) {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		observation, err := as.options.Observe(probeCtx, run.context)
		cancel()
		if ctx.Err() != nil {
			return
		}
		as.stateLock.Lock()
		if run.ctx.Err() != nil || run.exited {
			as.stateLock.Unlock()
			return
		}
		if err == nil {
			switch observation {
			case protocol.ObservationPending:
			case protocol.ObservationReady:
				if run.status.StartupStatus == protocol.StartupStatePending {
					run.status.StartupStatus = protocol.StartupStateStarted
				}
				run.status.State = protocol.RunRunning
			case protocol.ObservationWorkComplete:
				run.status.StartupStatus = protocol.StartupStateCompleted
				run.status.State = protocol.RunRunning
			default:
				err = fmt.Errorf("invalid engine observation %q", observation)
			}
		}
		if err != nil {
			run.failure = err
			run.failurePhase = protocol.FailureStartup
			if run.status.StartupStatus != protocol.StartupStatePending {
				run.failurePhase = protocol.FailureExecution
			}
			run.cancel()
			as.stateLock.Unlock()
			return
		}
		as.stateLock.Unlock()
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (as *AgentServer) collectProgress(ctx context.Context, run *agentRun) {
	progress, err := as.options.Progress(ctx, run.context)
	if err != nil {
		if ctx.Err() == nil {
			as.logger.WithError(err).Warn("Engine progress is unavailable")
		}
		return
	}
	if progress.Kind == "" || !json.Valid(progress.Data) {
		as.logger.Warn("Ignoring invalid engine progress")
		return
	}
	as.stateLock.Lock()
	defer as.stateLock.Unlock()
	if as.runtime.run == run {
		run.status.Progress = map[string]protocol.Progress{run.context.AgentID: progress}
	}
}

func (as *AgentServer) reportProgress(ctx context.Context, run *agentRun) {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		as.collectProgress(ctx, run)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (as *AgentServer) finishRunLocked(run *agentRun, err error, phase protocol.FailurePhase, exitCode int) {
	if as.runtime.run != run || run.status.State.Terminal() {
		return
	}
	run.cancel()
	run.busy = false
	run.process = nil
	run.status.ExitCode = exitCode
	as.labelsLock.Lock()
	delete(as.labelsByRun, run.context.RunID)
	as.labelsLock.Unlock()
	switch {
	case run.failure != nil:
		err, phase = run.failure, run.failurePhase
		run.status.State = protocol.RunFailed
	case run.stopReason != "" || errors.Is(err, context.Canceled):
		if run.stopReason == protocol.StopCompleted && run.launched {
			run.status.State = protocol.RunCompleted
			run.status.ExitCode = 0
		} else {
			run.status.State = protocol.RunCanceled
			if run.status.StartupStatus == protocol.StartupStatePending {
				run.status.StartupStatus = protocol.StartupStateCanceled
			}
			if run.preparation.Status == protocol.PreparationStatePending {
				run.preparation.Status = protocol.PreparationStateCanceled
				run.preparation.ErrorMessage = context.Canceled.Error()
			}
		}
	case err != nil:
		run.status.State = protocol.RunFailed
	default:
		run.status.State = protocol.RunCompleted
	}
	if run.status.State == protocol.RunFailed {
		run.status.FailurePhase = phase
		run.status.ErrorMessage = err.Error()
		if phase == protocol.FailurePreparation {
			run.status.StartupFailureKind = protocol.StartupFailureDependency
			if run.preparation.RunID != "" {
				run.preparation.Status = protocol.PreparationStateFailed
				run.preparation.ErrorMessage = err.Error()
			}
		}
		if run.status.StartupStatus == protocol.StartupStatePending {
			run.status.StartupStatus = protocol.StartupStateFailed
		}
	}
	if run.status.State == protocol.RunCompleted {
		run.status.StartupStatus = protocol.StartupStateCompleted
	}
	as.runtime.retired[run.context.RunID] = run.status.State
}
