//go:build unix

// Derived from reqfleet/shibuya; modified for engine-sdk.
package agent

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"sync"
	"syscall"
	"time"

	"github.com/reqfleet/engine-sdk/protocol"
)

func (as *AgentServer) Run() error {
	if err := as.validateMetricsOptions(); err != nil {
		return err
	}
	options := as.options
	if options.BuildCommand == nil {
		return fmt.Errorf("engine command builder is not configured")
	}
	if options.EngineMeta.CoordinatorIP == "" {
		return fmt.Errorf("coordinator endpoint is not configured")
	}
	go func() {
		if err := as.reportOwnMetrics(5 * time.Second); err != nil {
			options.Logger.Fatal(err)
		}
	}()
	go as.runRequestWorker(context.Background())
	go func() {
		for {
			time.Sleep(2 * time.Second)
			msgChan, err := as.SubscribeToCoordinator()
			if err != nil {
				options.Logger.Error(err)
				as.updateSubStatus(false)
				continue
			}
			as.updateSubStatus(true)
			as.listenToCoordinator(msgChan)
		}
	}()
	go as.handleMetricStream()
	return as.startHTTPServer()
}

func (as *AgentServer) runCommand(run *agentRun) error {
	if as.options.BuildCommand == nil {
		return fmt.Errorf("engine command builder is not configured")
	}
	spec, err := as.options.BuildCommand(run.ctx, run.context)
	if err != nil {
		return err
	}
	if spec.Command == "" {
		return fmt.Errorf("engine executable is empty")
	}
	command := spec.ToExec()
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	command.Env, err = as.buildCommandEnv(run.context.RunID, spec.Env)
	if err != nil {
		return err
	}
	stdout, err := command.StdoutPipe()
	if err != nil {
		return err
	}
	defer stdout.Close()
	stderr, err := command.StderrPipe()
	if err != nil {
		return err
	}
	defer stderr.Close()
	as.stateLock.Lock()
	if err := run.ctx.Err(); err != nil {
		as.stateLock.Unlock()
		return err
	}
	if err := command.Start(); err != nil {
		as.stateLock.Unlock()
		return err
	}
	run.process = command.Process
	run.launched = true
	if as.options.Observe == nil {
		run.status.StartupStatus = protocol.StartupStateStarted
		run.status.State = protocol.RunRunning
	}
	as.startMetricsRun(run.context.RunID)
	as.stateLock.Unlock()

	var readers sync.WaitGroup
	readers.Go(func() { as.streamToLogger(stdout, "stdout") })
	readers.Go(func() { as.streamToLogger(stderr, "stderr") })
	monitorCtx, stopMonitor := context.WithCancel(run.ctx)
	var monitors sync.WaitGroup
	if as.options.Observe != nil {
		monitors.Go(func() { as.observeRun(monitorCtx, run) })
	}
	if as.options.Progress != nil {
		monitors.Go(func() { as.reportProgress(monitorCtx, run) })
	}
	if as.usesResultFileMetrics() {
		monitors.Go(func() { as.tailFunc(monitorCtx, run.context.ResultFile, run.context.RunID) })
	}
	wait := make(chan error, 1)
	go func() { wait <- command.Wait() }()
	var waitErr error
	select {
	case waitErr = <-wait:
	case <-run.ctx.Done():
		waitErr = as.stopProcess(run, command, wait)
	}
	as.stateLock.Lock()
	run.exited = true
	as.stateLock.Unlock()
	// A primary process may exit while descendants remain. They cannot outlive
	// this run or retain pipes/workspace access into the next run.
	_ = signalProcessGroup(command.Process.Pid, syscall.SIGKILL)
	stopMonitor()
	monitors.Wait()
	readers.Wait()
	if as.options.Progress != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		as.collectProgress(ctx, run)
		cancel()
	}
	exitCode := command.ProcessState.ExitCode()
	as.stateLock.Lock()
	phase := protocol.FailureExecution
	if run.status.StartupStatus == protocol.StartupStatePending {
		phase = protocol.FailureStartup
	}
	as.finishRunLocked(run, waitErr, phase, exitCode)
	as.stateLock.Unlock()
	return nil
}

func signalProcessGroup(pid int, signal syscall.Signal) error {
	err := syscall.Kill(-pid, signal)
	if errors.Is(err, syscall.ESRCH) {
		return nil
	}
	return err
}

func (as *AgentServer) stopProcess(run *agentRun, command *exec.Cmd, wait <-chan error) error {
	ctx, cancel := context.WithTimeout(context.Background(), as.options.StopTimeout)
	defer cancel()
	var stopped <-chan error
	if as.options.Stop != nil {
		done := make(chan error, 1)
		stopped = done
		go func() { done <- as.options.Stop(ctx, run.context, Process{PID: command.Process.Pid}) }()
	} else if err := signalProcessGroup(command.Process.Pid, syscall.SIGTERM); err != nil {
		as.logger.Error(err)
	}
	for {
		select {
		case err := <-wait:
			cancel()
			if stopped != nil {
				<-stopped
			}
			return err
		case err := <-stopped:
			stopped = nil
			if err != nil {
				as.logger.Errorf("Graceful stop failed: %v", err)
				_ = signalProcessGroup(command.Process.Pid, syscall.SIGTERM)
			}
		case <-ctx.Done():
			_ = signalProcessGroup(command.Process.Pid, syscall.SIGKILL)
			err := <-wait
			if stopped != nil {
				<-stopped
			}
			return err
		}
	}
}
