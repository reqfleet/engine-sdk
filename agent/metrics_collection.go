package agent

import (
	"context"
	"fmt"
	"time"

	"github.com/reqfleet/engine-sdk/protocol"
)

func (as *AgentServer) reportMetrics(ctx context.Context, run *agentRun) {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			return
		}
		err := as.options.CustomIngestMetrics(ctx, run.context)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			err = fmt.Errorf("collect engine metrics: %w", err)
			as.metrics.setCollectionError(err)
			as.stateLock.Lock()
			if run.ctx.Err() == nil && !run.exited {
				run.failure = err
				run.failurePhase = protocol.FailureExecution
				if run.status.StartupStatus == protocol.StartupStatePending {
					run.failurePhase = protocol.FailureStartup
				}
				run.cancel()
			}
			as.stateLock.Unlock()
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// collectFinalMetrics drains remaining engine output before workspace release.
// The caller waits for workload exit and joins live collectors/readers first,
// preventing overlap with live collection. A fresh context allows this final
// drain even when shutdown has canceled the run context.
// Collection errors or an expired deadline make registry scrapes fail and are
// returned to the caller so the run cannot complete successfully with partial totals.
func (as *AgentServer) collectFinalMetrics(run *agentRun) error {
	ctx, cancel := context.WithTimeout(context.Background(), FinalMetricsTimeout)
	defer cancel()
	err := as.options.CustomIngestMetrics(ctx, run.context)
	if err == nil {
		err = ctx.Err()
	}
	if err != nil {
		err = fmt.Errorf("collect final engine metrics: %w", err)
		as.metrics.setCollectionError(err)
	}
	return err
}
