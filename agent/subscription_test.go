//go:build unix

package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/reqfleet/engine-sdk/protocol"
	"github.com/stretchr/testify/require"
)

func subscriptionHealth(t *testing.T, server *AgentServer, status int) protocol.AgentHealth {
	t.Helper()
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	require.Equal(t, status, response.Code)
	require.Equal(t, "application/json", response.Header().Get("Content-Type"))
	var health protocol.AgentHealth
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &health))
	require.Equal(t, protocol.WireProtocolVersion, health.ProtocolVersion)
	return health
}

func subscriptionPeer(t *testing.T, server *AgentServer, serve func(net.Conn) error) <-chan error {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	server.pubsubClient.Addr = listener.Addr().String()
	server.pubsubClient.TLSConfig = nil
	done := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			done <- err
			return
		}
		defer conn.Close()
		if err := conn.SetDeadline(time.Now().Add(8 * time.Second)); err != nil {
			done <- err
			return
		}
		var topic protocol.Topic
		if err := json.NewDecoder(conn).Decode(&topic); err != nil {
			done <- err
			return
		}
		if topic.Name != "collection:"+server.options.EngineMeta.CollectionID ||
			topic.Password != server.options.EngineMeta.APIKey || topic.ProtocolVersion != protocol.WireProtocolVersion {
			done <- fmt.Errorf("unexpected subscription identity or version")
			return
		}
		done <- serve(conn)
	}()
	return done
}

func awaitSubscriptionPeer(t *testing.T, done <-chan error) {
	t.Helper()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("subscription peer did not finish")
	}
}

func unsupportedSubscription() protocol.SubscriptionResponse {
	return protocol.SubscriptionResponse{
		Status: protocol.SubscriptionRejected,
		Error: &protocol.ProtocolError{
			Code: protocol.UnsupportedProtocolVersion, Message: "agent protocol 1 unsupported; coordinator supports 2",
			ReceivedVersion: protocol.WireProtocolVersion, SupportedVersions: []int{2},
		},
	}
}

func rejectSubscription(t *testing.T, server *AgentServer) *protocol.ProtocolError {
	t.Helper()
	done := subscriptionPeer(t, server, func(conn net.Conn) error {
		return json.NewEncoder(conn).Encode(unsupportedSubscription())
	})
	messages, err := server.SubscribeToCoordinator()
	require.Nil(t, messages)
	var failure *protocol.ProtocolError
	require.ErrorAs(t, err, &failure)
	awaitSubscriptionPeer(t, done)
	return failure
}

func TestSubscriptionHealthRequiresAcceptanceAndClearsOnClosure(t *testing.T) {
	server := NewAgentServer(AgentServerOptions{})
	topicReceived := make(chan struct{})
	releaseAck, closePeer := make(chan struct{}), make(chan struct{})
	peerCtx, stopPeer := context.WithCancel(context.Background())
	defer stopPeer()
	done := subscriptionPeer(t, server, func(conn net.Conn) error {
		close(topicReceived)
		select {
		case <-releaseAck:
		case <-peerCtx.Done():
			return nil
		}
		if err := json.NewEncoder(conn).Encode(protocol.SubscriptionResponse{
			Status: protocol.SubscriptionAccepted, ProtocolVersion: protocol.WireProtocolVersion,
		}); err != nil {
			return err
		}
		select {
		case <-closePeer:
		case <-peerCtx.Done():
		}
		return nil
	})
	type result struct {
		messages chan protocol.Message
		err      error
	}
	connected := make(chan result, 1)
	go func() {
		messages, err := server.SubscribeToCoordinator()
		connected <- result{messages, err}
	}()
	select {
	case <-topicReceived:
	case <-time.After(5 * time.Second):
		t.Fatal("subscription request did not arrive")
	}
	health := subscriptionHealth(t, server, http.StatusServiceUnavailable)
	require.False(t, health.Subscribed)
	require.Nil(t, health.Error)
	close(releaseAck)
	var subscription result
	select {
	case subscription = <-connected:
	case <-time.After(5 * time.Second):
		t.Fatal("accepted subscription did not return")
	}
	require.NoError(t, subscription.err)
	require.True(t, subscriptionHealth(t, server, http.StatusOK).Subscribed)
	listenerDone := make(chan struct{})
	go func() {
		defer close(listenerDone)
		server.listenToCoordinator(subscription.messages)
	}()
	close(closePeer)
	awaitSubscriptionPeer(t, done)
	select {
	case <-listenerDone:
	case <-time.After(time.Second):
		t.Fatal("command listener did not observe closure")
	}
	health = subscriptionHealth(t, server, http.StatusServiceUnavailable)
	require.False(t, health.Subscribed)
	require.Nil(t, health.Error)
}

func TestInvalidAcknowledgementDoesNotSetHealth(t *testing.T) {
	for _, response := range []string{"", `{"verb":"start"}`, `{"status":"accepted","protocol_version":2}`} {
		t.Run(response, func(t *testing.T) {
			server := NewAgentServer(AgentServerOptions{})
			done := subscriptionPeer(t, server, func(conn net.Conn) error {
				_, err := conn.Write([]byte(response))
				return err
			})
			messages, err := server.SubscribeToCoordinator()
			require.Error(t, err)
			require.Nil(t, messages)
			health := subscriptionHealth(t, server, http.StatusServiceUnavailable)
			require.False(t, health.Subscribed)
			require.Nil(t, health.Error)
			awaitSubscriptionPeer(t, done)
		})
	}
}

func TestProtocolRejectionCancelsPreparationAndRemainsObservable(t *testing.T) {
	entered, cleaned := make(chan struct{}), make(chan struct{})
	var starts atomic.Int32
	fixture := newAgentFixture(t, AgentServerOptions{
		PrepareDependencies: func(ctx context.Context, _ RunContext) error {
			close(entered)
			<-ctx.Done()
			close(cleaned)
			return ctx.Err()
		},
		BuildCommand: func(context.Context, RunContext) (Command, error) {
			starts.Add(1)
			return Command{Command: "true"}, nil
		},
	}, nil)
	fixture.send(runMessage(protocol.VerbPrepare, "preparing", "requirements.txt"))
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("preparation did not enter hook")
	}
	failure := rejectSubscription(t, fixture.server)
	status := waitRunState(t, fixture, "preparing", protocol.RunFailed)
	require.Equal(t, protocol.FailurePreparation, status.FailurePhase)
	require.Equal(t, failure.Message, status.ErrorMessage)
	select {
	case <-cleaned:
	default:
		t.Fatal("terminal status preceded preparation cleanup")
	}
	require.Equal(t, protocol.PreparationStateFailed, fixture.server.getPreparationProgress().Status)
	require.ErrorIs(t, fixture.server.queueStart(runMessage(protocol.VerbStart, "later", "")), failure)
	fixture.send(runMessage(protocol.VerbStart, "buffered", ""))
	health := subscriptionHealth(t, fixture.server, http.StatusServiceUnavailable)
	require.Equal(t, failure, health.Error)
	messages, err := fixture.server.SubscribeToCoordinator()
	require.Nil(t, messages)
	require.ErrorIs(t, err, failure)
	require.Zero(t, starts.Load())
}

func TestProtocolRejectionStopsActiveProcessThroughCleanup(t *testing.T) {
	stopped := make(chan struct{})
	fixture := newAgentFixture(t, AgentServerOptions{
		Stop: func(_ context.Context, _ RunContext, process Process) error {
			defer close(stopped)
			return signalProcessGroup(process.PID, syscall.SIGTERM)
		},
	}, nil)
	fixture.send(runMessage(protocol.VerbStart, "active", ""))
	waitRunState(t, fixture, "active", protocol.RunRunning)
	failure := rejectSubscription(t, fixture.server)
	status := waitRunState(t, fixture, "active", protocol.RunFailed)
	require.Equal(t, protocol.FailureExecution, status.FailurePhase)
	require.Equal(t, failure.Message, status.ErrorMessage)
	require.False(t, status.Running)
	select {
	case <-stopped:
	default:
		t.Fatal("protocol rejection bypassed the stop callback")
	}
	fixture.server.stateLock.RLock()
	busy := fixture.server.runtime.run.busy
	process := fixture.server.runtime.run.process
	fixture.server.stateLock.RUnlock()
	require.False(t, busy)
	require.Nil(t, process)
	require.Equal(t, failure, subscriptionHealth(t, fixture.server, http.StatusServiceUnavailable).Error)
	require.ErrorIs(t, fixture.server.queuePrepare(runMessage(protocol.VerbPrepare, "later", "")), failure)
}

func TestProtocolRejectionCancelsQueuedStartBeforeWorkerLaunch(t *testing.T) {
	var starts atomic.Int32
	server := NewAgentServer(AgentServerOptions{
		EngineMeta: EngineMeta{EngineID: "0"},
		BuildCommand: func(context.Context, RunContext) (Command, error) {
			starts.Add(1)
			return Command{Command: "true"}, nil
		},
	})
	root := t.TempDir()
	server.angentDir = AgentDir{dir: root, testFilesDir: filepath.Join(root, "test-data")}
	require.NoError(t, server.queueStart(runMessage(protocol.VerbStart, "queued", "")))
	failure := rejectSubscription(t, server)
	ctx, cancel := context.WithCancel(context.Background())
	workerDone := make(chan struct{})
	go func() { defer close(workerDone); server.runRequestWorker(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-workerDone:
		case <-time.After(5 * time.Second):
			t.Error("request worker did not stop")
		}
	})
	require.Eventually(t, func() bool { return server.getProgressStatus().State == protocol.RunFailed }, time.Second, time.Millisecond)
	require.Equal(t, failure.Message, server.getProgressStatus().ErrorMessage)
	require.Zero(t, starts.Load())
	require.ErrorIs(t, server.queueStart(runMessage(protocol.VerbStart, "later", "")), failure)
}

func TestTransientSubscriptionReconnectsButProtocolRejectionStopsLoop(t *testing.T) {
	fixture := newAgentFixture(t, AgentServerOptions{
		BuildCommand: func(context.Context, RunContext) (Command, error) {
			return Command{Command: "true"}, nil
		},
	}, nil)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	fixture.server.pubsubClient.Addr = listener.Addr().String()
	fixture.server.pubsubClient.TLSConfig = nil
	closeAccepted := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	peerDone := make(chan error, 1)
	go func() {
		for attempt := range 3 {
			conn, err := listener.Accept()
			if err != nil {
				peerDone <- err
				return
			}
			err = func() error {
				defer conn.Close()
				if err := conn.SetDeadline(time.Now().Add(8 * time.Second)); err != nil {
					return err
				}
				var topic protocol.Topic
				if err := json.NewDecoder(conn).Decode(&topic); err != nil {
					return err
				}
				encoder := json.NewEncoder(conn)
				switch attempt {
				case 0:
					// A legacy peer closing without an acknowledgement is transient,
					// but cannot authorize a command or set readiness.
					return nil
				case 1:
					if err := encoder.Encode(protocol.SubscriptionResponse{Status: protocol.SubscriptionAccepted, ProtocolVersion: protocol.WireProtocolVersion}); err != nil {
						return err
					}
					message := runMessage(protocol.VerbStart, "reconnected", "")
					if err := encoder.Encode(protocol.Payload{PlanMessage: protocol.PlanMessage{"plan-1": message}}); err != nil {
						return err
					}
					select {
					case <-closeAccepted:
					case <-ctx.Done():
					}
					return nil
				default:
					return encoder.Encode(unsupportedSubscription())
				}
			}()
			if err != nil {
				peerDone <- err
				return
			}
		}
		peerDone <- nil
	}()
	loopDone := make(chan struct{})
	go func() { defer close(loopDone); fixture.server.subscribeToCoordinator() }()
	require.Eventually(t, func() bool {
		status := fixture.server.getProgressStatus()
		return status.RunID == "reconnected" && status.State == protocol.RunCompleted
	}, 8*time.Second, 5*time.Millisecond)
	require.True(t, subscriptionHealth(t, fixture.server, http.StatusOK).Subscribed)
	close(closeAccepted)
	require.Eventually(t, func() bool { return !fixture.server.isSubscribed() }, time.Second, time.Millisecond)
	select {
	case <-loopDone:
	case <-time.After(5 * time.Second):
		t.Fatal("permanent mismatch did not stop automatic reconnect")
	}
	awaitSubscriptionPeer(t, peerDone)
	health := subscriptionHealth(t, fixture.server, http.StatusServiceUnavailable)
	require.NotNil(t, health.Error)
	require.Equal(t, protocol.UnsupportedProtocolVersion, health.Error.Code)
}
