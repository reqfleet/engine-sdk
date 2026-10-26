//go:build integration

package main

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	_ "embed"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/reqfleet/engine-sdk/agent"
	"github.com/reqfleet/engine-sdk/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

//go:embed testdata/original.curl
var originalConfig []byte

const (
	proofCollection = "proof-collection"
	proofPlan       = "proof-plan"
	proofKey        = "proof-secret"
	proofWait       = 15 * time.Second
)

// TestPublicAdapterRuntime intentionally uses the released wire protocol and a
// separately built child, rather than replacing any SDK lifecycle callback.
// Its fixed ports and the SDK's /test-data directory require a disposable Docker
// container; never run this test on a host or in a container with host mounts.
func TestPublicAdapterRuntime(t *testing.T) {
	require.Equal(t, "linux", runtime.GOOS, "run the conformance Docker image")
	require.Zero(t, os.Geteuid(), "the isolated artifact server binds port 443")
	_, err := os.Stat("/.dockerenv")
	require.NoError(t, err, "run in the disposable Docker image, without host mounts")

	certificate, caPEM := proofCertificate(t)
	peer := newProofPeer(t, certificate)
	requests := &proofRequests{}
	inflightStarted := make(chan struct{}, 3)
	inflightEnded := make(chan struct{}, 3)
	target := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.record(r.URL.Path, r.Header.Get("X-Plan-Name"))
		if r.Header.Get("X-Plan-Name") == "inflight-proof" {
			// Finish protocol negotiation while leaving the response body in flight.
			w.WriteHeader(http.StatusOK)
			w.(http.Flusher).Flush()
			select {
			case inflightStarted <- struct{}{}:
			default:
			}
			<-r.Context().Done()
			select {
			case inflightEnded <- struct{}{}:
			default:
			}
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "public adapter target\n")
	})
	startProofHTTP(t, "127.0.0.1:18080", target, nil)

	originalPath := proofArtifactPath(t, "original.curl")
	blockedPath := proofArtifactPath(t, "blocked.curl")
	downloadStarted := make(chan struct{}, 1)
	downloadEnded := make(chan struct{}, 1)
	var artifactMu sync.Mutex
	var artifactRequests []string
	artifacts := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !assert.Equal(t, http.MethodGet, r.Method) ||
			!assert.Equal(t, "Bearer "+proofKey, r.Header.Get("Authorization")) {
			http.Error(w, "unauthorized artifact request", http.StatusUnauthorized)
			return
		}
		artifactMu.Lock()
		artifactRequests = append(artifactRequests, r.URL.Path)
		artifactMu.Unlock()
		switch r.URL.Path {
		case originalPath:
			w.Header().Set("Content-Type", "application/octet-stream")
			_, _ = w.Write(originalConfig)
		case blockedPath:
			select {
			case downloadStarted <- struct{}{}:
			default:
			}
			<-r.Context().Done()
			select {
			case downloadEnded <- struct{}{}:
			default:
			}
		default:
			t.Errorf("unexpected artifact request %q (no generated config is served)", r.URL.Path)
			http.NotFound(w, r)
		}
	})
	startProofHTTP(t, "127.0.0.1:443", artifacts, &certificate)
	artifactCount := func() int {
		artifactMu.Lock()
		defer artifactMu.Unlock()
		return len(artifactRequests)
	}

	root := t.TempDir()
	client := &http.Client{Timeout: time.Second}
	t.Cleanup(client.CloseIdleConnections)
	var logs proofLog
	child := exec.Command("/usr/local/bin/preparing-adapter")
	child.Env = []string{
		"PATH=/usr/local/bin:/usr/bin:/bin", "HOME=" + root, "AGENT_ROOT=" + root,
		"engine_name=engine-proof-plan-local-0", "offset=7", "coordinator_ip=127.0.0.1",
		"coordinator_ca_cert=" + caPEM, "collection_id=" + proofCollection,
		"plan_id=" + proofPlan, "zone=local", "api_key=" + proofKey,
	}
	child.Stdout, child.Stderr = &logs, &logs
	require.NoError(t, child.Start())
	childDone := make(chan struct{})
	var childErr error
	go func() {
		childErr = child.Wait()
		close(childDone)
	}()
	var activeRun string
	t.Cleanup(func() {
		// Stop through the public protocol before killing the server: the SDK
		// owns a separate process group for curl and must reap it itself.
		if activeRun != "" {
			_ = peer.send(proofPayload(protocol.VerbStop, activeRun, "", nil))
			deadline := time.Now().Add(proofWait)
			for time.Now().Before(deadline) {
				status, statusErr := proofProgress(client, activeRun)
				if statusErr == nil && status.State.Terminal() {
					break
				}
				select {
				case <-childDone:
					deadline = time.Time{}
				case <-time.After(25 * time.Millisecond):
				}
			}
		}
		_ = child.Process.Kill()
		<-childDone
		if t.Failed() {
			t.Logf("adapter child output:\n%s", logs.String())
		}
	})
	peer.awaitSubscription(t)
	require.Eventually(t, func() bool {
		response, healthErr := client.Get("http://127.0.0.1:8080/healthz")
		if healthErr != nil {
			return false
		}
		defer response.Body.Close()
		return response.StatusCode == http.StatusOK
	}, proofWait, 25*time.Millisecond, "child did not become subscribed: %s", logs.String())
	t.Logf("subscribed child PID=%d, topic=collection:%s, assigned agent ID=7", child.Process.Pid, proofCollection)

	start := func(runID, name, concurrency, filename string) protocol.Payload {
		t.Helper()
		settings, marshalErr := json.Marshal(map[string]string{
			"plan_name": name, "duration": "1", "concurrency": concurrency, "rampup": "0",
		})
		require.NoError(t, marshalErr)
		payload := proofPayload(protocol.VerbStart, runID, filename, settings)
		activeRun = runID
		require.NoError(t, peer.send(payload))
		return payload
	}
	waitTerminal := func(runID string, expected protocol.RunState) protocol.RunStatus {
		t.Helper()
		var status protocol.RunStatus
		var statusErr error
		require.Eventually(t, func() bool {
			status, statusErr = proofProgress(client, runID)
			return statusErr == nil && status.State.Terminal()
		}, proofWait, 25*time.Millisecond, "run %s status=%+v error=%v", runID, status, statusErr)
		require.Equal(t, runID, status.RunID)
		require.Equal(t, expected, status.State, "status=%+v", status)
		require.False(t, status.Running)
		t.Logf("run %s status=%+v total target requests=%d", runID, status, len(requests.snapshot()))
		return status
	}
	stop := func(runID string) {
		t.Helper()
		require.NoError(t, peer.send(proofPayload(protocol.VerbStop, runID, "", nil)))
	}
	generatedPath := preparedConfigPath(agent.RunContext{Workspace: agent.Workspace{Temp: filepath.Join(root, "run-tmp")}})
	checkPrepared := func(runID, name string, concurrency int) []byte {
		t.Helper()
		source, readErr := os.ReadFile("/test-data/original.curl")
		require.NoError(t, readErr)
		require.Equal(t, originalConfig, source, "source must remain byte-for-byte original")
		generated, readErr := os.ReadFile(generatedPath)
		require.NoError(t, readErr)
		require.True(t, bytes.HasPrefix(generated, source), "original curl options must precede managed options")
		managed := strings.TrimSpace(string(generated[len(source):]))
		lines := strings.Split(managed, "\n")
		require.Len(t, lines, 4, "only one set of managed options may be appended")
		require.Equal(t, "parallel", strings.TrimSpace(lines[0]))
		values := make(map[string]string, 3)
		for _, line := range lines[1:] {
			key, value, ok := strings.Cut(line, "=")
			require.True(t, ok, "managed curl option %q", line)
			values[strings.TrimSpace(key)] = strings.TrimSpace(value)
		}
		require.Equal(t, map[string]string{
			"parallel-max": strconv.Itoa(concurrency), "max-time": "60",
			"header": strconv.Quote("X-Plan-Name: " + name),
		}, values)
		t.Logf("run %s: source unchanged (%d bytes), generated=%s (%d bytes), managed=%v", runID, len(source), generatedPath, len(generated), values)
		return generated
	}
	checkRequests := func(from int, name string) {
		t.Helper()
		observed := requests.snapshot()
		require.Len(t, observed, from+3)
		require.ElementsMatch(t, []proofRequest{
			{Path: "/one", Plan: name}, {Path: "/two", Plan: name}, {Path: "/three", Plan: name},
		}, observed[from:])
		t.Logf("observed three distinct requests for %q: %+v", name, observed[from:])
	}
	noRequests := func(count int) {
		t.Helper()
		require.Never(t, func() bool { return len(requests.snapshot()) != count }, 300*time.Millisecond, 10*time.Millisecond)
	}

	first := start("1001", "public-proof", "13", "original.curl")
	status := waitTerminal("1001", protocol.RunCompleted)
	require.Zero(t, status.ExitCode)
	firstGenerated := checkPrepared("1001", "public-proof", 13)
	checkRequests(0, "public-proof")

	beforeDuplicate := artifactCount()
	require.NoError(t, peer.send(first))
	// A stop for an otherwise unknown ID is a wire-order barrier. Seeing its
	// tombstone proves that the preceding duplicate was consumed by the SDK.
	stop("1999")
	waitTerminal("1999", protocol.RunCanceled)
	noRequests(3)
	require.Equal(t, beforeDuplicate, artifactCount(), "duplicate start must not download or prepare again")
	require.Equal(t, firstGenerated, checkPrepared("1001 duplicate", "public-proof", 13))
	t.Log("duplicate start consumed without new artifacts or requests")

	start("1002", "second-proof", "3", "original.curl")
	waitTerminal("1002", protocol.RunCompleted)
	secondGenerated := checkPrepared("1002", "second-proof", 3)
	require.NotEqual(t, firstGenerated, secondGenerated)
	require.NotContains(t, string(secondGenerated), "public-proof")
	checkRequests(3, "second-proof")

	start("1003", "invalid-proof", "not-an-integer", "original.curl")
	status = waitTerminal("1003", protocol.RunFailed)
	require.Equal(t, protocol.FailureStartup, status.FailurePhase)
	require.Equal(t, protocol.StartupStateFailed, status.StartupStatus)
	require.Equal(t, -1, status.ExitCode)
	require.Empty(t, status.StartupFailureKind, "native preparation is not dependency installation")
	require.Contains(t, strings.ToLower(status.ErrorMessage), "concurrency")
	require.NoFileExists(t, generatedPath, "invalid settings must not reuse the preceding prepared config")
	source, err := os.ReadFile("/test-data/original.curl")
	require.NoError(t, err)
	require.Equal(t, originalConfig, source)
	noRequests(6)

	start("1004", "download-canceled-proof", "3", "blocked.curl")
	awaitProofEvent(t, downloadStarted, "blocked HTTPS artifact request")
	stop("1004")
	status = waitTerminal("1004", protocol.RunCanceled)
	require.Equal(t, protocol.StopCanceled, status.StopReason)
	require.Equal(t, protocol.StartupStateCanceled, status.StartupStatus)
	awaitProofEvent(t, downloadEnded, "artifact request context cancellation")
	require.NoFileExists(t, generatedPath)
	require.NoFileExists(t, "/test-data/original.curl", "canceled download must not publish an input")
	noRequests(6)
	t.Log("blocked artifact download canceled before any workload or generated config")

	start("1005", "inflight-proof", "3", "original.curl")
	awaitProofEvent(t, inflightStarted, "in-flight curl target request")
	checkPrepared("1005", "inflight-proof", 3)
	status, err = proofProgress(client, "1005")
	require.NoError(t, err)
	require.Equal(t, protocol.RunRunning, status.State)
	require.True(t, status.Running)
	stop("1005")
	status = waitTerminal("1005", protocol.RunCanceled)
	require.Equal(t, protocol.StopCanceled, status.StopReason)
	canceledRequests := requests.snapshot()
	require.Greater(t, len(canceledRequests), 6)
	require.LessOrEqual(t, len(canceledRequests), 9)
	for _, request := range canceledRequests[6:] {
		require.Equal(t, "inflight-proof", request.Plan)
		awaitProofEvent(t, inflightEnded, "in-flight HTTP request context cancellation")
	}
	noRequests(len(canceledRequests))
	t.Logf("running curl canceled; all %d admitted target request contexts ended", len(canceledRequests)-6)

	start("1006", "recovery-proof", "3", "original.curl")
	waitTerminal("1006", protocol.RunCompleted)
	recoveryGenerated := checkPrepared("1006", "recovery-proof", 3)
	require.NotContains(t, string(recoveryGenerated), "inflight-proof")
	checkRequests(len(canceledRequests), "recovery-proof")
	noRequests(len(canceledRequests) + 3)
	require.Equal(t, 6, artifactCount(), "one original artifact fetch for each admitted run")
	select {
	case <-childDone:
		t.Fatalf("adapter exited before recovery completed: %v\n%s", childErr, logs.String())
	default:
	}
	t.Logf("same adapter child PID=%d recovered; six runs, six authenticated artifact requests, %d target requests", child.Process.Pid, len(requests.snapshot()))
}

func proofPayload(verb, runID, filename string, settings json.RawMessage) protocol.Payload {
	message := &protocol.EngineMessage{Verb: verb, RunID: runID, TestFile: filename}
	if verb == protocol.VerbStart {
		message.RunConfigs = map[string]protocol.RunSpec{"7": {Settings: settings}}
	} else if verb == protocol.VerbStop {
		message.StopReason = protocol.StopCanceled
	}
	return protocol.Payload{Verb: verb, PlanMessage: protocol.PlanMessage{proofPlan: message}}
}

func proofProgress(client *http.Client, runID string) (protocol.RunStatus, error) {
	request, err := http.NewRequest(http.MethodGet, "http://127.0.0.1:8080/progress?run_id="+runID, nil)
	if err != nil {
		return protocol.RunStatus{}, err
	}
	request.Header.Set("Authorization", "Bearer "+proofKey)
	response, err := client.Do(request)
	if err != nil {
		return protocol.RunStatus{}, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return protocol.RunStatus{}, fmt.Errorf("progress returned HTTP %d", response.StatusCode)
	}
	var status protocol.RunStatus
	err = json.NewDecoder(response.Body).Decode(&status)
	return status, err
}

func proofArtifactPath(t *testing.T, filename string) string {
	t.Helper()
	relative, err := protocol.TestFilePath(proofCollection, proofPlan, filename)
	require.NoError(t, err)
	return path.Join(protocol.ArtifactPathPrefix, relative)
}

func awaitProofEvent(t *testing.T, event <-chan struct{}, description string) {
	t.Helper()
	select {
	case <-event:
	case <-time.After(proofWait):
		t.Fatalf("timed out waiting for %s", description)
	}
}

type proofRequest struct {
	Path string
	Plan string
}

type proofRequests struct {
	mu       sync.Mutex
	requests []proofRequest
}

func (p *proofRequests) record(path, plan string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.requests = append(p.requests, proofRequest{Path: path, Plan: plan})
}

func (p *proofRequests) snapshot() []proofRequest {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]proofRequest(nil), p.requests...)
}

type proofLog struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (p *proofLog) Write(data []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.buffer.Write(data)
}

func (p *proofLog) String() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.buffer.String()
}

type proofPeer struct {
	mu         sync.Mutex
	connection net.Conn
	subscribed chan error
	done       chan struct{}
}

func newProofPeer(t *testing.T, certificate tls.Certificate) *proofPeer {
	t.Helper()
	listener, err := tls.Listen("tcp", "127.0.0.1:2416", &tls.Config{
		MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{certificate},
	})
	require.NoError(t, err)
	peer := &proofPeer{subscribed: make(chan error, 1), done: make(chan struct{})}
	go func() {
		defer close(peer.done)
		connection, acceptErr := listener.Accept()
		if acceptErr != nil {
			peer.subscribed <- acceptErr
			return
		}
		peer.mu.Lock()
		peer.connection = connection
		peer.mu.Unlock()
		_ = connection.SetReadDeadline(time.Now().Add(proofWait))
		var topic protocol.Topic
		decodeErr := json.NewDecoder(connection).Decode(&topic)
		if decodeErr == nil && (topic.Name != "collection:"+proofCollection || topic.Password != proofKey) {
			decodeErr = fmt.Errorf("unexpected subscription topic or API key: %+v", topic)
		}
		_ = connection.SetReadDeadline(time.Time{})
		peer.subscribed <- decodeErr
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		peer.mu.Lock()
		if peer.connection != nil {
			_ = peer.connection.Close()
		}
		peer.mu.Unlock()
		<-peer.done
		// Accept may have succeeded just before listener.Close but published
		// its connection after the first lock acquisition.
		peer.mu.Lock()
		if peer.connection != nil {
			_ = peer.connection.Close()
		}
		peer.mu.Unlock()
	})
	return peer
}

func (p *proofPeer) awaitSubscription(t *testing.T) {
	t.Helper()
	select {
	case err := <-p.subscribed:
		require.NoError(t, err)
	case <-time.After(proofWait):
		t.Fatal("adapter did not subscribe to the TLS command peer")
	}
}

func (p *proofPeer) send(payload protocol.Payload) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.connection == nil {
		return fmt.Errorf("adapter has not connected to command peer")
	}
	if err := p.connection.SetWriteDeadline(time.Now().Add(time.Second)); err != nil {
		return err
	}
	return json.NewEncoder(p.connection).Encode(payload)
}

func startProofHTTP(t *testing.T, address string, handler http.Handler, certificate *tls.Certificate) {
	t.Helper()
	listener, err := net.Listen("tcp", address)
	require.NoError(t, err)
	server := &httptest.Server{Listener: listener, Config: &http.Server{Handler: handler}}
	if certificate == nil {
		server.Start()
	} else {
		server.TLS = &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{*certificate}}
		server.StartTLS()
	}
	t.Cleanup(func() {
		server.CloseClientConnections()
		server.Close()
	})
}

func proofCertificate(t *testing.T) (tls.Certificate, string) {
	t.Helper()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	ca := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "Public adapter proof CA"},
		NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, &caKey.PublicKey, caKey)
	require.NoError(t, err)
	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	leaf := &x509.Certificate{
		SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "127.0.0.1"},
		NotBefore: ca.NotBefore, NotAfter: ca.NotAfter, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leaf, ca, &leafKey.PublicKey, caKey)
	require.NoError(t, err)
	keyDER, err := x509.MarshalECPrivateKey(leafKey)
	require.NoError(t, err)
	certificate, err := tls.X509KeyPair(
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leafDER}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}),
	)
	require.NoError(t, err)
	return certificate, string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}))
}
