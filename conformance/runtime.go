//go:build integration && linux

// Protocol harness adapted from engine-sdk v0.3.0 preparing-adapter example.
// Copyright reqfleet contributors. Apache-2.0; modified for real adapter processes.
package conformance

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	"github.com/reqfleet/engine-sdk/metricschema"
	"github.com/reqfleet/engine-sdk/protocol"
	"github.com/stretchr/testify/require"
)

const (
	// CollectionID is the collection identity supplied by the harness.
	CollectionID = proofCollection
	// PlanID is the plan identity supplied by the harness.
	PlanID = proofPlan
	// APIKey authenticates the harness command, artifact, and agent requests.
	// It is a test credential, not a production secret.
	APIKey = proofKey
	// Wait is the default deadline for harness lifecycle operations.
	Wait = proofWait
	// TargetURL is the local HTTP origin served by Contract.Target.
	TargetURL = "http://127.0.0.1:18080"
)

// Fixture names an artifact and its adapter-specific SDK run settings.
type Fixture struct {
	// File names a test artifact in Contract.Artifacts.
	File string
	// Settings contains the engine-specific settings assigned to agent "7".
	Settings json.RawMessage
}

// BasicScenarios requires a naturally completing fixture and a long-running one.
// AssertMetrics checks the adapter's real output against its native result data.
type BasicScenarios struct {
	// Complete must finish successfully without a stop request.
	Complete Fixture
	// Reuse must keep running until stopped and generate real requests.
	Reuse Fixture
	// CompletionTimeout bounds natural completion of Complete.
	CompletionTimeout time.Duration
	// AssertMetrics compares a run's scrape with independently parsed native
	// output and returns the validated snapshot. It is also used by Budgets.
	AssertMetrics func(*testing.T, *Runtime, string) MetricsSnapshot
}

// BudgetScenario exercises admission using real requests, then checks the final
// native-output oracle and exact labels. Fixtures must repeat and vary labels.
type BudgetScenario struct {
	// Name identifies the label-budget subtest.
	Name string
	// LabelLimit is passed to the adapter through label_limit.
	LabelLimit int
	// Fixture must keep generating repeated, varied request labels until stopped.
	Fixture Fixture
	// MinimumRequests is the run-scoped request count required before stopping.
	MinimumRequests uint64
	// RequestLabels is the exact expected set of normalized request labels.
	RequestLabels []string
	// ByteLabels is the exact expected set of normalized received-byte labels.
	ByteLabels []string
}

// Contract supplies engine-specific artifacts and behavior, never peer plumbing.
// DataArtifacts keys are SDK artifact paths (use DataArtifactPath to build them).
// Run owns fixed ports and /test-data and MUST only run in a disposable container.
type Contract struct {
	// Executable is the adapter executable available inside the test container.
	Executable string
	// Artifacts maps test filenames to their contents.
	Artifacts map[string]string
	// DataArtifacts maps full protocol artifact paths to their contents,
	// including data, configuration, and dependency artifacts as needed.
	DataArtifacts map[string]string
	// Target serves real workload requests at TargetURL; nil starts no target.
	Target http.Handler
	// LabelLimit configures lifecycle and adapter-specific scenarios.
	LabelLimit int
	// Basic supplies the mandatory lifecycle fixtures and metric oracle.
	Basic BasicScenarios
	// Budgets adds independently launched label-admission scenarios.
	Budgets []BudgetScenario
	// Scenarios runs additional engine-specific checks after Basic, using the
	// same live adapter. It must serialize access to Runtime.
	Scenarios func(*testing.T, *Runtime)
}

// Runtime is one real adapter process, launched with production UID/GID 65532.
type Runtime struct {
	// Root is the temporary AGENT_ROOT owned by the adapter user.
	Root string
	// Client issues HTTP requests to the live adapter.
	Client *http.Client
	// LabelLimit is the configured adapter admission budget.
	LabelLimit      int
	peer            *proofPeer
	logs            proofLog
	done            chan struct{}
	started         map[string]struct{}
	downloads       atomic.Int64
	blockedDownload chan struct{}
	endedDownload   chan struct{}
	blockedFile     string
}

// Run executes the reusable lifecycle, authentication, and attribution suite,
// then any adapter-specific scenarios. Do not call it from parallel tests.
// Run requires a root harness in an isolated disposable Linux Docker container;
// see the package documentation for prerequisites and container restrictions.
func Run(t *testing.T, contract Contract) {
	t.Helper()
	require.NotEmpty(t, contract.Executable)
	require.NotEmpty(t, contract.Basic.Complete.File)
	require.NotEmpty(t, contract.Basic.Reuse.File)
	require.NotNil(t, contract.Basic.AssertMetrics)
	require.Positive(t, contract.Basic.CompletionTimeout)
	t.Run("lifecycle", func(t *testing.T) {
		runtime := startRuntime(t, contract)
		runBasic(t, runtime, contract.Basic)
		if contract.Scenarios != nil {
			contract.Scenarios(t, runtime)
		}
	})
	for _, scenario := range contract.Budgets {
		t.Run("labels/"+scenario.Name, func(t *testing.T) {
			configured := contract
			configured.LabelLimit = scenario.LabelLimit
			runtime := startRuntime(t, configured)
			const id = "9001"
			runtime.Start(t, id, scenario.Fixture)
			runtime.Running(t, id)
			runtime.AwaitRequests(t, id, scenario.MinimumRequests)
			runtime.Stop(t, id)
			runtime.Terminal(t, id, protocol.RunCanceled, proofWait)
			snapshot := contract.Basic.AssertMetrics(t, runtime, id)
			labels := make([]string, 0, len(snapshot.Requests))
			for label := range snapshot.Requests {
				labels = append(labels, label)
			}
			require.ElementsMatch(t, scenario.RequestLabels, labels)
			labels = labels[:0]
			for label := range snapshot.Bytes {
				labels = append(labels, label)
			}
			require.ElementsMatch(t, scenario.ByteLabels, labels)
			require.Equal(t, snapshot, contract.Basic.AssertMetrics(t, runtime, id), "retained budget metrics must not change")
		})
	}
}

func startRuntime(t *testing.T, contract Contract) *Runtime {
	t.Helper()
	require.Zero(t, os.Geteuid(), "run the adapter's container-only conformance target")
	_, err := os.Stat("/.dockerenv")
	require.NoError(t, err, "conformance owns fixed ports and /test-data; use a disposable Docker container")
	startProofDNS(t)
	certificate, ca := proofCertificate(t)
	r := &Runtime{peer: newProofPeer(t, certificate), LabelLimit: contract.LabelLimit, blockedDownload: make(chan struct{}, 1), endedDownload: make(chan struct{}, 1)}
	r.blockedFile = "conformance-blocked" + filepath.Ext(contract.Basic.Complete.File)
	blockedDependency, err := protocol.DependencyFilePath(proofCollection, proofPlan, r.blockedFile)
	require.NoError(t, err)
	artifacts := make(map[string]string, len(contract.Artifacts)+len(contract.DataArtifacts))
	for name, body := range contract.Artifacts {
		artifacts[proofArtifactPath(t, name)] = body
	}
	for name, body := range contract.DataArtifacts {
		artifacts[name] = body
	}
	if contract.Target != nil {
		startProofHTTP(t, "127.0.0.1:18080", contract.Target, nil)
	}
	startProofHTTP(t, "127.0.0.1:443", http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "Bearer "+proofKey {
			t.Errorf("artifact request did not authenticate")
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		r.downloads.Add(1)
		if request.URL.Path == proofArtifactPath(t, r.blockedFile) || request.URL.Path == path.Join(protocol.ArtifactPathPrefix, blockedDependency) {
			select {
			case r.blockedDownload <- struct{}{}:
			default:
			}
			<-request.Context().Done()
			select {
			case r.endedDownload <- struct{}{}:
			default:
			}
			return
		}
		body, ok := artifacts[request.URL.Path]
		if !ok {
			http.NotFound(w, request)
			return
		}
		_, _ = io.WriteString(w, body)
	}), &certificate)
	r.Root, err = os.MkdirTemp("/tmp", "adapter-conformance-")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(r.Root) })
	require.NoError(t, os.Chown(r.Root, 65532, 65532))
	environment := []string{"PATH=/usr/local/bin:/usr/bin:/bin", "HOME=" + r.Root, "AGENT_ROOT=" + r.Root, "engine_name=engine-proof-plan-local-0", "offset=7", "coordinator_ip=127.0.0.1", "coordinator_ca_cert=" + ca, "collection_id=" + proofCollection, "plan_id=" + proofPlan, "zone=local", "api_key=" + proofKey, "label_limit=+" + strconv.Itoa(contract.LabelLimit)}
	process, err := launchRuntimeProcess(contract.Executable, environment, &r.logs)
	require.NoError(t, err)
	r.done = process.exited
	r.Client = &http.Client{Timeout: 3 * time.Second}
	t.Cleanup(r.Client.CloseIdleConnections)
	t.Cleanup(func() {
		r.stopSubmitted(proofWait)
		if err := process.close(); err != nil {
			t.Logf("adapter output:\n%s", r.Logs())
			abortRuntimeCleanup(err)
		}
		if t.Failed() {
			t.Logf("adapter output:\n%s", r.Logs())
		}
	})
	r.peer.awaitSubscription(t)
	require.Eventually(t, func() bool {
		response, e := r.Client.Get("http://127.0.0.1:8080/healthz")
		if e != nil {
			return false
		}
		defer response.Body.Close()
		return response.StatusCode == http.StatusOK
	}, proofWait, 25*time.Millisecond)
	status, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", process.pid))
	require.NoError(t, err)
	require.Contains(t, string(status), "Uid:\t65532\t65532\t65532\t65532")
	require.Contains(t, string(status), "Gid:\t65532\t65532\t65532\t65532")
	r.assertAuthentication(t)
	return r
}

// stopSubmitted shares one budget across all command writes and progress reads.
// Stop every submitted start/prepare: rejected overlaps must not hide the active
// run, and ordered stops also cover requests still in flight after Send returns.
// Forced process teardown remains the caller's responsibility after this returns.
func (r *Runtime) stopSubmitted(timeout time.Duration) {
	if len(r.started) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	deadline, _ := ctx.Deadline()
	for id := range r.started {
		if !time.Now().Before(deadline) {
			return
		}
		select {
		case <-r.done:
			return
		default:
		}
		_ = r.peer.sendBefore(proofPayload(protocol.VerbStop, id, "", nil), deadline)
	}
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for len(r.started) != 0 {
		for id := range r.started {
			if !time.Now().Before(deadline) {
				return
			}
			select {
			case <-r.done:
				return
			default:
			}
			status, err := proofProgressContext(ctx, r.Client, id)
			if err == nil && status.State.Terminal() {
				delete(r.started, id)
			}
		}
		if len(r.started) == 0 {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-r.done:
			return
		case <-ticker.C:
		}
	}
}

func (r *Runtime) assertAuthentication(t *testing.T) {
	t.Helper()
	for _, endpoint := range []string{"/progress?run_id=8900", "/metrics"} {
		for _, auth := range []struct {
			header string
			status int
		}{
			{"", http.StatusBadRequest},
			{"Bearer wrong-key", http.StatusForbidden},
			{"Basic " + proofKey, http.StatusBadRequest},
		} {
			request, err := http.NewRequest(http.MethodGet, "http://127.0.0.1:8080"+endpoint, nil)
			require.NoError(t, err)
			request.Header.Set("Authorization", auth.header)
			response, err := r.Client.Do(request)
			require.NoError(t, err)
			_, err = io.Copy(io.Discard, response.Body)
			_ = response.Body.Close()
			require.NoError(t, err)
			require.Equal(t, auth.status, response.StatusCode, "%s with authorization %q", endpoint, auth.header)
		}
	}
}

func runBasic(t *testing.T, r *Runtime, basic BasicScenarios) {
	first := r.Start(t, "8901", basic.Complete)
	r.Terminal(t, "8901", protocol.RunCompleted, basic.CompletionTimeout)
	final := basic.AssertMetrics(t, r, "8901")
	require.NotEmpty(t, final.Requests, "fixture must generate real requests")
	require.Equal(t, final, basic.AssertMetrics(t, r, "8901"), "final metrics must be retained without double ingestion")
	before := r.Downloads()
	r.Send(t, first)
	r.Stop(t, "8999")
	r.Terminal(t, "8999", protocol.RunCanceled, proofWait)
	require.Equal(t, before, r.Downloads(), "duplicate start must not download or execute again")
	r.Start(t, "8902", basic.Reuse)
	r.Running(t, "8902")
	r.AwaitRequests(t, "8902", 1)
	r.Stop(t, "8901")
	for until := time.Now().Add(400 * time.Millisecond); time.Now().Before(until); {
		status, err := r.Progress("8902")
		require.NoError(t, err, "stale stop must leave the active run running with readable progress")
		require.Equal(t, protocol.RunRunning, status.State, "stale stop must leave the active run running with readable progress")
		time.Sleep(25 * time.Millisecond)
	}
	r.Stop(t, "8902")
	r.Terminal(t, "8902", protocol.RunCanceled, proofWait)
	reset := basic.AssertMetrics(t, r, "8902")
	require.Equal(t, "8902", reset.Identity.RunID)
	r.Start(t, "8903", Fixture{File: r.blockedFile, Settings: basic.Reuse.Settings})
	r.AwaitDownload(t)
	r.Stop(t, "8903")
	r.Terminal(t, "8903", protocol.RunCanceled, proofWait)
	r.AwaitDownloadCancellation(t)
	r.Start(t, "8904", basic.Reuse)
	r.Running(t, "8904")
	r.AwaitRequests(t, "8904", 1)
	r.Stop(t, "8904")
	r.Terminal(t, "8904", protocol.RunCanceled, proofWait)
	basic.AssertMetrics(t, r, "8904")
	r.AssertAlive(t)
}

// Payload constructs a command for the harness plan and agent "7". Start and
// prepare commands carry settings; stop commands request cancellation. Callers
// may add protocol fields before submitting the payload with Runtime.Send.
func Payload(verb, runID, filename string, settings json.RawMessage) protocol.Payload {
	return proofPayload(verb, runID, filename, settings)
}

// DataArtifactPath returns the full artifact URL path for an agent "7" data file.
// Use the result as a key in Contract.DataArtifacts.
func DataArtifactPath(t *testing.T, filename string) string {
	t.Helper()
	relative, err := protocol.EngineDataPath(proofCollection, proofPlan, filename, "7")
	require.NoError(t, err)
	return path.Join(protocol.ArtifactPathPrefix, relative)
}

// Start submits a fixture for execution and returns its payload for replay tests.
// It does not wait for startup acknowledgement or completion.
func (r *Runtime) Start(t *testing.T, id string, fixture Fixture) protocol.Payload {
	t.Helper()
	payload := Payload(protocol.VerbStart, id, fixture.File, fixture.Settings)
	r.Send(t, payload)
	return payload
}

// Send submits a protocol command and tracks every start or prepare run ID for
// cleanup, including commands the adapter may reject as overlapping.
func (r *Runtime) Send(t *testing.T, payload protocol.Payload) {
	t.Helper()
	if payload.Verb == protocol.VerbStart || payload.Verb == protocol.VerbPrepare {
		if r.started == nil {
			r.started = make(map[string]struct{})
		}
		if id := payload.PlanMessage[proofPlan].RunID; id != "" {
			r.started[id] = struct{}{}
		}
	}
	require.NoError(t, r.peer.send(payload))
}

// Stop requests cancellation of id without waiting for its terminal state.
func (r *Runtime) Stop(t *testing.T, id string) {
	t.Helper()
	r.Send(t, Payload(protocol.VerbStop, id, "", nil))
}

// Progress fetches the authenticated lifecycle status for id.
func (r *Runtime) Progress(id string) (protocol.RunStatus, error) {
	return proofProgress(r.Client, id)
}

// Logs returns the adapter and supervisor output captured so far.
func (r *Runtime) Logs() string { return r.logs.String() }

// Downloads returns the number of authenticated artifact requests received.
func (r *Runtime) Downloads() int64 { return r.downloads.Load() }

// BlockedFile names the synthetic artifact whose download blocks until canceled.
// It can be used as either a test file or a dependency file.
func (r *Runtime) BlockedFile() string { return r.blockedFile }

// AwaitDownload waits for a request for BlockedFile to reach the artifact peer.
func (r *Runtime) AwaitDownload(t *testing.T) {
	t.Helper()
	awaitProofEvent(t, r.blockedDownload, "artifact download")
}

// AwaitDownloadCancellation waits for a blocked artifact request to be canceled.
func (r *Runtime) AwaitDownloadCancellation(t *testing.T) {
	t.Helper()
	awaitProofEvent(t, r.endedDownload, "download cancellation")
}

// Terminal waits for any terminal state, asserts it equals want, and returns it.
func (r *Runtime) Terminal(t *testing.T, id string, want protocol.RunState, timeout time.Duration) protocol.RunStatus {
	t.Helper()
	var status protocol.RunStatus
	require.Eventually(t, func() bool {
		var err error
		status, err = r.Progress(id)
		return err == nil && status.State.Terminal()
	}, timeout, 50*time.Millisecond, "run %s failed to finish; logs=%s", id, r.Logs())
	require.Equal(t, want, status.State, "%+v; logs=%s", status, r.Logs())
	return status
}

// Running waits for id to reach RunRunning. Naturally completing fixtures should
// use Terminal instead because the running state may be too brief to observe.
func (r *Runtime) Running(t *testing.T, id string) {
	t.Helper()
	require.Eventually(t, func() bool {
		status, err := r.Progress(id)
		return err == nil && status.State == protocol.RunRunning
	}, proofWait, 50*time.Millisecond, "logs=%s", r.Logs())
}

// Scrape returns the authenticated metrics exposition from the live adapter.
func (r *Runtime) Scrape(t *testing.T) string { t.Helper(); return Scrape(t, r.Client) }

// Scrape fetches the harness adapter's metrics with client and asserts HTTP 200.
func Scrape(t *testing.T, client *http.Client) string {
	t.Helper()
	body, err := scrapeMetrics(context.Background(), client)
	require.NoError(t, err)
	return body
}

func scrapeMetrics(ctx context.Context, client *http.Client) (string, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://127.0.0.1:8080/metrics", nil)
	if err != nil {
		return "", err
	}
	request.Header.Set("Authorization", "Bearer "+proofKey)
	response, err := client.Do(request)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return "", err
	}
	if response.StatusCode != http.StatusOK {
		return "", fmt.Errorf("metrics returned HTTP %d: %s", response.StatusCode, body)
	}
	return string(body), nil
}

// AssertAlive fails if the adapter has exited instead of remaining reusable.
func (r *Runtime) AssertAlive(t *testing.T) {
	t.Helper()
	select {
	case <-r.done:
		t.Fatalf("adapter exited instead of recovering: %s", r.Logs())
	default:
	}
}

// AwaitRequests waits for real normalized request counts for this run only.
func (r *Runtime) AwaitRequests(t *testing.T, id string, minimum uint64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), proofWait)
	defer cancel()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	var lastErr error
	var count float64
	for ctx.Err() == nil {
		body, err := scrapeMetrics(ctx, r.Client)
		if ctx.Err() != nil && lastErr != nil {
			break
		}
		lastErr = err
		if err == nil {
			parser := expfmt.NewTextParser(model.UTF8Validation)
			families, err := parser.TextToMetricFamilies(strings.NewReader(body))
			lastErr = err
			count = 0
			if err == nil && families[metricschema.StatusCounter] != nil {
				for _, metric := range families[metricschema.StatusCounter].Metric {
					for _, label := range metric.Label {
						if label.GetName() == metricschema.LabelRunID && label.GetValue() == id {
							count += metric.GetCounter().GetValue()
						}
					}
				}
				if count >= float64(minimum) {
					return
				}
			}
		}
		select {
		case <-ctx.Done():
		case <-ticker.C:
		}
	}
	t.Fatalf("run %s must generate at least %d real requests; observed %g; last scrape error: %v", id, minimum, count, lastErr)
}
