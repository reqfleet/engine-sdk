// Derived from reqfleet/shibuya; modified for engine-sdk.
package agent

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/hpcloud/tail"
	"github.com/reqfleet/engine-sdk/internal/coordinator"
	"github.com/reqfleet/engine-sdk/internal/pubsub"
	"github.com/reqfleet/engine-sdk/metricschema"
	"github.com/reqfleet/engine-sdk/protocol"
	"github.com/reqfleet/engine-sdk/resources"
)

const (
	// DefaultStopTimeout is the graceful-stop deadline used when StopTimeout is
	// nonpositive. The runtime then escalates to SIGKILL.
	DefaultStopTimeout = 10 * time.Second
	// ObservationTimeout bounds each Observe callback's context.
	// The callback must honor cancellation.
	ObservationTimeout = 5 * time.Second
	// FinalProgressTimeout bounds the final Progress callback's fresh context.
	// The callback must honor cancellation.
	FinalProgressTimeout = 2 * time.Second
)

var STDERR = "/dev/stderr"

type agentRuntimeProgress struct {
	run     *agentRun
	retired map[string]protocol.RunState
}

type metricEvent struct {
	runID string
	line  string
}

type agentRequest struct {
	verb string
	run  *agentRun
}

type startupFailure struct {
	err  error
	kind protocol.StartupFailureKind
}

func (e *startupFailure) Error() string {
	return e.err.Error()
}

func (e *startupFailure) Unwrap() error {
	return e.err
}

func newDependencyStartupFailure(err error) error {
	return &startupFailure{
		err:  err,
		kind: protocol.StartupFailureDependency,
	}
}

type AgentServer struct {
	// Hot path fields - frequently accessed during test execution
	runtime   agentRuntimeProgress
	stateLock sync.RWMutex

	// Concurrent access fields
	bus      chan metricEvent
	requests chan agentRequest
	// Label tracking
	labelsLock       sync.RWMutex
	labelsByRun      map[string]map[string]struct{}
	labelsUsageLimit int

	// Native admission survives run completion for the final upstream scrape.
	nativeMetricsLock sync.Mutex
	nativeMetricsRun  string
	nativeLabels      map[string]struct{}

	// Subscription status
	subStausMu            sync.RWMutex
	coordinatorSubscribed bool

	// Cold fields - initialization only, rarely accessed after setup
	cdrclient    *coordinator.Client
	reqOpts      coordinator.ReqOpts
	logger       *log.Entry
	angentDir    AgentDir
	options      AgentServerOptions
	pubsubClient pubsub.PubSubClient
}

// NewAgentServer constructs a server from explicit options without reloading the
// environment-backed settings handled by LoadAgentServerOptions. It does not
// start listeners or workloads; call Run to start the agent.
// A zero LabelsUsageLimit is used as-is, not replaced with a default.
func NewAgentServer(opts AgentServerOptions) *AgentServer {
	if opts.Logger == nil {
		opts.Logger = log.WithField("Source", "shibuya-agent")
	}
	if opts.StopTimeout <= 0 {
		opts.StopTimeout = DefaultStopTimeout
	}
	httpClient := &http.Client{Timeout: 3 * time.Second}
	httpClient.Transport = &http.Transport{
		TLSClientConfig: &tls.Config{
			RootCAs:    opts.CoordinatorRootCAs,
			MinVersion: tls.VersionTLS12,
		},
	}
	as := &AgentServer{
		runtime:          agentRuntimeProgress{retired: make(map[string]protocol.RunState)},
		bus:              make(chan metricEvent),
		requests:         make(chan agentRequest, 1),
		options:          opts,
		cdrclient:        coordinator.NewClient(httpClient),
		reqOpts:          opts.EngineMeta.makeReqOpts(),
		logger:           opts.Logger,
		angentDir:        NewAgentDirHandler(""),
		labelsUsageLimit: opts.LabelsUsageLimit,
		pubsubClient: pubsub.PubSubClient{
			Addr:     net.JoinHostPort(opts.EngineMeta.CoordinatorIP, "2416"),
			Password: opts.EngineMeta.APIKey,
			TLSConfig: &tls.Config{
				RootCAs:    opts.CoordinatorRootCAs,
				MinVersion: tls.VersionTLS12,
			},
		},
	}
	if opts.MetricsURL == "" && opts.MetricsHandler == nil {
		as.labelsByRun = make(map[string]map[string]struct{})
	}
	return as
}

func (as *AgentServer) handleMetricStream() {
	for event := range as.bus {
		as.makePromMetrics(event)
	}
}

func (as *AgentServer) makePromMetrics(event metricEvent) {
	as.labelsLock.Lock()
	labels, ok := as.labelsByRun[event.runID]
	if !ok {
		as.labelsLock.Unlock()
		return
	}
	defer as.labelsLock.Unlock()

	metricParser := as.options.MetricParser
	metric, err := metricParser(event.line)
	// we need to pass the engine meta(project, collection, plan), especially run id
	// Run id is generated at controller side
	if err != nil {
		return
	}

	if _, err := metric.LabelIsValid(); err != nil {
		log.Error(err)
		return
	}
	metric.Label = metricschema.AdmitRequestName(metric.Label, labels, as.labelsUsageLimit, "")
	em := as.options.EngineMeta
	metric.CollectionID = em.CollectionID
	metric.PlanID = em.PlanID
	metric.EngineID = em.EngineID
	metric.RunID = event.runID
	metric.Zone = em.Zone

	metric.ToPrometheus()
}

func (as *AgentServer) tailFunc(ctx context.Context, filepath, runID string) {
	var t *tail.Tail
	for {
		if ctx.Err() != nil {
			return
		}
		var err error
		t, err = tail.TailFile(filepath, tail.Config{MustExist: true, Follow: true, Poll: true})
		if err == nil {
			break
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case line, ok := <-t.Lines:
			if !ok {
				return
			}
			if line == nil || line.Err != nil {
				continue
			}
			select {
			case <-ctx.Done():
				return
			case as.bus <- metricEvent{runID: runID, line: line.Text}:
			}
		}
	}
}

func (as *AgentServer) reportOwnMetrics(interval time.Duration) error {
	var sampler resources.Sampler
	var sampledRunID string
	for {
		time.Sleep(interval)
		runID := as.getActiveRunID()
		if runID == "" {
			continue
		}
		if runID != sampledRunID {
			sampler.Reset()
			sampledRunID = runID
		}
		sample, ok, err := sampler.Sample()
		if err != nil {
			return err
		}
		as.publishResourceMetrics(runID, sample, ok)
	}
}

func (as *AgentServer) publishResourceMetrics(runID string, sample resources.Sample, ok bool) {
	meta := as.options.EngineMeta
	if !ok {
		cpuGauge.DeleteLabelValues(meta.CollectionID, meta.PlanID, runID, meta.Zone, meta.EngineID)
		memGauge.DeleteLabelValues(meta.CollectionID, meta.PlanID, runID, meta.Zone, meta.EngineID)
		return
	}
	cpuGauge.WithLabelValues(meta.CollectionID, meta.PlanID, runID, meta.Zone, meta.EngineID).Set(sample.CPUCores)
	memGauge.WithLabelValues(meta.CollectionID, meta.PlanID, runID, meta.Zone, meta.EngineID).Set(float64(sample.MemoryBytes))
}

func (as *AgentServer) SubscribeToCoordinator() (chan protocol.Message, error) {
	collectionID := as.options.EngineMeta.CollectionID
	msgChan, _, err := as.pubsubClient.Subscribe(fmt.Sprintf("collection:%s", collectionID), func() protocol.Message {
		return &protocol.Payload{}
	})
	if err != nil {
		return nil, err
	}
	as.logger.Infof("Subscribe to coordinator at %s", as.reqOpts.Endpoint)
	return msgChan, nil
}

// streamToLogger reads from a pipe and logs each line in a separate goroutine
func (as *AgentServer) streamToLogger(reader io.ReadCloser, prefix string) {
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		line := scanner.Text()
		as.logger.Infof("[%s] %s", prefix, line)
	}
	if err := scanner.Err(); err != nil {
		as.logger.Errorf("Error reading %s: %v", prefix, err)
	}
}

func (as *AgentServer) buildCommandEnv(runID string, overrides []string) ([]string, error) {
	env := os.Environ()
	for _, entry := range overrides {
		key, value, ok := strings.Cut(entry, "=")
		if !ok || key == "" {
			return nil, fmt.Errorf("invalid command environment entry %q", entry)
		}
		env = upsertEnv(env, key, value)
	}
	engineMeta := as.options.EngineMeta
	env = upsertEnv(env, "collection_id", engineMeta.CollectionID)
	env = upsertEnv(env, "plan_id", engineMeta.PlanID)
	env = upsertEnv(env, "engine_no", engineMeta.EngineID)
	env = upsertEnv(env, "zone", engineMeta.Zone)
	env = upsertEnv(env, "run_id", runID)
	return env, nil
}

func upsertEnv(env []string, key, value string) []string {
	if key == "" {
		return env
	}
	prefix := key + "="
	for i, entry := range env {
		if strings.HasPrefix(entry, prefix) {
			env[i] = prefix + value
			return env
		}
	}
	return append(env, prefix+value)
}

func (as *AgentServer) listenToCoordinator(msgChan chan protocol.Message) {
	engineMeta := as.options.EngineMeta
	for msg := range msgChan {
		pl := msg.(*protocol.Payload)
		planMsg, ok := pl.PlanMessage[engineMeta.PlanID]
		if !ok {
			continue
		}
		switch planMsg.Verb {
		case protocol.VerbPrepare:
			if err := as.queuePrepare(planMsg); err != nil {
				as.logger.Error(err)
			}
		case protocol.VerbStart:
			if err := as.queueStart(planMsg); err != nil {
				as.logger.Error(err)
			}
		case protocol.VerbStop:
			as.stopRun(planMsg.RunID, planMsg.StopReason)
		}
	}
}

type EngineMeta struct {
	CoordinatorIP string
	CollectionID  string
	Zone          string
	PlanID        string
	EngineID      string
	APIKey        string
}

func (em EngineMeta) makeReqOpts() coordinator.ReqOpts {
	return coordinator.ReqOpts{
		Endpoint: em.CoordinatorIP,
		APIKey:   em.APIKey,
	}
}

// MetricsSchema identifies the reporting profile promised by a native adapter.
type MetricsSchema string

const MetricsSchemaShibuyaV1 MetricsSchema = "shibuya-v1"

// AgentServerOptions configures an adapter, not an independent execution mode.
// Callbacks must honor cancellation and must not retain mutable run data beyond
// their return. The runtime waits for callbacks and cleanup before another run.
type AgentServerOptions struct {
	EngineMeta          EngineMeta
	CoordinatorRootCAs  *x509.CertPool
	TestFileName        string
	DependencyFileName  string
	PrepareDependencies func(context.Context, RunContext) error
	BeforeStart         func(context.Context, RunContext) error
	BuildCommand        func(context.Context, RunContext) (Command, error)
	Observe             func(context.Context, RunContext) (protocol.Observation, error)
	Progress            func(context.Context, RunContext) (protocol.Progress, error)
	Stop                func(context.Context, RunContext, Process) error
	// StopTimeout overrides DefaultStopTimeout when positive.
	StopTimeout time.Duration
	// MetricsURL proxies an external normalized endpoint; it cannot be combined
	// with MetricsHandler or MetricParser.
	MetricsURL string
	// MetricsHandler serves normalized metrics directly at the authenticated
	// GET /metrics route, using the incoming request and response writer.
	// It requires NativeMetricsSchema and cannot be combined with MetricsURL,
	// MetricParser, FoldNativeMetrics, or NativeMetricsOverflowLabel.
	// ResultFile may still name workload output; the SDK does not tail it.
	// The adapter owns normalization, label limits, concurrent access, run-local
	// state, and exporter shutdown. The SDK owns the HTTP listener and does not
	// parse, merge, cache, or close the handler's output or state.
	MetricsHandler http.Handler
	// NativeMetricsSchema declares the profile supplied by MetricsURL or MetricsHandler.
	NativeMetricsSchema MetricsSchema
	// FoldNativeMetrics opts into SDK parsing and label folding for MetricsURL
	// as a last resort. It requires MetricsURL. By default the native endpoint
	// streams unchanged and the engine must enforce its own configured label limit.
	FoldNativeMetrics bool
	// NativeMetricsOverflowLabel selects the folding fallback's overflow name.
	// It requires MetricsURL and defaults to "others"; use "_other_" for Replay's convention.
	NativeMetricsOverflowLabel string
	MetricParser               func(string) (ShibuyaMetric, error)
	ResultFile                 string
	Logger                     *log.Entry
	ConfFileName               string
	// LabelsUsageLimit is the named request-label budget, excluding overflow.
	// LoadAgentServerOptions uses metricschema.DefaultLabelLimit when label_limit
	// is unset or empty. NewAgentServer preserves zero as an explicit budget.
	LabelsUsageLimit int
}

func coordinatorRootCAs(caPEM string) (*x509.CertPool, error) {
	if strings.TrimSpace(caPEM) == "" {
		return nil, fmt.Errorf("missing required coordinator CA certificate: coordinator_ca_cert")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM([]byte(caPEM)) {
		return nil, fmt.Errorf("invalid coordinator CA certificate: coordinator_ca_cert")
	}
	return roots, nil
}

// LoadAgentServerOptions resolves SDK-owned environment settings before server
// construction. It does not log, terminate the process, start services, or
// validate adapter callbacks and metrics integration. On error it returns zero
// options; the caller's options are not modified.
//
// Precedence and validation match MakeAgentServer:
//   - LabelsUsageLimit is always replaced by label_limit, or 10 when that variable
//     is unset or empty, even if the input option is nonzero. Nonempty values use
//     strconv.Atoi: decimal digits with an optional sign, no surrounding whitespace,
//     within the platform int range. Negative results are rejected; 0, +0, -0,
//     and +1 are valid. Zero permits only the overflow label in SDK-managed modes.
//   - A non-nil CoordinatorRootCAs is preserved and coordinator_ca_cert is ignored.
//     Otherwise coordinator_ca_cert must contain at least one parseable PEM
//     certificate. Unset, empty, whitespace-only, and malformed values are errors.
//   - A nonzero EngineMeta is preserved as a whole, without filling missing fields
//     or validating it. Otherwise engine_name must have five hyphen-separated
//     components with an integer fifth component, and offset must be an integer.
//     Both integers use strconv.Atoi; unset, empty, malformed, and overflowing
//     integers are errors. EngineID is their sum, formatted in decimal.
//     coordinator_ip, collection_id, zone, plan_id, and api_key are copied as-is;
//     unset and empty values both become empty strings, without validation.
//
// All other options are preserved. To override a resolved value (including a zero
// label limit), assign it after loading. Configure adapter-owned components from
// the resolved options, then pass them to NewAgentServer, not MakeAgentServer,
// to avoid another environment read.
func LoadAgentServerOptions(options AgentServerOptions) (AgentServerOptions, error) {
	if options.CoordinatorRootCAs == nil {
		var err error
		options.CoordinatorRootCAs, err = coordinatorRootCAs(os.Getenv("coordinator_ca_cert"))
		if err != nil {
			return AgentServerOptions{}, err
		}
	}
	if options.EngineMeta == (EngineMeta{}) {
		var err error
		options.EngineMeta, err = fetchEngineMeta()
		if err != nil {
			return AgentServerOptions{}, err
		}
	}
	options.LabelsUsageLimit = metricschema.DefaultLabelLimit
	if value := os.Getenv("label_limit"); value != "" {
		limit, err := strconv.Atoi(value)
		if err != nil {
			return AgentServerOptions{}, fmt.Errorf("invalid label_limit: %w", err)
		}
		if limit < 0 {
			return AgentServerOptions{}, fmt.Errorf("label_limit must be nonnegative")
		}
		options.LabelsUsageLimit = limit
	}
	return options, nil
}

// MakeAgentServer loads environment-backed options using LoadAgentServerOptions,
// then constructs a server with NewAgentServer. It logs fatally on configuration
// errors. Call LoadAgentServerOptions directly to handle errors without exiting
// or to configure adapter components before constructing the server.
func MakeAgentServer(options AgentServerOptions) *AgentServer {
	if options.Logger == nil {
		options.Logger = log.WithField("Source", "shibuya-agent")
	}
	resolved, err := LoadAgentServerOptions(options)
	if err != nil {
		options.Logger.Fatal(err)
		return nil
	}
	return NewAgentServer(resolved)
}

func (as *AgentServer) updateSubStatus(subscribed bool) {
	as.subStausMu.Lock()
	defer as.subStausMu.Unlock()
	as.coordinatorSubscribed = subscribed
}

func (as *AgentServer) isSubscribed() bool {
	as.subStausMu.RLock()
	defer as.subStausMu.RUnlock()
	return as.coordinatorSubscribed
}

func (as *AgentServer) validateMetricsOptions() error {
	options := as.options
	if options.LabelsUsageLimit < 0 {
		return fmt.Errorf("metrics label limit must be nonnegative")
	}
	if options.MetricsHandler != nil && options.MetricsURL != "" {
		return fmt.Errorf("native metrics cannot configure both MetricsHandler and MetricsURL")
	}
	if options.MetricsURL == "" && (options.FoldNativeMetrics || options.NativeMetricsOverflowLabel != "") {
		return fmt.Errorf("FoldNativeMetrics and NativeMetricsOverflowLabel require MetricsURL")
	}
	if options.MetricsURL == "" && options.MetricsHandler == nil {
		if options.NativeMetricsSchema != "" {
			return fmt.Errorf("native metrics schema requires MetricsURL or MetricsHandler")
		}
		if (options.ResultFile != "") != (options.MetricParser != nil) {
			return fmt.Errorf("result-file metrics require both ResultFile and MetricParser")
		}
		return nil
	}
	if options.MetricParser != nil {
		return fmt.Errorf("native metrics cannot also configure MetricParser")
	}
	if options.NativeMetricsSchema != MetricsSchemaShibuyaV1 {
		return fmt.Errorf("unsupported native metrics schema %q; require shibuya-v1 from an adapter-normalized endpoint", options.NativeMetricsSchema)
	}
	if options.MetricsHandler != nil {
		return nil
	}
	if options.NativeMetricsOverflowLabel != "" && options.NativeMetricsOverflowLabel != metricschema.OverflowRequestName && options.NativeMetricsOverflowLabel != "_other_" {
		return fmt.Errorf("native metrics overflow label must be others or _other_")
	}
	target, err := url.Parse(options.MetricsURL)
	if err != nil || (target.Scheme != "http" && target.Scheme != "https") || target.Host == "" {
		return fmt.Errorf("native metrics URL must use http or https and include a host")
	}
	return nil
}

func (as *AgentServer) usesResultFileMetrics() bool {
	return as.options.MetricsURL == "" && as.options.MetricsHandler == nil && as.options.ResultFile != "" && as.options.MetricParser != nil
}

func fetchEngineMeta() (EngineMeta, error) {
	engineID, err := extractEngineIDFromName(os.Getenv("engine_name"))
	if err != nil {
		return EngineMeta{}, fmt.Errorf("invalid engine_name: %w", err)
	}
	offset, err := strconv.Atoi(os.Getenv("offset"))
	if err != nil {
		return EngineMeta{}, fmt.Errorf("invalid offset: %w", err)
	}
	engineID += offset
	return EngineMeta{
		CoordinatorIP: os.Getenv("coordinator_ip"),
		CollectionID:  os.Getenv("collection_id"),
		Zone:          os.Getenv("zone"),
		PlanID:        os.Getenv("plan_id"),
		EngineID:      strconv.Itoa(engineID),
		APIKey:        os.Getenv("api_key"),
	}, nil
}

func extractEngineIDFromName(engineName string) (int, error) {
	t := strings.Split(engineName, "-")
	if len(t) != 5 {
		return 0, fmt.Errorf("invalid engine name %s", engineName)
	}
	return strconv.Atoi(t[4])
}
