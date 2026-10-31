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
	"github.com/reqfleet/engine-sdk/internal/containerstats"
	"github.com/reqfleet/engine-sdk/internal/coordinator"
	"github.com/reqfleet/engine-sdk/internal/pubsub"
	"github.com/reqfleet/engine-sdk/protocol"
)

const labelOthers = "others"

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

func NewAgentServer(opts AgentServerOptions) *AgentServer {
	if opts.Logger == nil {
		opts.Logger = log.WithField("Source", "shibuya-agent")
	}
	if opts.StopTimeout <= 0 {
		opts.StopTimeout = 10 * time.Second
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
	if opts.MetricsURL == "" {
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
	// we limit the number of unique labels to avoid high cardinality in prometheus.
	// If the label is new and we have reached the limit, we set it to "others"
	if _, exists := labels[metric.Label]; metric.Label != labelOthers && !exists {
		if len(labels) >= as.labelsUsageLimit {
			metric.Label = labelOthers
		} else {
			labels[metric.Label] = struct{}{}
		}
	}
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
	prev := uint64(0)
	engineMeta := as.options.EngineMeta
	engineID := engineMeta.EngineID
	collectionID := engineMeta.CollectionID
	planID := engineMeta.PlanID
	engineNumber := engineID
	zone := engineMeta.Zone
	for {
		time.Sleep(interval)
		// we don't need to strictly thread safety here
		// this is for code simplicity
		runID := as.getActiveRunID()
		if runID == "" {
			continue
		}
		cpuUsage, err := containerstats.ReadCPUUsage()
		if err != nil {
			return err
		}
		if prev == 0 {
			prev = cpuUsage
			continue
		}
		used := float64(cpuUsage-prev) / float64(interval.Microseconds())
		prev = cpuUsage
		memoryUsage, err := containerstats.ReadMemoryUsage()
		if err != nil {
			return err
		}
		cpuGauge.WithLabelValues(collectionID,
			planID, runID, zone, engineNumber).Set(used)
		memGauge.WithLabelValues(collectionID,
			planID, runID, zone, engineNumber).Set(float64(memoryUsage))
	}
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
	StopTimeout         time.Duration
	MetricsURL          string
	NativeMetricsSchema MetricsSchema
	// FoldNativeMetrics opts into SDK parsing and label folding as a last resort.
	// By default the native endpoint streams unchanged and the engine must enforce
	// its own configured label limit.
	FoldNativeMetrics bool
	// NativeMetricsOverflowLabel selects the folding fallback's overflow name.
	// It defaults to "others"; use "_other_" for Replay's convention.
	NativeMetricsOverflowLabel string
	MetricParser               func(string) (ShibuyaMetric, error)
	ResultFile                 string
	Logger                     *log.Entry
	ConfFileName               string
	LabelsUsageLimit           int
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

func MakeAgentServer(options AgentServerOptions) *AgentServer {
	if options.Logger == nil {
		options.Logger = log.WithFields(log.Fields{
			"Source": "shibuya-agent",
		})
	}
	if options.CoordinatorRootCAs == nil {
		var err error
		options.CoordinatorRootCAs, err = coordinatorRootCAs(os.Getenv("coordinator_ca_cert"))
		if err != nil {
			options.Logger.Fatal(err)
		}
	}
	em := EngineMeta{}
	if options.EngineMeta == em {
		options.EngineMeta = fetchEngineMeta()
	}
	labelUsageLimit := 10
	t := os.Getenv("label_limit")
	if t != "" {
		var err error
		labelUsageLimit, err = strconv.Atoi(t)
		if err != nil {
			options.Logger.Fatal(err)
		}
	}
	options.LabelsUsageLimit = labelUsageLimit
	as := NewAgentServer(options)
	return as
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
	if options.MetricsURL == "" {
		if options.NativeMetricsSchema != "" {
			return fmt.Errorf("native metrics schema requires MetricsURL")
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
	if options.NativeMetricsOverflowLabel != "" && options.NativeMetricsOverflowLabel != labelOthers && options.NativeMetricsOverflowLabel != "_other_" {
		return fmt.Errorf("native metrics overflow label must be others or _other_")
	}
	target, err := url.Parse(options.MetricsURL)
	if err != nil || (target.Scheme != "http" && target.Scheme != "https") || target.Host == "" {
		return fmt.Errorf("native metrics URL must use http or https and include a host")
	}
	return nil
}

func (as *AgentServer) usesResultFileMetrics() bool {
	return as.options.MetricsURL == "" && as.options.ResultFile != "" && as.options.MetricParser != nil
}

func fetchEngineMeta() EngineMeta {
	engineID, err := extractEngineIDFromName(os.Getenv("engine_name"))
	if err != nil {
		log.Fatal(err)
	}
	offset, err := strconv.Atoi(os.Getenv("offset"))
	if err != nil {
		log.Fatal(err)
	}
	engineID += offset
	log.Infof("Engine starts with id %d", engineID)
	return EngineMeta{
		CoordinatorIP: os.Getenv("coordinator_ip"),
		CollectionID:  os.Getenv("collection_id"),
		Zone:          os.Getenv("zone"),
		PlanID:        os.Getenv("plan_id"),
		EngineID:      strconv.Itoa(engineID),
		APIKey:        os.Getenv("api_key"),
	}
}

func extractEngineIDFromName(engineName string) (int, error) {
	t := strings.Split(engineName, "-")
	if len(t) != 5 {
		return 0, fmt.Errorf("invalid engine name %s", engineName)
	}
	return strconv.Atoi(t[4])
}
