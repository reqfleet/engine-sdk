// Derived from reqfleet/shibuya; modified for engine-sdk.
package agent

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httputil"
	"net/url"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/reqfleet/lib/http/auth"
	"github.com/reqfleet/lib/http/route"
	httpserver "github.com/reqfleet/lib/http/server"
)

func (as *AgentServer) metricsHandler() http.Handler {
	if err := as.validateMetricsOptions(); err != nil {
		return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
		})
	}
	if as.options.MetricsHandler != nil {
		return as.options.MetricsHandler
	}
	if as.options.MetricsURL == "" {
		return promhttp.HandlerFor(prometheus.GathererFunc(as.gatherMetrics), promhttp.HandlerOpts{})
	}
	target, _ := url.Parse(as.options.MetricsURL) // Checked by validateMetricsOptions.
	proxy := &httputil.ReverseProxy{
		Rewrite: func(r *httputil.ProxyRequest) {
			r.SetURL(target)
			out := r.Out
			out.Host = target.Host
			out.URL.Path = target.Path
			out.URL.RawPath = target.RawPath
			if as.options.FoldNativeMetrics {
				out.Header.Set("Accept", "text/plain; version=0.0.4")
				out.Header.Set("Accept-Encoding", "identity")
			}
		},
		ErrorHandler: func(w http.ResponseWriter, _ *http.Request, err error) {
			http.Error(w, "native metrics: "+err.Error(), http.StatusBadGateway)
		},
	}
	if as.options.FoldNativeMetrics {
		proxy.ModifyResponse = as.foldNativeMetrics
	}
	return proxy
}

// Handler returns the authenticated agent HTTP endpoints. Health checks bypass authentication.
func (as *AgentServer) Handler() http.Handler {
	public := route.NewRouter("agent-public", "")
	public.AddRoutes(route.Routes{
		{Name: "healthz", Method: http.MethodGet, Path: "/healthz", HandlerFunc: as.handleHealthz},
	})
	protected := route.NewRouter("agent", "")
	protected.AddRoutes(route.Routes{
		{Name: "progress", Method: http.MethodGet, Path: "/progress", HandlerFunc: as.handleProcessCheck},
		{Name: "prepare-progress", Method: http.MethodGet, Path: "/prepare-progress", HandlerFunc: as.handlePreparationCheck},
		{Name: "metrics", Method: http.MethodGet, Path: "/metrics", HandlerFunc: as.metricsHandler().ServeHTTP},
	})
	mux := public.Mux()
	mux.Handle("/", auth.AuthRequiredWithToken(protected.Mux(), as.options.EngineMeta.APIKey))
	return mux
}

func (as *AgentServer) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	if as.isSubscribed() {
		w.WriteHeader(http.StatusOK)
		return
	}
	http.Error(w, "not subscribed to coordinator", http.StatusServiceUnavailable)
}

func (as *AgentServer) newHTTPServer(addr string) *http.Server {
	return &http.Server{
		Addr:    addr,
		Handler: as.Handler(),
	}
}

func (as *AgentServer) serveHTTP(ctx context.Context, srv *http.Server) error {
	return httpserver.StartServer(srv, "", "", ctx)
}

func (as *AgentServer) startHTTPServer(ctx context.Context) error {
	// Engines are never directly exposed to the public network.
	return as.serveHTTP(ctx, as.newHTTPServer(":8080"))
}

func (as *AgentServer) handleProcessCheck(w http.ResponseWriter, r *http.Request) {
	progress := as.getRunProgressStatus(r.URL.Query().Get("run_id"))
	payload, err := json.Marshal(progress)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if _, err := w.Write(payload); err != nil {
		return
	}
}

func (as *AgentServer) handlePreparationCheck(w http.ResponseWriter, _ *http.Request) {
	progress := as.getPreparationProgress()
	payload, err := json.Marshal(progress)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if _, err := w.Write(payload); err != nil {
		return
	}
}
