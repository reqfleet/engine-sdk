// Derived from reqfleet/shibuya; modified for engine-sdk.
package agent

import (
	"encoding/json"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func metricsRegistry() *prometheus.Registry {
	registry := prometheus.NewRegistry()
	registry.MustRegister(labelLatencyHistogram)
	registry.MustRegister(labelLLMRespLatencyHistogram)
	registry.MustRegister(statusCounter)
	registry.MustRegister(threadsGauge)
	registry.MustRegister(cpuGauge)
	registry.MustRegister(memGauge)
	registry.MustRegister(egressCounter)
	return registry
}

func (as *AgentServer) metricsHandler() http.Handler {
	if err := as.validateMetricsOptions(); err != nil {
		return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
		})
	}
	if as.options.MetricsURL == "" {
		return promhttp.HandlerFor(metricsRegistry(), promhttp.HandlerOpts{})
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
	mux := http.NewServeMux()
	mux.HandleFunc("GET /progress", as.handleProcessCheck)
	mux.HandleFunc("GET /prepare-progress", as.handlePreparationCheck)
	mux.Handle("GET /metrics", as.metricsHandler())
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		if as.isSubscribed() {
			w.WriteHeader(http.StatusOK)
			return
		}
		http.Error(w, "not subscribed to coordinator", http.StatusServiceUnavailable)
	})
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" {
			mux.ServeHTTP(w, r)
			return
		}
		bearer := r.Header.Get("Authorization")
		if bearer == "" {
			http.Error(w, "bearer header is empty", http.StatusBadRequest)
			return
		}
		token := strings.Split(bearer, " ")
		if len(token) != 2 || token[0] != "Bearer" {
			http.Error(w, "token is invalid", http.StatusBadRequest)
			return
		}
		if token[1] != as.options.EngineMeta.APIKey {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		mux.ServeHTTP(w, r)
	})
}

func (as *AgentServer) startHTTPServer() error {
	// Engines are never directly exposed to the public network.
	return http.ListenAndServe(":8080", as.Handler())
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
