package metrics

import (
	"log"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (

	OAuthTokenCheckCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "token_check_total",
			Help: "Total oauth token checks run",
		},
		[]string{"result"},
	)

	PermissionCheckCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "permission_check_total",
			Help: "Total permission checks run",
		},
		[]string{"result"},
	)

	IdentityCheckCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "identity_check_total",
			Help: "Total identity checks run",
		},
		[]string{"result"},
	)
)

func Init() {

    // Metrics from all
    prometheus.MustRegister(OAuthTokenCheckCounter)
    prometheus.MustRegister(IdentityCheckCounter)
    prometheus.MustRegister(PermissionCheckCounter)


	// Health and metrics endpoints
	http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	http.Handle("/metrics", promhttp.Handler())

	go func() {
		log.Println("📡 Starting metrics HTTP server on :2112")
		if err := http.ListenAndServe("0.0.0.0:2112", nil); err != nil {
			log.Fatalf("❌ Metrics server failed: %v", err)
		}
	}()
}
