package metrics

import (
	"net/http"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	once sync.Once

	httpRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "uge_http_requests_total",
			Help: "Total HTTP requests served.",
		},
		[]string{"path", "method", "code"},
	)
	grpcRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "uge_grpc_requests_total",
			Help: "Total gRPC requests served.",
		},
		[]string{"method", "code"},
	)
	wsConnectionsOpened = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "uge_websocket_connections_opened_total",
			Help: "Total WebSocket connections opened.",
		},
	)
	wsConnectionsClosed = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "uge_websocket_connections_closed_total",
			Help: "Total WebSocket connections closed.",
		},
	)
	wsSessionsGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "uge_websocket_sessions",
			Help: "Current active WebSocket sessions.",
		},
	)
	dbPoolAcquiredConns = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "uge_db_pool_acquired_conns",
			Help: "Database connections currently acquired from the pool.",
		},
	)
	dbPoolIdleConns = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "uge_db_pool_idle_conns",
			Help: "Database connections currently idle in the pool.",
		},
	)
	dbPoolMaxConns = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "uge_db_pool_max_conns",
			Help: "Maximum database connections allowed in the pool.",
		},
	)
)

func initRegistry() {
	once.Do(func() {
		prometheus.MustRegister(
			httpRequestsTotal,
			grpcRequestsTotal,
			wsConnectionsOpened,
			wsConnectionsClosed,
			wsSessionsGauge,
			dbPoolAcquiredConns,
			dbPoolIdleConns,
			dbPoolMaxConns,
		)
	})
}

// Handler returns the Prometheus scrape handler for GET /metrics.
func Handler() http.Handler {
	initRegistry()
	return promhttp.Handler()
}

// ObserveHTTP increments HTTP request counters.
func ObserveHTTP(method, path, code string) {
	initRegistry()
	httpRequestsTotal.WithLabelValues(path, method, code).Inc()
}

// ObserveGRPC increments gRPC request counters.
func ObserveGRPC(method, code string) {
	initRegistry()
	grpcRequestsTotal.WithLabelValues(method, code).Inc()
}

// IncWSOpened records a new WebSocket connection.
func IncWSOpened() {
	initRegistry()
	wsConnectionsOpened.Inc()
}

// IncWSClosed records a closed WebSocket connection.
func IncWSClosed() {
	initRegistry()
	wsConnectionsClosed.Inc()
}

// SetWSSessions updates the active session gauge.
func SetWSSessions(n float64) {
	initRegistry()
	wsSessionsGauge.Set(n)
}

// UpdateDBPool publishes pgxpool statistics.
func UpdateDBPool(pool *pgxpool.Pool) {
	if pool == nil {
		return
	}
	initRegistry()
	stat := pool.Stat()
	dbPoolAcquiredConns.Set(float64(stat.AcquiredConns()))
	dbPoolIdleConns.Set(float64(stat.IdleConns()))
	dbPoolMaxConns.Set(float64(stat.MaxConns()))
}
