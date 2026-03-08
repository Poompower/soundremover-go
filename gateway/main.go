package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/sony/gobreaker"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
)

type Cfg struct {
	UserURL       string
	FileURL       string
	ProcessingURL string
	LibraryURL    string
	ConsulAddr    string
	JWTSecret     []byte
}

func (c Cfg) userBase() string {
	return resolveServiceBase(c.ConsulAddr, "user-service", c.UserURL)
}
func (c Cfg) fileBase() string {
	return resolveServiceBase(c.ConsulAddr, "file-service", c.FileURL)
}
func (c Cfg) processingBase() string {
	return resolveServiceBase(c.ConsulAddr, "processing-service", c.ProcessingURL)
}
func (c Cfg) libraryBase() string {
	return resolveServiceBase(c.ConsulAddr, "library-service", c.LibraryURL)
}

type User struct {
	ID       uint   `json:"id"`
	Username string `json:"username"`
	Email    string `json:"email"`
	IsStaff  bool   `json:"is_staff"`
}

type RegisterReq struct {
	Username string `json:"username"`
	Email    string `json:"email"`
	Password string `json:"password"`
}
type LoginReq struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type CreateFileReq struct {
	Filename string `json:"filename"`
}
type CreateJobReq struct {
	FileID uint   `json:"file_id"`
	Model  string `json:"model"`
}
type YTMP3Req struct {
	URL      string `json:"url"`
	Filename string `json:"filename"`
}
type ValidateTokenReq struct {
	Token string `json:"token"`
}

var upstreamHTTPClient *http.Client
var cbMu sync.RWMutex
var breakers map[string]*gobreaker.CircuitBreaker
var discoveryMu sync.RWMutex
var discoveryCache = map[string]discoveryEntry{}
var httpRequestsTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "soundremover_http_requests_total",
		Help: "Total number of HTTP requests handled.",
	},
	[]string{"service", "method", "path", "status"},
)
var httpRequestDuration = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "soundremover_http_request_duration_seconds",
		Help:    "HTTP request duration in seconds.",
		Buckets: prometheus.DefBuckets,
	},
	[]string{"service", "method", "path"},
)
var rabbitPublishTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "soundremover_rabbit_publish_total",
		Help: "Total RabbitMQ publish attempts.",
	},
	[]string{"service", "exchange", "result"},
)
var circuitStateGauge = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: "soundremover_circuit_breaker_state",
		Help: "Circuit breaker state for upstream services (1 active, 0 inactive).",
	},
	[]string{"service", "upstream", "state"},
)
var tracingEnabledGauge = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: "soundremover_tracing_enabled",
		Help: "Whether tracing is enabled (1) or disabled (0).",
	},
	[]string{"service"},
)

type discoveryEntry struct {
	BaseURL string
	Expire  time.Time
}

func initMetrics() {
	prometheus.MustRegister(httpRequestsTotal, httpRequestDuration, rabbitPublishTotal, circuitStateGauge, tracingEnabledGauge)
}

func normalizePath(path string) string {
	parts := strings.Split(path, "/")
	for i, p := range parts {
		if p == "" {
			continue
		}
		if _, err := strconv.Atoi(p); err == nil {
			parts[i] = ":id"
		}
	}
	return strings.Join(parts, "/")
}

func setCircuitState(service, upstream string, state gobreaker.State) {
	states := []gobreaker.State{gobreaker.StateClosed, gobreaker.StateHalfOpen, gobreaker.StateOpen}
	for _, s := range states {
		val := 0.0
		if s == state {
			val = 1.0
		}
		circuitStateGauge.WithLabelValues(service, upstream, s.String()).Set(val)
	}
}

func registerWithConsul(consulAddr, serviceName, serviceID, serviceHost string, servicePort int) error {
	if strings.TrimSpace(consulAddr) == "" {
		return nil
	}
	payload := map[string]any{
		"ID":      serviceID,
		"Name":    serviceName,
		"Address": serviceHost,
		"Port":    servicePort,
		"Check": map[string]any{
			"HTTP":                           fmt.Sprintf("http://%s:%d/health", serviceHost, servicePort),
			"Method":                         "GET",
			"Interval":                       "10s",
			"Timeout":                        "2s",
			"DeregisterCriticalServiceAfter": "1m",
		},
	}
	b, _ := json.Marshal(payload)
	endpoint := fmt.Sprintf("http://%s/v1/agent/service/register", consulAddr)
	req, _ := http.NewRequest(http.MethodPut, endpoint, bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return fmt.Errorf("consul register status=%d", res.StatusCode)
	}
	return nil
}

func initConsulRegistration(consulAddr, serviceName string, servicePort int) {
	if consulAddr == "" {
		return
	}
	serviceHost := strings.TrimSpace(os.Getenv("SERVICE_HOST"))
	if serviceHost == "" {
		serviceHost = serviceName
	}
	serviceID := serviceName + "-" + url.QueryEscape(serviceHost)
	for i := 0; i < 10; i++ {
		if err := registerWithConsul(consulAddr, serviceName, serviceID, serviceHost, servicePort); err == nil {
			log.Printf("[consul][%s] registered addr=%s:%d consul=%s", serviceName, serviceHost, servicePort, consulAddr)
			return
		} else {
			log.Printf("[consul][%s] register failed: %v", serviceName, err)
			time.Sleep(2 * time.Second)
		}
	}
	log.Printf("[consul][%s] registration skipped after retries", serviceName)
}

func discoverServiceBase(consulAddr, serviceName string) (string, error) {
	if strings.TrimSpace(consulAddr) == "" {
		return "", errors.New("consul disabled")
	}

	discoveryMu.RLock()
	if c, ok := discoveryCache[serviceName]; ok && time.Now().Before(c.Expire) {
		discoveryMu.RUnlock()
		return c.BaseURL, nil
	}
	discoveryMu.RUnlock()

	endpoint := fmt.Sprintf("http://%s/v1/health/service/%s?passing=true", consulAddr, url.QueryEscape(serviceName))
	req, _ := http.NewRequest(http.MethodGet, endpoint, nil)
	client := &http.Client{Timeout: 2 * time.Second}
	res, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return "", fmt.Errorf("consul status=%d", res.StatusCode)
	}

	var entries []struct {
		Service struct {
			Address string `json:"Address"`
			Port    int    `json:"Port"`
		} `json:"Service"`
		Node struct {
			Address string `json:"Address"`
		} `json:"Node"`
	}
	if err := json.NewDecoder(res.Body).Decode(&entries); err != nil {
		return "", err
	}
	if len(entries) == 0 {
		return "", errors.New("no healthy instance")
	}
	host := strings.TrimSpace(entries[0].Service.Address)
	if host == "" {
		host = strings.TrimSpace(entries[0].Node.Address)
	}
	port := entries[0].Service.Port
	if host == "" || port == 0 {
		return "", errors.New("invalid consul service address")
	}
	base := fmt.Sprintf("http://%s:%d", host, port)

	discoveryMu.Lock()
	discoveryCache[serviceName] = discoveryEntry{
		BaseURL: base,
		Expire:  time.Now().Add(5 * time.Second),
	}
	discoveryMu.Unlock()
	return base, nil
}

func resolveServiceBase(consulAddr, serviceName, fallback string) string {
	base, err := discoverServiceBase(consulAddr, serviceName)
	if err != nil {
		if fallback != "" {
			log.Printf("[consul][gateway] discover failed service=%s err=%v fallback=%s", serviceName, err, fallback)
			return strings.TrimRight(fallback, "/")
		}
		log.Printf("[consul][gateway] discover failed service=%s err=%v", serviceName, err)
		return fallback
	}
	return strings.TrimRight(base, "/")
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

func initRabbit(service string) (*amqp.Connection, *amqp.Channel) {
	rabbitURL := os.Getenv("RABBITMQ_URL")
	if rabbitURL == "" {
		rabbitURL = "amqp://guest:guest@rabbitmq:5672/"
	}
	var conn *amqp.Connection
	var err error
	for i := 0; i < 5; i++ {
		conn, err = amqp.Dial(rabbitURL)
		if err == nil {
			break
		}
		log.Printf("[rabbitmq][%s] dial failed: %v", service, err)
		time.Sleep(1 * time.Second)
	}
	if err != nil {
		log.Printf("[rabbitmq][%s] disabled after retries", service)
		return nil, nil
	}
	ch, err := conn.Channel()
	if err != nil {
		log.Printf("[rabbitmq][%s] channel failed: %v", service, err)
		_ = conn.Close()
		return nil, nil
	}
	if err := ch.ExchangeDeclare("rest.events", "topic", true, false, false, false, nil); err != nil {
		log.Printf("[rabbitmq][%s] exchange declare failed: %v", service, err)
		_ = ch.Close()
		_ = conn.Close()
		return nil, nil
	}
	log.Printf("[rabbitmq][%s] ready exchange=rest.events", service)
	return conn, ch
}

func publishREST(ch *amqp.Channel, service string, r *http.Request, status int, elapsedMs int64) {
	if ch == nil {
		return
	}
	path := r.URL.Path
	if r.URL.RawQuery != "" {
		path += "?" + r.URL.RawQuery
	}
	payload, _ := json.Marshal(map[string]any{
		"service":    service,
		"method":     r.Method,
		"path":       path,
		"status":     status,
		"elapsed_ms": elapsedMs,
		"ts":         time.Now().UTC().Format(time.RFC3339),
	})
	key := service + ".request"
	err := ch.PublishWithContext(context.Background(), "rest.events", key, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Body:         payload,
	})
	if err != nil {
		log.Printf("[rabbitmq][%s] publish failed: %v", service, err)
		rabbitPublishTotal.WithLabelValues(service, "rest.events", "error").Inc()
		return
	}
	rabbitPublishTotal.WithLabelValues(service, "rest.events", "ok").Inc()
	log.Printf("[rabbitmq][%s] published method=%s path=%s status=%d", service, r.Method, path, status)
}

func withRESTLogging(service string, ch *amqp.Channel, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w, status: 200}
		next.ServeHTTP(sw, r)
		elapsed := time.Since(start).Milliseconds()
		path := r.URL.Path
		if r.URL.RawQuery != "" {
			path += "?" + r.URL.RawQuery
		}
		metricPath := normalizePath(r.URL.Path)
		httpRequestsTotal.WithLabelValues(service, r.Method, metricPath, strconv.Itoa(sw.status)).Inc()
		httpRequestDuration.WithLabelValues(service, r.Method, metricPath).Observe(float64(elapsed) / 1000.0)
		log.Printf("[rest][%s] method=%s path=%s status=%d elapsed_ms=%d", service, r.Method, path, sw.status, elapsed)
		publishREST(ch, service, r, sw.status, elapsed)
	})
}

func initTracer(service string) func(context.Context) error {
	endpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if endpoint == "" {
		endpoint = "jaeger:4318"
	}
	exp, err := otlptracehttp.New(context.Background(),
		otlptracehttp.WithEndpoint(endpoint),
		otlptracehttp.WithInsecure(),
	)
	if err != nil {
		log.Printf("[tracing][%s] disabled: %v", service, err)
		tracingEnabledGauge.WithLabelValues(service).Set(0)
		return func(context.Context) error { return nil }
	}
	res, _ := resource.Merge(resource.Default(), resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceNameKey.String(service),
	))
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))
	tracingEnabledGauge.WithLabelValues(service).Set(1)
	log.Printf("[tracing][%s] ready endpoint=%s", service, endpoint)
	return tp.Shutdown
}

func initBreakers() {
	settings := gobreaker.Settings{
		Interval:    30 * time.Second,
		Timeout:     20 * time.Second,
		MaxRequests: 2,
		OnStateChange: func(name string, from gobreaker.State, to gobreaker.State) {
			log.Printf("[circuit-breaker][gateway] service=%s state=%s->%s", name, from.String(), to.String())
			setCircuitState("gateway", name, to)
		},
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return counts.ConsecutiveFailures >= 5
		},
	}
	breakers = map[string]*gobreaker.CircuitBreaker{
		"user-service": gobreaker.NewCircuitBreaker(func() gobreaker.Settings {
			s := settings
			s.Name = "user-service"
			return s
		}()),
		"file-service": gobreaker.NewCircuitBreaker(func() gobreaker.Settings {
			s := settings
			s.Name = "file-service"
			return s
		}()),
		"processing-service": gobreaker.NewCircuitBreaker(func() gobreaker.Settings {
			s := settings
			s.Name = "processing-service"
			return s
		}()),
		"library-service": gobreaker.NewCircuitBreaker(func() gobreaker.Settings {
			s := settings
			s.Name = "library-service"
			return s
		}()),
	}
	setCircuitState("gateway", "user-service", gobreaker.StateClosed)
	setCircuitState("gateway", "file-service", gobreaker.StateClosed)
	setCircuitState("gateway", "processing-service", gobreaker.StateClosed)
	setCircuitState("gateway", "library-service", gobreaker.StateClosed)
}

func callWithBreaker(service string, fn func() (*http.Response, error)) (*http.Response, error) {
	cbMu.RLock()
	cb := breakers[service]
	cbMu.RUnlock()
	if cb == nil {
		return fn()
	}
	result, err := cb.Execute(func() (any, error) {
		return fn()
	})
	if err != nil {
		return nil, err
	}
	resp, _ := result.(*http.Response)
	return resp, nil
}

func upstreamStatusFromErr(err error) int {
	if errors.Is(err, gobreaker.ErrOpenState) || errors.Is(err, gobreaker.ErrTooManyRequests) {
		return http.StatusServiceUnavailable
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return http.StatusServiceUnavailable
	}
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		return http.StatusServiceUnavailable
	}
	return http.StatusServiceUnavailable
}

func writeUpstreamError(w http.ResponseWriter, service string, err error) {
	status := upstreamStatusFromErr(err)
	msg := service + " down"
	if errors.Is(err, gobreaker.ErrOpenState) || errors.Is(err, gobreaker.ErrTooManyRequests) {
		msg = service + " temporarily unavailable (circuit open)"
	}
	writeJSON(w, status, map[string]string{"error": msg})
}

func callJSON(service, method, targetURL string, body any) (int, []byte, error) {
	var buf io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		buf = bytes.NewReader(b)
	}
	req, _ := http.NewRequest(method, targetURL, buf)
	req.Header.Set("Content-Type", "application/json")
	resp, err := callWithBreaker(service, func() (*http.Response, error) {
		return upstreamHTTPClient.Do(req)
	})
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, data, nil
}

func makeToken(secret []byte, u User) (string, error) {
	claims := jwt.MapClaims{
		"sub":      u.ID,
		"username": u.Username,
		"email":    u.Email,
		"is_staff": u.IsStaff,
		"exp":      time.Now().Add(24 * time.Hour).Unix(),
		"iat":      time.Now().Unix(),
	}
	t := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return t.SignedString(secret)
}

func parseBearer(r *http.Request, secret []byte) (User, error) {
	h := r.Header.Get("Authorization")
	if !strings.HasPrefix(h, "Bearer ") {
		return User{}, errors.New("missing bearer")
	}
	raw := strings.TrimPrefix(h, "Bearer ")
	return parseRawToken(raw, secret)
}

func proxyTo(w http.ResponseWriter, r *http.Request, service, targetURL string, body io.Reader, copyAuth bool) {
	req, _ := http.NewRequest(r.Method, targetURL, body)
	if ct := r.Header.Get("Content-Type"); ct != "" {
		req.Header.Set("Content-Type", ct)
	}
	if copyAuth {
		if auth := r.Header.Get("Authorization"); auth != "" {
			req.Header.Set("Authorization", auth)
		}
	}
	resp, err := callWithBreaker(service, func() (*http.Response, error) {
		return upstreamHTTPClient.Do(req)
	})
	if err != nil {
		writeUpstreamError(w, service, err)
		return
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); ct != "" {
		w.Header().Set("Content-Type", ct)
	}
	if cd := resp.Header.Get("Content-Disposition"); cd != "" {
		w.Header().Set("Content-Disposition", cd)
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

func parseRawToken(raw string, secret []byte) (User, error) {
	token, err := jwt.Parse(raw, func(t *jwt.Token) (any, error) {
		if t.Method != jwt.SigningMethodHS256 {
			return nil, errors.New("bad signing method")
		}
		return secret, nil
	})
	if err != nil || !token.Valid {
		return User{}, errors.New("invalid token")
	}

	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		return User{}, errors.New("bad claims")
	}
	sub, _ := claims["sub"].(float64)

	u := User{
		ID:       uint(sub),
		Username: getStr(claims["username"]),
		Email:    getStr(claims["email"]),
		IsStaff:  getBool(claims["is_staff"]),
	}
	return u, nil
}

func getStr(v any) string {
	s, _ := v.(string)
	return s
}
func getBool(v any) bool {
	b, _ := v.(bool)
	return b
}

func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin == "http://localhost:3000" || origin == "http://127.0.0.1:3000" {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Vary", "Origin")
			w.Header().Set("Access-Control-Allow-Credentials", "true")
		}

		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		next.ServeHTTP(w, r)
	})
}

func main() {
	serviceName := "gateway"
	initMetrics()
	shutdownTracer := initTracer(serviceName)
	defer func() {
		_ = shutdownTracer(context.Background())
	}()
	cfg := Cfg{
		UserURL:       os.Getenv("USER_SERVICE_URL"),
		FileURL:       os.Getenv("FILE_SERVICE_URL"),
		ProcessingURL: os.Getenv("PROCESSING_SERVICE_URL"),
		LibraryURL:    os.Getenv("LIBRARY_SERVICE_URL"),
		ConsulAddr:    strings.TrimSpace(os.Getenv("CONSUL_ADDR")),
		JWTSecret:     []byte(os.Getenv("JWT_SECRET")),
	}
	if cfg.UserURL == "" || cfg.FileURL == "" || cfg.ProcessingURL == "" || cfg.LibraryURL == "" || len(cfg.JWTSecret) == 0 {
		log.Fatal("missing env: USER_SERVICE_URL / FILE_SERVICE_URL / PROCESSING_SERVICE_URL / LIBRARY_SERVICE_URL / JWT_SECRET")
	}
	upstreamHTTPClient = &http.Client{
		Timeout:   10 * time.Second,
		Transport: otelhttp.NewTransport(http.DefaultTransport),
	}
	initBreakers()
	log.Printf("[circuit-breaker][%s] ready", serviceName)
	initConsulRegistration(cfg.ConsulAddr, serviceName, 8080)
	rmqConn, rmqCh := initRabbit(serviceName)
	if rmqConn != nil {
		defer rmqConn.Close()
	}
	if rmqCh != nil {
		defer rmqCh.Close()
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, map[string]any{"ok": true})
	})

	mux.HandleFunc("/internal/validate-token", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeJSON(w, 405, map[string]string{"error": "method not allowed"})
			return
		}
		var req ValidateTokenReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, 400, map[string]any{"valid": false, "error": "invalid json"})
			return
		}
		raw := strings.TrimSpace(strings.TrimPrefix(req.Token, "Bearer "))
		if raw == "" {
			writeJSON(w, 200, map[string]any{"valid": false})
			return
		}
		u, err := parseRawToken(raw, cfg.JWTSecret)
		if err != nil {
			writeJSON(w, 200, map[string]any{"valid": false})
			return
		}
		writeJSON(w, 200, map[string]any{
			"valid":   true,
			"user_id": u.ID,
			"email":   u.Email,
		})
	})

	// Template compatibility auth endpoints
	mux.HandleFunc("/api/auth/register", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeJSON(w, 405, map[string]string{"error": "method not allowed"})
			return
		}
		var req RegisterReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, 400, map[string]string{"error": "invalid json"})
			return
		}
		status, data, err := callJSON("user-service", "POST", cfg.userBase()+"/register", req)
		if err != nil {
			writeUpstreamError(w, "user-service", err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write(data)
	})

	mux.HandleFunc("/api/auth/login", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeJSON(w, 405, map[string]string{"error": "method not allowed"})
			return
		}
		var req struct {
			Email    string `json:"email"`
			Username string `json:"username"`
			Password string `json:"password"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, 400, map[string]string{"error": "invalid json"})
			return
		}
		loginID := strings.TrimSpace(req.Username)
		if loginID == "" {
			loginID = strings.TrimSpace(req.Email)
		}
		status, data, err := callJSON("user-service", "POST", cfg.userBase()+"/login", LoginReq{
			Username: loginID,
			Password: req.Password,
		})
		if err != nil {
			writeUpstreamError(w, "user-service", err)
			return
		}
		if status != 200 {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			_, _ = w.Write(data)
			return
		}

		var parsed map[string]any
		_ = json.Unmarshal(data, &parsed)
		uBytes, _ := json.Marshal(parsed["user"])
		var u User
		_ = json.Unmarshal(uBytes, &u)
		tok, err := makeToken(cfg.JWTSecret, u)
		if err != nil {
			writeJSON(w, 500, map[string]string{"error": "token error"})
			return
		}
		writeJSON(w, 200, map[string]any{"token": tok, "user": u})
	})

	mux.HandleFunc("/api/users/me", func(w http.ResponseWriter, r *http.Request) {
		u, err := parseBearer(r, cfg.JWTSecret)
		if err != nil {
			writeJSON(w, 401, map[string]string{"error": "unauthorized"})
			return
		}
		if r.Method != http.MethodGet && r.Method != http.MethodPut {
			writeJSON(w, 405, map[string]string{"error": "method not allowed"})
			return
		}
		target := cfg.userBase() + "/users/" + strconv.FormatUint(uint64(u.ID), 10)
		var body io.Reader
		if r.Method == http.MethodPut {
			b, _ := io.ReadAll(r.Body)
			body = bytes.NewReader(b)
		}
		status, data, err := func() (int, []byte, error) {
			req, _ := http.NewRequest(r.Method, target, body)
			req.Header.Set("Content-Type", "application/json")
			resp, err := callWithBreaker("user-service", func() (*http.Response, error) {
				return upstreamHTTPClient.Do(req)
			})
			if err != nil {
				return 0, nil, err
			}
			defer resp.Body.Close()
			d, _ := io.ReadAll(resp.Body)
			return resp.StatusCode, d, nil
		}()
		if err != nil {
			writeUpstreamError(w, "user-service", err)
			return
		}

		var wrapped map[string]any
		_ = json.Unmarshal(data, &wrapped)
		if user, ok := wrapped["user"]; ok {
			writeJSON(w, status, user)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write(data)
	})

	// Library template endpoints
	mux.HandleFunc("/api/albums", func(w http.ResponseWriter, r *http.Request) {
		target := cfg.libraryBase() + "/albums"
		if r.URL.RawQuery != "" {
			target += "?" + r.URL.RawQuery
		}
		var body io.Reader
		if r.Body != nil && (r.Method == http.MethodPost || r.Method == http.MethodPut || r.Method == http.MethodPatch) {
			b, _ := io.ReadAll(r.Body)
			body = bytes.NewReader(b)
		}
		proxyTo(w, r, "library-service", target, body, true)
	})

	mux.HandleFunc("/api/albums/", func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/api")
		target := cfg.libraryBase() + path
		if r.URL.RawQuery != "" {
			target += "?" + r.URL.RawQuery
		}
		var body io.Reader
		if r.Body != nil && (r.Method == http.MethodPost || r.Method == http.MethodPut || r.Method == http.MethodPatch) {
			b, _ := io.ReadAll(r.Body)
			body = bytes.NewReader(b)
		}
		proxyTo(w, r, "library-service", target, body, true)
	})

	mux.HandleFunc("/api/my-albums", func(w http.ResponseWriter, r *http.Request) {
		target := cfg.libraryBase() + "/my-albums"
		proxyTo(w, r, "library-service", target, nil, true)
	})

	mux.HandleFunc("/api/playlists", func(w http.ResponseWriter, r *http.Request) {
		target := cfg.libraryBase() + "/playlists"
		var body io.Reader
		if r.Body != nil && (r.Method == http.MethodPost || r.Method == http.MethodPut || r.Method == http.MethodPatch) {
			b, _ := io.ReadAll(r.Body)
			body = bytes.NewReader(b)
		}
		proxyTo(w, r, "library-service", target, body, true)
	})

	mux.HandleFunc("/api/playlists/", func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/api")
		target := cfg.libraryBase() + path
		if r.URL.RawQuery != "" {
			target += "?" + r.URL.RawQuery
		}
		var body io.Reader
		if r.Body != nil && (r.Method == http.MethodPost || r.Method == http.MethodPut || r.Method == http.MethodPatch) {
			b, _ := io.ReadAll(r.Body)
			body = bytes.NewReader(b)
		}
		proxyTo(w, r, "library-service", target, body, true)
	})

	// POST /api/register -> user-service /register
	mux.HandleFunc("/api/register", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeJSON(w, 405, map[string]string{"error": "method not allowed"})
			return
		}
		var req RegisterReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, 400, map[string]string{"error": "invalid json"})
			return
		}
		status, data, err := callJSON("user-service", "POST", cfg.userBase()+"/register", req)
		if err != nil {
			writeUpstreamError(w, "user-service", err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write(data)
	})

	// POST /api/login -> user-service /login -> JWT
	mux.HandleFunc("/api/login", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeJSON(w, 405, map[string]string{"error": "method not allowed"})
			return
		}
		var req LoginReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, 400, map[string]string{"error": "invalid json"})
			return
		}
		status, data, err := callJSON("user-service", "POST", cfg.userBase()+"/login", req)
		if err != nil {
			writeUpstreamError(w, "user-service", err)
			return
		}
		if status != 200 {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			_, _ = w.Write(data)
			return
		}

		var parsed map[string]any
		_ = json.Unmarshal(data, &parsed)
		uBytes, _ := json.Marshal(parsed["user"])
		var u User
		_ = json.Unmarshal(uBytes, &u)

		tok, err := makeToken(cfg.JWTSecret, u)
		if err != nil {
			writeJSON(w, 500, map[string]string{"error": "token error"})
			return
		}

		writeJSON(w, 200, map[string]any{
			"token": tok,
			"user":  u,
		})
	})

	// POST /api/files (auth) -> file-service /files
	mux.HandleFunc("/api/files", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeJSON(w, 405, map[string]string{"error": "method not allowed"})
			return
		}
		u, err := parseBearer(r, cfg.JWTSecret)
		if err != nil {
			writeJSON(w, 401, map[string]string{"error": "unauthorized"})
			return
		}

		var req CreateFileReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, 400, map[string]string{"error": "invalid json"})
			return
		}
		if strings.TrimSpace(req.Filename) == "" {
			writeJSON(w, 400, map[string]string{"error": "missing filename"})
			return
		}

		payload := map[string]any{"user_id": u.ID, "filename": req.Filename}
		status, data, err := callJSON("file-service", "POST", cfg.fileBase()+"/files", payload)
		if err != nil {
			writeUpstreamError(w, "file-service", err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write(data)
	})

	// POST /api/jobs (auth) -> processing-service /jobs -> push redis
	// GET /api/jobs (auth) -> processing-service /jobs?user_id=...
	mux.HandleFunc("/api/jobs", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost && r.Method != http.MethodGet {
			writeJSON(w, 405, map[string]string{"error": "method not allowed"})
			return
		}
		u, err := parseBearer(r, cfg.JWTSecret)
		if err != nil {
			writeJSON(w, 401, map[string]string{"error": "unauthorized"})
			return
		}
		if r.Method == http.MethodGet {
			q := url.Values{}
			q.Set("user_id", strconv.FormatUint(uint64(u.ID), 10))
			if status := strings.TrimSpace(r.URL.Query().Get("status")); status != "" {
				q.Set("status", status)
			}
			if limit := strings.TrimSpace(r.URL.Query().Get("limit")); limit != "" {
				q.Set("limit", limit)
			}
			target := cfg.processingBase() + "/jobs?" + q.Encode()
			req, _ := http.NewRequest(http.MethodGet, target, nil)
			resp, err := callWithBreaker("processing-service", func() (*http.Response, error) {
				return upstreamHTTPClient.Do(req)
			})
			if err != nil {
				writeUpstreamError(w, "processing-service", err)
				return
			}
			defer resp.Body.Close()
			if ct := resp.Header.Get("Content-Type"); ct != "" {
				w.Header().Set("Content-Type", ct)
			}
			w.WriteHeader(resp.StatusCode)
			_, _ = io.Copy(w, resp.Body)
			return
		}

		var req CreateJobReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, 400, map[string]string{"error": "invalid json"})
			return
		}
		if req.FileID == 0 {
			writeJSON(w, 400, map[string]string{"error": "missing file_id"})
			return
		}
		if req.Model == "" {
			req.Model = "2stems"
		}

		payload := map[string]any{"user_id": u.ID, "file_id": req.FileID, "model": req.Model}
		status, data, err := callJSON("processing-service", "POST", cfg.processingBase()+"/jobs", payload)
		if err != nil {
			writeUpstreamError(w, "processing-service", err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write(data)
	})

	// POST /api/ytmp3 (auth) -> processing-service /ytmp3
	mux.HandleFunc("/api/ytmp3", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeJSON(w, 405, map[string]string{"error": "method not allowed"})
			return
		}
		u, err := parseBearer(r, cfg.JWTSecret)
		if err != nil {
			writeJSON(w, 401, map[string]string{"error": "unauthorized"})
			return
		}

		var req YTMP3Req
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, 400, map[string]string{"error": "invalid json"})
			return
		}
		if strings.TrimSpace(req.URL) == "" {
			writeJSON(w, 400, map[string]string{"error": "missing url"})
			return
		}

		payload := map[string]any{
			"url":      req.URL,
			"filename": req.Filename,
			"user_id":  u.ID,
		}
		status, data, err := callJSON("processing-service", "POST", cfg.processingBase()+"/ytmp3", payload)
		if err != nil {
			writeUpstreamError(w, "processing-service", err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write(data)
	})

	// POST /api/input/upload (auth) -> processing-service /input/upload
	mux.HandleFunc("/api/input/upload", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeJSON(w, 405, map[string]string{"error": "method not allowed"})
			return
		}
		_, err := parseBearer(r, cfg.JWTSecret)
		if err != nil {
			writeJSON(w, 401, map[string]string{"error": "unauthorized"})
			return
		}
		body, _ := io.ReadAll(r.Body)
		target := cfg.processingBase() + "/input/upload"
		proxyTo(w, r, "processing-service", target, bytes.NewReader(body), false)
	})

	// GET /api/jobs/{id}/download?stem=...
	// GET /api/jobs/{id}/stream?stem=...
	mux.HandleFunc("/api/jobs/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeJSON(w, 405, map[string]string{"error": "method not allowed"})
			return
		}
		path := strings.TrimPrefix(r.URL.Path, "/api/jobs/")
		parts := strings.Split(strings.Trim(path, "/"), "/")
		if parts[0] == "" {
			writeJSON(w, 404, map[string]string{"error": "not found"})
			return
		}

		authorized := false
		if _, err := parseBearer(r, cfg.JWTSecret); err == nil {
			authorized = true
		}
		if !authorized {
			if rawToken := strings.TrimSpace(r.URL.Query().Get("token")); rawToken != "" {
				reqClone := r.Clone(r.Context())
				reqClone.Header = r.Header.Clone()
				reqClone.Header.Set("Authorization", "Bearer "+rawToken)
				if _, err := parseBearer(reqClone, cfg.JWTSecret); err == nil {
					authorized = true
				}
			}
		}
		if !authorized {
			writeJSON(w, 401, map[string]string{"error": "unauthorized"})
			return
		}

		upstream := cfg.processingBase() + "/jobs/" + parts[0]
		if len(parts) == 2 && parts[1] == "download" {
			stem := r.URL.Query().Get("stem")
			upstream = upstream + "/download?stem=" + url.QueryEscape(stem)
		} else if len(parts) == 2 && parts[1] == "stream" {
			stem := r.URL.Query().Get("stem")
			upstream = upstream + "/stream?stem=" + url.QueryEscape(stem)
		} else if len(parts) != 1 {
			writeJSON(w, 404, map[string]string{"error": "not found"})
			return
		}

		req, _ := http.NewRequest(http.MethodGet, upstream, nil)
		resp, err := callWithBreaker("processing-service", func() (*http.Response, error) {
			return upstreamHTTPClient.Do(req)
		})
		if err != nil {
			writeUpstreamError(w, "processing-service", err)
			return
		}
		defer resp.Body.Close()

		if ct := resp.Header.Get("Content-Type"); ct != "" {
			w.Header().Set("Content-Type", ct)
		}
		if cd := resp.Header.Get("Content-Disposition"); cd != "" {
			w.Header().Set("Content-Disposition", cd)
		}
		w.WriteHeader(resp.StatusCode)
		_, _ = io.Copy(w, resp.Body)
	})

	log.Println("gateway on :8080")
	root := withRESTLogging(serviceName, rmqCh, withCORS(mux))
	log.Fatal(http.ListenAndServe(":8080", otelhttp.NewHandler(root, "gateway-http")))
}
