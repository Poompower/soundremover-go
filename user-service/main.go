package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/glebarez/sqlite"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
	"golang.org/x/crypto/bcrypt"
	"gorm.io/gorm"
)

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
var tracingEnabledGauge = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: "soundremover_tracing_enabled",
		Help: "Whether tracing is enabled (1) or disabled (0).",
	},
	[]string{"service"},
)

func initMetrics() {
	prometheus.MustRegister(httpRequestsTotal, httpRequestDuration, rabbitPublishTotal, tracingEnabledGauge)
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

type User struct {
	ID           uint   `gorm:"primaryKey" json:"id"`
	Username     string `gorm:"uniqueIndex;not null" json:"username"`
	Email        string `gorm:"uniqueIndex;not null" json:"email"`
	DisplayName  string `gorm:"not null;default:''" json:"display_name"`
	PasswordHash string `gorm:"not null" json:"-"`
	IsStaff      bool   `gorm:"not null;default:false" json:"is_staff"`
}

type RegisterReq struct {
	Username  string `json:"username"`
	Email     string `json:"email"`
	Password  string `json:"password"`
	FirstName string `json:"first_name"`
	LastName  string `json:"last_name"`
}
type LoginReq struct {
	Username string `json:"username"`
	Password string `json:"password"`
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

func initConsulRegistration(serviceName string, servicePort int) {
	consulAddr := strings.TrimSpace(os.Getenv("CONSUL_ADDR"))
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

func main() {
	serviceName := "user-service"
	initMetrics()
	shutdownTracer := initTracer(serviceName)
	defer func() {
		_ = shutdownTracer(context.Background())
	}()
	dbPath := os.Getenv("DB_PATH")
	if dbPath == "" {
		dbPath = "./user.db"
	}

	db, err := gorm.Open(sqlite.Open(dbPath), &gorm.Config{})
	if err != nil {
		log.Fatal(err)
	}
	if err := db.AutoMigrate(&User{}); err != nil {
		log.Fatal(err)
	}

	rmqConn, rmqCh := initRabbit(serviceName)
	if rmqConn != nil {
		defer rmqConn.Close()
	}
	if rmqCh != nil {
		defer rmqCh.Close()
	}
	initConsulRegistration(serviceName, 8081)

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, map[string]any{"ok": true})
	})
	http.Handle("/metrics", promhttp.Handler())

	http.HandleFunc("/register", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeJSON(w, 405, map[string]string{"error": "method not allowed"})
			return
		}
		var req RegisterReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, 400, map[string]string{"error": "invalid json"})
			return
		}
		req.Username = strings.TrimSpace(req.Username)
		req.Email = strings.TrimSpace(req.Email)
		if req.Username == "" || req.Email == "" || req.Password == "" {
			writeJSON(w, 400, map[string]string{"error": "missing fields"})
			return
		}

		var count int64
		db.Model(&User{}).Where("lower(username)=lower(?)", req.Username).Count(&count)
		if count > 0 {
			writeJSON(w, 400, map[string]string{"error": "username exists"})
			return
		}
		db.Model(&User{}).Where("lower(email)=lower(?)", req.Email).Count(&count)
		if count > 0 {
			writeJSON(w, 400, map[string]string{"error": "email exists"})
			return
		}

		hash, _ := bcrypt.GenerateFromPassword([]byte(req.Password), bcrypt.DefaultCost)
		u := User{
			Username:     req.Username,
			Email:        req.Email,
			DisplayName:  req.Username,
			PasswordHash: string(hash),
			IsStaff:      false,
		}
		if err := db.Create(&u).Error; err != nil {
			writeJSON(w, 500, map[string]string{"error": "db error"})
			return
		}
		writeJSON(w, 201, map[string]any{"user": u})
	})

	http.HandleFunc("/login", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeJSON(w, 405, map[string]string{"error": "method not allowed"})
			return
		}
		var req LoginReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, 400, map[string]string{"error": "invalid json"})
			return
		}
		req.Username = strings.TrimSpace(req.Username)
		if req.Username == "" || req.Password == "" {
			writeJSON(w, 400, map[string]string{"error": "missing fields"})
			return
		}

		var u User
		if err := db.Where("lower(username)=lower(?) OR lower(email)=lower(?)", req.Username, req.Username).First(&u).Error; err != nil {
			writeJSON(w, 401, map[string]string{"error": "invalid credentials"})
			return
		}
		if bcrypt.CompareHashAndPassword([]byte(u.PasswordHash), []byte(req.Password)) != nil {
			writeJSON(w, 401, map[string]string{"error": "invalid credentials"})
			return
		}

		writeJSON(w, 200, map[string]any{"user": u})
	})

	http.HandleFunc("/users/", func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/users/")
		id64, err := strconv.ParseUint(strings.Trim(path, "/"), 10, 64)
		if err != nil || id64 == 0 {
			writeJSON(w, 400, map[string]string{"error": "invalid user id"})
			return
		}
		id := uint(id64)

		switch r.Method {
		case http.MethodGet:
			var u User
			if err := db.First(&u, id).Error; err != nil {
				writeJSON(w, 404, map[string]string{"error": "user not found"})
				return
			}
			writeJSON(w, 200, map[string]any{"user": u})
		case http.MethodPut:
			var req struct {
				DisplayName string `json:"display_name"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				writeJSON(w, 400, map[string]string{"error": "invalid json"})
				return
			}
			req.DisplayName = strings.TrimSpace(req.DisplayName)
			if req.DisplayName == "" {
				var u User
				if err := db.First(&u, id).Error; err != nil {
					writeJSON(w, 404, map[string]string{"error": "user not found"})
					return
				}
				req.DisplayName = u.Username
			}
			if err := db.Model(&User{}).Where("id = ?", id).Update("display_name", req.DisplayName).Error; err != nil {
				writeJSON(w, 500, map[string]string{"error": "db error"})
				return
			}
			var u User
			_ = db.First(&u, id).Error
			writeJSON(w, 200, map[string]any{"user": u})
		default:
			writeJSON(w, 405, map[string]string{"error": "method not allowed"})
		}
	})

	log.Println("user-service on :8081")
	root := withRESTLogging(serviceName, rmqCh, http.DefaultServeMux)
	log.Fatal(http.ListenAndServe(":8081", otelhttp.NewHandler(root, "user-http")))
}
