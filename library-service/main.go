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
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/glebarez/sqlite"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/sony/gobreaker"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
	"gorm.io/gorm"
)

type Album struct {
	ID          uint    `gorm:"primaryKey" json:"id"`
	OwnerID     uint    `json:"owner_id"`
	Title       string  `json:"title"`
	Description string  `json:"description"`
	CoverURL    string  `json:"cover_url"`
	Genre       string  `json:"genre"`
	Year        int     `json:"year"`
	Visibility  string  `gorm:"default:private" json:"visibility"`
	Tracks      []Track `gorm:"foreignKey:AlbumID" json:"tracks,omitempty"`
}

type Track struct {
	ID       uint   `gorm:"primaryKey" json:"id"`
	AlbumID  uint   `json:"album_id"`
	Title    string `json:"title"`
	Artists  string `json:"artists"`
	Duration int    `json:"duration"`
	AudioURL string `json:"audio_url"`
	TrackNum int    `json:"track_num"`
	Plays    int64  `gorm:"default:0" json:"plays"`
}

type Playlist struct {
	ID        uint           `gorm:"primaryKey" json:"id"`
	OwnerID   uint           `gorm:"index;not null" json:"owner_id"`
	Name      string         `gorm:"not null" json:"name"`
	Items     []PlaylistItem `gorm:"foreignKey:PlaylistID" json:"items,omitempty"`
	CreatedAt time.Time      `json:"created_at"`
	UpdatedAt time.Time      `json:"updated_at"`
}

type PlaylistItem struct {
	ID         uint      `gorm:"primaryKey" json:"id"`
	PlaylistID uint      `gorm:"index;not null;uniqueIndex:idx_playlist_job" json:"playlist_id"`
	JobID      uint      `gorm:"not null;uniqueIndex:idx_playlist_job" json:"job_id"`
	CreatedAt  time.Time `json:"created_at"`
}

var db *gorm.DB
var amqpCh *amqp.Channel
var amqpMu sync.Mutex
var userServiceURL string
var userHTTPClient *http.Client
var userBreaker *gobreaker.CircuitBreaker
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

func initMetrics() {
	prometheus.MustRegister(httpRequestsTotal, httpRequestDuration, rabbitPublishTotal, circuitStateGauge, tracingEnabledGauge)
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
	serviceName := "library-service"
	initMetrics()
	shutdownTracer := initTracer(serviceName)
	defer func() {
		_ = shutdownTracer(context.Background())
	}()
	userServiceURL = getenv("USER_SERVICE_URL", "http://user-service:8081")
	userHTTPClient = &http.Client{
		Timeout:   10 * time.Second,
		Transport: otelhttp.NewTransport(http.DefaultTransport),
	}
	userBreaker = gobreaker.NewCircuitBreaker(gobreaker.Settings{
		Name:        "user-service",
		Interval:    30 * time.Second,
		Timeout:     20 * time.Second,
		MaxRequests: 2,
		OnStateChange: func(name string, from gobreaker.State, to gobreaker.State) {
			log.Printf("[circuit-breaker][library-service] service=%s state=%s->%s", name, from.String(), to.String())
			setCircuitState("library-service", name, to)
		},
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return counts.ConsecutiveFailures >= 5
		},
	})
	setCircuitState("library-service", "user-service", gobreaker.StateClosed)
	log.Printf("[circuit-breaker][%s] target=user-service ready", serviceName)

	os.MkdirAll("/data", 0o755)
	var err error
	db, err = gorm.Open(sqlite.Open("/data/library.db"), &gorm.Config{})
	if err != nil {
		log.Fatal(err)
	}
	_ = db.AutoMigrate(&Album{}, &Track{}, &Playlist{}, &PlaylistItem{})

	// RabbitMQ is optional in this merged template.
	var conn *amqp.Connection
	for i := 0; i < 3; i++ {
		conn, err = amqp.Dial(getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/"))
		if err == nil {
			break
		}
		log.Printf("[rabbitmq][library-service] dial failed: %v", err)
		time.Sleep(1 * time.Second)
	}
	if err == nil {
		defer conn.Close()
		amqpCh, _ = conn.Channel()
		if amqpCh != nil {
			_ = amqpCh.ExchangeDeclare("library.events", "topic", true, false, false, false, nil)
			_ = amqpCh.ExchangeDeclare("rest.events", "topic", true, false, false, false, nil)
			log.Printf("[rabbitmq][library-service] ready exchange=library.events,rest.events")
		}
	} else {
		log.Printf("[rabbitmq][library-service] disabled after retries")
	}

	r := gin.Default()
	r.Use(cors(), restLogMiddleware(serviceName), otelgin.Middleware(serviceName))

	r.GET("/health", func(c *gin.Context) {
		c.JSON(200, gin.H{"status": "ok", "service": "library-service"})
	})
	r.GET("/metrics", gin.WrapH(promhttp.Handler()))
	r.GET("/albums", listPublicAlbums)
	r.GET("/albums/:id", softAuth(), getAlbum)
	r.POST("/albums/:id/tracks/:track_id/play", playTrack)

	p := r.Group("/")
	p.Use(requireAuth())
	{
		p.GET("/my-albums", listMyAlbums)
		p.GET("/playlists", listPlaylists)
		p.POST("/playlists", createPlaylist)
		p.DELETE("/playlists/:id", deletePlaylist)
		p.POST("/playlists/:id/jobs", addJobToPlaylist)
		p.DELETE("/playlists/:id/jobs/:job_id", removeJobFromPlaylist)
		p.POST("/albums", createAlbum)
		p.PUT("/albums/:id", updateAlbum)
		p.PATCH("/albums/:id/visibility", setVisibility)
		p.DELETE("/albums/:id", deleteAlbum)
		p.POST("/albums/:id/tracks", addTrack)
		p.DELETE("/albums/:id/tracks/:track_id", deleteTrack)
	}

	port := getenv("PORT", "8084")
	portNum, err := strconv.Atoi(port)
	if err == nil && portNum > 0 {
		initConsulRegistration(serviceName, portNum)
	}
	log.Println("library-service :" + port)
	_ = r.Run(":" + port)
}

func listPublicAlbums(c *gin.Context) {
	var albums []Album
	db.Where("visibility = ?", "public").Preload("Tracks").Find(&albums)
	c.JSON(200, albums)
}

func listMyAlbums(c *gin.Context) {
	var albums []Album
	db.Where("owner_id = ?", c.GetUint("user_id")).Preload("Tracks").Find(&albums)
	c.JSON(200, albums)
}

func listPlaylists(c *gin.Context) {
	var playlists []Playlist
	db.Where("owner_id = ?", c.GetUint("user_id")).Preload("Items").Order("created_at desc").Find(&playlists)

	type playlistResp struct {
		ID        uint      `json:"id"`
		Name      string    `json:"name"`
		JobIDs    []uint    `json:"job_ids"`
		CreatedAt time.Time `json:"created_at"`
		UpdatedAt time.Time `json:"updated_at"`
	}
	resp := make([]playlistResp, 0, len(playlists))
	for _, p := range playlists {
		jobIDs := make([]uint, 0, len(p.Items))
		for _, item := range p.Items {
			jobIDs = append(jobIDs, item.JobID)
		}
		resp = append(resp, playlistResp{
			ID:        p.ID,
			Name:      p.Name,
			JobIDs:    jobIDs,
			CreatedAt: p.CreatedAt,
			UpdatedAt: p.UpdatedAt,
		})
	}
	c.JSON(200, resp)
}

func createPlaylist(c *gin.Context) {
	var req struct {
		Name string `json:"name"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(400, gin.H{"error": "invalid json"})
		return
	}
	name := strings.TrimSpace(req.Name)
	if name == "" {
		c.JSON(400, gin.H{"error": "name is required"})
		return
	}
	playlist := Playlist{
		OwnerID: c.GetUint("user_id"),
		Name:    name,
	}
	if err := db.Create(&playlist).Error; err != nil {
		c.JSON(500, gin.H{"error": "db error"})
		return
	}
	c.JSON(201, gin.H{
		"id":         playlist.ID,
		"name":       playlist.Name,
		"job_ids":    []uint{},
		"created_at": playlist.CreatedAt,
		"updated_at": playlist.UpdatedAt,
	})
}

func deletePlaylist(c *gin.Context) {
	id, _ := strconv.Atoi(c.Param("id"))
	var playlist Playlist
	if err := db.First(&playlist, id).Error; err != nil {
		c.JSON(404, gin.H{"error": "playlist not found"})
		return
	}
	if playlist.OwnerID != c.GetUint("user_id") {
		c.JSON(403, gin.H{"error": "forbidden"})
		return
	}
	db.Where("playlist_id = ?", playlist.ID).Delete(&PlaylistItem{})
	db.Delete(&playlist)
	c.Status(204)
}

func addJobToPlaylist(c *gin.Context) {
	id, _ := strconv.Atoi(c.Param("id"))
	var playlist Playlist
	if err := db.First(&playlist, id).Error; err != nil {
		c.JSON(404, gin.H{"error": "playlist not found"})
		return
	}
	if playlist.OwnerID != c.GetUint("user_id") {
		c.JSON(403, gin.H{"error": "forbidden"})
		return
	}
	var req struct {
		JobID uint `json:"job_id"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(400, gin.H{"error": "invalid json"})
		return
	}
	if req.JobID == 0 {
		c.JSON(400, gin.H{"error": "job_id is required"})
		return
	}
	item := PlaylistItem{
		PlaylistID: playlist.ID,
		JobID:      req.JobID,
	}
	if err := db.Where("playlist_id = ? AND job_id = ?", playlist.ID, req.JobID).FirstOrCreate(&item).Error; err != nil {
		c.JSON(500, gin.H{"error": "db error"})
		return
	}
	c.JSON(201, gin.H{"ok": true})
}

func removeJobFromPlaylist(c *gin.Context) {
	id, _ := strconv.Atoi(c.Param("id"))
	jobID, _ := strconv.Atoi(c.Param("job_id"))
	if jobID <= 0 {
		c.JSON(400, gin.H{"error": "invalid job_id"})
		return
	}
	var playlist Playlist
	if err := db.First(&playlist, id).Error; err != nil {
		c.JSON(404, gin.H{"error": "playlist not found"})
		return
	}
	if playlist.OwnerID != c.GetUint("user_id") {
		c.JSON(403, gin.H{"error": "forbidden"})
		return
	}
	db.Where("playlist_id = ? AND job_id = ?", playlist.ID, jobID).Delete(&PlaylistItem{})
	c.Status(204)
}

func getAlbum(c *gin.Context) {
	id, _ := strconv.Atoi(c.Param("id"))
	var album Album
	if err := db.Preload("Tracks").First(&album, id).Error; err != nil {
		c.JSON(404, gin.H{"error": "album not found"})
		return
	}
	uid := c.GetUint("user_id")
	if album.Visibility == "private" && album.OwnerID != uid {
		c.JSON(403, gin.H{"error": "album is private"})
		return
	}
	c.JSON(200, album)
}

func createAlbum(c *gin.Context) {
	var req struct {
		Title       string `json:"title"`
		Description string `json:"description"`
		CoverURL    string `json:"cover_url"`
		Genre       string `json:"genre"`
		Year        int    `json:"year"`
	}
	_ = c.ShouldBindJSON(&req)
	album := Album{
		OwnerID: c.GetUint("user_id"), Title: req.Title,
		Description: req.Description, CoverURL: req.CoverURL,
		Genre: req.Genre, Year: req.Year, Visibility: "private",
	}
	db.Create(&album)
	publish("album.created", map[string]any{"album_id": album.ID, "owner_id": album.OwnerID})
	c.JSON(201, album)
}

func updateAlbum(c *gin.Context) {
	id, _ := strconv.Atoi(c.Param("id"))
	var album Album
	if err := db.First(&album, id).Error; err != nil {
		c.JSON(404, gin.H{"error": "not found"})
		return
	}
	if album.OwnerID != c.GetUint("user_id") {
		c.JSON(403, gin.H{"error": "forbidden"})
		return
	}
	var req struct {
		Title       string `json:"title"`
		Description string `json:"description"`
		CoverURL    string `json:"cover_url"`
	}
	_ = c.ShouldBindJSON(&req)
	if req.Title != "" {
		album.Title = req.Title
	}
	if req.Description != "" {
		album.Description = req.Description
	}
	if req.CoverURL != "" {
		album.CoverURL = req.CoverURL
	}
	db.Save(&album)
	c.JSON(200, album)
}

func setVisibility(c *gin.Context) {
	id, _ := strconv.Atoi(c.Param("id"))
	var album Album
	if err := db.First(&album, id).Error; err != nil {
		c.JSON(404, gin.H{"error": "not found"})
		return
	}
	if album.OwnerID != c.GetUint("user_id") {
		c.JSON(403, gin.H{"error": "forbidden"})
		return
	}
	var req struct {
		Visibility string `json:"visibility"`
	}
	_ = c.ShouldBindJSON(&req)
	old := album.Visibility
	album.Visibility = req.Visibility
	db.Save(&album)
	publish("album.visibility_changed", map[string]any{
		"album_id": album.ID, "old": old, "new": req.Visibility,
	})
	c.JSON(200, album)
}

func deleteAlbum(c *gin.Context) {
	id, _ := strconv.Atoi(c.Param("id"))
	var album Album
	if err := db.First(&album, id).Error; err != nil {
		c.JSON(404, gin.H{"error": "not found"})
		return
	}
	if album.OwnerID != c.GetUint("user_id") {
		c.JSON(403, gin.H{"error": "forbidden"})
		return
	}
	db.Where("album_id = ?", id).Delete(&Track{})
	db.Delete(&album)
	c.Status(204)
}

func addTrack(c *gin.Context) {
	albumID, _ := strconv.Atoi(c.Param("id"))
	var album Album
	if err := db.First(&album, albumID).Error; err != nil {
		c.JSON(404, gin.H{"error": "album not found"})
		return
	}
	if album.OwnerID != c.GetUint("user_id") {
		c.JSON(403, gin.H{"error": "forbidden"})
		return
	}
	var req struct {
		Title    string `json:"title"`
		Artists  string `json:"artists"`
		Duration int    `json:"duration"`
		AudioURL string `json:"audio_url"`
		TrackNum int    `json:"track_num"`
	}
	_ = c.ShouldBindJSON(&req)
	track := Track{
		AlbumID: uint(albumID), Title: req.Title, Artists: req.Artists,
		Duration: req.Duration, AudioURL: req.AudioURL, TrackNum: req.TrackNum,
	}
	db.Create(&track)
	c.JSON(201, track)
}

func playTrack(c *gin.Context) {
	trackID, _ := strconv.Atoi(c.Param("track_id"))
	db.Model(&Track{}).Where("id = ?", trackID).UpdateColumn("plays", gorm.Expr("plays + 1"))
	c.JSON(200, gin.H{"message": "play recorded"})
}

func deleteTrack(c *gin.Context) {
	trackID, _ := strconv.Atoi(c.Param("track_id"))
	albumID, _ := strconv.Atoi(c.Param("id"))
	var album Album
	if err := db.First(&album, albumID).Error; err != nil {
		c.JSON(404, gin.H{"error": "not found"})
		return
	}
	if album.OwnerID != c.GetUint("user_id") {
		c.JSON(403, gin.H{"error": "forbidden"})
		return
	}
	db.Delete(&Track{}, trackID)
	c.Status(204)
}

func validateWithUserService(token string) (uint, bool) {
	body, _ := json.Marshal(map[string]string{"token": token})
	req, _ := http.NewRequest(http.MethodPost, userServiceURL+"/internal/validate-token", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	respAny, err := userBreaker.Execute(func() (any, error) {
		return userHTTPClient.Do(req)
	})
	if err != nil {
		return 0, false
	}
	resp, _ := respAny.(*http.Response)
	if resp == nil || resp.StatusCode != 200 {
		return 0, false
	}
	defer resp.Body.Close()
	var parsed struct {
		Valid  bool `json:"valid"`
		UserID uint `json:"user_id"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&parsed)
	return parsed.UserID, parsed.Valid
}

func softAuth() gin.HandlerFunc {
	return func(c *gin.Context) {
		token := strings.TrimPrefix(c.GetHeader("Authorization"), "Bearer ")
		if token != "" {
			if uid, ok := validateWithUserService(token); ok {
				c.Set("user_id", uid)
			}
		}
		c.Next()
	}
}

func requireAuth() gin.HandlerFunc {
	return func(c *gin.Context) {
		token := strings.TrimPrefix(c.GetHeader("Authorization"), "Bearer ")
		if token == "" {
			c.AbortWithStatusJSON(401, gin.H{"error": "missing token"})
			return
		}
		uid, ok := validateWithUserService(token)
		if !ok {
			c.AbortWithStatusJSON(401, gin.H{"error": "invalid token"})
			return
		}
		c.Set("user_id", uid)
		c.Next()
	}
}

func cors() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Header("Access-Control-Allow-Origin", "*")
		c.Header("Access-Control-Allow-Methods", "GET,POST,PUT,DELETE,PATCH,OPTIONS")
		c.Header("Access-Control-Allow-Headers", "Authorization,Content-Type")
		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}
		c.Next()
	}
}

func restLogMiddleware(service string) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()
		elapsed := time.Since(start).Milliseconds()
		path := c.Request.URL.Path
		if c.Request.URL.RawQuery != "" {
			path += "?" + c.Request.URL.RawQuery
		}
		metricPath := c.FullPath()
		if metricPath == "" {
			metricPath = c.Request.URL.Path
		}
		status := c.Writer.Status()
		httpRequestsTotal.WithLabelValues(service, c.Request.Method, metricPath, strconv.Itoa(status)).Inc()
		httpRequestDuration.WithLabelValues(service, c.Request.Method, metricPath).Observe(float64(elapsed) / 1000.0)
		log.Printf("[rest][%s] method=%s path=%s status=%d elapsed_ms=%d", service, c.Request.Method, path, status, elapsed)
		publishRaw("rest.events", service+".request", map[string]any{
			"service":    service,
			"method":     c.Request.Method,
			"path":       path,
			"status":     status,
			"elapsed_ms": elapsed,
			"ts":         time.Now().UTC().Format(time.RFC3339),
		})
	}
}

func publishRaw(exchange, key string, payload map[string]any) {
	if amqpCh == nil {
		return
	}
	body, _ := json.Marshal(payload)
	amqpMu.Lock()
	err := amqpCh.PublishWithContext(context.Background(), exchange, key, false, false,
		amqp.Publishing{ContentType: "application/json", Body: body})
	amqpMu.Unlock()
	if err != nil {
		log.Printf("[rabbitmq][library-service] publish failed exchange=%s key=%s err=%v", exchange, key, err)
		rabbitPublishTotal.WithLabelValues("library-service", exchange, "error").Inc()
		return
	}
	rabbitPublishTotal.WithLabelValues("library-service", exchange, "ok").Inc()
	log.Printf("[rabbitmq][library-service] published exchange=%s key=%s", exchange, key)
}

func publish(key string, payload map[string]any) {
	publishRaw("library.events", key, payload)
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
