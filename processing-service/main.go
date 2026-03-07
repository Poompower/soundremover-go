package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/glebarez/sqlite"
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
	"gorm.io/gorm"
)

type Job struct {
	ID        uint      `gorm:"primaryKey" json:"id"`
	UserID    uint      `gorm:"index;not null" json:"user_id"`
	FileID    uint      `gorm:"index;not null" json:"file_id"`
	Model     string    `gorm:"not null;default:2stems" json:"model"`
	Status    string    `gorm:"not null;default:QUEUED" json:"status"` // QUEUED/PROCESSING/SUCCEEDED/FAILED
	ErrorMsg  string    `gorm:"type:text" json:"error_msg,omitempty"`
	CreatedAt time.Time `json:"created_at"`
}

type CreateJobReq struct {
	UserID uint   `json:"user_id"`
	FileID uint   `json:"file_id"`
	Model  string `json:"model"`
}

type YTMP3Req struct {
	URL      string `json:"url"`
	Filename string `json:"filename"`
	UserID   uint   `json:"user_id"`
}

type File struct {
	ID       uint   `json:"id"`
	UserID   uint   `json:"user_id"`
	Filename string `json:"filename"`
	Status   string `json:"status"`
}

type fileResp struct {
	File File `json:"file"`
}

type JobListItem struct {
	Job
	SourceFilename string         `json:"source_filename"`
	Stems          []string       `json:"stems,omitempty"`
	Metadata       *AudioMetadata `json:"metadata,omitempty"`
}

type AudioMetadata struct {
	FileName   string  `json:"file_name"`
	Title      string  `json:"title"`
	Artist     string  `json:"artist"`
	Genre      string  `json:"genre"`
	Duration   float64 `json:"duration"`
	Bitrate    int64   `json:"bitrate"`
	Size       int64   `json:"size"`
	UploadDate string  `json:"upload_date"`
}

type ffprobeResult struct {
	Format ffprobeFormat `json:"format"`
}

type ffprobeFormat struct {
	Duration string      `json:"duration"`
	BitRate  string      `json:"bit_rate"`
	Tags     ffprobeTags `json:"tags"`
}

type ffprobeTags struct {
	Title  string `json:"title"`
	Artist string `json:"artist"`
	Genre  string `json:"genre"`
}

var fileHTTPClient *http.Client
var fileBreaker *gobreaker.CircuitBreaker
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
var queueEventsTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "soundremover_queue_events_total",
		Help: "Queue event totals for job queue processing.",
	},
	[]string{"service", "queue", "event"},
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
	prometheus.MustRegister(
		httpRequestsTotal,
		httpRequestDuration,
		rabbitPublishTotal,
		queueEventsTotal,
		circuitStateGauge,
		tracingEnabledGauge,
	)
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

func initFileCircuitBreaker() {
	fileBreaker = gobreaker.NewCircuitBreaker(gobreaker.Settings{
		Name:        "file-service",
		Interval:    30 * time.Second,
		Timeout:     20 * time.Second,
		MaxRequests: 2,
		OnStateChange: func(name string, from gobreaker.State, to gobreaker.State) {
			log.Printf("[circuit-breaker][processing-service] service=%s state=%s->%s", name, from.String(), to.String())
			setCircuitState("processing-service", name, to)
		},
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return counts.ConsecutiveFailures >= 5
		},
	})
	setCircuitState("processing-service", "file-service", gobreaker.StateClosed)
}

func callFileService(fn func() (*http.Response, error)) (*http.Response, error) {
	result, err := fileBreaker.Execute(func() (any, error) {
		return fn()
	})
	if err != nil {
		return nil, err
	}
	resp, _ := result.(*http.Response)
	return resp, nil
}

func sanitizeFilename(name string) string {
	name = strings.TrimSpace(name)
	name = filepath.Base(name)
	if name == "" {
		return ""
	}
	name = strings.ReplaceAll(name, "\\", "_")
	name = strings.ReplaceAll(name, "/", "_")
	name = strings.ReplaceAll(name, ":", "_")
	name = strings.ReplaceAll(name, "*", "_")
	name = strings.ReplaceAll(name, "?", "_")
	name = strings.ReplaceAll(name, "\"", "_")
	name = strings.ReplaceAll(name, "<", "_")
	name = strings.ReplaceAll(name, ">", "_")
	name = strings.ReplaceAll(name, "|", "_")
	return strings.TrimSpace(name)
}

func downloadYTMP3(audioRoot, url, preferredName string, userID uint) (string, error) {
	if strings.TrimSpace(url) == "" {
		return "", fmt.Errorf("missing url")
	}

	inputDir := filepath.Join(audioRoot, "input")
	if err := os.MkdirAll(inputDir, 0o755); err != nil {
		return "", err
	}

	base := sanitizeFilename(preferredName)
	if base == "" {
		base = fmt.Sprintf("yt_%d_%d", userID, time.Now().Unix())
	}
	base = strings.TrimSuffix(base, filepath.Ext(base))
	outTpl := filepath.Join(inputDir, base+".%(ext)s")

	runYTDLP := func(extraArgs ...string) (string, string, error) {
		args := []string{
			"-x",
			"--audio-format", "mp3",
			"--no-playlist",
			"--force-ipv4",
			"--embed-metadata",
			"--parse-metadata", "%(uploader)s:%(artist)s",
			"--parse-metadata", "%(title)s:%(title)s",
			"--print", "after_move:filepath",
			"-o", outTpl,
		}
		args = append(args, extraArgs...)
		args = append(args, url)

		cmd := exec.Command("yt-dlp", args...)
		var stdout bytes.Buffer
		var stderr bytes.Buffer
		cmd.Stdout = &stdout
		cmd.Stderr = &stderr
		err := cmd.Run()
		return stdout.String(), stderr.String(), err
	}

	stdout, stderr, err := runYTDLP()
	if err != nil {
		// Fallback for YouTube signature/client issues (HTTP 403 / signature extraction failed).
		stdout, stderr, err = runYTDLP(
			"--extractor-args", "youtube:player_client=android,web",
			"--user-agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
		)
		if err != nil {
			return "", fmt.Errorf("yt-dlp failed: %v: %s", err, strings.TrimSpace(stderr))
		}
	}

	lines := strings.Split(strings.TrimSpace(stdout), "\n")
	last := ""
	for i := len(lines) - 1; i >= 0; i-- {
		if strings.TrimSpace(lines[i]) != "" {
			last = strings.TrimSpace(lines[i])
			break
		}
	}
	if last == "" {
		return "", fmt.Errorf("yt-dlp did not return output path")
	}
	return filepath.Base(last), nil
}

func saveUploadedInput(audioRoot string, src multipart.File, header *multipart.FileHeader) (string, error) {
	defer src.Close()
	inputDir := filepath.Join(audioRoot, "input")
	if err := os.MkdirAll(inputDir, 0o755); err != nil {
		return "", err
	}
	base := sanitizeFilename(header.Filename)
	if base == "" {
		return "", fmt.Errorf("invalid filename")
	}
	dstPath := filepath.Join(inputDir, base)
	dst, err := os.Create(dstPath)
	if err != nil {
		return "", err
	}
	defer dst.Close()
	if _, err := io.Copy(dst, src); err != nil {
		return "", err
	}
	return base, nil
}

func getFileByID(fileServiceURL string, fileID uint) (File, error) {
	url := fmt.Sprintf("%s/files/%d", strings.TrimRight(fileServiceURL, "/"), fileID)
	req, _ := http.NewRequest(http.MethodGet, url, nil)
	res, err := callFileService(func() (*http.Response, error) {
		return fileHTTPClient.Do(req)
	})
	if err != nil {
		return File{}, err
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		return File{}, fmt.Errorf("file-service status %d", res.StatusCode)
	}

	var out fileResp
	if err := json.NewDecoder(res.Body).Decode(&out); err != nil {
		return File{}, err
	}
	if out.File.ID == 0 || strings.TrimSpace(out.File.Filename) == "" {
		return File{}, fmt.Errorf("file metadata invalid")
	}
	return out.File, nil
}

func updateFileStatus(fileServiceURL string, fileID uint, status string) error {
	body, _ := json.Marshal(map[string]string{"status": status})
	target := fmt.Sprintf("%s/files/%d", strings.TrimRight(fileServiceURL, "/"), fileID)
	req, _ := http.NewRequest(http.MethodPut, target, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := callFileService(func() (*http.Response, error) {
		return fileHTTPClient.Do(req)
	})
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("file-service status %d: %s", resp.StatusCode, strings.TrimSpace(string(respBody)))
	}
	return nil
}

func parseFloatOrZero(v string) float64 {
	f, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
	if err != nil {
		return 0
	}
	return f
}

func parseIntOrZero(v string) int64 {
	i, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
	if err != nil {
		return 0
	}
	return i
}

func probeAudioMetadata(filePath string) (AudioMetadata, error) {
	stat, err := os.Stat(filePath)
	if err != nil {
		return AudioMetadata{}, err
	}

	base := filepath.Base(filePath)
	meta := AudioMetadata{
		FileName:   base,
		Title:      strings.TrimSuffix(base, filepath.Ext(base)),
		Artist:     "",
		Genre:      "",
		Duration:   0,
		Bitrate:    0,
		Size:       stat.Size(),
		UploadDate: stat.ModTime().UTC().Format(time.RFC3339),
	}

	cmd := exec.Command(
		"ffprobe",
		"-v", "error",
		"-show_entries", "format=duration,bit_rate:format_tags=title,artist,genre",
		"-of", "json",
		filePath,
	)
	out, err := cmd.Output()
	if err != nil {
		return meta, nil
	}

	var parsed ffprobeResult
	if err := json.Unmarshal(out, &parsed); err != nil {
		return meta, nil
	}
	if strings.TrimSpace(parsed.Format.Tags.Title) != "" {
		meta.Title = strings.TrimSpace(parsed.Format.Tags.Title)
	}
	if strings.TrimSpace(parsed.Format.Tags.Artist) != "" {
		meta.Artist = strings.TrimSpace(parsed.Format.Tags.Artist)
	}
	if strings.TrimSpace(parsed.Format.Tags.Genre) != "" {
		meta.Genre = strings.TrimSpace(parsed.Format.Tags.Genre)
	}
	meta.Duration = parseFloatOrZero(parsed.Format.Duration)
	meta.Bitrate = parseIntOrZero(parsed.Format.BitRate)
	return meta, nil
}

func separateAudio(inputPath, outputPath, model, image string) error {
	if model == "" {
		model = "2stems"
	}
	if strings.TrimSpace(image) == "" {
		image = "poompower/spleeter:latest"
	}
	if _, err := os.Stat(inputPath); err != nil {
		return fmt.Errorf("input not found: %s", inputPath)
	}

	inputAbs, err := filepath.Abs(inputPath)
	if err != nil {
		return err
	}
	outputAbs, err := filepath.Abs(outputPath)
	if err != nil {
		return err
	}

	inputFile := filepath.Base(inputAbs)
	outputName := filepath.Base(outputAbs)
	inputDirInContainer := filepath.Dir(inputAbs)
	outputParentInContainer := filepath.Dir(outputAbs)
	containerID, err := os.Hostname()
	if err != nil || strings.TrimSpace(containerID) == "" {
		return fmt.Errorf("cannot detect current container id")
	}

	cmd := exec.Command(
		"docker", "run", "--rm",
		"--volumes-from", containerID,
		image,
		"separate",
		"-i", filepath.Join(inputDirInContainer, inputFile),
		"-p", fmt.Sprintf("spleeter:%s", model),
		"-o", filepath.Join(outputParentInContainer, outputName),
	)

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("spleeter failed: %v: %s", err, strings.TrimSpace(stderr.String()))
	}
	return nil
}

func updateJobStatus(db *gorm.DB, jobID uint, status, errMsg string) {
	_ = db.Model(&Job{}).Where("id = ?", jobID).Updates(map[string]any{
		"status":    status,
		"error_msg": errMsg,
	}).Error
}

func findStemFile(audioRoot string, jobID uint64, stem string) (string, error) {
	stem = strings.TrimSpace(strings.ToLower(stem))
	if stem == "" || strings.Contains(stem, "/") || strings.Contains(stem, "\\") {
		return "", fmt.Errorf("invalid stem")
	}

	patterns := []string{
		filepath.Join(audioRoot, fmt.Sprintf("separated_%d", jobID), "*", stem+".wav"),
		filepath.Join(audioRoot, "input", fmt.Sprintf("separated_%d", jobID), "*", stem+".wav"),
	}

	for _, pattern := range patterns {
		matches, err := filepath.Glob(pattern)
		if err == nil && len(matches) > 0 {
			return matches[0], nil
		}
	}
	return "", fmt.Errorf("stem file not found")
}

func listStemNames(audioRoot string, jobID uint64) []string {
	patterns := []string{
		filepath.Join(audioRoot, fmt.Sprintf("separated_%d", jobID), "*", "*.wav"),
		filepath.Join(audioRoot, "input", fmt.Sprintf("separated_%d", jobID), "*", "*.wav"),
	}
	found := map[string]struct{}{}
	for _, pattern := range patterns {
		matches, err := filepath.Glob(pattern)
		if err != nil {
			continue
		}
		for _, p := range matches {
			name := strings.TrimSpace(strings.ToLower(strings.TrimSuffix(filepath.Base(p), filepath.Ext(p))))
			if name == "" {
				continue
			}
			found[name] = struct{}{}
		}
	}
	stems := make([]string, 0, len(found))
	for stem := range found {
		stems = append(stems, stem)
	}
	sort.Strings(stems)
	return stems
}

func findOriginalFile(db *gorm.DB, fileServiceURL, audioRoot string, jobID uint64) (string, error) {
	var job Job
	if err := db.First(&job, jobID).Error; err != nil {
		return "", fmt.Errorf("job not found")
	}
	f, err := getFileByID(fileServiceURL, job.FileID)
	if err != nil {
		return "", fmt.Errorf("cannot load file metadata: %v", err)
	}
	p := filepath.Join(audioRoot, "input", filepath.Base(f.Filename))
	if _, err := os.Stat(p); err != nil {
		return "", fmt.Errorf("original file not found")
	}
	return p, nil
}

func serveAudioFile(w http.ResponseWriter, filePath, filename, contentDisposition string) {
	ext := strings.ToLower(filepath.Ext(filePath))
	contentType := "application/octet-stream"
	if ext == ".mp3" {
		contentType = "audio/mpeg"
	}
	if ext == ".wav" {
		contentType = "audio/wav"
	}
	if ext == ".flac" {
		contentType = "audio/flac"
	}
	if ext == ".ogg" {
		contentType = "audio/ogg"
	}
	if ext == ".m4a" {
		contentType = "audio/mp4"
	}

	w.Header().Set("Content-Type", contentType)
	if contentDisposition != "" {
		w.Header().Set("Content-Disposition", fmt.Sprintf("%s; filename=\"%s\"", contentDisposition, filename))
	}
	f, err := os.Open(filePath)
	if err != nil {
		writeJSON(w, 500, map[string]string{"error": "cannot open file"})
		return
	}
	defer f.Close()
	_, _ = io.Copy(w, f)
}

func processJob(db *gorm.DB, fileServiceURL, audioRoot, spleeterImage string, jobID uint) {
	var job Job
	if err := db.First(&job, jobID).Error; err != nil {
		return
	}
	updateJobStatus(db, job.ID, "PROCESSING", "")

	f, err := getFileByID(fileServiceURL, job.FileID)
	if err != nil {
		updateJobStatus(db, job.ID, "FAILED", "cannot load file metadata: "+err.Error())
		_ = updateFileStatus(fileServiceURL, job.FileID, "FAILED")
		return
	}
	if f.UserID != job.UserID {
		updateJobStatus(db, job.ID, "FAILED", "file does not belong to job user")
		_ = updateFileStatus(fileServiceURL, job.FileID, "FAILED")
		return
	}
	if err := updateFileStatus(fileServiceURL, job.FileID, "PROCESSING"); err != nil {
		updateJobStatus(db, job.ID, "FAILED", "cannot update file status: "+err.Error())
		return
	}

	inputPath := filepath.Join(audioRoot, "input", filepath.Base(f.Filename))
	outputPath := filepath.Join(audioRoot, fmt.Sprintf("separated_%d", job.ID))

	if err := separateAudio(inputPath, outputPath, job.Model, spleeterImage); err != nil {
		updateJobStatus(db, job.ID, "FAILED", err.Error())
		_ = updateFileStatus(fileServiceURL, job.FileID, "FAILED")
		return
	}
	updateJobStatus(db, job.ID, "SUCCEEDED", "")
	_ = updateFileStatus(fileServiceURL, job.FileID, "DONE")
}

func startWorker(ctx context.Context, db *gorm.DB, deliveries <-chan amqp.Delivery, fileServiceURL, audioRoot, spleeterImage string) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-deliveries:
				if !ok {
					log.Printf("worker consume channel closed")
					return
				}
				id64, err := strconv.ParseUint(strings.TrimSpace(string(msg.Body)), 10, 64)
				if err != nil || id64 == 0 {
					log.Printf("worker invalid job id: %q", string(msg.Body))
					_ = msg.Ack(false)
					continue
				}
				log.Printf("[rabbitmq] consumed job_id=%d delivery_tag=%d", id64, msg.DeliveryTag)
				queueEventsTotal.WithLabelValues("processing-service", "jobs", "consumed").Inc()
				processJob(db, fileServiceURL, audioRoot, spleeterImage, uint(id64))
				_ = msg.Ack(false)
				queueEventsTotal.WithLabelValues("processing-service", "jobs", "acked").Inc()
				log.Printf("[rabbitmq] acked job_id=%d delivery_tag=%d", id64, msg.DeliveryTag)
			}
		}
	}()
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
	serviceName := "processing-service"
	initMetrics()
	shutdownTracer := initTracer(serviceName)
	defer func() {
		_ = shutdownTracer(context.Background())
	}()
	dbPath := os.Getenv("DB_PATH")
	if dbPath == "" {
		dbPath = "./processing.db"
	}
	rabbitURL := os.Getenv("RABBITMQ_URL")
	if rabbitURL == "" {
		rabbitURL = "amqp://guest:guest@localhost:5672/"
	}
	fileServiceURL := os.Getenv("FILE_SERVICE_URL")
	if fileServiceURL == "" {
		fileServiceURL = "http://localhost:8082"
	}
	audioRoot := os.Getenv("AUDIO_ROOT")
	if audioRoot == "" {
		audioRoot = "/data/audio"
	}
	spleeterImage := os.Getenv("SPLEETER_IMAGE")
	if spleeterImage == "" {
		spleeterImage = "poompower/spleeter:latest"
	}
	initConsulRegistration(serviceName, 8083)
	fileHTTPClient = &http.Client{
		Timeout:   10 * time.Second,
		Transport: otelhttp.NewTransport(http.DefaultTransport),
	}
	initFileCircuitBreaker()
	log.Printf("[circuit-breaker][%s] target=file-service ready", serviceName)

	db, err := gorm.Open(sqlite.Open(dbPath), &gorm.Config{})
	if err != nil {
		log.Fatal(err)
	}
	if err := db.AutoMigrate(&Job{}); err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	queueName := "jobs"
	var rmqConn *amqp.Connection
	for {
		rmqConn, err = amqp.Dial(rabbitURL)
		if err == nil {
			log.Printf("[rabbitmq] connected url=%s", rabbitURL)
			break
		}
		log.Printf("rabbitmq dial failed, retrying: %v", err)
		time.Sleep(2 * time.Second)
	}
	defer rmqConn.Close()
	rmqCh, err := rmqConn.Channel()
	if err != nil {
		log.Fatalf("rabbitmq channel failed: %v", err)
	}
	defer rmqCh.Close()
	rmqRestCh, err := rmqConn.Channel()
	if err != nil {
		log.Fatalf("rabbitmq rest channel failed: %v", err)
	}
	defer rmqRestCh.Close()
	if err := rmqRestCh.ExchangeDeclare("rest.events", "topic", true, false, false, false, nil); err != nil {
		log.Fatalf("rabbitmq exchange declare failed: %v", err)
	}
	log.Printf("[rabbitmq][%s] ready exchange=rest.events", serviceName)
	if err := rmqCh.Qos(1, 0, false); err != nil {
		log.Fatalf("rabbitmq qos failed: %v", err)
	}
	if _, err := rmqCh.QueueDeclare(queueName, true, false, false, false, nil); err != nil {
		log.Fatalf("rabbitmq queue declare failed: %v", err)
	}
	log.Printf("[rabbitmq] queue ready name=%s", queueName)
	deliveries, err := rmqCh.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		log.Fatalf("rabbitmq consume failed: %v", err)
	}
	log.Printf("[rabbitmq] consumer started queue=%s", queueName)
	startWorker(ctx, db, deliveries, fileServiceURL, audioRoot, spleeterImage)

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		if rmqConn.IsClosed() {
			writeJSON(w, 500, map[string]any{"ok": false, "rabbitmq": "down"})
			return
		}
		writeJSON(w, 200, map[string]any{"ok": true})
	})
	http.Handle("/metrics", promhttp.Handler())

	http.HandleFunc("/jobs", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			var req CreateJobReq
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				writeJSON(w, 400, map[string]string{"error": "invalid json"})
				return
			}
			if req.UserID == 0 || req.FileID == 0 {
				writeJSON(w, 400, map[string]string{"error": "missing fields"})
				return
			}
			if req.Model == "" {
				req.Model = "2stems"
			}

			fileMeta, err := getFileByID(fileServiceURL, req.FileID)
			if err != nil {
				writeJSON(w, 400, map[string]string{"error": "invalid file_id"})
				return
			}
			if fileMeta.UserID != req.UserID {
				writeJSON(w, 403, map[string]string{"error": "file does not belong to user"})
				return
			}

			var activeCount int64
			if err := db.Model(&Job{}).
				Where("user_id = ? AND file_id = ? AND status IN ?", req.UserID, req.FileID, []string{"QUEUED", "PROCESSING"}).
				Count(&activeCount).Error; err != nil {
				writeJSON(w, 500, map[string]string{"error": "db error"})
				return
			}
			if activeCount > 0 {
				writeJSON(w, 409, map[string]string{"error": "job already active for this file"})
				return
			}

			job := Job{UserID: req.UserID, FileID: req.FileID, Model: req.Model, Status: "QUEUED"}
			if err := db.Create(&job).Error; err != nil {
				writeJSON(w, 500, map[string]string{"error": "db error"})
				return
			}

			if err := rmqCh.PublishWithContext(
				ctx,
				"",
				queueName,
				false,
				false,
				amqp.Publishing{
					ContentType:  "text/plain",
					DeliveryMode: amqp.Persistent,
					Body:         []byte(strconv.FormatUint(uint64(job.ID), 10)),
				},
			); err != nil {
				_ = db.Model(&Job{}).Where("id=?", job.ID).Update("status", "FAILED").Error
				queueEventsTotal.WithLabelValues("processing-service", queueName, "publish_error").Inc()
				writeJSON(w, 500, map[string]string{"error": "rabbitmq queue error"})
				return
			}
			queueEventsTotal.WithLabelValues("processing-service", queueName, "published").Inc()
			log.Printf("[rabbitmq] published job_id=%d queue=%s", job.ID, queueName)

			writeJSON(w, 201, map[string]any{"job": job, "queued": true})
			return
		}

		if r.Method != http.MethodGet {
			writeJSON(w, 405, map[string]string{"error": "method not allowed"})
			return
		}

		userID, err := strconv.ParseUint(strings.TrimSpace(r.URL.Query().Get("user_id")), 10, 64)
		if err != nil || userID == 0 {
			writeJSON(w, 400, map[string]string{"error": "invalid user_id"})
			return
		}
		statusFilter := strings.TrimSpace(strings.ToUpper(r.URL.Query().Get("status")))
		limit, _ := strconv.Atoi(strings.TrimSpace(r.URL.Query().Get("limit")))
		if limit <= 0 || limit > 200 {
			limit = 60
		}

		q := db.Where("user_id = ?", userID)
		if statusFilter != "" {
			q = q.Where("status = ?", statusFilter)
		}

		var jobs []Job
		if err := q.Order("created_at desc").Limit(limit).Find(&jobs).Error; err != nil {
			writeJSON(w, 500, map[string]string{"error": "db error"})
			return
		}

		items := make([]JobListItem, 0, len(jobs))
		for _, job := range jobs {
			item := JobListItem{Job: job}
			if f, ferr := getFileByID(fileServiceURL, job.FileID); ferr == nil {
				item.SourceFilename = f.Filename
				inputPath := filepath.Join(audioRoot, "input", filepath.Base(f.Filename))
				if meta, merr := probeAudioMetadata(inputPath); merr == nil {
					item.Metadata = &meta
				}
			}
			if strings.EqualFold(job.Status, "SUCCEEDED") {
				item.Stems = listStemNames(audioRoot, uint64(job.ID))
			}
			items = append(items, item)
		}
		writeJSON(w, 200, map[string]any{"jobs": items})
	})

	http.HandleFunc("/ytmp3", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeJSON(w, 405, map[string]string{"error": "method not allowed"})
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

		filename, err := downloadYTMP3(audioRoot, req.URL, req.Filename, req.UserID)
		if err != nil {
			writeJSON(w, 500, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, 201, map[string]any{
			"ok":       true,
			"filename": filename,
			"path":     filepath.Join(audioRoot, "input", filename),
		})
	})

	http.HandleFunc("/input/upload", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeJSON(w, 405, map[string]string{"error": "method not allowed"})
			return
		}
		if err := r.ParseMultipartForm(256 << 20); err != nil {
			writeJSON(w, 400, map[string]string{"error": "invalid multipart payload"})
			return
		}
		file, header, err := r.FormFile("file")
		if err != nil {
			writeJSON(w, 400, map[string]string{"error": "missing file field"})
			return
		}
		filename, err := saveUploadedInput(audioRoot, file, header)
		if err != nil {
			writeJSON(w, 500, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, 201, map[string]any{
			"ok":       true,
			"filename": filename,
			"path":     filepath.Join(audioRoot, "input", filename),
		})
	})

	http.HandleFunc("/jobs/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeJSON(w, 405, map[string]string{"error": "method not allowed"})
			return
		}
		path := strings.TrimPrefix(r.URL.Path, "/jobs/")
		parts := strings.Split(strings.Trim(path, "/"), "/")
		if len(parts) == 0 || parts[0] == "" {
			writeJSON(w, 400, map[string]string{"error": "invalid job id"})
			return
		}
		id, err := strconv.ParseUint(parts[0], 10, 64)
		if err != nil || id == 0 {
			writeJSON(w, 400, map[string]string{"error": "invalid job id"})
			return
		}

		if len(parts) == 2 && (parts[1] == "download" || parts[1] == "stream") {
			stem := r.URL.Query().Get("stem")
			var filePath string
			var filename string
			contentDisposition := "attachment"
			if parts[1] == "stream" {
				contentDisposition = "inline"
			}

			if strings.EqualFold(stem, "original") {
				filePath, err = findOriginalFile(db, fileServiceURL, audioRoot, id)
				if err != nil {
					writeJSON(w, 404, map[string]string{"error": err.Error()})
					return
				}
				ext := strings.ToLower(filepath.Ext(filePath))
				filename = fmt.Sprintf("job_%d_original%s", id, ext)
			} else {
				filePath, err = findStemFile(audioRoot, id, stem)
				if err != nil {
					writeJSON(w, 404, map[string]string{"error": err.Error()})
					return
				}
				filename = fmt.Sprintf("job_%d_%s%s", id, strings.ToLower(stem), filepath.Ext(filePath))
			}
			serveAudioFile(w, filePath, filename, contentDisposition)
			return
		}

		var job Job
		if err := db.First(&job, id).Error; err != nil {
			writeJSON(w, 404, map[string]string{"error": "job not found"})
			return
		}
		writeJSON(w, 200, map[string]any{"job": job})
	})

	log.Println("processing-service on :8083")
	root := withRESTLogging(serviceName, rmqRestCh, http.DefaultServeMux)
	log.Fatal(http.ListenAndServe(":8083", otelhttp.NewHandler(root, "processing-http")))
}
