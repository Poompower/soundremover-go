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
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/glebarez/sqlite"
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
	SourceFilename string   `json:"source_filename"`
	Stems          []string `json:"stems,omitempty"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
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
	res, err := http.Get(url)
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
		return
	}

	inputPath := filepath.Join(audioRoot, "input", filepath.Base(f.Filename))
	outputPath := filepath.Join(audioRoot, fmt.Sprintf("separated_%d", job.ID))

	if err := separateAudio(inputPath, outputPath, job.Model, spleeterImage); err != nil {
		updateJobStatus(db, job.ID, "FAILED", err.Error())
		return
	}
	updateJobStatus(db, job.ID, "SUCCEEDED", "")
}

func startWorker(ctx context.Context, db *gorm.DB, rdb *redis.Client, fileServiceURL, audioRoot, spleeterImage string) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			res, err := rdb.BRPop(ctx, 0*time.Second, "jobs").Result()
			if err != nil {
				log.Printf("worker BRPOP error: %v", err)
				time.Sleep(1 * time.Second)
				continue
			}
			if len(res) < 2 {
				continue
			}
			id64, err := strconv.ParseUint(res[1], 10, 64)
			if err != nil || id64 == 0 {
				log.Printf("worker invalid job id: %q", res[1])
				continue
			}

			processJob(db, fileServiceURL, audioRoot, spleeterImage, uint(id64))
		}
	}()
}

func main() {
	dbPath := os.Getenv("DB_PATH")
	if dbPath == "" {
		dbPath = "./processing.db"
	}
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
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

	db, err := gorm.Open(sqlite.Open(dbPath), &gorm.Config{})
	if err != nil {
		log.Fatal(err)
	}
	if err := db.AutoMigrate(&Job{}); err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	startWorker(ctx, db, rdb, fileServiceURL, audioRoot, spleeterImage)

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		if err := rdb.Ping(ctx).Err(); err != nil {
			writeJSON(w, 500, map[string]any{"ok": false, "redis": "down"})
			return
		}
		writeJSON(w, 200, map[string]any{"ok": true})
	})

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

            job := Job{UserID: req.UserID, FileID: req.FileID, Model: req.Model, Status: "QUEUED"}
            if err := db.Create(&job).Error; err != nil {
                writeJSON(w, 500, map[string]string{"error": "db error"})
                return
            }

            if err := rdb.LPush(ctx, "jobs", job.ID).Err(); err != nil {
                _ = db.Model(&Job{}).Where("id=?", job.ID).Update("status", "FAILED").Error
                writeJSON(w, 500, map[string]string{"error": "redis queue error"})
                return
            }

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
	log.Fatal(http.ListenAndServe(":8083", nil))
}
