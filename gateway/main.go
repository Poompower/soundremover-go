package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

type Cfg struct {
	UserURL       string
	FileURL       string
	ProcessingURL string
	LibraryURL    string
	JWTSecret     []byte
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

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func callJSON(method, url string, body any) (int, []byte, error) {
	var buf io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		buf = bytes.NewReader(b)
	}
	req, _ := http.NewRequest(method, url, buf)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
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

func proxyTo(w http.ResponseWriter, r *http.Request, targetURL string, body io.Reader, copyAuth bool) {
	req, _ := http.NewRequest(r.Method, targetURL, body)
	if ct := r.Header.Get("Content-Type"); ct != "" {
		req.Header.Set("Content-Type", ct)
	}
	if copyAuth {
		if auth := r.Header.Get("Authorization"); auth != "" {
			req.Header.Set("Authorization", auth)
		}
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		writeJSON(w, 502, map[string]string{"error": "upstream unavailable"})
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
	cfg := Cfg{
		UserURL:       os.Getenv("USER_SERVICE_URL"),
		FileURL:       os.Getenv("FILE_SERVICE_URL"),
		ProcessingURL: os.Getenv("PROCESSING_SERVICE_URL"),
		LibraryURL:    os.Getenv("LIBRARY_SERVICE_URL"),
		JWTSecret:     []byte(os.Getenv("JWT_SECRET")),
	}
	if cfg.UserURL == "" || cfg.FileURL == "" || cfg.ProcessingURL == "" || cfg.LibraryURL == "" || len(cfg.JWTSecret) == 0 {
		log.Fatal("missing env: USER_SERVICE_URL / FILE_SERVICE_URL / PROCESSING_SERVICE_URL / LIBRARY_SERVICE_URL / JWT_SECRET")
	}

	mux := http.NewServeMux()

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
		status, data, err := callJSON("POST", cfg.UserURL+"/register", req)
		if err != nil {
			writeJSON(w, 502, map[string]string{"error": "user-service down"})
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
		status, data, err := callJSON("POST", cfg.UserURL+"/login", LoginReq{
			Username: loginID,
			Password: req.Password,
		})
		if err != nil {
			writeJSON(w, 502, map[string]string{"error": "user-service down"})
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
		target := strings.TrimRight(cfg.UserURL, "/") + "/users/" + strconv.FormatUint(uint64(u.ID), 10)
		var body io.Reader
		if r.Method == http.MethodPut {
			b, _ := io.ReadAll(r.Body)
			body = bytes.NewReader(b)
		}
		status, data, err := func() (int, []byte, error) {
			req, _ := http.NewRequest(r.Method, target, body)
			req.Header.Set("Content-Type", "application/json")
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				return 0, nil, err
			}
			defer resp.Body.Close()
			d, _ := io.ReadAll(resp.Body)
			return resp.StatusCode, d, nil
		}()
		if err != nil {
			writeJSON(w, 502, map[string]string{"error": "user-service down"})
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
		target := strings.TrimRight(cfg.LibraryURL, "/") + "/albums"
		if r.URL.RawQuery != "" {
			target += "?" + r.URL.RawQuery
		}
		var body io.Reader
		if r.Body != nil && (r.Method == http.MethodPost || r.Method == http.MethodPut || r.Method == http.MethodPatch) {
			b, _ := io.ReadAll(r.Body)
			body = bytes.NewReader(b)
		}
		proxyTo(w, r, target, body, true)
	})

	mux.HandleFunc("/api/albums/", func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/api")
		target := strings.TrimRight(cfg.LibraryURL, "/") + path
		if r.URL.RawQuery != "" {
			target += "?" + r.URL.RawQuery
		}
		var body io.Reader
		if r.Body != nil && (r.Method == http.MethodPost || r.Method == http.MethodPut || r.Method == http.MethodPatch) {
			b, _ := io.ReadAll(r.Body)
			body = bytes.NewReader(b)
		}
		proxyTo(w, r, target, body, true)
	})

	mux.HandleFunc("/api/my-albums", func(w http.ResponseWriter, r *http.Request) {
		target := strings.TrimRight(cfg.LibraryURL, "/") + "/my-albums"
		proxyTo(w, r, target, nil, true)
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
		status, data, err := callJSON("POST", cfg.UserURL+"/register", req)
		if err != nil {
			writeJSON(w, 502, map[string]string{"error": "user-service down"})
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
		status, data, err := callJSON("POST", cfg.UserURL+"/login", req)
		if err != nil {
			writeJSON(w, 502, map[string]string{"error": "user-service down"})
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
		status, data, err := callJSON("POST", cfg.FileURL+"/files", payload)
		if err != nil {
			writeJSON(w, 502, map[string]string{"error": "file-service down"})
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
			target := cfg.ProcessingURL + "/jobs?" + q.Encode()
			req, _ := http.NewRequest(http.MethodGet, target, nil)
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				writeJSON(w, 502, map[string]string{"error": "processing-service down"})
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
		status, data, err := callJSON("POST", cfg.ProcessingURL+"/jobs", payload)
		if err != nil {
			writeJSON(w, 502, map[string]string{"error": "processing-service down"})
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
		status, data, err := callJSON("POST", cfg.ProcessingURL+"/ytmp3", payload)
		if err != nil {
			writeJSON(w, 502, map[string]string{"error": "processing-service down"})
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
		target := strings.TrimRight(cfg.ProcessingURL, "/") + "/input/upload"
		proxyTo(w, r, target, bytes.NewReader(body), false)
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

		upstream := cfg.ProcessingURL + "/jobs/" + parts[0]
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
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			writeJSON(w, 502, map[string]string{"error": "processing-service down"})
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
	log.Fatal(http.ListenAndServe(":8080", withCORS(mux)))
}
