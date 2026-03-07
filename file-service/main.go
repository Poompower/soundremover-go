package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

type File struct {
	ID        uint      `gorm:"primaryKey" json:"id"`
	UserID    uint      `gorm:"index;not null" json:"user_id"`
	Filename  string    `gorm:"not null" json:"filename"`
	Status    string    `gorm:"not null;default:VALIDATED" json:"status"` // VALIDATED/PROCESSING/DONE/FAILED
	CreatedAt time.Time `json:"created_at"`
}

type CreateFileReq struct {
	UserID   uint   `json:"user_id"`
	Filename string `json:"filename"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func main() {
	dbPath := os.Getenv("DB_PATH")
	if dbPath == "" {
		dbPath = "./file.db"
	}

	db, err := gorm.Open(sqlite.Open(dbPath), &gorm.Config{})
	if err != nil {
		log.Fatal(err)
	}
	if err := db.AutoMigrate(&File{}); err != nil {
		log.Fatal(err)
	}

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, map[string]any{"ok": true})
	})

	http.HandleFunc("/files", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeJSON(w, 405, map[string]string{"error": "method not allowed"})
			return
		}
		var req CreateFileReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, 400, map[string]string{"error": "invalid json"})
			return
		}
		if req.UserID == 0 || req.Filename == "" {
			writeJSON(w, 400, map[string]string{"error": "missing fields"})
			return
		}
		f := File{UserID: req.UserID, Filename: req.Filename, Status: "VALIDATED"}
		if err := db.Create(&f).Error; err != nil {
			writeJSON(w, 500, map[string]string{"error": "db error"})
			return
		}
		writeJSON(w, 201, map[string]any{"file": f})
	})

	http.HandleFunc("/files/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeJSON(w, 405, map[string]string{"error": "method not allowed"})
			return
		}

		idStr := strings.TrimPrefix(r.URL.Path, "/files/")
		id, err := strconv.ParseUint(idStr, 10, 64)
		if err != nil || id == 0 {
			writeJSON(w, 400, map[string]string{"error": "invalid file id"})
			return
		}

		var f File
		if err := db.First(&f, id).Error; err != nil {
			writeJSON(w, 404, map[string]string{"error": fmt.Sprintf("file %d not found", id)})
			return
		}

		writeJSON(w, 200, map[string]any{"file": f})
	})

	log.Println("file-service on :8082")
	log.Fatal(http.ListenAndServe(":8082", nil))
}
