package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"

	"golang.org/x/crypto/bcrypt"
	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

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

func main() {
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

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, map[string]any{"ok": true})
	})

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
	log.Fatal(http.ListenAndServe(":8081", nil))
}
