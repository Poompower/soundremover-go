package main

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/glebarez/sqlite"
	amqp "github.com/rabbitmq/amqp091-go"
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

var db *gorm.DB
var amqpCh *amqp.Channel
var userServiceURL string

func main() {
	userServiceURL = getenv("USER_SERVICE_URL", "http://user-service:8081")

	os.MkdirAll("/data", 0o755)
	var err error
	db, err = gorm.Open(sqlite.Open("/data/library.db"), &gorm.Config{})
	if err != nil {
		log.Fatal(err)
	}
	_ = db.AutoMigrate(&Album{}, &Track{})

	// RabbitMQ is optional in this merged template.
	var conn *amqp.Connection
	for i := 0; i < 3; i++ {
		conn, err = amqp.Dial(getenv("RABBITMQ_URL", "amqp://admin:admin123@rabbitmq:5672/"))
		if err == nil {
			break
		}
		log.Println("RabbitMQ not ready, continue without events...", i+1)
		time.Sleep(1 * time.Second)
	}
	if err == nil {
		defer conn.Close()
		amqpCh, _ = conn.Channel()
		if amqpCh != nil {
			_ = amqpCh.ExchangeDeclare("library.events", "topic", true, false, false, false, nil)
		}
	}

	r := gin.Default()
	r.Use(cors())

	r.GET("/health", func(c *gin.Context) {
		c.JSON(200, gin.H{"status": "ok", "service": "library-service"})
	})
	r.GET("/albums", listPublicAlbums)
	r.GET("/albums/:id", softAuth(), getAlbum)
	r.POST("/albums/:id/tracks/:track_id/play", playTrack)

	p := r.Group("/")
	p.Use(requireAuth())
	{
		p.GET("/my-albums", listMyAlbums)
		p.POST("/albums", createAlbum)
		p.PUT("/albums/:id", updateAlbum)
		p.PATCH("/albums/:id/visibility", setVisibility)
		p.DELETE("/albums/:id", deleteAlbum)
		p.POST("/albums/:id/tracks", addTrack)
		p.DELETE("/albums/:id/tracks/:track_id", deleteTrack)
	}

	port := getenv("PORT", "8084")
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
	resp, err := http.Post(userServiceURL+"/internal/validate-token", "application/json", bytes.NewReader(body))
	if err != nil || resp.StatusCode != 200 {
		return 0, false
	}
	defer resp.Body.Close()
	var result struct {
		Valid  bool `json:"valid"`
		UserID uint `json:"user_id"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&result)
	return result.UserID, result.Valid
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

func publish(key string, payload map[string]any) {
	if amqpCh == nil {
		return
	}
	body, _ := json.Marshal(payload)
	_ = amqpCh.PublishWithContext(context.Background(), "library.events", key, false, false,
		amqp.Publishing{ContentType: "application/json", Body: body})
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
