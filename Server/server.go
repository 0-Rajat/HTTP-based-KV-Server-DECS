package main

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

type Cache struct {
	mu      sync.RWMutex
	items   map[string]string
	maxSize int
	hits    int64
	misses  int64
}

func (c *Cache) Get(key string) (string, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	val, ok := c.items[key]
	if ok {
		atomic.AddInt64(&c.hits, 1)
	} else {
		atomic.AddInt64(&c.misses, 1)
	}
	return val, ok
}

func (c *Cache) Set(key, value string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.items) >= c.maxSize {
		for k := range c.items {
			delete(c.items, k)
			break
		}
	}
	c.items[key] = value
}

func (c *Cache) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.items, key)
}

func NewCache(maxSize int) *Cache {
	return &Cache{
		items:   make(map[string]string),
		maxSize: maxSize,
	}
}

type Server struct {
	db    *sql.DB
	cache *Cache
}

func main() {
	connStr := "user=postgres password=R@jat010120 host=localhost port=5432 dbname=kv_store"

	db, err := sql.Open("pgx", connStr)
	if err != nil {
		log.Fatalf("Failed to open database connection: %v", err)
	}

	if err := db.Ping(); err != nil {
		log.Fatalf("Failed to ping database: %v", err)
	}

	createTableSQL := `
	CREATE TABLE IF NOT EXISTS kv_store (
		key TEXT PRIMARY KEY,
		value TEXT
	);`

	if _, err := db.Exec(createTableSQL); err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}

	s := &Server{
		db:    db,
		cache: NewCache(1000),
	}

	go func() {
		for {
			time.Sleep(5 * time.Second)
			h := atomic.LoadInt64(&s.cache.hits)
			m := atomic.LoadInt64(&s.cache.misses)
			total := h + m
			if total > 0 {
				rate := float64(h) / float64(total) * 100
				log.Printf("Cache Hits: %d | Misses: %d | Hit Rate: %.2f%%", h, m, rate)
			}
		}
	}()

	http.HandleFunc("/kv/", s.kvHandler)
	fmt.Println("Server starting on port 8080...")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func (s *Server) kvHandler(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/kv/")
	if key == "" {
		http.Error(w, "Key is missing", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case "GET":
		s.handleGet(w, r, key)
	case "PUT":
		s.handlePut(w, r, key)
	case "DELETE":
		s.handleDelete(w, r, key)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request, key string) {
	val, ok := s.cache.Get(key)
	if ok {
		fmt.Println("Cache HIT for key:", key)
		w.Write([]byte(val))
		return
	}

	fmt.Println("Cache MISS for key:", key)
	var valueFromDB string
	err := s.db.QueryRow("SELECT value FROM kv_store WHERE key = $1", key).Scan(&valueFromDB)
	if err != nil {
		if err == sql.ErrNoRows {
			http.Error(w, "Key not found", http.StatusNotFound)
		} else {
			http.Error(w, "Database error", http.StatusInternalServerError)
		}
		return
	}

	s.cache.Set(key, valueFromDB)
	w.Write([]byte(valueFromDB))
}

func (s *Server) handlePut(w http.ResponseWriter, r *http.Request, key string) {
	body := make([]byte, 1024*1024)
	n, _ := r.Body.Read(body)
	value := string(body[:n])

	_, err := s.db.Exec(`
		INSERT INTO kv_store (key, value) VALUES ($1, $2)
		ON CONFLICT (key) DO UPDATE SET value = $2`,
		key, value)

	if err != nil {
		http.Error(w, "Database error", http.StatusInternalServerError)
		return
	}

	s.cache.Set(key, value)
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request, key string) {
	_, err := s.db.Exec("DELETE FROM kv_store WHERE key = $1", key)
	if err != nil {
		http.Error(w, "Database error", http.StatusInternalServerError)
		return
	}
	s.cache.Delete(key)
	w.WriteHeader(http.StatusOK)
}
