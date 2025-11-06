package cmd

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/gorilla/websocket"
	"github.com/spf13/cobra"
)

var (
	serverPort int
	upgrader   = websocket.Upgrader{
		CheckOrigin: func(_ *http.Request) bool {
			return true // Allow all origins for local development
		},
	}

	// WebSocket client manager
	clients   = make(map[*websocket.Conn]bool)
	clientsMu sync.RWMutex
	broadcast = make(chan interface{}, 100)

	// Ensure background goroutines are started only once
	startOnce sync.Once
)

var cacheServerCmd = &cobra.Command{
	Use:   "cache-viewer",
	Short: "Start a web server to view cache data",
	Long:  `Starts a local web server that provides a beautiful interface for viewing and monitoring the archiver's cache data.`,
	RunE:  runCacheServer,
}

func init() {
	rootCmd.AddCommand(cacheServerCmd)
	cacheServerCmd.Flags().IntVarP(&serverPort, "port", "p", 8080, "Port to run the web server on")
}

type CacheResponse struct {
	Tables    []TableCache `json:"tables"`
	Timestamp time.Time    `json:"timestamp"`
}

type TableCache struct {
	TableName string       `json:"tableName"`
	Entries   []CacheEntry `json:"entries"`
}

type CacheEntry struct {
	Table            string    `json:"table"`
	Partition        string    `json:"partition"`
	RowCount         int64     `json:"rowCount"`
	CountTime        time.Time `json:"countTime"`
	FileSize         int64     `json:"fileSize"`
	UncompressedSize int64     `json:"uncompressedSize"`
	FileMD5          string    `json:"fileMD5"`
	FileTime         time.Time `json:"fileTime"`
	S3Key            string    `json:"s3Key"`
	S3Uploaded       bool      `json:"s3Uploaded"`
	S3UploadTime     time.Time `json:"s3UploadTime"`
	LastError        string    `json:"lastError"`
	ErrorTime        time.Time `json:"errorTime"`
}

type StatusResponse struct {
	ArchiverRunning bool      `json:"archiverRunning"`
	PID             int       `json:"pid,omitempty"`
	CurrentTask     *TaskInfo `json:"currentTask,omitempty"`
	Version         string    `json:"version"`
	UpdateAvailable bool      `json:"updateAvailable"`
	LatestVersion   string    `json:"latestVersion,omitempty"`
	ReleaseURL      string    `json:"releaseUrl,omitempty"`
}

// WebSocket message types
type WSMessage struct {
	Type string      `json:"type"`
	Data interface{} `json:"data"`
}

// startBackgroundServices ensures broadcast manager and data monitor are started only once
func startBackgroundServices() {
	startOnce.Do(func() {
		go broadcastManager()
		go dataMonitor()
	})
}

func runCacheServer(_ *cobra.Command, _ []string) error {
	// Set up HTTP routes
	http.HandleFunc("/", serveCacheViewer)
	http.HandleFunc("/api/cache", serveCacheData)
	http.HandleFunc("/api/status", serveStatusData)
	http.HandleFunc("/ws", handleWebSocket)

	// Start background goroutines (only starts once even if called multiple times)
	startBackgroundServices()

	addr := fmt.Sprintf(":%d", serverPort)
	// Initialize logger if not already initialized
	if logger == nil {
		initLogger(false, "text") // Default to text format for cache viewer
	}
	logger.Info("")
	logger.Info("🚀 PostgreSQL Archiver Cache Viewer")
	logger.Info(fmt.Sprintf("📊 Starting web server on http://localhost%s", addr))
	logger.Info("🌐 Open your browser to view cache data")
	logger.Info("⌨️  Press Ctrl+C to stop the server\n")

	server := &http.Server{
		Addr:              addr,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
	}
	return server.ListenAndServe()
}

func serveCacheViewer(w http.ResponseWriter, _ *http.Request) {
	html := getCacheViewerHTML()
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(html))
}

func serveCacheData(w http.ResponseWriter, _ *http.Request) {
	// Enable CORS for local development
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-Type", "application/json")

	// Get cache directory
	homeDir, _ := os.UserHomeDir()
	cacheDir := filepath.Join(homeDir, ".postgresql-archiver", "cache")

	// Read all cache files
	files, err := os.ReadDir(cacheDir)
	if err != nil {
		if os.IsNotExist(err) {
			// Cache directory doesn't exist
			_ = json.NewEncoder(w).Encode(CacheResponse{
				Tables:    []TableCache{},
				Timestamp: time.Now(),
			})
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	response := CacheResponse{
		Tables:    []TableCache{},
		Timestamp: time.Now(),
	}

	// Process each cache file
	for _, file := range files {
		if !strings.HasSuffix(file.Name(), ".json") {
			continue
		}

		filePath := filepath.Join(cacheDir, file.Name())
		data, err := os.ReadFile(filePath)
		if err != nil {
			continue
		}

		// Extract table name from filename
		tableName := strings.TrimSuffix(file.Name(), "_metadata.json")
		tableName = strings.TrimSuffix(tableName, "_counts.json")

		// Try to parse as new format
		var cache PartitionCache
		if err := json.Unmarshal(data, &cache); err == nil && cache.Entries != nil {
			tableCache := TableCache{
				TableName: tableName,
				Entries:   []CacheEntry{},
			}

			for partition, entry := range cache.Entries {
				tableCache.Entries = append(tableCache.Entries, CacheEntry{
					Table:            tableName,
					Partition:        partition,
					RowCount:         entry.RowCount,
					CountTime:        entry.CountTime,
					FileSize:         entry.FileSize,
					UncompressedSize: entry.UncompressedSize,
					FileMD5:          entry.FileMD5,
					FileTime:         entry.FileTime,
					S3Key:            entry.S3Key,
					S3Uploaded:       entry.S3Uploaded,
					S3UploadTime:     entry.S3UploadTime,
					LastError:        entry.LastError,
					ErrorTime:        entry.ErrorTime,
				})
			}

			response.Tables = append(response.Tables, tableCache)
			continue
		}

		// Try to parse as old format
		var oldCache RowCountCache
		if err := json.Unmarshal(data, &oldCache); err == nil && oldCache.Counts != nil {
			tableCache := TableCache{
				TableName: tableName,
				Entries:   []CacheEntry{},
			}

			for partition, entry := range oldCache.Counts {
				tableCache.Entries = append(tableCache.Entries, CacheEntry{
					Table:     tableName,
					Partition: partition,
					RowCount:  entry.Count,
					CountTime: entry.Timestamp,
				})
			}

			response.Tables = append(response.Tables, tableCache)
		}
	}

	_ = json.NewEncoder(w).Encode(response)
}

func serveStatusData(w http.ResponseWriter, _ *http.Request) {
	// Enable CORS for local development
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-Type", "application/json")

	response := StatusResponse{
		ArchiverRunning: false,
		Version:         Version,
	}

	// Add version check information if available
	if versionCheckResult != nil {
		response.UpdateAvailable = versionCheckResult.UpdateAvailable
		response.LatestVersion = versionCheckResult.LatestVersion
		response.ReleaseURL = versionCheckResult.ReleaseURL
	}

	// Check if PID file exists and process is running
	pid, err := ReadPIDFile()
	if err == nil && IsProcessRunning(pid) {
		response.ArchiverRunning = true
		response.PID = pid

		// Try to read task info
		taskInfo, err := ReadTaskInfo()
		if err == nil {
			response.CurrentTask = taskInfo
		}
	}

	_ = json.NewEncoder(w).Encode(response)
}

func getCacheViewerHTML() string {
	return cacheViewerHTML
}

// WebSocket handler
func handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("WebSocket upgrade error: %v", err)
		return
	}
	defer conn.Close()

	// Register client
	clientsMu.Lock()
	clients[conn] = true
	clientsMu.Unlock()

	// Send initial data
	sendCacheData(conn)
	sendStatusData(conn)

	// Clean up on disconnect
	defer func() {
		clientsMu.Lock()
		delete(clients, conn)
		clientsMu.Unlock()
	}()

	// Keep connection alive and handle incoming messages
	for {
		_, _, err := conn.ReadMessage()
		if err != nil {
			break
		}
	}
}

// Broadcast manager sends messages to all connected clients
func broadcastManager() {
	for {
		msg := <-broadcast
		clientsMu.RLock()
		// Collect failed clients while holding read lock
		var failedClients []*websocket.Conn
		for client := range clients {
			err := client.WriteJSON(msg)
			if err != nil {
				failedClients = append(failedClients, client)
			}
		}
		clientsMu.RUnlock()

		// Clean up failed clients with write lock
		if len(failedClients) > 0 {
			clientsMu.Lock()
			for _, client := range failedClients {
				delete(clients, client)
				client.Close()
			}
			clientsMu.Unlock()
		}
	}
}

// setupWatchDirs creates and watches required directories
func setupWatchDirs(watcher *fsnotify.Watcher) {
	homeDir, _ := os.UserHomeDir()
	cacheDir := filepath.Join(homeDir, ".postgresql-archiver", "cache")
	taskDir := filepath.Join(homeDir, ".postgresql-archiver")

	// Create directories if they don't exist
	_ = os.MkdirAll(cacheDir, 0o755)
	_ = os.MkdirAll(taskDir, 0o755)

	// Watch cache directory
	if err := watcher.Add(cacheDir); err != nil {
		log.Printf("Failed to watch cache directory: %v", err)
	}

	// Watch task directory (for task file changes)
	if err := watcher.Add(taskDir); err != nil {
		log.Printf("Failed to watch task directory: %v", err)
	}
}

// handleFileEvent processes file system events
func handleFileEvent(event fsnotify.Event, debounceTimer **time.Timer, debounceDuration time.Duration) {
	// Check if it's a cache or task file
	isCacheFile := strings.HasSuffix(event.Name, ".json") && strings.Contains(event.Name, "cache")
	isTaskFile := strings.HasSuffix(event.Name, "current_task.json")

	if event.Op&(fsnotify.Write|fsnotify.Create|fsnotify.Remove) != 0 {
		if isCacheFile || isTaskFile {
			// Debounce updates to avoid flooding
			if *debounceTimer != nil {
				(*debounceTimer).Stop()
			}
			*debounceTimer = time.AfterFunc(debounceDuration, func() {
				if isCacheFile {
					broadcastCacheUpdate()
				}
				if isTaskFile {
					broadcastStatusUpdate()
				}
			})
		}
	}
}

// Data monitor watches for changes and broadcasts updates using fsnotify
func dataMonitor() {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Printf("Failed to create file watcher, falling back to polling: %v", err)
		dataMonitorFallback()
		return
	}
	defer watcher.Close()

	setupWatchDirs(watcher)

	// Debounce timer to avoid too many updates
	var debounceTimer *time.Timer
	debounceDuration := 100 * time.Millisecond

	for {
		select {
		case event, ok := <-watcher.Events:
			if !ok {
				return
			}
			handleFileEvent(event, &debounceTimer, debounceDuration)

		case err, ok := <-watcher.Errors:
			if !ok {
				return
			}
			log.Printf("File watcher error: %v", err)
		}
	}
}

// dataMonitorFallback is the original polling-based monitor as a fallback
func dataMonitorFallback() {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	var lastCacheModTime time.Time
	var lastTaskModTime time.Time

	for range ticker.C {
		// Check cache files
		homeDir, _ := os.UserHomeDir()
		cacheDir := filepath.Join(homeDir, ".postgresql-archiver", "cache")

		// Check if cache directory was modified
		if info, err := os.Stat(cacheDir); err == nil {
			if info.ModTime().After(lastCacheModTime) {
				lastCacheModTime = info.ModTime()
				broadcastCacheUpdate()
			}
		}

		// Check task file
		taskPath := GetTaskFilePath()
		if info, err := os.Stat(taskPath); err == nil {
			if info.ModTime().After(lastTaskModTime) {
				lastTaskModTime = info.ModTime()
				broadcastStatusUpdate()
			}
		} else {
			// Task file doesn't exist, but maybe it was deleted
			if !lastTaskModTime.IsZero() {
				lastTaskModTime = time.Time{}
				broadcastStatusUpdate()
			}
		}
	}
}

func broadcastCacheUpdate() {
	cacheData := getCacheDataForWS()
	broadcast <- WSMessage{
		Type: "cache",
		Data: cacheData,
	}
}

func broadcastStatusUpdate() {
	statusData := getStatusDataForWS()
	broadcast <- WSMessage{
		Type: "status",
		Data: statusData,
	}
}

func sendCacheData(conn *websocket.Conn) {
	cacheData := getCacheDataForWS()
	_ = conn.WriteJSON(WSMessage{
		Type: "cache",
		Data: cacheData,
	})
}

func sendStatusData(conn *websocket.Conn) {
	statusData := getStatusDataForWS()
	_ = conn.WriteJSON(WSMessage{
		Type: "status",
		Data: statusData,
	})
}

func getCacheDataForWS() CacheResponse {
	// Get cache directory
	homeDir, _ := os.UserHomeDir()
	cacheDir := filepath.Join(homeDir, ".postgresql-archiver", "cache")

	response := CacheResponse{
		Tables:    []TableCache{},
		Timestamp: time.Now(),
	}

	// Read all cache files
	files, err := os.ReadDir(cacheDir)
	if err != nil {
		return response
	}

	// Process each cache file
	for _, file := range files {
		if !strings.HasSuffix(file.Name(), ".json") {
			continue
		}

		filePath := filepath.Join(cacheDir, file.Name())
		data, err := os.ReadFile(filePath)
		if err != nil {
			continue
		}

		// Extract table name from filename
		tableName := strings.TrimSuffix(file.Name(), "_metadata.json")
		tableName = strings.TrimSuffix(tableName, "_counts.json")

		// Try to parse as new format
		var cache PartitionCache
		if err := json.Unmarshal(data, &cache); err == nil && cache.Entries != nil {
			tableCache := TableCache{
				TableName: tableName,
				Entries:   []CacheEntry{},
			}

			for partition, entry := range cache.Entries {
				tableCache.Entries = append(tableCache.Entries, CacheEntry{
					Table:            tableName,
					Partition:        partition,
					RowCount:         entry.RowCount,
					CountTime:        entry.CountTime,
					FileSize:         entry.FileSize,
					UncompressedSize: entry.UncompressedSize,
					FileMD5:          entry.FileMD5,
					FileTime:         entry.FileTime,
					S3Key:            entry.S3Key,
					S3Uploaded:       entry.S3Uploaded,
					S3UploadTime:     entry.S3UploadTime,
					LastError:        entry.LastError,
					ErrorTime:        entry.ErrorTime,
				})
			}

			response.Tables = append(response.Tables, tableCache)
		}
	}

	return response
}

func getStatusDataForWS() StatusResponse {
	response := StatusResponse{
		ArchiverRunning: false,
		Version:         Version,
	}

	// Add version check information if available
	if versionCheckResult != nil {
		response.UpdateAvailable = versionCheckResult.UpdateAvailable
		response.LatestVersion = versionCheckResult.LatestVersion
		response.ReleaseURL = versionCheckResult.ReleaseURL
	}

	// Check if PID file exists and process is running
	pid, err := ReadPIDFile()
	if err == nil && IsProcessRunning(pid) {
		response.ArchiverRunning = true
		response.PID = pid

		// Try to read task info
		taskInfo, err := ReadTaskInfo()
		if err == nil {
			response.CurrentTask = taskInfo
		}
	}

	return response
}
