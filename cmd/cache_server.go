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
	clients   = make(map[*websocket.Conn]*clientWrapper)
	clientsMu sync.RWMutex
	broadcast = make(chan interface{}, 100)

	// Log streaming clients
	logClients   = make(map[*websocket.Conn]*clientWrapper)
	logClientsMu sync.RWMutex
	logBroadcast = make(chan LogMessage, 1000)

	// Ensure background goroutines are started only once
	startOnce sync.Once
)

// clientWrapper wraps a websocket connection with a write mutex to ensure thread-safe writes
type clientWrapper struct {
	conn *websocket.Conn
	mu   sync.Mutex
}

// writeJSON safely writes JSON to the websocket connection with mutex protection
func (cw *clientWrapper) writeJSON(v interface{}) error {
	cw.mu.Lock()
	defer cw.mu.Unlock()
	return cw.conn.WriteJSON(v)
}

var cacheServerCmd = &cobra.Command{
	Use:   "viewer",
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
	MultipartETag    string    `json:"multipartETag,omitempty"` // S3 multipart ETag for files >100MB
	FileTime         time.Time `json:"fileTime"`
	S3Key            string    `json:"s3Key"`
	S3Uploaded       bool      `json:"s3Uploaded"`
	S3UploadTime     time.Time `json:"s3UploadTime"`
	LastError        string    `json:"lastError"`
	ErrorTime        time.Time `json:"errorTime"`
	ProcessStartTime time.Time `json:"processStartTime,omitempty"` // When processing started for this job
}

type StatusResponse struct {
	ArchiverRunning bool      `json:"archiverRunning"`
	PID             int       `json:"pid,omitempty"`
	CurrentTask     *TaskInfo `json:"currentTask,omitempty"`
	Version         string    `json:"version"`
	UpdateAvailable bool      `json:"updateAvailable"`
	LatestVersion   string    `json:"latestVersion,omitempty"`
	ReleaseURL      string    `json:"releaseUrl,omitempty"`
	// Slice tracking fields
	CurrentSliceIndex int    `json:"currentSliceIndex,omitempty"`
	TotalSlices       int    `json:"totalSlices,omitempty"`
	CurrentSliceDate  string `json:"currentSliceDate,omitempty"`
	IsSlicing         bool   `json:"isSlicing"`
}

// WebSocket message types
type WSMessage struct {
	Type string      `json:"type"`
	Data interface{} `json:"data"`
}

type LogMessage struct {
	Timestamp string `json:"timestamp"`
	Level     string `json:"level"`
	Message   string `json:"message"`
}

// startBackgroundServices ensures broadcast manager and data monitor are started only once
func startBackgroundServices() {
	startOnce.Do(func() {
		go broadcastManager()
		go dataMonitor()
	})
}

// logBroadcastManager sends log messages to all connected log clients
func logBroadcastManager() {
	log.Printf("DEBUG: logBroadcastManager started")
	for {
		logMsg := <-logBroadcast
		logClientsMu.RLock()
		clientCount := len(logClients)
		var failedClients []*websocket.Conn
		for conn, wrapper := range logClients {
			err := wrapper.writeJSON(logMsg)
			if err != nil {
				log.Printf("DEBUG: Failed to send log to client: %v", err)
				failedClients = append(failedClients, conn)
			}
		}
		logClientsMu.RUnlock()

		// Debug: log when we send messages
		if clientCount > 0 {
			log.Printf("DEBUG: Sent log message to %d client(s): %s", clientCount, logMsg.Message)
		}

		// Clean up failed clients
		if len(failedClients) > 0 {
			logClientsMu.Lock()
			for _, conn := range failedClients {
				if wrapper, exists := logClients[conn]; exists {
					wrapper.conn.Close()
					delete(logClients, conn)
				}
			}
			logClientsMu.Unlock()
		}
	}
}

// handleLogsWebSocket handles WebSocket connections for log streaming
func handleLogsWebSocket(w http.ResponseWriter, r *http.Request) {
	// Log the incoming request for debugging
	log.Printf("Logs WebSocket connection attempt from %s", r.RemoteAddr)

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("Logs WebSocket upgrade error: %v", err)
		return
	}
	defer conn.Close()

	log.Printf("Logs WebSocket connection established from %s", r.RemoteAddr)

	// Register log client with wrapper
	logClientsMu.Lock()
	logClients[conn] = &clientWrapper{conn: conn}
	logClientsMu.Unlock()

	// Send a test log message to verify the connection works
	// Use a goroutine with a small delay to ensure the client is fully registered
	go func() {
		time.Sleep(100 * time.Millisecond) // Small delay to ensure client is registered
		if logBroadcast != nil {
			testMsg := LogMessage{
				Timestamp: time.Now().Format("2006-01-02 15:04:05"),
				Level:     "INFO",
				Message:   "Log streaming connected successfully",
			}
			select {
			case logBroadcast <- testMsg:
				log.Printf("DEBUG: Sent test log message to channel")
			default:
				log.Printf("DEBUG: Failed to send test log message (channel full)")
			}
		}
	}()

	// Clean up on disconnect
	defer func() {
		logClientsMu.Lock()
		delete(logClients, conn)
		logClientsMu.Unlock()
		log.Printf("Logs WebSocket client disconnected")
	}()

	// Keep connection alive
	for {
		_, _, err := conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("Logs WebSocket error: %v", err)
			}
			break
		}
	}
}

func runCacheServer(_ *cobra.Command, _ []string) error {
	// Set up HTTP routes
	http.HandleFunc("/", serveCacheViewer)
	http.HandleFunc("/api/cache", serveCacheData)
	http.HandleFunc("/api/status", serveStatusData)
	http.HandleFunc("/ws", handleWebSocket)
	http.HandleFunc("/ws/logs", handleLogsWebSocket)

	// Start background goroutines (only starts once even if called multiple times)
	startBackgroundServices()
	go logBroadcastManager()

	addr := fmt.Sprintf(":%d", serverPort)
	// Initialize logger if not already initialized
	// IMPORTANT: logBroadcast must be initialized before calling initLogger
	// so that the broadcastLogHandler can use it
	if logger == nil {
		// Respect debug flag and log format from global flags
		isDebug := false
		logFormat := "text"
		if val, err := rootCmd.PersistentFlags().GetBool("debug"); err == nil {
			isDebug = val
		}
		if val, err := rootCmd.PersistentFlags().GetString("log-format"); err == nil && val != "" {
			logFormat = val
		}
		initLogger(isDebug, logFormat)
	}
	// Wrap logger to broadcast logs to WebSocket clients
	// This ensures logs from the cache viewer itself are also streamed
	// Test that logBroadcast is working by sending a test log
	if logBroadcast != nil {
		log.Printf("DEBUG: logBroadcast channel is initialized, ready to broadcast logs")
	}

	// Generate some test logs to verify broadcasting works
	logger.Info("")
	logger.Info("🚀 Data Archiver Viewer")
	logger.Info(fmt.Sprintf("📊 Starting web server on http://localhost%s", addr))
	logger.Info("🌐 Open your browser to view cache data")
	logger.Info("⌨️  Press Ctrl+C to stop the server\n")

	// Send a test log after a brief delay to ensure the system is ready
	go func() {
		time.Sleep(500 * time.Millisecond)
		logger.Info("✅ Log streaming system initialized and ready")
	}()

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
	cacheDir := filepath.Join(homeDir, ".data-archiver", "cache")

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
					MultipartETag:    entry.MultipartETag,
					FileTime:         entry.FileTime,
					S3Key:            entry.S3Key,
					S3Uploaded:       entry.S3Uploaded,
					S3UploadTime:     entry.S3UploadTime,
					LastError:        entry.LastError,
					ErrorTime:        entry.ErrorTime,
					ProcessStartTime: entry.ProcessStartTime,
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
			// Populate slice tracking fields from TaskInfo
			if taskInfo.TotalSlices > 0 {
				response.CurrentSliceIndex = taskInfo.CurrentSliceIndex
				response.TotalSlices = taskInfo.TotalSlices
				response.CurrentSliceDate = taskInfo.CurrentSliceDate
				response.IsSlicing = true
			}
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

	// Register client with wrapper
	wrapper := &clientWrapper{conn: conn}
	clientsMu.Lock()
	clients[conn] = wrapper
	clientsMu.Unlock()

	// Send initial data
	sendCacheData(wrapper)
	sendStatusData(wrapper)

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
		for conn, wrapper := range clients {
			err := wrapper.writeJSON(msg)
			if err != nil {
				failedClients = append(failedClients, conn)
			}
		}
		clientsMu.RUnlock()

		// Clean up failed clients with write lock
		if len(failedClients) > 0 {
			clientsMu.Lock()
			for _, conn := range failedClients {
				if wrapper, exists := clients[conn]; exists {
					wrapper.conn.Close()
					delete(clients, conn)
				}
			}
			clientsMu.Unlock()
		}
	}
}

// setupWatchDirs creates and watches required directories
func setupWatchDirs(watcher *fsnotify.Watcher) {
	homeDir, _ := os.UserHomeDir()
	cacheDir := filepath.Join(homeDir, ".data-archiver", "cache")
	taskDir := filepath.Join(homeDir, ".data-archiver")

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
	// Cache files are named like {table}_metadata.json in the cache directory
	isCacheFile := strings.HasSuffix(event.Name, "_metadata.json") || strings.HasSuffix(event.Name, "_counts.json")
	isTaskFile := strings.HasSuffix(event.Name, "current_task.json")

	// Handle Write, Create, Rename (atomic writes), and Chmod events
	if event.Op&(fsnotify.Write|fsnotify.Create|fsnotify.Rename|fsnotify.Chmod) != 0 {
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
	debounceDuration := 200 * time.Millisecond // Increased from 100ms to ensure file writes complete

	// Also add a periodic refresh to catch any missed updates (every 2 seconds)
	refreshTicker := time.NewTicker(2 * time.Second)
	defer refreshTicker.Stop()

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

		case <-refreshTicker.C:
			// Periodic refresh to catch any missed file updates
			broadcastCacheUpdate()
			broadcastStatusUpdate()
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
		cacheDir := filepath.Join(homeDir, ".data-archiver", "cache")

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

func sendCacheData(wrapper *clientWrapper) {
	cacheData := getCacheDataForWS()
	_ = wrapper.writeJSON(WSMessage{
		Type: "cache",
		Data: cacheData,
	})
}

func sendStatusData(wrapper *clientWrapper) {
	statusData := getStatusDataForWS()
	_ = wrapper.writeJSON(WSMessage{
		Type: "status",
		Data: statusData,
	})
}

func getCacheDataForWS() CacheResponse {
	// Get cache directory
	homeDir, _ := os.UserHomeDir()
	cacheDir := filepath.Join(homeDir, ".data-archiver", "cache")

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
					MultipartETag:    entry.MultipartETag,
					FileTime:         entry.FileTime,
					S3Key:            entry.S3Key,
					S3Uploaded:       entry.S3Uploaded,
					S3UploadTime:     entry.S3UploadTime,
					LastError:        entry.LastError,
					ErrorTime:        entry.ErrorTime,
					ProcessStartTime: entry.ProcessStartTime,
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
			// Populate slice tracking fields from TaskInfo
			if taskInfo.TotalSlices > 0 {
				response.CurrentSliceIndex = taskInfo.CurrentSliceIndex
				response.TotalSlices = taskInfo.TotalSlices
				response.CurrentSliceDate = taskInfo.CurrentSliceDate
				response.IsSlicing = true
			}
		}
	}

	return response
}
