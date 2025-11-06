# Software Engineering Improvements for PostgreSQL Archiver

**Review Date:** 2025-10-22
**Project:** PostgreSQL Archiver CLI Tool
**Go Version:** 1.21+
**Current LOC:** ~5,600 lines (cmd package only)

---

## Executive Summary

This Go-based CLI tool for archiving PostgreSQL partitioned tables to S3 has a solid foundation but suffers from common technical debt issues found in rapidly developed projects. The main concerns are:

1. **Monolithic design** - archiver.go (945 lines) mixes DB, S3, caching, and business logic
2. **Limited test coverage** - Only 3 test files, core archiver logic untested
3. **Frontend coupling** - 1,376 lines of HTML/CSS/JS embedded as Go string
4. **Missing abstractions** - Tight coupling to AWS SDK, no storage interface
5. **Error handling gaps** - Silent failures, no structured logging or retry logic

**Estimated Technical Debt:** 4-6 weeks to address major issues
**Risk Level:** Medium (works but fragile for changes/maintenance)

---

## 1. Code Modularization Plan

### Current Structure Issues

```
cmd/
├── archiver.go (945 lines) ⚠️ TOO LARGE
│   - Database connection
│   - S3 operations
│   - Partition discovery
│   - Data extraction
│   - Compression
│   - Upload logic
│   - MD5 calculations
│   - Cache integration
├── progress.go (1,101 lines) ⚠️ TOO LARGE
├── cache_viewer_html.go (1,376 lines) ⚠️ EMBEDDED HTML
```

### Proposed Modular Structure

```
postgresql-archiver/
├── cmd/
│   ├── root.go           # CLI root command
│   ├── archive.go        # Archive command (orchestration only)
│   ├── cache_viewer.go   # Cache viewer command
│   └── version.go        # Version command
├── internal/
│   ├── archiver/
│   │   ├── archiver.go           # Main orchestrator
│   │   ├── config.go             # Configuration
│   │   ├── partition.go          # Partition discovery logic
│   │   └── processor.go          # Partition processing
│   ├── storage/
│   │   ├── storage.go            # Storage interface
│   │   ├── s3.go                 # S3 implementation
│   │   ├── gcs.go                # (Future) GCS implementation
│   │   └── local.go              # (Testing) Local filesystem
│   ├── database/
│   │   ├── postgres.go           # PostgreSQL operations
│   │   ├── connection.go         # Connection management
│   │   └── extractor.go          # Data extraction
│   ├── compression/
│   │   ├── compressor.go         # Compression interface
│   │   └── zstd.go               # Zstd implementation
│   ├── cache/
│   │   ├── cache.go              # Cache operations
│   │   ├── metadata.go           # Metadata structures
│   │   └── store.go              # File-based store
│   ├── ui/
│   │   ├── progress.go           # TUI progress display
│   │   └── messages.go           # UI message types
│   └── viewer/
│       ├── server.go             # HTTP server
│       ├── websocket.go          # WebSocket handling
│       └── handlers.go           # HTTP handlers
├── web/
│   ├── index.html                # Viewer HTML (separate file)
│   ├── style.css                 # Viewer styles
│   └── app.js                    # Viewer JavaScript
├── pkg/
│   └── retry/
│       └── retry.go              # Retry logic (reusable)
└── test/
    ├── integration/              # Integration tests
    │   └── archiver_test.go
    └── fixtures/                 # Test data
        └── sample_data.sql
```

### Migration Strategy

**Phase 1: Extract Interfaces (Week 1)**
```go
// internal/storage/storage.go
package storage

import (
    "context"
    "io"
)

// Provider defines storage operations
type Provider interface {
    // Upload uploads data to storage
    Upload(ctx context.Context, key string, data io.Reader, size int64, metadata map[string]string) error

    // Exists checks if object exists and returns size and ETag
    Exists(ctx context.Context, key string) (exists bool, size int64, etag string, err error)

    // Download downloads data from storage
    Download(ctx context.Context, key string) (io.ReadCloser, error)

    // Delete removes an object
    Delete(ctx context.Context, key string) error
}

// Config holds storage configuration
type Config struct {
    Provider   string // "s3", "gcs", "azure", "local"
    Endpoint   string
    Bucket     string
    Region     string
    AccessKey  string
    SecretKey  string
}

// New creates a storage provider based on config
func New(cfg Config) (Provider, error) {
    switch cfg.Provider {
    case "s3":
        return NewS3Provider(cfg)
    case "local":
        return NewLocalProvider(cfg)
    default:
        return nil, fmt.Errorf("unsupported provider: %s", cfg.Provider)
    }
}
```

**Phase 2: Extract Database Layer (Week 1)**
```go
// internal/database/postgres.go
package database

import (
    "context"
    "database/sql"
)

// Client wraps PostgreSQL operations
type Client struct {
    db     *sql.DB
    config Config
}

// Config holds database configuration
type Config struct {
    Host     string
    Port     int
    User     string
    Password string
    Database string
    SSLMode  string
}

// New creates a new database client
func New(cfg Config) (*Client, error) {
    connStr := fmt.Sprintf(
        "host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
        cfg.Host, cfg.Port, cfg.User, cfg.Password, cfg.Database, cfg.SSLMode,
    )

    db, err := sql.Open("postgres", connStr)
    if err != nil {
        return nil, fmt.Errorf("failed to open database: %w", err)
    }

    if err := db.Ping(); err != nil {
        db.Close()
        return nil, fmt.Errorf("failed to ping database: %w", err)
    }

    return &Client{db: db, config: cfg}, nil
}

// DiscoverPartitions finds table partitions matching pattern and date range
func (c *Client) DiscoverPartitions(ctx context.Context, baseTable string, startDate, endDate time.Time) ([]Partition, error) {
    // Implementation moved from archiver.go
}

// ExtractPartitionData extracts all rows from a partition as JSON
func (c *Client) ExtractPartitionData(ctx context.Context, partition Partition, progress ProgressFunc) ([]byte, error) {
    // Implementation moved from archiver.go
}

// CheckPermissions verifies read permissions on table
func (c *Client) CheckPermissions(ctx context.Context, tableName string) error {
    // Implementation moved from archiver.go
}

// Close closes the database connection
func (c *Client) Close() error {
    return c.db.Close()
}

// Partition represents a table partition
type Partition struct {
    TableName string
    Date      time.Time
    RowCount  int64
}

// ProgressFunc reports extraction progress
type ProgressFunc func(current, total int64)
```

**Phase 3: Extract Compression Layer (Week 1)**
```go
// internal/compression/compressor.go
package compression

import "io"

// Compressor defines compression operations
type Compressor interface {
    Compress(data []byte) ([]byte, error)
    Decompress(data []byte) ([]byte, error)
    CompressStream(src io.Reader, dst io.Writer) error
    DecompressStream(src io.Reader, dst io.Writer) error
}

// internal/compression/zstd.go
package compression

import (
    "bytes"
    "github.com/klauspost/compress/zstd"
)

type ZstdCompressor struct {
    level       zstd.EncoderLevel
    concurrency int
}

func NewZstd(level zstd.EncoderLevel, concurrency int) *ZstdCompressor {
    return &ZstdCompressor{
        level:       level,
        concurrency: concurrency,
    }
}

func (z *ZstdCompressor) Compress(data []byte) ([]byte, error) {
    var buffer bytes.Buffer
    encoder, err := zstd.NewWriter(&buffer,
        zstd.WithEncoderLevel(z.level),
        zstd.WithEncoderConcurrency(z.concurrency))
    if err != nil {
        return nil, err
    }
    defer encoder.Close()

    if _, err := encoder.Write(data); err != nil {
        return nil, err
    }

    if err := encoder.Close(); err != nil {
        return nil, err
    }

    return buffer.Bytes(), nil
}
```

**Phase 4: Refactor Main Archiver (Week 2)**
```go
// internal/archiver/archiver.go
package archiver

import (
    "context"
    "fmt"
    "postgresql-archiver/internal/cache"
    "postgresql-archiver/internal/compression"
    "postgresql-archiver/internal/database"
    "postgresql-archiver/internal/storage"
)

// Archiver orchestrates the archival process
type Archiver struct {
    db          *database.Client
    storage     storage.Provider
    compressor  compression.Compressor
    cache       *cache.Cache
    config      Config
    logger      *slog.Logger
}

// Config holds archiver configuration
type Config struct {
    Table       string
    StartDate   time.Time
    EndDate     time.Time
    DryRun      bool
    Workers     int
    SkipCount   bool
}

// New creates a new archiver instance
func New(
    db *database.Client,
    storage storage.Provider,
    compressor compression.Compressor,
    cache *cache.Cache,
    config Config,
    logger *slog.Logger,
) *Archiver {
    return &Archiver{
        db:         db,
        storage:    storage,
        compressor: compressor,
        cache:      cache,
        config:     config,
        logger:     logger,
    }
}

// Run executes the archival process
func (a *Archiver) Run(ctx context.Context, ui UI) error {
    // Discover partitions
    partitions, err := a.db.DiscoverPartitions(ctx, a.config.Table, a.config.StartDate, a.config.EndDate)
    if err != nil {
        return fmt.Errorf("failed to discover partitions: %w", err)
    }

    ui.SetTotal(len(partitions))

    // Process each partition
    results := make([]Result, 0, len(partitions))
    for i, partition := range partitions {
        result := a.ProcessPartition(ctx, partition, ui)
        results = append(results, result)
        ui.UpdateProgress(i+1, result)
    }

    return nil
}

// ProcessPartition processes a single partition
func (a *Archiver) ProcessPartition(ctx context.Context, partition database.Partition, ui UI) Result {
    // Check cache
    // Extract data
    // Compress
    // Upload
    // Update cache
}

// UI interface for progress reporting (allows TUI or JSON output)
type UI interface {
    SetTotal(count int)
    UpdateProgress(current int, result Result)
    ShowError(err error)
}

// Result holds processing result for a partition
type Result struct {
    Partition    database.Partition
    Success      bool
    Skipped      bool
    SkipReason   string
    Error        error
    BytesWritten int64
    Duration     time.Duration
}
```

---

## 2. Test Strategy

### Current Coverage Analysis

```bash
# Current test coverage (estimated from inspection)
config.go:        ~80% (config_test.go covers validation)
cache.go:         ~60% (cache_test.go covers basic operations)
pid.go:           ~70% (pid_test.go covers file operations)
archiver.go:      ~0%  ⚠️ NO TESTS
progress.go:      ~0%  ⚠️ NO TESTS
cache_server.go:  ~0%  ⚠️ NO TESTS
```

### Proposed Test Structure

```
test/
├── unit/
│   ├── storage/
│   │   ├── s3_test.go
│   │   └── local_test.go
│   ├── database/
│   │   └── postgres_test.go
│   ├── compression/
│   │   └── zstd_test.go
│   ├── cache/
│   │   └── cache_test.go
│   └── archiver/
│       └── processor_test.go
├── integration/
│   ├── archiver_test.go
│   ├── cache_viewer_test.go
│   └── testdata/
│       └── sample.sql
├── e2e/
│   └── full_archive_test.go
└── fixtures/
    └── postgres_test_helpers.go
```

### Unit Test Examples

**Database Layer Tests:**
```go
// internal/database/postgres_test.go
package database

import (
    "context"
    "testing"
    "time"

    "github.com/DATA-DOG/go-sqlmock"
)

func TestDiscoverPartitions(t *testing.T) {
    db, mock, err := sqlmock.New()
    if err != nil {
        t.Fatalf("failed to create mock: %v", err)
    }
    defer db.Close()

    client := &Client{db: db}

    // Mock the query
    rows := sqlmock.NewRows([]string{"tablename"}).
        AddRow("events_20240101").
        AddRow("events_20240102")

    mock.ExpectQuery("SELECT tablename FROM pg_tables").
        WithArgs("events_%").
        WillReturnRows(rows)

    ctx := context.Background()
    partitions, err := client.DiscoverPartitions(ctx, "events", time.Time{}, time.Time{})

    if err != nil {
        t.Errorf("unexpected error: %v", err)
    }

    if len(partitions) != 2 {
        t.Errorf("expected 2 partitions, got %d", len(partitions))
    }

    if err := mock.ExpectationsWereMet(); err != nil {
        t.Errorf("unfulfilled expectations: %v", err)
    }
}

func TestExtractPartitionData(t *testing.T) {
    db, mock, err := sqlmock.New()
    if err != nil {
        t.Fatalf("failed to create mock: %v", err)
    }
    defer db.Close()

    client := &Client{db: db}

    // Mock the extraction query
    rows := sqlmock.NewRows([]string{"row_to_json"}).
        AddRow(`{"id":1,"data":"test1"}`).
        AddRow(`{"id":2,"data":"test2"}`)

    mock.ExpectQuery("SELECT row_to_json").WillReturnRows(rows)

    ctx := context.Background()
    partition := Partition{TableName: "events_20240101"}

    data, err := client.ExtractPartitionData(ctx, partition, nil)

    if err != nil {
        t.Errorf("unexpected error: %v", err)
    }

    if len(data) == 0 {
        t.Error("expected data, got empty")
    }

    // Verify JSON formatting
    lines := strings.Split(string(data), "\n")
    if len(lines) < 2 {
        t.Errorf("expected at least 2 lines, got %d", len(lines))
    }
}
```

**Storage Layer Tests:**
```go
// internal/storage/s3_test.go
package storage

import (
    "bytes"
    "context"
    "testing"
)

func TestS3Upload(t *testing.T) {
    // Use localstack or mock S3 client
    cfg := Config{
        Endpoint: "http://localhost:4566", // LocalStack
        Bucket:   "test-bucket",
        Region:   "us-east-1",
    }

    provider, err := NewS3Provider(cfg)
    if err != nil {
        t.Skipf("S3 not available: %v", err)
    }

    ctx := context.Background()
    key := "test/file.txt"
    data := []byte("test data")

    err = provider.Upload(ctx, key, bytes.NewReader(data), int64(len(data)), nil)
    if err != nil {
        t.Errorf("upload failed: %v", err)
    }

    // Verify exists
    exists, size, _, err := provider.Exists(ctx, key)
    if err != nil {
        t.Errorf("exists check failed: %v", err)
    }

    if !exists {
        t.Error("expected object to exist")
    }

    if size != int64(len(data)) {
        t.Errorf("expected size %d, got %d", len(data), size)
    }
}
```

**Integration Tests:**
```go
// test/integration/archiver_test.go
//go:build integration

package integration

import (
    "context"
    "database/sql"
    "testing"

    _ "github.com/lib/pq"
)

func TestFullArchiveWorkflow(t *testing.T) {
    // Requires PostgreSQL and S3/LocalStack
    if testing.Short() {
        t.Skip("skipping integration test")
    }

    // Setup test database
    db := setupTestDB(t)
    defer db.Close()

    // Create test partitions
    createTestPartitions(t, db)

    // Setup test S3
    storage := setupTestStorage(t)

    // Run archiver
    archiver := setupArchiver(t, db, storage)

    ctx := context.Background()
    err := archiver.Run(ctx, &testUI{})

    if err != nil {
        t.Errorf("archiver failed: %v", err)
    }

    // Verify uploads
    verifyS3Objects(t, storage)
}

func setupTestDB(t *testing.T) *sql.DB {
    connStr := "host=localhost port=5432 user=test password=test dbname=test sslmode=disable"
    db, err := sql.Open("postgres", connStr)
    if err != nil {
        t.Fatalf("failed to connect to test DB: %v", err)
    }
    return db
}

func createTestPartitions(t *testing.T, db *sql.DB) {
    // Create partitioned table
    _, err := db.Exec(`
        CREATE TABLE IF NOT EXISTS events (
            id SERIAL,
            data TEXT,
            created_at TIMESTAMP
        ) PARTITION BY RANGE (created_at);

        CREATE TABLE events_20240101 PARTITION OF events
        FOR VALUES FROM ('2024-01-01') TO ('2024-01-02');

        INSERT INTO events_20240101 (data, created_at)
        VALUES ('test', '2024-01-01');
    `)
    if err != nil {
        t.Fatalf("failed to create test data: %v", err)
    }
}
```

### Test Coverage Goals

| Component | Current | Target | Priority |
|-----------|---------|--------|----------|
| Database layer | 0% | 80% | HIGH |
| Storage layer | 0% | 80% | HIGH |
| Archiver core | 0% | 75% | HIGH |
| Compression | 0% | 90% | MEDIUM |
| Cache | 60% | 85% | MEDIUM |
| UI/Progress | 0% | 40% | LOW |
| HTTP/WebSocket | 0% | 60% | MEDIUM |

---

## 3. Refactoring Roadmap

### Priority Matrix

```
High Impact, Low Effort (Do First):
1. Extract storage interface [Week 1]
2. Add structured logging [Week 1]
3. Separate HTML to files [Week 1]
4. Add context.Context to all operations [Week 1]

High Impact, Medium Effort:
5. Modularize archiver.go [Week 2]
6. Add comprehensive tests [Week 2-3]
7. Implement retry logic [Week 2]
8. Add error wrapping [Week 2]

Medium Impact, Medium Effort:
9. Refactor progress.go UI [Week 3]
10. Add observability (metrics) [Week 3]
11. Improve cache design [Week 3]

Low Impact (Technical Debt):
12. Consolidate config naming [Week 4]
13. Add code generation for viewer [Week 4]
14. Optimize imports [Week 4]
```

### Detailed Refactoring Steps

**Week 1: Foundation**

**Day 1-2: Add Context Support**
```go
// Before (archiver.go)
func (a *Archiver) connect() error {
    db, err := sql.Open("postgres", connStr)
    // ...
}

// After
func (a *Archiver) connect(ctx context.Context) error {
    db, err := sql.Open("postgres", connStr)
    if err != nil {
        return err
    }

    // Use context for ping with timeout
    ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
    defer cancel()

    if err := db.PingContext(ctx); err != nil {
        db.Close()
        return fmt.Errorf("failed to ping database: %w", err)
    }

    a.db = db
    return nil
}
```

**Day 3-4: Add Structured Logging**
```go
// Use standard library log/slog (Go 1.21+)
import "log/slog"

// internal/archiver/archiver.go
type Archiver struct {
    db     *database.Client
    storage storage.Provider
    logger *slog.Logger // Add logger
}

func (a *Archiver) ProcessPartition(ctx context.Context, partition Partition) Result {
    logger := a.logger.With(
        "partition", partition.TableName,
        "date", partition.Date,
    )

    logger.Info("starting partition processing")

    // Extract data
    data, err := a.db.ExtractPartitionData(ctx, partition, nil)
    if err != nil {
        logger.Error("extraction failed", "error", err)
        return Result{Error: err}
    }

    logger.Info("extraction completed",
        "bytes", len(data),
        "rows", partition.RowCount)

    // ... rest of processing
}
```

**Day 5: Extract HTML to Separate Files**
```bash
# Create web directory
mkdir -p web/{css,js}

# Extract HTML
cat > web/index.html <<'EOF'
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>PostgreSQL Archiver - Cache Viewer</title>
    <link rel="stylesheet" href="css/style.css">
</head>
<body>
    <div id="app"></div>
    <script src="js/app.js"></script>
</body>
</html>
EOF
```

```go
// Use go:embed to embed files at build time
// internal/viewer/server.go
package viewer

import (
    "embed"
    "io/fs"
    "net/http"
)

//go:embed web/*
var webFS embed.FS

func serveCacheViewer(w http.ResponseWriter, r *http.Request) {
    fsys, _ := fs.Sub(webFS, "web")
    http.FileServer(http.FS(fsys)).ServeHTTP(w, r)
}
```

**Week 2: Core Refactoring**

**Extract Storage Interface** (already shown above)

**Add Retry Logic:**
```go
// pkg/retry/retry.go
package retry

import (
    "context"
    "time"
)

type Config struct {
    MaxAttempts int
    InitialDelay time.Duration
    MaxDelay time.Duration
    Multiplier float64
}

// Do executes fn with exponential backoff retry
func Do(ctx context.Context, cfg Config, fn func() error) error {
    var lastErr error
    delay := cfg.InitialDelay

    for attempt := 0; attempt < cfg.MaxAttempts; attempt++ {
        if attempt > 0 {
            select {
            case <-ctx.Done():
                return ctx.Err()
            case <-time.After(delay):
            }

            // Exponential backoff
            delay = time.Duration(float64(delay) * cfg.Multiplier)
            if delay > cfg.MaxDelay {
                delay = cfg.MaxDelay
            }
        }

        if err := fn(); err != nil {
            lastErr = err
            continue
        }

        return nil
    }

    return fmt.Errorf("failed after %d attempts: %w", cfg.MaxAttempts, lastErr)
}

// Usage in archiver
func (a *Archiver) uploadWithRetry(ctx context.Context, key string, data []byte) error {
    return retry.Do(ctx, retry.Config{
        MaxAttempts: 3,
        InitialDelay: 1 * time.Second,
        MaxDelay: 10 * time.Second,
        Multiplier: 2.0,
    }, func() error {
        return a.storage.Upload(ctx, key, bytes.NewReader(data), int64(len(data)), nil)
    })
}
```

---

## 4. Design Patterns & Better Abstractions

### Current Issues

1. **No Dependency Injection** - Hard to test, tightly coupled
2. **Mixed Concerns** - Business logic mixed with infrastructure
3. **No Strategy Pattern** - Compression/storage hardcoded
4. **God Object** - Archiver does everything

### Recommended Patterns

**1. Dependency Injection (Constructor Injection)**
```go
// Before
type Archiver struct {
    config *Config
    db     *sql.DB
    s3Client *s3.S3
}

func NewArchiver(config *Config) *Archiver {
    a := &Archiver{config: config}
    a.connect() // Side effect in constructor
    return a
}

// After - Explicit dependencies
type Archiver struct {
    db         database.Client
    storage    storage.Provider
    compressor compression.Compressor
    cache      cache.Store
    logger     *slog.Logger
}

func New(
    db database.Client,
    storage storage.Provider,
    compressor compression.Compressor,
    cache cache.Store,
    logger *slog.Logger,
) *Archiver {
    return &Archiver{
        db:         db,
        storage:    storage,
        compressor: compressor,
        cache:      cache,
        logger:     logger,
    }
}

// Easy to test with mocks
func TestArchiver(t *testing.T) {
    mockDB := &mockDatabase{}
    mockStorage := &mockStorage{}
    mockCompressor := &mockCompressor{}
    mockCache := &mockCache{}
    logger := slog.Default()

    archiver := New(mockDB, mockStorage, mockCompressor, mockCache, logger)
    // Test with full control
}
```

**2. Strategy Pattern for Compression**
```go
// Already shown above in compression layer
// Allows easy swapping: zstd, gzip, lz4, snappy

func NewCompressor(algorithm string, level int) (compression.Compressor, error) {
    switch algorithm {
    case "zstd":
        return compression.NewZstd(level, runtime.NumCPU()), nil
    case "gzip":
        return compression.NewGzip(level), nil
    default:
        return nil, fmt.Errorf("unsupported algorithm: %s", algorithm)
    }
}
```

**3. Factory Pattern for Storage**
```go
// Already shown in storage section
// Allows: S3, GCS, Azure, Local, Memory (for tests)
```

**4. Observer Pattern for Progress**
```go
// internal/archiver/observer.go
package archiver

type Event string

const (
    EventPartitionStart    Event = "partition.start"
    EventPartitionComplete Event = "partition.complete"
    EventExtractStart      Event = "extract.start"
    EventExtractProgress   Event = "extract.progress"
    EventCompressStart     Event = "compress.start"
    EventUploadStart       Event = "upload.start"
    EventUploadComplete    Event = "upload.complete"
    EventError             Event = "error"
)

type Observer interface {
    OnEvent(event Event, data interface{})
}

type Archiver struct {
    // ... existing fields
    observers []Observer
}

func (a *Archiver) AddObserver(o Observer) {
    a.observers = append(a.observers, o)
}

func (a *Archiver) notify(event Event, data interface{}) {
    for _, observer := range a.observers {
        observer.OnEvent(event, data)
    }
}

// Usage
func (a *Archiver) ProcessPartition(ctx context.Context, p Partition) Result {
    a.notify(EventPartitionStart, p)

    // Process...

    a.notify(EventPartitionComplete, result)
    return result
}

// TUI implements Observer
type TUIObserver struct {
    program *tea.Program
}

func (t *TUIObserver) OnEvent(event Event, data interface{}) {
    switch event {
    case EventExtractProgress:
        progress := data.(ProgressData)
        t.program.Send(updateProgressMsg(progress))
    // ... other events
    }
}

// Metrics implements Observer
type MetricsObserver struct {
    registry *prometheus.Registry
}

func (m *MetricsObserver) OnEvent(event Event, data interface{}) {
    switch event {
    case EventPartitionComplete:
        result := data.(Result)
        m.recordPartitionProcessed(result)
    }
}
```

**5. Builder Pattern for Configuration**
```go
// internal/archiver/config.go
type ConfigBuilder struct {
    config Config
}

func NewConfigBuilder() *ConfigBuilder {
    return &ConfigBuilder{
        config: Config{
            Workers: runtime.NumCPU(),
        },
    }
}

func (b *ConfigBuilder) WithTable(table string) *ConfigBuilder {
    b.config.Table = table
    return b
}

func (b *ConfigBuilder) WithDateRange(start, end time.Time) *ConfigBuilder {
    b.config.StartDate = start
    b.config.EndDate = end
    return b
}

func (b *ConfigBuilder) WithDryRun(dryRun bool) *ConfigBuilder {
    b.config.DryRun = dryRun
    return b
}

func (b *ConfigBuilder) Build() (Config, error) {
    if err := b.config.Validate(); err != nil {
        return Config{}, err
    }
    return b.config, nil
}

// Usage
config, err := NewConfigBuilder().
    WithTable("events").
    WithDateRange(startDate, endDate).
    WithDryRun(false).
    Build()
```

**6. Repository Pattern for Cache**
```go
// internal/cache/repository.go
package cache

import "context"

type Repository interface {
    // Metadata operations
    GetMetadata(ctx context.Context, partition string) (*Metadata, error)
    SaveMetadata(ctx context.Context, partition string, meta *Metadata) error

    // Row count operations
    GetRowCount(ctx context.Context, partition string) (int64, bool, error)
    SaveRowCount(ctx context.Context, partition string, count int64) error

    // File metadata
    GetFileMetadata(ctx context.Context, partition string) (*FileMetadata, error)
    SaveFileMetadata(ctx context.Context, partition string, meta *FileMetadata) error

    // Cleanup
    CleanExpired(ctx context.Context, maxAge time.Duration) error
}

// File-based implementation
type FileRepository struct {
    basePath string
}

// Memory-based implementation (for tests)
type MemoryRepository struct {
    data map[string]*Metadata
    mu   sync.RWMutex
}
```

---

## 5. Error Handling Improvements

### Current Issues

```go
// Issue 1: Silent failures
_ = RemovePIDFile()

// Issue 2: Generic errors
return fmt.Errorf("extraction failed: %w", err)

// Issue 3: No context
return err

// Issue 4: Lost error details
if err := rows.Scan(&tableName); err != nil {
    continue // Silently skip
}
```

### Improved Error Handling

**1. Custom Error Types**
```go
// internal/errors/errors.go
package errors

import "fmt"

// ErrorCode represents error categories
type ErrorCode string

const (
    ErrCodeDatabase    ErrorCode = "DATABASE"
    ErrCodeStorage     ErrorCode = "STORAGE"
    ErrCodeCompression ErrorCode = "COMPRESSION"
    ErrCodePermission  ErrorCode = "PERMISSION"
    ErrCodeValidation  ErrorCode = "VALIDATION"
    ErrCodeNetwork     ErrorCode = "NETWORK"
)

// ArchiverError wraps errors with context
type ArchiverError struct {
    Code       ErrorCode
    Message    string
    Err        error
    Partition  string
    Retryable  bool
    StackTrace string
}

func (e *ArchiverError) Error() string {
    if e.Partition != "" {
        return fmt.Sprintf("[%s] %s (partition: %s): %v", e.Code, e.Message, e.Partition, e.Err)
    }
    return fmt.Sprintf("[%s] %s: %v", e.Code, e.Message, e.Err)
}

func (e *ArchiverError) Unwrap() error {
    return e.Err
}

// New creates a new ArchiverError
func New(code ErrorCode, message string, err error) *ArchiverError {
    return &ArchiverError{
        Code:    code,
        Message: message,
        Err:     err,
    }
}

// Retryable marks an error as retryable
func Retryable(err error) error {
    if ae, ok := err.(*ArchiverError); ok {
        ae.Retryable = true
        return ae
    }
    return &ArchiverError{
        Code:      ErrCodeNetwork,
        Message:   "retryable error",
        Err:       err,
        Retryable: true,
    }
}
```

**2. Error Wrapping with Context**
```go
// internal/archiver/processor.go
func (a *Archiver) ProcessPartition(ctx context.Context, p Partition) Result {
    logger := a.logger.With("partition", p.TableName)

    // Extract
    data, err := a.db.ExtractPartitionData(ctx, p, nil)
    if err != nil {
        err = &errors.ArchiverError{
            Code:      errors.ErrCodeDatabase,
            Message:   "failed to extract partition data",
            Err:       err,
            Partition: p.TableName,
            Retryable: false,
        }
        logger.Error("extraction failed", "error", err)
        return Result{Error: err}
    }

    // Compress
    compressed, err := a.compressor.Compress(data)
    if err != nil {
        err = &errors.ArchiverError{
            Code:      errors.ErrCodeCompression,
            Message:   "failed to compress data",
            Err:       err,
            Partition: p.TableName,
            Retryable: true,
        }
        logger.Error("compression failed", "error", err)
        return Result{Error: err}
    }

    // Upload with retry
    err = a.uploadWithRetry(ctx, p, compressed)
    if err != nil {
        err = &errors.ArchiverError{
            Code:      errors.ErrCodeStorage,
            Message:   "failed to upload after retries",
            Err:       err,
            Partition: p.TableName,
            Retryable: false,
        }
        logger.Error("upload failed", "error", err)
        return Result{Error: err}
    }

    return Result{Success: true, BytesWritten: int64(len(compressed))}
}
```

**3. Structured Logging**
```go
// Use log/slog with consistent fields
logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
    Level: slog.LevelInfo,
}))

logger = logger.With(
    "service", "postgresql-archiver",
    "version", version,
)

// In processing
logger.Info("partition processing started",
    "partition", partition.TableName,
    "date", partition.Date,
    "row_count", partition.RowCount,
)

logger.Error("processing failed",
    "partition", partition.TableName,
    "error", err,
    "error_code", errorCode,
    "retryable", isRetryable,
    "elapsed_ms", elapsed.Milliseconds(),
)
```

**4. Panic Recovery**
```go
// internal/archiver/archiver.go
func (a *Archiver) ProcessPartition(ctx context.Context, p Partition) (result Result) {
    // Recover from panics
    defer func() {
        if r := recover(); r != nil {
            a.logger.Error("panic in partition processing",
                "partition", p.TableName,
                "panic", r,
                "stack", string(debug.Stack()),
            )
            result = Result{
                Error: fmt.Errorf("panic: %v", r),
            }
        }
    }()

    // Actual processing
    return a.processPartitionInternal(ctx, p)
}
```

**5. Retry with Backoff**
```go
// Use retry package (shown earlier)
func (a *Archiver) uploadWithRetry(ctx context.Context, p Partition, data []byte) error {
    key := a.buildS3Key(p)

    return retry.Do(ctx, retry.Config{
        MaxAttempts:  3,
        InitialDelay: 1 * time.Second,
        MaxDelay:     10 * time.Second,
        Multiplier:   2.0,
    }, func() error {
        err := a.storage.Upload(ctx, key, bytes.NewReader(data), int64(len(data)), nil)
        if err != nil {
            // Check if error is retryable
            if isRetryableError(err) {
                a.logger.Warn("upload failed, will retry",
                    "partition", p.TableName,
                    "error", err,
                )
                return err
            }
            // Non-retryable, fail immediately
            return retry.Permanent(err)
        }
        return nil
    })
}

func isRetryableError(err error) bool {
    // Network errors, timeouts, 5xx errors are retryable
    if errors.Is(err, context.DeadlineExceeded) {
        return true
    }
    // Check for AWS SDK errors
    // ...
    return false
}
```

---

## 6. Front-end Extraction Plan

### Current State
- **1,376 lines** of HTML/CSS/JS embedded in `cache_viewer_html.go`
- Hard to maintain, no syntax highlighting, no linting
- Can't use modern build tools (Vite, webpack, etc.)

### Proposed Structure

```
web/
├── package.json              # npm dependencies
├── vite.config.js           # Build config
├── src/
│   ├── index.html           # Main HTML
│   ├── main.js              # Entry point
│   ├── App.vue              # Main component (or App.jsx for React)
│   ├── components/
│   │   ├── CacheTable.vue
│   │   ├── StatusCard.vue
│   │   └── Metrics.vue
│   └── styles/
│       └── main.css
└── dist/                     # Build output (gitignored)
```

### Migration Steps

**Step 1: Extract to Separate Files**
```bash
# Extract HTML, CSS, JS from cache_viewer_html.go
mkdir -p web/src web/dist

# Create HTML
cat > web/src/index.html <<'EOF'
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>PostgreSQL Archiver - Cache Viewer</title>
</head>
<body>
    <div id="app"></div>
    <script type="module" src="/main.js"></script>
</body>
</html>
EOF

# Extract CSS to main.css
# Extract JS to main.js
```

**Step 2: Setup Build System (Optional - for dev experience)**
```json
// web/package.json
{
  "name": "postgresql-archiver-viewer",
  "version": "1.0.0",
  "type": "module",
  "scripts": {
    "dev": "vite",
    "build": "vite build",
    "preview": "vite preview"
  },
  "devDependencies": {
    "vite": "^5.0.0"
  }
}
```

```js
// web/vite.config.js
import { defineConfig } from 'vite'

export default defineConfig({
  build: {
    outDir: 'dist',
    rollupOptions: {
      output: {
        manualChunks: undefined
      }
    }
  }
})
```

**Step 3: Embed Built Files**
```go
// internal/viewer/embed.go
package viewer

import (
    "embed"
    "io/fs"
    "net/http"
)

//go:embed dist/*
var distFS embed.FS

func GetWebFS() (fs.FS, error) {
    return fs.Sub(distFS, "dist")
}

// internal/viewer/server.go
func serveCacheViewer(w http.ResponseWriter, r *http.Request) {
    webFS, err := GetWebFS()
    if err != nil {
        http.Error(w, "Failed to load web assets", http.StatusInternalServerError)
        return
    }

    http.FileServer(http.FS(webFS)).ServeHTTP(w, r)
}
```

**Step 4: Update Build Process**
```makefile
# Makefile
.PHONY: build-web
build-web:
	cd web && npm install && npm run build

.PHONY: build
build: build-web
	go build -o postgresql-archiver

.PHONY: dev
dev:
	# Run web dev server and Go backend concurrently
	cd web && npm run dev &
	go run main.go cache-viewer --port 8080
```

**Step 5: Development Workflow**
```bash
# For development (hot reload)
cd web && npm run dev    # Frontend on :5173
go run main.go cache-viewer --port 8080  # Backend proxy to :5173

# For production build
make build-web
go build
```

### Alternative: Keep Simple but Separate

If you want to avoid npm/build tools:

```
web/
├── index.html
├── style.css
└── app.js
```

```go
//go:embed web/*
var webFS embed.FS

// Simpler, no build step needed
```

**Effort Estimate:** 2-4 hours (simple extraction) to 1-2 days (full build system)

---

## 7. Performance Optimization Opportunities

### Current Performance Analysis

**Bottlenecks Identified:**

1. **Sequential Processing** - Partitions processed one at a time
2. **No Connection Pooling** - Single DB connection
3. **Memory Allocation** - Large byte slices allocated repeatedly
4. **No Streaming** - Entire partition loaded into memory
5. **MD5 Calculation** - Done on full compressed data in memory

### Optimizations

**1. Parallel Partition Processing**
```go
// internal/archiver/archiver.go
func (a *Archiver) Run(ctx context.Context, ui UI) error {
    partitions, err := a.db.DiscoverPartitions(ctx, a.config.Table, a.config.StartDate, a.config.EndDate)
    if err != nil {
        return err
    }

    // Process partitions in parallel with worker pool
    workers := a.config.Workers
    if workers == 0 {
        workers = runtime.NumCPU()
    }

    jobs := make(chan Partition, len(partitions))
    results := make(chan Result, len(partitions))

    // Start workers
    var wg sync.WaitGroup
    for i := 0; i < workers; i++ {
        wg.Add(1)
        go func() {
            defer wg.Done()
            for partition := range jobs {
                result := a.ProcessPartition(ctx, partition, ui)
                results <- result
            }
        }()
    }

    // Send jobs
    for _, partition := range partitions {
        jobs <- partition
    }
    close(jobs)

    // Wait for completion
    go func() {
        wg.Wait()
        close(results)
    }()

    // Collect results
    for result := range results {
        a.logger.Info("partition processed",
            "partition", result.Partition.TableName,
            "success", result.Success)
    }

    return nil
}
```

**2. Streaming Data Processing**
```go
// internal/database/extractor.go
func (c *Client) ExtractPartitionDataStream(
    ctx context.Context,
    partition Partition,
    w io.Writer,
) (int64, error) {
    query := fmt.Sprintf("SELECT row_to_json(t) FROM %s t", partition.TableName)

    rows, err := c.db.QueryContext(ctx, query)
    if err != nil {
        return 0, err
    }
    defer rows.Close()

    var totalBytes int64
    encoder := json.NewEncoder(w)

    for rows.Next() {
        var jsonData json.RawMessage
        if err := rows.Scan(&jsonData); err != nil {
            return totalBytes, err
        }

        // Stream encode
        if err := encoder.Encode(jsonData); err != nil {
            return totalBytes, err
        }
        totalBytes += int64(len(jsonData))
    }

    return totalBytes, rows.Err()
}

// internal/archiver/processor.go
func (a *Archiver) ProcessPartition(ctx context.Context, p Partition) Result {
    // Create streaming pipeline: DB -> Compress -> S3
    pr, pw := io.Pipe()

    // Extraction + Compression in goroutine
    errChan := make(chan error, 1)
    go func() {
        defer pw.Close()

        // Compress directly from DB stream
        compressor, err := zstd.NewWriter(pw, zstd.WithEncoderLevel(zstd.SpeedBetterCompression))
        if err != nil {
            errChan <- err
            return
        }
        defer compressor.Close()

        // Extract directly into compressor
        _, err = a.db.ExtractPartitionDataStream(ctx, p, compressor)
        errChan <- err
    }()

    // Upload from pipe reader (streaming)
    key := a.buildS3Key(p)
    err := a.storage.Upload(ctx, key, pr, -1, nil) // -1 = unknown size, use chunked upload
    if err != nil {
        return Result{Error: err}
    }

    // Check extraction error
    if err := <-errChan; err != nil {
        return Result{Error: err}
    }

    return Result{Success: true}
}
```

**3. Connection Pooling**
```go
// internal/database/postgres.go
func New(cfg Config) (*Client, error) {
    // ... connection string

    db, err := sql.Open("postgres", connStr)
    if err != nil {
        return nil, err
    }

    // Configure connection pool
    db.SetMaxOpenConns(25)
    db.SetMaxIdleConns(5)
    db.SetConnMaxLifetime(5 * time.Minute)
    db.SetConnMaxIdleTime(2 * time.Minute)

    // Verify connectivity
    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
    defer cancel()

    if err := db.PingContext(ctx); err != nil {
        db.Close()
        return nil, err
    }

    return &Client{db: db, config: cfg}, nil
}
```

**4. Buffer Pool for Memory Reuse**
```go
// internal/archiver/pools.go
package archiver

import "sync"

var (
    // Pool for large byte slices
    bufferPool = sync.Pool{
        New: func() interface{} {
            buf := make([]byte, 1024*1024) // 1MB buffer
            return &buf
        },
    }
)

func getBuffer() *[]byte {
    return bufferPool.Get().(*[]byte)
}

func putBuffer(buf *[]byte) {
    // Reset slice but keep capacity
    *buf = (*buf)[:0]
    bufferPool.Put(buf)
}

// Usage in extraction
func (c *Client) ExtractPartitionData(ctx context.Context, p Partition) ([]byte, error) {
    buf := getBuffer()
    defer putBuffer(buf)

    // Use buffer...
}
```

**5. Incremental MD5 Calculation**
```go
// Calculate MD5 while streaming, not after
func (a *Archiver) ProcessPartitionStreaming(ctx context.Context, p Partition) Result {
    pr, pw := io.Pipe()

    // MD5 hasher
    hasher := md5.New()

    // Multi-writer: compress and hash simultaneously
    multiWriter := io.MultiWriter(pw, hasher)

    go func() {
        defer pw.Close()
        _, err := a.db.ExtractPartitionDataStream(ctx, p, multiWriter)
        // handle err
    }()

    // Upload
    err := a.storage.Upload(ctx, key, pr, -1, nil)
    if err != nil {
        return Result{Error: err}
    }

    // Get MD5 after streaming completes
    md5Sum := hex.EncodeToString(hasher.Sum(nil))

    return Result{Success: true, MD5: md5Sum}
}
```

**6. Cache Optimizations**
```go
// Use sync.Map for concurrent cache access
type Cache struct {
    data sync.Map // map[string]CacheEntry
}

func (c *Cache) Get(key string) (CacheEntry, bool) {
    val, ok := c.data.Load(key)
    if !ok {
        return CacheEntry{}, false
    }
    return val.(CacheEntry), true
}

func (c *Cache) Set(key string, entry CacheEntry) {
    c.data.Store(key, entry)
}
```

### Performance Benchmarks

```go
// benchmark_test.go
func BenchmarkExtractSequential(b *testing.B) {
    // Test current implementation
}

func BenchmarkExtractStreaming(b *testing.B) {
    // Test streaming implementation
}

func BenchmarkExtractParallel(b *testing.B) {
    // Test parallel processing
}
```

**Expected Improvements:**
- **Sequential → Parallel:** 3-5x speedup (with 4-8 workers)
- **Memory → Streaming:** 50-90% memory reduction
- **Connection pooling:** 20-30% throughput increase
- **Buffer pooling:** 10-20% reduction in GC pressure

---

## 8. Dependency Management Review

### Current Dependencies (go.mod)

```
Direct Dependencies:
- github.com/aws/aws-sdk-go v1.50.0          ⚠️ Old SDK (v1)
- github.com/charmbracelet/bubbletea v0.25.0 ✓ Up to date
- github.com/charmbracelet/lipgloss v0.10.0  ✓ Up to date
- github.com/klauspost/compress v1.17.4      ✓ Good choice
- github.com/lib/pq v1.10.9                  ⚠️ Maintenance mode
- github.com/spf13/cobra v1.8.0              ✓ Standard CLI lib
- github.com/spf13/viper v1.18.2             ✓ Good for config
- github.com/gorilla/websocket v1.5.3        ✓ Mature
- github.com/fsnotify/fsnotify v1.7.0        ✓ Good for file watching
```

### Issues & Recommendations

**1. AWS SDK v1 → v2**

The project uses AWS SDK v1, which is in maintenance mode. Migrate to v2:

```bash
go get github.com/aws/aws-sdk-go-v2
go get github.com/aws/aws-sdk-go-v2/config
go get github.com/aws/aws-sdk-go-v2/service/s3
```

```go
// Before (v1)
import (
    "github.com/aws/aws-sdk-go/aws"
    "github.com/aws/aws-sdk-go/aws/session"
    "github.com/aws/aws-sdk-go/service/s3"
)

sess, err := session.NewSession(&aws.Config{
    Endpoint: aws.String(endpoint),
    Region:   aws.String(region),
})
client := s3.New(sess)

// After (v2)
import (
    "context"
    "github.com/aws/aws-sdk-go-v2/aws"
    "github.com/aws/aws-sdk-go-v2/config"
    "github.com/aws/aws-sdk-go-v2/service/s3"
)

cfg, err := config.LoadDefaultConfig(context.TODO(),
    config.WithRegion(region),
)
client := s3.NewFromConfig(cfg, func(o *s3.Options) {
    o.BaseEndpoint = aws.String(endpoint)
    o.UsePathStyle = true
})

// Upload
_, err = client.PutObject(ctx, &s3.PutObjectInput{
    Bucket: aws.String(bucket),
    Key:    aws.String(key),
    Body:   reader,
})
```

**Benefits:**
- Modular imports (smaller binaries)
- Better context support
- Improved error handling
- Active development

**Effort:** 1-2 days

**2. lib/pq → pgx**

`lib/pq` is in maintenance mode. Consider migrating to `pgx`:

```bash
go get github.com/jackc/pgx/v5
```

```go
// Before (lib/pq)
import (
    "database/sql"
    _ "github.com/lib/pq"
)

db, err := sql.Open("postgres", connStr)

// After (pgx with stdlib)
import (
    "database/sql"
    _ "github.com/jackc/pgx/v5/stdlib"
)

db, err := sql.Open("pgx", connStr)

// Or use pgx directly for better performance
import "github.com/jackc/pgx/v5/pgxpool"

pool, err := pgxpool.New(ctx, connStr)
```

**Benefits:**
- Better performance (30-50% faster)
- Native PostgreSQL types
- Connection pooling
- Active development

**Effort:** 1-2 days

**3. Add Observability Dependencies**

```bash
# Structured logging (already in stdlib)
# Use log/slog (Go 1.21+)

# Metrics (optional)
go get github.com/prometheus/client_golang

# Tracing (optional)
go get go.opentelemetry.io/otel
```

**4. Add Testing Dependencies**

```bash
# Better assertions
go get github.com/stretchr/testify

# DB mocking (already have go-sqlmock)
# Good to keep

# HTTP testing
go get github.com/golang/mock
```

**5. Dependency Audit**

```bash
# Check for vulnerabilities
go install golang.org/x/vuln/cmd/govulncheck@latest
govulncheck ./...

# Check for outdated dependencies
go install github.com/psampaz/go-mod-outdated@latest
go list -u -m -json all | go-mod-outdated -update -direct

# Update all to latest
go get -u ./...
go mod tidy
```

### Recommended go.mod (Updated)

```go
module github.com/airframesio/postgresql-archiver

go 1.21

require (
    // Database
    github.com/jackc/pgx/v5 v5.5.1 // or keep v1.10.9 for minimal change

    // Storage
    github.com/aws/aws-sdk-go-v2 v1.24.0
    github.com/aws/aws-sdk-go-v2/config v1.26.0
    github.com/aws/aws-sdk-go-v2/service/s3 v1.47.0

    // CLI & Config
    github.com/spf13/cobra v1.8.0
    github.com/spf13/viper v1.18.2

    // UI
    github.com/charmbracelet/bubbletea v0.25.0
    github.com/charmbracelet/lipgloss v0.10.0

    // Compression
    github.com/klauspost/compress v1.17.4

    // Web
    github.com/gorilla/websocket v1.5.3
    github.com/fsnotify/fsnotify v1.7.0

    // Testing
    github.com/DATA-DOG/go-sqlmock v1.5.2
    github.com/stretchr/testify v1.8.4
)
```

---

## 9. Additional Recommendations

### 1. Add Observability

**Prometheus Metrics:**
```go
// internal/metrics/metrics.go
package metrics

import (
    "github.com/prometheus/client_golang/prometheus"
    "github.com/prometheus/client_golang/prometheus/promauto"
)

var (
    partitionsProcessed = promauto.NewCounterVec(
        prometheus.CounterOpts{
            Name: "archiver_partitions_processed_total",
            Help: "Total number of partitions processed",
        },
        []string{"table", "status"},
    )

    processingDuration = promauto.NewHistogramVec(
        prometheus.HistogramOpts{
            Name:    "archiver_partition_processing_duration_seconds",
            Help:    "Time spent processing partitions",
            Buckets: prometheus.ExponentialBuckets(1, 2, 10),
        },
        []string{"table"},
    )

    bytesProcessed = promauto.NewCounterVec(
        prometheus.CounterOpts{
            Name: "archiver_bytes_processed_total",
            Help: "Total bytes processed",
        },
        []string{"table", "stage"},
    )
)

// Usage
func (a *Archiver) ProcessPartition(ctx context.Context, p Partition) Result {
    start := time.Now()
    defer func() {
        processingDuration.WithLabelValues(a.config.Table).Observe(time.Since(start).Seconds())
    }()

    // ... processing

    partitionsProcessed.WithLabelValues(a.config.Table, "success").Inc()
    bytesProcessed.WithLabelValues(a.config.Table, "compressed").Add(float64(result.BytesWritten))

    return result
}
```

### 2. Add Health Checks

```go
// cmd/health.go
var healthCmd = &cobra.Command{
    Use:   "health",
    Short: "Check system health",
    RunE:  runHealth,
}

func runHealth(cmd *cobra.Command, args []string) error {
    // Check DB connectivity
    db, err := connectDB(config)
    if err != nil {
        fmt.Printf("❌ Database: FAIL (%v)\n", err)
    } else {
        fmt.Println("✅ Database: OK")
        db.Close()
    }

    // Check S3 connectivity
    storage, err := connectS3(config)
    if err != nil {
        fmt.Printf("❌ S3: FAIL (%v)\n", err)
    } else {
        fmt.Println("✅ S3: OK")
    }

    // Check cache
    cache, err := loadCache()
    if err != nil {
        fmt.Printf("⚠️  Cache: WARN (%v)\n", err)
    } else {
        fmt.Println("✅ Cache: OK")
    }

    return nil
}
```

### 3. Add Configuration Validation Command

```go
// cmd/validate.go
var validateCmd = &cobra.Command{
    Use:   "validate",
    Short: "Validate configuration",
    RunE:  runValidate,
}

func runValidate(cmd *cobra.Command, args []string) error {
    config, err := loadConfig()
    if err != nil {
        return err
    }

    if err := config.Validate(); err != nil {
        fmt.Printf("❌ Configuration invalid: %v\n", err)
        return err
    }

    fmt.Println("✅ Configuration valid")
    return nil
}
```

### 4. Add Documentation

```
docs/
├── architecture.md      # System design
├── configuration.md     # All config options
├── development.md       # Dev setup
├── deployment.md        # Production deployment
├── troubleshooting.md   # Common issues
└── api.md              # Cache viewer API
```

### 5. Add Pre-commit Hooks

```bash
# .pre-commit-config.yaml
repos:
  - repo: https://github.com/dnephin/pre-commit-golang
    rev: master
    hooks:
      - id: go-fmt
      - id: go-vet
      - id: go-lint
      - id: go-unit-tests
      - id: go-mod-tidy
```

---

## 10. Effort Estimates

### Summary Table

| Task | Priority | Effort | Impact | Dependencies |
|------|----------|--------|--------|--------------|
| Extract storage interface | HIGH | 2 days | HIGH | None |
| Add structured logging | HIGH | 1 day | HIGH | None |
| Separate HTML files | HIGH | 4 hours | MEDIUM | None |
| Add context.Context | HIGH | 1 day | HIGH | None |
| Write database tests | HIGH | 3 days | HIGH | Extract interface |
| Modularize archiver.go | HIGH | 4 days | HIGH | Interfaces done |
| Add retry logic | HIGH | 2 days | HIGH | None |
| Migrate AWS SDK v2 | MEDIUM | 2 days | MEDIUM | Extract storage |
| Add parallel processing | MEDIUM | 3 days | HIGH | Tests done |
| Add streaming | MEDIUM | 3 days | HIGH | Parallel done |
| Refactor progress UI | MEDIUM | 3 days | LOW | None |
| Add metrics | LOW | 2 days | MEDIUM | None |
| Migrate to pgx | LOW | 2 days | MEDIUM | Tests done |
| Full documentation | LOW | 3 days | MEDIUM | None |

### Phased Approach

**Phase 1: Foundation (2 weeks)**
- Extract interfaces (storage, database, compression)
- Add structured logging with slog
- Add context.Context everywhere
- Write comprehensive tests
- Separate HTML from Go code

**Phase 2: Refactoring (2 weeks)**
- Modularize archiver.go into packages
- Implement retry logic with backoff
- Add proper error handling
- Migrate AWS SDK to v2
- Add configuration validation

**Phase 3: Performance (1 week)**
- Implement parallel processing
- Add streaming data pipeline
- Optimize memory usage
- Add connection pooling

**Phase 4: Observability (1 week)**
- Add Prometheus metrics
- Add health checks
- Improve logging
- Add tracing (optional)

**Total Estimated Time:** 6-8 weeks (full-time) or 12-16 weeks (part-time)

---

## Conclusion

This PostgreSQL archiver is a functional tool with solid foundations, but suffers from common technical debt accumulated during rapid development. The main issues are:

1. **Monolithic design** - Needs modularization
2. **Testing gaps** - Critical paths untested
3. **Tight coupling** - No interfaces, hard to change
4. **Error handling** - Silent failures, generic errors
5. **Frontend coupling** - HTML embedded in Go

**Recommended Priorities:**

1. **Quick wins (Week 1):**
   - Extract HTML to separate files
   - Add structured logging
   - Add retry logic
   - Add context.Context

2. **Foundation (Weeks 2-3):**
   - Extract storage/database interfaces
   - Write comprehensive tests
   - Modularize archiver.go

3. **Performance (Weeks 4-5):**
   - Parallel processing
   - Streaming data
   - Connection pooling

4. **Production-ready (Week 6):**
   - Metrics
   - Health checks
   - Documentation

This refactoring will make the codebase maintainable, testable, and performant for long-term use.
