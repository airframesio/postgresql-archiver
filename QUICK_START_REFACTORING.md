# Quick Start Refactoring Guide

**Start here** if you want to make immediate improvements to the codebase.

This guide shows the **highest impact, lowest effort** changes you can make in the first week.

---

## Day 1: Add Context Support (2 hours)

### Why
- Proper cancellation
- Request timeouts
- Production-ready code

### How

**Step 1:** Update database connection (10 mins)

```go
// cmd/archiver.go - BEFORE
func (a *Archiver) connect() error {
    db, err := sql.Open("postgres", connStr)
    if err != nil {
        return err
    }
    if err := db.Ping(); err != nil {
        db.Close()
        return err
    }
    a.db = db
    return nil
}

// cmd/archiver.go - AFTER
func (a *Archiver) connect(ctx context.Context) error {
    db, err := sql.Open("postgres", connStr)
    if err != nil {
        return err
    }

    // Add timeout for ping
    pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
    defer cancel()

    if err := db.PingContext(pingCtx); err != nil {
        db.Close()
        return fmt.Errorf("failed to ping database: %w", err)
    }

    a.db = db
    return nil
}
```

**Step 2:** Update extraction (20 mins)

```go
// cmd/archiver.go - BEFORE
func (a *Archiver) extractDataWithProgress(partition PartitionInfo, program *tea.Program) ([]byte, error) {
    query := fmt.Sprintf("SELECT row_to_json(t) FROM %s t", partition.TableName)
    rows, err := a.db.Query(query)
    // ...
}

// cmd/archiver.go - AFTER
func (a *Archiver) extractDataWithProgress(ctx context.Context, partition PartitionInfo, program *tea.Program) ([]byte, error) {
    query := fmt.Sprintf("SELECT row_to_json(t) FROM %s t", partition.TableName)

    // Add timeout for long queries
    queryCtx, cancel := context.WithTimeout(ctx, 30*time.Minute)
    defer cancel()

    rows, err := a.db.QueryContext(queryCtx, query)
    if err != nil {
        return nil, fmt.Errorf("query failed: %w", err)
    }
    defer rows.Close()

    // ... rest of extraction
    // Check context in loop
    for rows.Next() {
        // Check if cancelled
        select {
        case <-ctx.Done():
            return nil, ctx.Err()
        default:
        }

        // ... process row
    }
}
```

**Step 3:** Update Run method (10 mins)

```go
// cmd/archiver.go - BEFORE
func (a *Archiver) Run() error {
    // ... existing code
}

// cmd/archiver.go - AFTER
func (a *Archiver) Run(ctx context.Context) error {
    // Create root context with cancellation
    ctx, cancel := context.WithCancel(ctx)
    defer cancel()

    // ... existing code, pass ctx to methods
    if err := a.connect(ctx); err != nil {
        return err
    }
}
```

**Step 4:** Update main (5 mins)

```go
// cmd/root.go or wherever Run is called - BEFORE
if err := archiver.Run(); err != nil {
    return err
}

// cmd/root.go - AFTER
ctx := context.Background()

// Handle Ctrl+C gracefully
ctx, cancel := context.WithCancel(ctx)
defer cancel()

// Setup signal handling
sigChan := make(chan os.Signal, 1)
signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
go func() {
    <-sigChan
    cancel()
}()

if err := archiver.Run(ctx); err != nil {
    if errors.Is(err, context.Canceled) {
        fmt.Println("\nOperation cancelled by user")
        return nil
    }
    return err
}
```

---

## Day 1-2: Add Structured Logging (4 hours)

### Why
- Better debugging
- Production observability
- Log aggregation friendly

### How

**Step 1:** Initialize logger in main (10 mins)

```go
// main.go or cmd/root.go
import (
    "log/slog"
    "os"
)

var logger *slog.Logger

func init() {
    // Development: human-readable text
    if os.Getenv("ENV") == "development" {
        logger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
            Level: slog.LevelDebug,
        }))
    } else {
        // Production: JSON for log aggregation
        logger = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
            Level: slog.LevelInfo,
        }))
    }

    // Set as default
    slog.SetDefault(logger)
}
```

**Step 2:** Add logger to Archiver (10 mins)

```go
// cmd/archiver.go - UPDATE STRUCT
type Archiver struct {
    config       *Config
    db           *sql.DB
    s3Client     *s3.S3
    s3Uploader   *s3manager.Uploader
    progressChan chan tea.Cmd
    logger       *slog.Logger  // ADD THIS
}

// UPDATE CONSTRUCTOR
func NewArchiver(config *Config, logger *slog.Logger) *Archiver {
    return &Archiver{
        config:       config,
        progressChan: make(chan tea.Cmd, 100),
        logger:       logger.With("component", "archiver"),  // ADD THIS
    }
}

// WHEN CREATING ARCHIVER
archiver := NewArchiver(config, logger)
```

**Step 3:** Replace fmt.Printf with structured logs (2 hours)

```go
// BEFORE - Scattered print statements
if a.config.Debug {
    fmt.Printf("  💾 Using cached metadata for %s:\n", partition.TableName)
    fmt.Printf("     Cached: size=%d, md5=%s\n", cachedSize, cachedMD5)
}

// AFTER - Structured logging
a.logger.Debug("using cached metadata",
    "partition", partition.TableName,
    "cached_size", cachedSize,
    "cached_md5", cachedMD5,
)

// BEFORE
fmt.Println(debugStyle.Render(fmt.Sprintf("  🗜️  Compressed: %d → %d bytes (%.1fx ratio)",
    len(data), buffer.Len(), compressionRatio)))

// AFTER
a.logger.Info("compression completed",
    "partition", partition.TableName,
    "uncompressed_bytes", len(data),
    "compressed_bytes", buffer.Len(),
    "ratio", compressionRatio,
)

// BEFORE - Error handling
if err != nil {
    return fmt.Errorf("extraction failed: %w", err)
}

// AFTER - With logging
if err != nil {
    a.logger.Error("extraction failed",
        "partition", partition.TableName,
        "error", err,
    )
    return fmt.Errorf("extraction failed: %w", err)
}
```

**Step 4:** Add request ID for tracing (30 mins)

```go
// cmd/archiver.go
import "github.com/google/uuid"

func (a *Archiver) ProcessPartitionWithProgress(partition PartitionInfo, index int, program *tea.Program) ProcessResult {
    // Generate request ID
    requestID := uuid.New().String()

    // Create logger with partition context
    logger := a.logger.With(
        "request_id", requestID,
        "partition", partition.TableName,
        "partition_date", partition.Date,
        "partition_index", index,
    )

    logger.Info("partition processing started")

    // Use logger throughout
    logger.Debug("extracting data")
    data, err := a.extractDataWithProgress(ctx, partition, program)
    if err != nil {
        logger.Error("extraction failed", "error", err)
        return ProcessResult{Error: err}
    }

    logger.Info("extraction completed", "bytes", len(data))

    // ... rest of processing
    logger.Info("partition processing completed",
        "bytes_written", result.BytesWritten,
        "duration_ms", time.Since(start).Milliseconds(),
    )

    return result
}
```

---

## Day 3: Extract HTML (4 hours)

### Why
- Easier to maintain
- Use proper editor with syntax highlighting
- Separate concerns

### How

**Step 1:** Create web directory structure (5 mins)

```bash
cd /Users/kevin/Cloud/Dropbox/work/airframes/postgresql-archiver

mkdir -p web/css web/js

# Verify
tree web
# web/
# ├── css/
# └── js/
```

**Step 2:** Extract HTML content (1 hour)

```bash
# Open cache_viewer_html.go
# Find the HTML content between backticks
# Copy everything to web/index.html

# Extract inline CSS to web/css/style.css
# Extract inline JavaScript to web/js/app.js
```

**Step 3:** Update Go code to use embedded files (30 mins)

```go
// cmd/cache_viewer_html.go - DELETE THIS FILE ENTIRELY

// cmd/cache_server.go - ADD EMBED
package cmd

import (
    "embed"
    "io/fs"
    "net/http"
    // ... other imports
)

//go:embed web
var webFS embed.FS

func serveCacheViewer(w http.ResponseWriter, r *http.Request) {
    // Serve from embedded filesystem
    fsys, err := fs.Sub(webFS, "web")
    if err != nil {
        http.Error(w, "Failed to load web assets", http.StatusInternalServerError)
        return
    }

    // Serve index.html for root
    if r.URL.Path == "/" {
        r.URL.Path = "/index.html"
    }

    http.FileServer(http.FS(fsys)).ServeHTTP(w, r)
}

// DELETE getCacheViewerHTML() function entirely
```

**Step 4:** Update HTML to load external CSS/JS (30 mins)

```html
<!-- web/index.html -->
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>PostgreSQL Archiver - Cache Viewer</title>
    <!-- Load external CSS -->
    <link rel="stylesheet" href="/css/style.css">
</head>
<body>
    <div id="app">
        <!-- Your existing HTML content -->
    </div>

    <!-- Load external JS -->
    <script src="/js/app.js"></script>
</body>
</html>
```

**Step 5:** Test (30 mins)

```bash
# Rebuild
go build

# Run cache viewer
./postgresql-archiver cache-viewer --port 8080

# Open browser
open http://localhost:8080

# Verify:
# - Page loads correctly
# - CSS is applied
# - JavaScript works
# - WebSocket connects
```

**Step 6:** Remove old file (5 mins)

```bash
# After confirming everything works
git rm cmd/cache_viewer_html.go
git commit -m "refactor: extract HTML/CSS/JS to separate files"
```

---

## Day 4: Add Retry Logic (4 hours)

### Why
- Handle transient failures
- Production reliability
- Better S3 upload success rate

### How

**Step 1:** Create retry package (30 mins)

```go
// pkg/retry/retry.go
package retry

import (
    "context"
    "errors"
    "fmt"
    "time"
)

// Config holds retry configuration
type Config struct {
    MaxAttempts  int
    InitialDelay time.Duration
    MaxDelay     time.Duration
    Multiplier   float64
}

// DefaultConfig returns sensible defaults
func DefaultConfig() Config {
    return Config{
        MaxAttempts:  3,
        InitialDelay: 1 * time.Second,
        MaxDelay:     30 * time.Second,
        Multiplier:   2.0,
    }
}

// PermanentError wraps an error to indicate it should not be retried
type PermanentError struct {
    Err error
}

func (e *PermanentError) Error() string {
    return fmt.Sprintf("permanent error: %v", e.Err)
}

func (e *PermanentError) Unwrap() error {
    return e.Err
}

// Permanent marks an error as non-retryable
func Permanent(err error) error {
    return &PermanentError{Err: err}
}

// Do executes fn with exponential backoff retry
func Do(ctx context.Context, cfg Config, fn func() error) error {
    var lastErr error
    delay := cfg.InitialDelay

    for attempt := 1; attempt <= cfg.MaxAttempts; attempt++ {
        // First attempt or after delay
        if attempt > 1 {
            select {
            case <-ctx.Done():
                return ctx.Err()
            case <-time.After(delay):
                // Calculate next delay
                delay = time.Duration(float64(delay) * cfg.Multiplier)
                if delay > cfg.MaxDelay {
                    delay = cfg.MaxDelay
                }
            }
        }

        // Execute function
        err := fn()
        if err == nil {
            return nil
        }

        // Check if permanent error
        var permErr *PermanentError
        if errors.As(err, &permErr) {
            return permErr.Err
        }

        lastErr = err
    }

    return fmt.Errorf("failed after %d attempts: %w", cfg.MaxAttempts, lastErr)
}
```

**Step 2:** Add retry to S3 uploads (1 hour)

```go
// cmd/archiver.go

import "postgresql-archiver/pkg/retry"

func (a *Archiver) uploadToS3(key string, data []byte) error {
    a.logger.Info("uploading to S3",
        "key", key,
        "size_bytes", len(data),
    )

    // Retry configuration for S3
    retryCfg := retry.Config{
        MaxAttempts:  3,
        InitialDelay: 2 * time.Second,
        MaxDelay:     30 * time.Second,
        Multiplier:   2.0,
    }

    err := retry.Do(context.Background(), retryCfg, func() error {
        // Large file - use multipart
        if len(data) > 100*1024*1024 {
            uploadInput := &s3manager.UploadInput{
                Bucket:      aws.String(a.config.S3.Bucket),
                Key:         aws.String(key),
                Body:        bytes.NewReader(data),
                ContentType: aws.String("application/zstd"),
            }

            _, err := a.s3Uploader.Upload(uploadInput)
            if err != nil {
                a.logger.Warn("multipart upload failed, will retry",
                    "key", key,
                    "error", err,
                )
                return err
            }
        } else {
            // Small file - simple put
            putInput := &s3.PutObjectInput{
                Bucket:      aws.String(a.config.S3.Bucket),
                Key:         aws.String(key),
                Body:        bytes.NewReader(data),
                ContentType: aws.String("application/zstd"),
            }

            _, err := a.s3Client.PutObject(putInput)
            if err != nil {
                // Check if error is retryable
                if isRetryableS3Error(err) {
                    a.logger.Warn("upload failed, will retry",
                        "key", key,
                        "error", err,
                    )
                    return err
                }

                // Non-retryable error (permissions, bucket doesn't exist)
                a.logger.Error("upload failed with non-retryable error",
                    "key", key,
                    "error", err,
                )
                return retry.Permanent(err)
            }
        }

        a.logger.Info("upload successful", "key", key)
        return nil
    })

    if err != nil {
        return fmt.Errorf("S3 upload failed: %w", err)
    }

    return nil
}

// Helper to determine if S3 error is retryable
func isRetryableS3Error(err error) bool {
    if err == nil {
        return false
    }

    // Network errors are retryable
    if errors.Is(err, context.DeadlineExceeded) {
        return true
    }

    // Check AWS error codes
    errMsg := err.Error()
    retryableCodes := []string{
        "RequestTimeout",
        "InternalError",
        "ServiceUnavailable",
        "SlowDown",
        "RequestLimitExceeded",
    }

    for _, code := range retryableCodes {
        if contains(errMsg, code) {
            return true
        }
    }

    return false
}

func contains(s, substr string) bool {
    return len(s) >= len(substr) && (s == substr || len(s) > len(substr) && (s[:len(substr)] == substr || s[len(s)-len(substr):] == substr))
}
```

**Step 3:** Add tests (1 hour)

```go
// pkg/retry/retry_test.go
package retry

import (
    "context"
    "errors"
    "testing"
    "time"
)

func TestRetrySuccess(t *testing.T) {
    attempt := 0
    err := Do(context.Background(), DefaultConfig(), func() error {
        attempt++
        if attempt < 3 {
            return errors.New("temporary failure")
        }
        return nil
    })

    if err != nil {
        t.Errorf("expected success, got: %v", err)
    }

    if attempt != 3 {
        t.Errorf("expected 3 attempts, got: %d", attempt)
    }
}

func TestRetryPermanentError(t *testing.T) {
    attempt := 0
    err := Do(context.Background(), DefaultConfig(), func() error {
        attempt++
        return Permanent(errors.New("permanent failure"))
    })

    if err == nil {
        t.Error("expected error, got nil")
    }

    if attempt != 1 {
        t.Errorf("expected 1 attempt for permanent error, got: %d", attempt)
    }
}

func TestRetryContextCancellation(t *testing.T) {
    ctx, cancel := context.WithCancel(context.Background())

    attempt := 0
    errChan := make(chan error, 1)

    go func() {
        errChan <- Do(ctx, DefaultConfig(), func() error {
            attempt++
            time.Sleep(100 * time.Millisecond)
            return errors.New("failure")
        })
    }()

    // Cancel after first attempt
    time.Sleep(50 * time.Millisecond)
    cancel()

    err := <-errChan
    if !errors.Is(err, context.Canceled) {
        t.Errorf("expected context.Canceled, got: %v", err)
    }
}
```

---

## Day 5: Improve Error Handling (4 hours)

### Why
- Better debugging
- Clearer error messages
- Production troubleshooting

### How

**Step 1:** Create custom errors package (1 hour)

```go
// internal/errors/errors.go
package errors

import "fmt"

type ErrorCode string

const (
    ErrCodeDatabase    ErrorCode = "DATABASE"
    ErrCodeStorage     ErrorCode = "STORAGE"
    ErrCodeCompression ErrorCode = "COMPRESSION"
    ErrCodePermission  ErrorCode = "PERMISSION"
    ErrCodeValidation  ErrorCode = "VALIDATION"
    ErrCodeNetwork     ErrorCode = "NETWORK"
)

type Error struct {
    Code      ErrorCode
    Message   string
    Err       error
    Partition string
    Retryable bool
}

func (e *Error) Error() string {
    if e.Partition != "" {
        return fmt.Sprintf("[%s] %s (partition: %s): %v",
            e.Code, e.Message, e.Partition, e.Err)
    }
    return fmt.Sprintf("[%s] %s: %v", e.Code, e.Message, e.Err)
}

func (e *Error) Unwrap() error {
    return e.Err
}

// Constructors
func Database(msg string, err error) *Error {
    return &Error{Code: ErrCodeDatabase, Message: msg, Err: err}
}

func Storage(msg string, err error) *Error {
    return &Error{Code: ErrCodeStorage, Message: msg, Err: err}
}

func Compression(msg string, err error) *Error {
    return &Error{Code: ErrCodeCompression, Message: msg, Err: err}
}

// With partition context
func (e *Error) WithPartition(partition string) *Error {
    e.Partition = partition
    return e
}

// Mark as retryable
func (e *Error) AsRetryable() *Error {
    e.Retryable = true
    return e
}
```

**Step 2:** Use custom errors in archiver (2 hours)

```go
// cmd/archiver.go

import apperrors "postgresql-archiver/internal/errors"

func (a *Archiver) ProcessPartitionWithProgress(partition PartitionInfo, index int, program *tea.Program) ProcessResult {
    logger := a.logger.With("partition", partition.TableName)

    // Extraction
    data, err := a.extractDataWithProgress(ctx, partition, program)
    if err != nil {
        err = apperrors.Database("failed to extract partition", err).
            WithPartition(partition.TableName)

        logger.Error("extraction failed",
            "error", err,
            "error_code", apperrors.ErrCodeDatabase,
        )

        return ProcessResult{Error: err}
    }

    // Compression
    compressed, err := a.compressData(data)
    if err != nil {
        err = apperrors.Compression("failed to compress data", err).
            WithPartition(partition.TableName).
            AsRetryable()

        logger.Error("compression failed",
            "error", err,
            "error_code", apperrors.ErrCodeCompression,
            "retryable", true,
        )

        return ProcessResult{Error: err}
    }

    // Upload
    key := fmt.Sprintf("export/%s/%s/%s.jsonl.zst",
        a.config.Table,
        partition.Date.Format("2006/01"),
        partition.Date.Format("2006-01-02"))

    if err := a.uploadToS3(key, compressed); err != nil {
        err = apperrors.Storage("failed to upload to S3", err).
            WithPartition(partition.TableName)

        logger.Error("upload failed",
            "error", err,
            "error_code", apperrors.ErrCodeStorage,
            "s3_key", key,
        )

        return ProcessResult{Error: err}
    }

    return ProcessResult{
        Success:      true,
        BytesWritten: int64(len(compressed)),
    }
}
```

---

## Testing Your Changes

After each day, verify:

```bash
# Run tests
go test ./...

# Check for race conditions
go test -race ./...

# Run vet
go vet ./...

# Build
go build -o postgresql-archiver

# Manual test
export PGHOST=localhost
export PGPORT=5432
export PGUSER=postgres
export PGDATABASE=test
export S3_ENDPOINT=http://localhost:9000
export S3_BUCKET=archives

./postgresql-archiver --table events --start-date 2024-01-01 --end-date 2024-01-31
```

---

## Commit Strategy

Commit after each major change:

```bash
# Day 1
git add cmd/archiver.go cmd/root.go
git commit -m "feat: add context.Context support for cancellation and timeouts"

# Day 1-2
git add cmd/ main.go
git commit -m "feat: add structured logging with slog"

# Day 3
git rm cmd/cache_viewer_html.go
git add cmd/cache_server.go web/
git commit -m "refactor: extract HTML/CSS/JS to separate files"

# Day 4
git add pkg/retry/ cmd/archiver.go
git commit -m "feat: add retry logic with exponential backoff"

# Day 5
git add internal/errors/ cmd/archiver.go
git commit -m "feat: add custom error types with context"
```

---

## Next Steps

After completing this week:

1. Review the full `SOFTWARE_ENGINEERING_IMPROVEMENTS.md`
2. Start Phase 2: Interface extraction
3. Write comprehensive tests
4. Add metrics and observability

---

## Getting Help

If you get stuck:

1. Check `SOFTWARE_ENGINEERING_IMPROVEMENTS.md` for detailed examples
2. Check `REFACTORING_CHECKLIST.md` for the complete task list
3. Run `go doc` for package documentation
4. Check existing tests for patterns

Good luck! 🚀
