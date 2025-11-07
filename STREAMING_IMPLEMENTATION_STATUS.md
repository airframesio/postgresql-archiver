# Streaming Architecture Implementation Status

**Last Updated**: 2025-01-07
**Branch**: feature/configurable-output-and-partition-fix
**Status**: Phase 1 & 2 Complete, Phase 3 Partial

## Problem Statement

The current architecture has two critical issues:

1. **Memory Bloat**: Loads entire partitions into memory (10+ GB), causing OOM crashes
2. **Retry Failure**: Statement timeout occurs during `rows.Next()` iteration, outside retry logic wrapper, so retries never happen

## Solution Overview

Implement chunked streaming architecture:
- Process rows in 10,000-row chunks (configurable)
- Stream directly to temp files via formatter → compressor pipeline
- Wrap entire streaming process in retry logic
- Upload completed temp file to S3
- **Memory**: Constant ~100 MB regardless of partition size

---

## ✅ COMPLETED WORK

### Phase 1: Foundation (100% Complete)

#### 1. Configuration
**File**: `cmd/config.go`
- Added `ChunkSize int` field to `Config` struct (line 48)
- Added validation: 100 minimum, 1,000,000 maximum (lines 252-260)
- Added error constants: `ErrChunkSizeMinimum`, `ErrChunkSizeMaximum`

**File**: `cmd/root.go`
- Added `--chunk-size` flag (default: 10000) (line 202)
- Added viper binding for `chunk_size` (line 221)
- Wired into Config struct (line 288)

#### 2. Schema Querying
**File**: `cmd/schema.go` (NEW FILE)
- `ColumnInfo` struct: Stores column name, data_type, udt_name
- `TableSchema` struct: Stores table name and columns
- `getTableSchema(ctx, tableName)`: Queries `information_schema.columns`
- Implements `formatters.ColumnSchema` and `formatters.TableSchema` interfaces

#### 3. Temp File Infrastructure
**File**: `cmd/archiver.go`
- `getTempDir()`: Returns OS temp directory (line 107-112)
- `createTempFile()`: Creates temp file with "data-archiver-*.tmp" prefix (line 114-118)
- `cleanupTempFile(path)`: Removes temp file, ignoring errors (line 120-125)

#### 4. Imports Added
**File**: `cmd/archiver.go`
- Added `hash` package (line 12)
- Added `io` package (line 13)

---

### Phase 2: Streaming Formatters (100% Complete)

#### 1. Streaming Interface
**File**: `cmd/formatters/formatter.go`
- `TableSchema` interface: GetColumns()
- `ColumnSchema` interface: GetName(), GetType()
- `StreamWriter` interface:
  - `WriteChunk(rows []map[string]interface{}) error`
  - `Close() error`
- `StreamingFormatter` interface:
  - `NewWriter(w io.Writer, schema TableSchema) (StreamWriter, error)`
  - `Extension() string`
  - `MIMEType() string`
- `GetStreamingFormatter(format string)`: Factory function

#### 2. JSONL Streaming Formatter
**File**: `cmd/formatters/jsonl.go`
- `JSONLStreamingFormatter` struct
- `NewJSONLStreamingFormatter()`: Constructor
- `jsonlStreamWriter`: Implements StreamWriter
  - `WriteChunk()`: Marshals each row to JSON, writes with newline
  - `Close()`: No-op (JSONL has no footer)
- **Memory**: Constant - writes rows immediately
- **Headers/Footers**: None

#### 3. CSV Streaming Formatter
**File**: `cmd/formatters/csv.go`
- `CSVStreamingFormatter` struct
- `NewCSVStreamingFormatter()`: Constructor
- `csvStreamWriter`: Implements StreamWriter
  - Stores sorted column names from schema
  - Writes CSV header in `NewWriter()`
  - `WriteChunk()`: Writes rows using column order
  - `Close()`: Flushes CSV writer
- **Memory**: Constant - uses buffered CSV writer
- **Headers/Footers**: Header written upfront, no footer

#### 4. Parquet Streaming Formatter
**File**: `cmd/formatters/parquet.go`
- `ParquetStreamingFormatter` struct (supports compression: snappy, zstd, gzip, lz4, none)
- `NewParquetStreamingFormatter()`: Constructor with snappy default
- `NewParquetStreamingFormatterWithCompression(compression)`: Custom compression
- `mapPostgreSQLTypeToParquetNode(udtName)`: Maps PostgreSQL types to Parquet nodes
  - Handles: int2/4/8, float4/8, bool, timestamp/tz, date, varchar, text, json/jsonb, uuid, bytea
  - Default: String type
- `parquetStreamWriter`: Implements StreamWriter
  - Uses `parquet.GenericWriter[map[string]any]`
  - `WriteChunk()`: Calls `writer.Write(rows)` - Parquet handles batching
  - `Close()`: Closes Parquet writer (writes footer metadata)
- **Memory**: Constant - Parquet library buffers row groups internally
- **Headers/Footers**: Schema written upfront, footer with metadata on close

---

### Phase 3: Streaming Extraction (60% Complete)

#### 1. Compressor Streaming Support
**File**: `cmd/compressors/compressor.go`
- Added `NewWriter(w io.Writer, level int) io.WriteCloser` to interface
- Added `nopWriteCloser` helper type (wraps io.Writer with no-op Close)

**File**: `cmd/compressors/zstd.go`
- `NewWriter()`: Creates `zstd.NewWriter()` with encoder level and concurrency

**File**: `cmd/compressors/gzip.go`
- `NewWriter()`: Creates `gzip.NewWriterLevel()` with specified level

**File**: `cmd/compressors/lz4.go`
- `NewWriter()`: Creates `lz4.NewWriter()` with compression level option

**File**: `cmd/compressors/none.go`
- `NewWriter()`: Returns `nopWriteCloser` (pass-through)

#### 2. Core Streaming Extraction Function
**File**: `cmd/archiver.go`
**Function**: `extractPartitionDataStreaming()` (lines 1871-2131)

**Signature**:
```go
func (a *Archiver) extractPartitionDataStreaming(
    partition PartitionInfo,
    program *tea.Program,
    cache *PartitionCache,
    updateTaskStage func(string),
) (tempFilePath string, fileSize int64, md5Hash string, uncompressedSize int64, err error)
```

**Pipeline Architecture**:

For **Parquet** (internal compression):
```
PostgreSQL → JSON rows → formatter → hasher → tempFile
                                      ↓
                                    MD5 hash
```

For **JSONL/CSV** (external compression):
```
PostgreSQL → JSON rows → formatter → compressor → hasher → tempFile
                                                    ↓
                                                  MD5 hash
```

**Implementation Details**:

1. **Schema Query** (lines 1877-1883):
   - Calls `getTableSchema(ctx, partition.TableName)`
   - Returns error if schema query fails

2. **Temp File Creation** (lines 1892-1907):
   - Creates temp file with `createTempFile()`
   - Deferred cleanup on error

3. **Pipeline Setup** (lines 1911-1951):
   - Gets streaming formatter: `GetStreamingFormatter(config.OutputFormat)`
   - Branching logic based on `UsesInternalCompression()`:
     - **Parquet**: `formatter → MultiWriter(tempFile, hasher)`
     - **JSONL/CSV**: `formatter → compressor → MultiWriter(tempFile, hasher)`

4. **Chunked Row Processing** (lines 1959-2075):
   - Query: `SELECT row_to_json(t) FROM table t`
   - Pre-allocate chunk slice with capacity = chunkSize
   - For each row:
     - Scan JSON data
     - Unmarshal to `map[string]interface{}`
     - Append to chunk
     - When chunk full (10,000 rows):
       - Call `streamWriter.WriteChunk(chunk)`
       - Track uncompressed size (approximate)
       - Reset chunk slice (keep capacity)
       - Update progress UI
   - Write final partial chunk

5. **Cleanup** (lines 2077-2098):
   - Close stream writer (flushes formatters, writes footers)
   - Close compressor (if used)
   - Close temp file
   - Get file size from stat
   - Get MD5 hash from hasher

6. **Return Values**:
   - `tempFilePath`: Path to completed temp file
   - `fileSize`: Size of compressed file
   - `md5Hash`: MD5 hex string
   - `uncompressedSize`: Approximate uncompressed size (JSON)
   - `err`: Any error that occurred

**Memory Characteristics**:
- Chunk buffer: ~10,000 rows × ~10 KB/row = ~100 MB max
- Formatter buffer: Minimal (writes immediately)
- Compressor buffer: 4-32 MB (library-specific)
- **Total**: ~150 MB worst case, regardless of partition size

**Error Handling**:
- Context cancellation checked every 100 rows
- Errors during query, scan, unmarshal, or write return immediately
- Deferred cleanup closes writers and removes temp file on error

---

## 🚧 REMAINING WORK

### Phase 3: Integration (40% Remaining)

#### 1. Retry Wrapper Function
**Location**: `cmd/archiver.go` (NEW FUNCTION)
**Name**: `extractPartitionDataWithRetry()`

**Requirements**:
- Wrap `extractPartitionDataStreaming()` in retry loop
- Use `config.Database.MaxRetries` and `config.Database.RetryDelay`
- Check `isRetryableError()` for errors from:
  - Schema query failures
  - PostgreSQL query failures
  - `rows.Err()` errors (statement timeout happens here!)
- On retry:
  - Log warning with attempt count
  - Clean up partial temp file
  - Sleep for retry delay (respect context cancellation)
- Return final error after max retries

**Pseudo-code**:
```go
func (a *Archiver) extractPartitionDataWithRetry(...) (...) {
    maxRetries := a.config.Database.MaxRetries
    retryDelay := time.Duration(a.config.Database.RetryDelay) * time.Second

    for attempt := 0; attempt <= maxRetries; attempt++ {
        tempPath, size, md5, uncompSize, err := a.extractPartitionDataStreaming(...)

        if err == nil {
            return tempPath, size, md5, uncompSize, nil
        }

        // Clean up failed temp file
        cleanupTempFile(tempPath)

        if !isRetryableError(err) {
            return "", 0, "", 0, err
        }

        if attempt < maxRetries {
            a.logger.Warn(fmt.Sprintf("Extraction failed (attempt %d/%d): %v. Retrying in %v...",
                attempt+1, maxRetries+1, err, retryDelay))

            select {
            case <-time.After(retryDelay):
                continue
            case <-a.ctx.Done():
                return "", 0, "", 0, a.ctx.Err()
            }
        }
    }

    return "", 0, "", 0, fmt.Errorf("extraction failed after %d attempts: %w", maxRetries+1, lastErr)
}
```

#### 2. Integration into Processing Flow
**Location**: `cmd/archiver.go`
**Function**: `processSinglePartition()` (around line 1080)

**Current Flow**:
```
processSinglePartition()
  → extractPartitionData()        // Loads everything into memory
      → extractRowsWithProgress() // Returns []map[string]interface{}
      → formatter.Format(rows)    // Formats in memory
      → return []byte
  → compressPartitionData(data)   // Compresses in memory
  → uploadToS3(compressed)
```

**New Flow**:
```
processSinglePartition()
  → extractPartitionDataWithRetry()  // Streams to temp file with retry
      → tempFilePath, size, md5, uncompSize returned
  → uploadTempFileToS3(tempFilePath, objectKey)
  → cleanupTempFile(tempFilePath)
```

**Changes Needed**:
1. Replace call to `extractPartitionData()` with `extractPartitionDataWithRetry()`
2. Remove call to `compressPartitionData()` (now done in streaming)
3. Change `uploadToS3()` to read from temp file instead of byte slice
4. Add cleanup of temp file after successful upload
5. Update cache metadata saving (size, md5 already calculated)

#### 3. Temp File Upload to S3
**Location**: `cmd/archiver.go`
**Function**: `uploadTempFileToS3()` (NEW FUNCTION)

**Requirements**:
- Open temp file for reading
- Check file size for multipart upload threshold (100 MB)
- Use existing S3 uploader logic
- Preserve multipart upload for large files
- Close file after upload

**Pseudo-code**:
```go
func (a *Archiver) uploadTempFileToS3(tempFilePath, objectKey string) error {
    file, err := os.Open(tempFilePath)
    if err != nil {
        return fmt.Errorf("failed to open temp file: %w", err)
    }
    defer file.Close()

    fileInfo, err := file.Stat()
    if err != nil {
        return fmt.Errorf("failed to stat temp file: %w", err)
    }

    _, err = a.s3Uploader.Upload(&s3manager.UploadInput{
        Bucket: aws.String(a.config.S3.Bucket),
        Key:    aws.String(objectKey),
        Body:   file,
    })

    if err != nil {
        return fmt.Errorf("S3 upload failed: %w", err)
    }

    return nil
}
```

---

### Phase 4: Testing (Not Started)

#### 1. Unit Tests Needed
**File**: `cmd/formatters/jsonl_test.go` (expand existing)
- Test streaming formatter with small chunks (10 rows)
- Test streaming formatter with large chunks (100,000 rows)
- Test chunk boundary conditions (exact chunk size, off-by-one)
- Verify output matches non-streaming formatter

**File**: `cmd/formatters/csv_test.go` (expand existing)
- Test header is written first
- Test column ordering consistency
- Test NULL handling
- Test special characters (commas, quotes, newlines)

**File**: `cmd/formatters/parquet_test.go` (expand existing)
- Test schema mapping for all PostgreSQL types
- Test internal compression (snappy, zstd, gzip, none)
- Test footer metadata integrity

**File**: `cmd/compressors/zstd_test.go`, etc. (expand existing)
- Test NewWriter() produces same output as Compress()
- Test streaming compression matches batch compression

#### 2. Integration Tests
**File**: `cmd/archiver_streaming_test.go` (NEW FILE)
- Test `extractPartitionDataStreaming()` with mock database
- Test retry logic with simulated timeout
- Test temp file creation and cleanup
- Test memory usage stays constant (use runtime.ReadMemStats)

#### 3. End-to-End Tests
**File**: `cmd/archiver_e2e_test.go` (NEW FILE)
- Test full pipeline: DB → extraction → compression → S3
- Test with docker-compose dev environment
- Verify data integrity (download, decompress, parse, compare)
- Test all three formats (JSONL, CSV, Parquet)
- Test large partitions (1M+ rows)

---

### Phase 5: Documentation & Polish (Not Started)

#### 1. CHANGELOG Update
**File**: `CHANGELOG.md`

Add new section for v1.4.0:
```markdown
## [1.4.0] - 2025-01-XX

### Changed
- **Streaming Architecture:**
  - Refactored data extraction to use streaming/chunked processing
  - Memory usage now constant (~150 MB) regardless of partition size
  - Eliminates OOM crashes on large partitions (10+ GB)
  - Data streams: PostgreSQL → formatter → compressor → temp file → S3
  - Chunk size configurable via `--chunk-size` (default: 10,000 rows)

### Fixed
- **Statement Timeout Retry:**
  - Retry logic now wraps entire extraction process, not just query start
  - Statement timeouts during row iteration are now properly retried
  - Prevents silent failures on large partition extractions

### Added
- **Streaming Formatters:**
  - New streaming interfaces for JSONL, CSV, and Parquet formats
  - Schema pre-query for CSV headers and Parquet type mapping
  - Compression handled in streaming mode for all formats
- **Configuration:**
  - New `--chunk-size` flag (100-1,000,000, default: 10,000)
  - YAML config: `chunk_size`
```

#### 2. README Update
**File**: `README.md`

Add section on streaming and memory usage:
```markdown
### Memory Usage

The archiver uses a streaming architecture that maintains constant memory usage regardless of partition size:

- **Chunk Size**: 10,000 rows (configurable via `--chunk-size`)
- **Memory Footprint**: ~150 MB constant
- **Large Partitions**: No OOM crashes on multi-GB partitions

#### Tuning Chunk Size

Adjust chunk size based on average row size:

- Small rows (~1 KB): `--chunk-size 50000` (~50 MB memory)
- Medium rows (~10 KB): `--chunk-size 10000` (~100 MB memory) - default
- Large rows (~100 KB): `--chunk-size 1000` (~100 MB memory)
- Very large rows (1+ MB): `--chunk-size 100` (~100 MB memory)
```

---

## Testing Checklist

Before merge, verify:

- [ ] All unit tests pass: `go test ./...`
- [ ] All linters pass: `golangci-lint run`
- [ ] Code formatted: `gofmt -w .`
- [ ] Build succeeds: `go build`
- [ ] Docker build succeeds (AMD64): `docker build --platform linux/amd64 .`
- [ ] Docker build succeeds (ARM64): `docker build --platform linux/arm64 .`
- [ ] Dev environment works: `docker compose -f docker-compose.dev.yaml up`
- [ ] Memory usage verified on large partition (use `docker stats`)
- [ ] Statement timeout retry verified (set low timeout, test on large partition)
- [ ] Data integrity verified (download S3 file, decompress, parse, compare with DB)

---

## Next Steps for Fresh Context

1. **Review this document** to understand current state
2. **Implement retry wrapper** (`extractPartitionDataWithRetry`)
3. **Integrate into main flow** (modify `processSinglePartition`)
4. **Add temp file upload** (`uploadTempFileToS3`)
5. **Test locally** with docker-compose dev environment
6. **Run all tests** and fix any failures
7. **Update CHANGELOG** and README
8. **Commit and tag** v1.4.0

---

## Key Files Modified

### New Files
- `cmd/schema.go` - Schema querying and type definitions
- `STREAMING_IMPLEMENTATION_STATUS.md` - This document

### Modified Files
- `cmd/config.go` - Added ChunkSize config
- `cmd/root.go` - Added --chunk-size flag
- `cmd/archiver.go` - Added streaming extraction function, temp file utils
- `cmd/formatters/formatter.go` - Added streaming interfaces
- `cmd/formatters/jsonl.go` - Added streaming implementation
- `cmd/formatters/csv.go` - Added streaming implementation
- `cmd/formatters/parquet.go` - Added streaming implementation
- `cmd/compressors/compressor.go` - Added NewWriter to interface
- `cmd/compressors/zstd.go` - Added NewWriter implementation
- `cmd/compressors/gzip.go` - Added NewWriter implementation
- `cmd/compressors/lz4.go` - Added NewWriter implementation
- `cmd/compressors/none.go` - Added NewWriter implementation

---

## Architecture Diagrams

### Current (In-Memory) Architecture
```
┌──────────────┐
│  PostgreSQL  │
└──────┬───────┘
       │ SELECT * FROM partition (all rows)
       ↓
┌──────────────────────────────────┐
│  extractRowsWithProgress()       │
│  • Loads ALL rows into memory    │
│  • Returns []map[string]any      │
│  • Memory: O(partition_size)     │
└──────────────┬───────────────────┘
               ↓ All rows in memory
┌──────────────────────────────────┐
│  formatter.Format(rows)          │
│  • Formats ALL rows              │
│  • Returns []byte                │
│  • Memory: 2x partition size     │
└──────────────┬───────────────────┘
               ↓ Formatted data
┌──────────────────────────────────┐
│  compressor.Compress(data)       │
│  • Compresses entire file        │
│  • Returns []byte                │
│  • Memory: 3x partition size     │
└──────────────┬───────────────────┘
               ↓ Compressed data
┌──────────────────────────────────┐
│  uploadToS3(compressed)          │
│  • Uploads from memory           │
└──────────────────────────────────┘

Total Memory: ~3x partition size (10+ GB for large partitions)
```

### New (Streaming) Architecture
```
┌──────────────┐
│  PostgreSQL  │
└──────┬───────┘
       │ SELECT row_to_json(t) FROM t
       │
       ↓ Stream rows
┌──────────────────────────────────┐
│  extractPartitionDataStreaming() │
│  • Process in 10K row chunks     │
│  • Memory: O(chunk_size)         │
└──────────────┬───────────────────┘
               │ Chunks
               ↓
       ┌───────────────┐
       │  Chunk Buffer │ 10,000 rows
       │  ~100 MB      │
       └───────┬───────┘
               ↓
       ┌───────────────────┐
       │ StreamingFormatter│
       │  • JSONL/CSV/Parq │
       │  • WriteChunk()   │
       └───────┬───────────┘
               ↓
       ┌───────────────────┐
       │   Compressor      │ (if not Parquet)
       │  • zstd/gzip/lz4  │
       │  • ~32 MB buffer  │
       └───────┬───────────┘
               ↓
       ┌───────────────────┐
       │  MultiWriter      │
       │  • Temp File      │
       │  • MD5 Hasher     │
       └───────┬───────────┘
               ↓
       ┌───────────────────┐
       │   Temp File       │
       │  • On disk        │
       │  • ~partition sz  │
       └───────┬───────────┘
               ↓
┌──────────────────────────────────┐
│  uploadTempFileToS3()            │
│  • Reads from disk               │
│  • Multipart for large files     │
└──────────────────────────────────┘

Total Memory: ~150 MB constant (chunk + compressor buffers)
```

---

## Contact / Questions

If unclear on any implementation details, refer to:
1. This document for architecture overview
2. Inline comments in modified files
3. Research report in previous conversation (if needed)

Good luck with the continuation! 🚀
