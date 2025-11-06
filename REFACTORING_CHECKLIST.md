# PostgreSQL Archiver - Refactoring Checklist

Quick reference for implementing improvements from SOFTWARE_ENGINEERING_IMPROVEMENTS.md

## Phase 1: Quick Wins (Week 1)

### Day 1-2: Infrastructure Improvements
- [ ] Add `context.Context` to all database operations
- [ ] Add `context.Context` to all S3 operations
- [ ] Add timeout contexts where appropriate (10s for DB ping, 5m for queries)
- [ ] Replace `fmt.Printf` with structured logging using `log/slog`
- [ ] Add logger to Archiver struct with consistent fields

### Day 3: HTML Extraction
- [ ] Create `web/` directory
- [ ] Extract HTML from `cache_viewer_html.go` to `web/index.html`
- [ ] Extract CSS to `web/style.css`
- [ ] Extract JavaScript to `web/app.js`
- [ ] Use `//go:embed` to embed web files
- [ ] Update `cache_server.go` to serve from embedded FS
- [ ] Test cache viewer still works

### Day 4-5: Error Handling
- [ ] Create `internal/errors` package with custom error types
- [ ] Add `ErrorCode` enum (DATABASE, STORAGE, COMPRESSION, etc.)
- [ ] Replace generic errors with wrapped errors
- [ ] Add partition context to errors
- [ ] Mark retryable vs non-retryable errors
- [ ] Add panic recovery to ProcessPartition

## Phase 2: Core Refactoring (Weeks 2-3)

### Week 2: Extract Interfaces

#### Storage Interface
- [ ] Create `internal/storage/storage.go` with Provider interface
- [ ] Create `internal/storage/s3.go` implementing S3 provider
- [ ] Create `internal/storage/local.go` for testing
- [ ] Update archiver to use storage.Provider interface
- [ ] Add tests for S3Provider
- [ ] Add tests for LocalProvider

#### Database Interface
- [ ] Create `internal/database/postgres.go` with Client
- [ ] Move `DiscoverPartitions()` to database.Client
- [ ] Move `ExtractPartitionData()` to database.Client
- [ ] Move `CheckPermissions()` to database.Client
- [ ] Add connection pooling configuration
- [ ] Add database tests using go-sqlmock

#### Compression Interface
- [ ] Create `internal/compression/compressor.go` interface
- [ ] Create `internal/compression/zstd.go` implementation
- [ ] Move compression logic from archiver
- [ ] Add compression tests
- [ ] Add benchmarks

### Week 3: Modularize Archiver

#### Split archiver.go
- [ ] Create `internal/archiver/archiver.go` (orchestrator only)
- [ ] Create `internal/archiver/processor.go` (partition processing)
- [ ] Create `internal/archiver/config.go` (configuration)
- [ ] Move partition discovery to `internal/archiver/partition.go`
- [ ] Update constructor to use dependency injection
- [ ] Add unit tests for each component

#### Refactor Cache
- [ ] Create `internal/cache/repository.go` interface
- [ ] Create `internal/cache/file.go` (file-based implementation)
- [ ] Create `internal/cache/memory.go` (for testing)
- [ ] Move cache logic to repository pattern
- [ ] Add cache tests

## Phase 3: Testing (Week 3 continued)

### Unit Tests
- [ ] Test storage/s3.go (Upload, Exists, Download)
- [ ] Test database/postgres.go (DiscoverPartitions, ExtractData)
- [ ] Test compression/zstd.go (Compress, Decompress)
- [ ] Test archiver/processor.go (ProcessPartition)
- [ ] Test cache operations
- [ ] Achieve 80% coverage on new code

### Integration Tests
- [ ] Create `test/integration/archiver_test.go`
- [ ] Setup PostgreSQL test container
- [ ] Setup LocalStack for S3 testing
- [ ] Test full archive workflow
- [ ] Test error scenarios

## Phase 4: Advanced Features (Week 4)

### Retry Logic
- [ ] Create `pkg/retry/retry.go` with exponential backoff
- [ ] Add retry to S3 uploads
- [ ] Add retry to database queries (where appropriate)
- [ ] Make retry configurable
- [ ] Add tests for retry logic

### Parallel Processing
- [ ] Add worker pool to archiver
- [ ] Process partitions in parallel
- [ ] Add concurrency control (max workers)
- [ ] Update progress UI for parallel processing
- [ ] Add benchmarks (sequential vs parallel)

### Streaming
- [ ] Implement `ExtractPartitionDataStream()` for DB
- [ ] Use io.Pipe for streaming compression
- [ ] Use streaming upload to S3
- [ ] Add MD5 calculation during streaming
- [ ] Test memory usage (should be O(1) not O(n))

## Phase 5: Dependencies (Week 5)

### AWS SDK v2 Migration
- [ ] Replace `github.com/aws/aws-sdk-go` with v2
- [ ] Update S3 client initialization
- [ ] Update PutObject calls
- [ ] Update HeadObject calls
- [ ] Update multipart upload logic
- [ ] Test all S3 operations still work

### Optional: pgx Migration
- [ ] Replace `lib/pq` with `pgx/v5`
- [ ] Update connection string
- [ ] Update query methods
- [ ] Add pgxpool for connection pooling
- [ ] Benchmark performance difference

## Phase 6: Observability (Week 6)

### Metrics
- [ ] Add Prometheus metrics package
- [ ] Add `partitions_processed_total` counter
- [ ] Add `partition_processing_duration_seconds` histogram
- [ ] Add `bytes_processed_total` counter
- [ ] Expose `/metrics` endpoint
- [ ] Add Grafana dashboard example

### Health Checks
- [ ] Add `health` command
- [ ] Check database connectivity
- [ ] Check S3 connectivity
- [ ] Check cache availability
- [ ] Return structured health status

### Logging Improvements
- [ ] Add request ID to all logs
- [ ] Add table/partition context
- [ ] Log at appropriate levels (DEBUG, INFO, WARN, ERROR)
- [ ] Add log sampling for high-frequency events
- [ ] Add structured error logging

## Testing Checklist

After each phase, verify:

- [ ] All tests pass: `go test ./...`
- [ ] No race conditions: `go test -race ./...`
- [ ] Vet passes: `go vet ./...`
- [ ] Linting passes: `golangci-lint run`
- [ ] Build succeeds: `go build`
- [ ] Coverage report: `go test -coverprofile=coverage.out ./...`
- [ ] Manual testing with real database
- [ ] Manual testing with real S3

## Documentation Checklist

- [ ] Update README.md with new architecture
- [ ] Document configuration options
- [ ] Add code examples
- [ ] Document API endpoints
- [ ] Add troubleshooting guide
- [ ] Add performance tuning guide
- [ ] Generate godoc comments
- [ ] Add CONTRIBUTING.md

## Performance Benchmarks

Run before and after major changes:

```bash
# Benchmark database extraction
go test -bench=BenchmarkExtract -benchmem ./internal/database

# Benchmark compression
go test -bench=BenchmarkCompress -benchmem ./internal/compression

# Benchmark full processing
go test -bench=BenchmarkProcess -benchmem ./internal/archiver

# Profile CPU
go test -cpuprofile=cpu.prof -bench=. ./...
go tool pprof cpu.prof

# Profile memory
go test -memprofile=mem.prof -bench=. ./...
go tool pprof mem.prof
```

## CI/CD Updates

- [ ] Add coverage reporting to CI
- [ ] Add integration tests to CI (with test containers)
- [ ] Add staticcheck to CI
- [ ] Add golangci-lint to CI
- [ ] Add dependency vulnerability scanning
- [ ] Update build matrix (Go 1.21, 1.22, 1.23)

## Release Checklist

Before releasing refactored version:

- [ ] All tests passing
- [ ] Code coverage > 75%
- [ ] No critical linting issues
- [ ] Documentation updated
- [ ] CHANGELOG.md updated
- [ ] Version bumped (semver)
- [ ] Migration guide written
- [ ] Backwards compatibility maintained (or documented breaking changes)
- [ ] Performance benchmarks show improvement (or no regression)
- [ ] Security scan passes
- [ ] Manual testing in staging environment

---

## Notes

- This is a living checklist - update as you progress
- Mark items complete with `[x]` instead of `[ ]`
- Add notes/blockers as needed
- Reference issue numbers for tracking

## Estimated Timeline

- **Phase 1 (Quick Wins):** 1 week
- **Phase 2 (Core Refactoring):** 2 weeks
- **Phase 3 (Testing):** 1 week (concurrent with Phase 2)
- **Phase 4 (Advanced Features):** 1 week
- **Phase 5 (Dependencies):** 1 week
- **Phase 6 (Observability):** 1 week

**Total: 6-8 weeks** for full refactoring
