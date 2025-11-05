# GitHub Copilot Instructions for postgresql-archiver

## Project Overview

This is a high-performance CLI tool written in Go for archiving PostgreSQL partitioned table data to S3-compatible object storage. The project emphasizes performance, data integrity, and user experience with a beautiful TUI (Terminal User Interface) and embedded web-based cache viewer.

## Development Environment

### Language and Version
- **Go Version**: 1.21 or later (see `go.mod` for the exact version)
- The project is tested against Go 1.21, 1.22, and 1.23 in CI

### Key Dependencies
- **AWS SDK for Go**: S3 operations and uploads
- **Bubble Tea/Bubbles/Lipgloss**: TUI framework for interactive progress display
- **Cobra**: CLI framework
- **Viper**: Configuration management
- **zstd (klauspost/compress)**: High-performance compression
- **lib/pq**: PostgreSQL driver
- **WebSocket (github.com/gorilla/websocket)**: Real-time cache viewer updates

## Code Structure

### Main Components
- `main.go`: Entry point with error handling
- `cmd/`: Contains all command and business logic
  - `archiver.go`: Core archiving logic
  - `cache.go`: Intelligent caching system for metadata
  - `cache_server.go`: WebSocket server for cache viewer
  - `cache_viewer_html.go`: Embedded HTML/JS for web UI
  - `config.go`: Configuration handling
  - `progress.go`: TUI progress display
  - `pid.go`: Process tracking
  - `root.go`: Cobra command setup

### Test Files
- Follow Go conventions: `*_test.go`
- Use `github.com/DATA-DOG/go-sqlmock` for database mocking
- Tests are in the same package as the code they test

## Coding Standards

### Style Guidelines
- Follow standard Go conventions and idiomatic Go practices
- Use `go fmt` for formatting (enforced in CI)
- Run `go vet` to catch common mistakes
- Use `staticcheck` for additional linting
- Run `golangci-lint` for comprehensive linting

### Error Handling
- Always wrap errors with context using `fmt.Errorf` with `%w` verb
- Return errors rather than panicking unless absolutely necessary
- Use descriptive error messages that help debugging

### Naming Conventions
- Use camelCase for private functions/variables
- Use PascalCase for exported functions/types
- Use descriptive names that clearly indicate purpose
- Struct fields that are exported should be PascalCase

### Comments
- Document all exported functions, types, and methods
- Use complete sentences in documentation comments
- Start documentation comments with the name of the thing being documented
- Keep comments up-to-date with code changes

## Testing Requirements

### Running Tests
```bash
go test ./...                    # Run all tests
go test -v ./...                 # Verbose output
go test -race ./...              # With race detector
go test -coverprofile=coverage.out ./...  # With coverage
```

### Test Expectations
- All new features must include tests
- Maintain or improve code coverage
- Use table-driven tests where appropriate
- Mock external dependencies (database, S3) in unit tests
- Tests must pass with the race detector enabled

## Build and Development Workflow

### Building
```bash
go build -v .                    # Build for current platform
go build -v ./...                # Build all packages
```

### Linting
```bash
go vet ./...                     # Standard Go linting
staticcheck ./...                # Advanced static analysis
golangci-lint run --timeout=5m   # Comprehensive linting
```

### CI/CD
- All PRs must pass CI checks before merging
- CI runs tests on Go 1.21, 1.22, and 1.23
- CI includes: tests, linting, building, and multi-platform builds
- Coverage is uploaded to Codecov

## Architecture Principles

### Performance
- Use goroutines for parallel processing (configurable workers)
- Implement efficient caching to avoid redundant operations
- Use zstd compression with multi-core support
- Buffer I/O operations appropriately

### Data Integrity
- Verify file sizes (compressed and uncompressed)
- Use MD5 checksums for single-part uploads
- Use multipart ETags for large files (>100MB)
- Never skip verification steps

### User Experience
- Provide real-time progress feedback via TUI
- Show dual progress bars (per-partition and overall)
- Enable embedded web viewer for monitoring
- Display clear error messages with context
- Support graceful interruption

### Caching Strategy
- Cache row counts for 24 hours (refreshed daily)
- Cache file metadata permanently (size, MD5, compression ratio)
- Skip extraction/compression when cached metadata matches S3
- Track errors with timestamps

## Configuration

### Config File
- Primary config: `~/.postgresql-archiver.yaml`
- Example config: `example-config.yaml` in repository root
- Uses Viper for flexible configuration (env vars, flags, config files)

### Required Settings
- Database connection (host, port, user, password, name)
- S3 settings (endpoint, bucket, access_key, secret_key, region)
- Table name (base name without date suffix)

### Optional Settings
- Workers (parallel processing count, default: 4)
- Date range (start_date, end_date)
- Debug mode
- Dry run mode
- Cache viewer (enable/disable, port)

## Partition Formats Supported

The archiver handles multiple partition naming formats:
- `table_YYYYMMDD` (e.g., `messages_20240315`)
- `table_pYYYYMMDD` (e.g., `messages_p20240315`)
- `table_YYYY_MM` (e.g., `messages_2024_03`)

## Special Considerations

### Embedded Resources
- `cache-viewer.html` is embedded into the binary via `cache_viewer_html.go`
- When modifying the web UI, regenerate the Go file

### WebSocket Communication
- Real-time updates use WebSocket protocol
- Auto-reconnecting for reliability
- JSON message format for cache updates

### S3 Uploads
- Automatic multipart upload for files >100MB
- Proper ETag handling for multipart uploads
- Support for S3-compatible storage (Hetzner, MinIO, etc.)

### Process Management
- PID file: `/tmp/postgresql-archiver.pid`
- Task info file: `/tmp/postgresql-archiver-task.json`
- Clean up on exit or interrupt

## Dependencies Management

### Adding Dependencies
- Use `go get` to add new dependencies
- Run `go mod tidy` to clean up
- Verify with `go mod verify`
- Check for vulnerabilities with known tools

### Updating Dependencies
- Review changes carefully, especially for core dependencies
- Run full test suite after updates
- Update go.mod and go.sum together

## Common Patterns

### Error Style
```go
if err != nil {
    return fmt.Errorf("descriptive message: %w", err)
}
```

### Logging with Lipgloss
```go
errorStyle := lipgloss.NewStyle().
    Foreground(lipgloss.Color("#FF0000")).
    Bold(true)
fmt.Fprintln(os.Stderr, errorStyle.Render("❌ Error: "+err.Error()))
```

### Struct Initialization
```go
archiver := &Archiver{
    config:       cfg,
    progressChan: make(chan tea.Cmd, 100),
}
```

## Documentation

### README
- Keep README.md up-to-date with features
- Include screenshots for visual features
- Document configuration options
- Provide usage examples

### Code Documentation
- Use godoc-compatible comments
- Document exported types and functions
- Include examples in complex functions

## Security

### Credentials
- Never commit credentials or secrets
- Use environment variables or config files
- `.gitignore` includes common credential files

### Validation
- Validate all user input
- Sanitize file paths
- Verify S3 operations before execution

## Contributing Workflow

1. Create a feature branch from `main`
2. Make changes following these guidelines
3. Write/update tests for your changes
4. Run the full test suite locally
5. Run linters and fix any issues
6. Build for multiple platforms to ensure compatibility
7. Submit a PR to `main`
8. Ensure CI passes before requesting review

## Additional Notes

- The project uses Bubble Tea for TUI, which has its own event loop and message-passing architecture
- Be careful with goroutine management to avoid leaks
- The cache system is critical for performance—don't bypass it without good reason
- The web viewer uses embedded HTML/JS, not a separate frontend build process
