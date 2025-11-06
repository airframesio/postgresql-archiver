# Development Guide

This guide covers setting up your development environment for contributing to PostgreSQL Archiver.

## Prerequisites

- Go 1.21 or later
- PostgreSQL 12+ (for integration testing)
- Git

## Quick Start

### 1. Install Git Hooks

To enable automatic code quality checks before commits:

```bash
git config core.hooksPath .githooks
chmod +x .githooks/pre-commit
```

This configures git to run the pre-commit hook from `.githooks/pre-commit`, which will:
- Check code formatting (`go fmt`)
- Run static analysis (`go vet` and `staticcheck`)
- Run all tests with coverage tracking

### 2. Install Linting Tools

```bash
# Install golangci-lint
go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest

# Install staticcheck (optional, but recommended)
go install honnef.co/go/tools/cmd/staticcheck@latest
```

## Development Workflow

### Running Tests

```bash
# Run all tests with coverage
CGO_ENABLED=0 go test -v -cover ./...

# Run tests for a specific package
CGO_ENABLED=0 go test -v ./cmd

# Run with coverage profile
CGO_ENABLED=0 go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out  # Open coverage report in browser
```

### Code Quality Checks

```bash
# Format code
go fmt ./...

# Run static analysis
go vet ./...
staticcheck ./...

# Run all linters (configured in .golangci.yml)
golangci-lint run

# Fix common issues automatically
golangci-lint run --fix
```

### Building

```bash
# Build for current platform
go build -v ./...

# Build for specific platform
GOOS=linux GOARCH=amd64 go build -o postgresql-archiver-linux-amd64

# Build all supported platforms
./scripts/build-all.sh  # (if available)
```

## Code Style Guidelines

### Formatting

- All code must be formatted with `gofmt`
- The CI/CD pipeline enforces this automatically

### Naming Conventions

- Package names should be short, concise, and lowercase
- Function names should be clear about what they do
- Unexported functions start with lowercase letters
- Exported functions start with uppercase letters

### Documentation

- All exported functions and types must have documentation comments
- Comments should start with the function/type name
- Example: `// NewArchiver creates a new Archiver instance with the given configuration`

### Error Handling

- Always check and handle errors explicitly
- Don't use `_` to ignore errors unless you have a good reason
- Return wrapped errors with context: `fmt.Errorf("operation failed: %w", err)`

### Testing

- Write tests for all new functionality
- Test table-driven tests for functions with multiple code paths
- Test edge cases and error conditions
- Aim for coverage above 80%

## Pre-commit Hook

The `.githooks/pre-commit` hook runs automatically before each commit and:

1. **Checks code formatting** - Ensures code is properly formatted
2. **Runs go vet** - Catches common Go mistakes
3. **Runs staticcheck** - Additional static analysis checks
4. **Runs tests** - Ensures all tests pass

If any check fails, the commit will be cancelled. You can:

- Fix the issues and commit again
- Skip the hook with `git commit --no-verify` (not recommended)

## Debugging

### Enable Debug Mode

Set the `-debug` flag when running the archiver:

```bash
go run ./cmd -debug -table flights ...
```

### Check Logs

The archiver logs to stdout. For more detailed information:

```bash
# Run with verbose output
postgresql-archiver -v -table flights ...

# Pipe output to a file
postgresql-archiver -table flights ... > archiver.log 2>&1
```

### Test Coverage Analysis

```bash
# Generate coverage report
CGO_ENABLED=0 go test -coverprofile=coverage.out ./...

# View coverage for a specific package
go tool cover -html=coverage.out -o coverage.html
open coverage.html

# Check coverage percentage
go tool cover -func=coverage.out | tail -1
```

## Contributing

### Before Submitting a PR

1. ✅ Ensure all tests pass: `CGO_ENABLED=0 go test ./...`
2. ✅ Check code formatting: `go fmt ./...`
3. ✅ Run linters: `golangci-lint run`
4. ✅ Verify coverage hasn't decreased
5. ✅ Update CHANGELOG.md with your changes
6. ✅ Update README.md if needed

### PR Checklist

- [ ] Tests added/updated for new functionality
- [ ] Code follows style guidelines
- [ ] Documentation updated (README, comments, etc.)
- [ ] CHANGELOG.md updated
- [ ] All tests pass locally
- [ ] No linting errors

## Troubleshooting

### Pre-commit hook not running?

```bash
# Verify hook is executable
ls -la .githooks/pre-commit

# Make it executable if needed
chmod +x .githooks/pre-commit

# Verify git configuration
git config core.hooksPath
```

### Tests failing with CGO_ENABLED?

Some systems require `CGO_ENABLED=0` for compilation. This is set up in CI/CD.

```bash
# If you're having issues, try:
CGO_ENABLED=0 go test -v ./...
```

### Linter errors?

```bash
# Try auto-fixing
golangci-lint run --fix

# For specific linters, check configuration in .golangci.yml
```

## Resources

- [Go Best Practices](https://golang.org/doc/effective_go)
- [golangci-lint Documentation](https://golangci-lint.run/)
- [Go Testing Package](https://pkg.go.dev/testing)

## Questions?

Refer to the main [README.md](README.md) or open an issue on GitHub.
