# Docker Guide for Data Archiver

This guide covers how to use Data Archiver with Docker and Docker Compose. Data Archiver currently supports PostgreSQL input and S3-compatible output.

## Table of Contents

- [Quick Start](#quick-start)
- [Docker Images](#docker-images)
- [Docker Compose Configurations](#docker-compose-configurations)
- [Configuration](#configuration)
- [Examples](#examples)
- [Building Images](#building-images)
- [Troubleshooting](#troubleshooting)

## Quick Start

### Pull and Run

Pull the latest image from GitHub Container Registry:

```bash
docker pull ghcr.io/airframesio/data-archiver:latest
```

Run with environment variables:

```bash
docker run --rm \
  -e ARCHIVE_DB_HOST=postgres.example.com \
  -e ARCHIVE_DB_PORT=5432 \
  -e ARCHIVE_DB_USER=archiver \
  -e ARCHIVE_DB_PASSWORD=secret \
  -e ARCHIVE_DB_NAME=production \
  -e ARCHIVE_S3_ENDPOINT=https://s3.amazonaws.com \
  -e ARCHIVE_S3_BUCKET=archives \
  -e ARCHIVE_S3_ACCESS_KEY=AKIAIOSFODNN7EXAMPLE \
  -e ARCHIVE_S3_SECRET_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY \
  -e ARCHIVE_S3_REGION=us-east-1 \
  -e ARCHIVE_S3_PATH_TEMPLATE='archives/{table}/{YYYY}/{MM}/{DD}/{table}_{YYYY}{MM}{DD}.jsonl.zst' \
  -e ARCHIVE_TABLE=events \
  -e ARCHIVE_START_DATE=2024-01-01 \
  -e ARCHIVE_END_DATE=2024-12-31 \
  ghcr.io/airframesio/data-archiver:latest
```

## Docker Images

### Available Tags

Images are automatically built and published to GitHub Container Registry (`ghcr.io`) via GitHub Actions.

| Tag | Description | When Updated |
|-----|-------------|--------------|
| `latest` | Latest stable release | On push to main branch |
| `edge` | Development build | On push to develop branch |
| `vX.Y.Z` | Specific version (e.g., `v1.1.1`) | On git tag push |
| `vX.Y` | Latest patch version (e.g., `v1.1`) | On git tag push |
| `vX` | Latest minor version (e.g., `v1`) | On git tag push |
| `main-<sha>` | Specific commit from main | On every push to main |

### Image Details

- **Base Image**: `gcr.io/distroless/static-debian12:nonroot` (minimal, secure)
- **Platforms**: `linux/amd64`, `linux/arm64`
- **Size**: ~30-40 MB (multi-stage build with static binary)
- **User**: Non-root user (UID 65532)
- **Exposed Ports**: 8080 (web viewer)

### Pulling Images

```bash
# Latest stable release
docker pull ghcr.io/airframesio/data-archiver:latest

# Specific version
docker pull ghcr.io/airframesio/data-archiver:v1.1.1

# Development build
docker pull ghcr.io/airframesio/data-archiver:edge

# Specific platform
docker pull --platform linux/arm64 ghcr.io/airframesio/data-archiver:latest
```

## Docker Compose Configurations

Three Docker Compose configurations are provided:

### 1. `docker-compose.yaml` - Production (Archiver Only)

For production deployments with external PostgreSQL and S3.

```bash
# Create .env file with your configuration
cp .env.example .env
nano .env

# Start the archiver
docker compose up -d

# View logs
docker compose logs -f archiver

# Stop
docker compose down
```

**When to use**: Production environments with managed PostgreSQL and S3 services.

### 2. `docker-compose.full.yaml` - Production (Full Stack)

Includes PostgreSQL, MinIO (S3-compatible), and Archiver.

```bash
# Start all services
docker compose -f docker-compose.full.yaml up -d

# Access MinIO console
open http://localhost:9001  # minioadmin/minioadmin

# Access web viewer
open http://localhost:8080

# Stop all services
docker compose -f docker-compose.full.yaml down

# Remove all data
docker compose -f docker-compose.full.yaml down -v
```

**When to use**: Single-server production deployments or self-hosted environments.

### 3. `docker-compose.dev.yaml` - Development Environment

Includes PostgreSQL with sample data, MinIO, pgAdmin, and Archiver.

```bash
# Start development environment
docker compose -f docker-compose.dev.yaml up -d

# Access services
open http://localhost:5050   # pgAdmin: admin@local.dev / admin
open http://localhost:9001   # MinIO Console: minioadmin / minioadmin
open http://localhost:8080   # Archiver Web UI

# Run archiver manually with custom args
docker compose -f docker-compose.dev.yaml run --rm archiver \
  --table events \
  --start-date 2024-01-01 \
  --end-date 2024-01-07 \
  --debug

# View archiver logs
docker compose -f docker-compose.dev.yaml logs -f archiver

# Stop all services
docker compose -f docker-compose.dev.yaml down

# Reset all data and start fresh
docker compose -f docker-compose.dev.yaml down -v
docker compose -f docker-compose.dev.yaml up -d
```

**When to use**: Local development, testing, or learning.

## Configuration

### Environment Variables

All configuration options are available as environment variables with the `ARCHIVE_` prefix:

| Variable | Description | Required | Default |
|----------|-------------|----------|---------|
| `ARCHIVE_DEBUG` | Enable debug logging | No | `false` |
| `ARCHIVE_LOG_FORMAT` | Log format: text, logfmt, json | No | `text` |
| `ARCHIVE_DB_HOST` | PostgreSQL host | Yes | - |
| `ARCHIVE_DB_PORT` | PostgreSQL port | No | `5432` |
| `ARCHIVE_DB_USER` | PostgreSQL user | Yes | - |
| `ARCHIVE_DB_PASSWORD` | PostgreSQL password | Yes | - |
| `ARCHIVE_DB_NAME` | PostgreSQL database | Yes | - |
| `ARCHIVE_DB_SSLMODE` | SSL mode (disable, require, verify-ca, verify-full) | No | `disable` |
| `ARCHIVE_S3_ENDPOINT` | S3 endpoint URL | Yes | - |
| `ARCHIVE_S3_BUCKET` | S3 bucket name | Yes | - |
| `ARCHIVE_S3_ACCESS_KEY` | S3 access key | Yes | - |
| `ARCHIVE_S3_SECRET_KEY` | S3 secret key | Yes | - |
| `ARCHIVE_S3_REGION` | S3 region | No | `auto` |
| `ARCHIVE_S3_PATH_TEMPLATE` | S3 path template with placeholders | Yes | - |
| `ARCHIVE_TABLE` | Base table name to archive | Yes | - |
| `ARCHIVE_START_DATE` | Start date (YYYY-MM-DD) | No | - |
| `ARCHIVE_END_DATE` | End date (YYYY-MM-DD) | No | Today |
| `ARCHIVE_WORKERS` | Number of parallel workers | No | `4` |
| `ARCHIVE_DRY_RUN` | Dry run mode (no upload) | No | `false` |
| `ARCHIVE_SKIP_COUNT` | Skip row counting | No | `false` |
| `ARCHIVE_OUTPUT_DURATION` | Output duration (hourly, daily, weekly, monthly, yearly) | No | `daily` |
| `ARCHIVE_OUTPUT_FORMAT` | Output format (jsonl, csv, parquet) | No | `jsonl` |
| `ARCHIVE_COMPRESSION` | Compression (zstd, lz4, gzip, none) | No | `zstd` |
| `ARCHIVE_COMPRESSION_LEVEL` | Compression level | No | `3` |
| `ARCHIVE_DATE_COLUMN` | Timestamp column for duration splitting | No | - |
| `ARCHIVE_CACHE_VIEWER` | Enable web viewer | No | `false` |
| `ARCHIVE_VIEWER_PORT` | Web viewer port | No | `8080` |

### Configuration File

Mount a YAML configuration file:

```bash
docker run --rm \
  -v $(pwd)/config.yaml:/root/.data-archiver.yaml:ro \
  ghcr.io/airframesio/data-archiver:latest
```

See `docker/config/example.yaml` for a complete configuration example.

### .env File Example

Create a `.env` file in your project directory:

```env
# PostgreSQL
ARCHIVE_DB_HOST=postgres.example.com
ARCHIVE_DB_PORT=5432
ARCHIVE_DB_USER=archiver
ARCHIVE_DB_PASSWORD=your-secure-password
ARCHIVE_DB_NAME=production
ARCHIVE_DB_SSLMODE=require

# S3
ARCHIVE_S3_ENDPOINT=https://s3.amazonaws.com
ARCHIVE_S3_BUCKET=your-archive-bucket
ARCHIVE_S3_ACCESS_KEY=AKIAIOSFODNN7EXAMPLE
ARCHIVE_S3_SECRET_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
ARCHIVE_S3_REGION=us-east-1
ARCHIVE_S3_PATH_TEMPLATE=archives/{table}/{YYYY}/{MM}/{DD}/{table}_{YYYY}{MM}{DD}.jsonl.zst

# Archival
ARCHIVE_TABLE=events
ARCHIVE_START_DATE=2024-01-01
ARCHIVE_END_DATE=2024-12-31
ARCHIVE_WORKERS=4

# Optional
ARCHIVE_CACHE_VIEWER=true
VIEWER_PORT=8080
```

## Examples

### Example 1: Archive to AWS S3

```bash
docker run --rm \
  -e ARCHIVE_DB_HOST=prod-db.us-east-1.rds.amazonaws.com \
  -e ARCHIVE_DB_USER=archiver \
  -e ARCHIVE_DB_PASSWORD="${DB_PASSWORD}" \
  -e ARCHIVE_DB_NAME=production \
  -e ARCHIVE_DB_SSLMODE=verify-full \
  -e ARCHIVE_S3_ENDPOINT=https://s3.amazonaws.com \
  -e ARCHIVE_S3_BUCKET=company-archives \
  -e ARCHIVE_S3_ACCESS_KEY="${AWS_ACCESS_KEY_ID}" \
  -e ARCHIVE_S3_SECRET_KEY="${AWS_SECRET_ACCESS_KEY}" \
  -e ARCHIVE_S3_REGION=us-east-1 \
  -e ARCHIVE_S3_PATH_TEMPLATE='prod/{table}/year={YYYY}/month={MM}/day={DD}/data.parquet.zst' \
  -e ARCHIVE_TABLE=user_events \
  -e ARCHIVE_START_DATE=2024-01-01 \
  -e ARCHIVE_END_DATE=2024-01-31 \
  -e ARCHIVE_OUTPUT_FORMAT=parquet \
  -e ARCHIVE_COMPRESSION=zstd \
  -e ARCHIVE_COMPRESSION_LEVEL=9 \
  ghcr.io/airframesio/data-archiver:latest
```

### Example 2: Archive to MinIO with Web Viewer

```bash
docker run --rm \
  -p 8080:8080 \
  -e ARCHIVE_DB_HOST=postgres \
  -e ARCHIVE_DB_USER=archiver \
  -e ARCHIVE_DB_PASSWORD=devpassword \
  -e ARCHIVE_DB_NAME=archiver_dev \
  -e ARCHIVE_S3_ENDPOINT=http://minio:9000 \
  -e ARCHIVE_S3_BUCKET=archives \
  -e ARCHIVE_S3_ACCESS_KEY=minioadmin \
  -e ARCHIVE_S3_SECRET_KEY=minioadmin \
  -e ARCHIVE_S3_PATH_TEMPLATE='archives/{table}/{YYYY}/{MM}/{table}_{YYYY}{MM}.jsonl.zst' \
  -e ARCHIVE_TABLE=events \
  -e ARCHIVE_OUTPUT_DURATION=monthly \
  -e ARCHIVE_CACHE_VIEWER=true \
  --network host \
  ghcr.io/airframesio/data-archiver:latest
```

Then open http://localhost:8080 to view progress.

### Example 3: Dry Run Mode

Test your configuration without uploading:

```bash
docker run --rm \
  -e ARCHIVE_DRY_RUN=true \
  -e ARCHIVE_DEBUG=true \
  -e ARCHIVE_DB_HOST=localhost \
  -e ARCHIVE_DB_USER=archiver \
  -e ARCHIVE_DB_PASSWORD=password \
  -e ARCHIVE_DB_NAME=test \
  -e ARCHIVE_S3_ENDPOINT=http://minio:9000 \
  -e ARCHIVE_S3_BUCKET=test \
  -e ARCHIVE_S3_ACCESS_KEY=test \
  -e ARCHIVE_S3_SECRET_KEY=test \
  -e ARCHIVE_S3_PATH_TEMPLATE='test/{table}/{YYYY}/{MM}/{DD}/data.jsonl.zst' \
  -e ARCHIVE_TABLE=events \
  --network host \
  ghcr.io/airframesio/data-archiver:latest
```

### Example 4: High-Frequency Hourly Archiving

```bash
docker run --rm \
  -e ARCHIVE_DB_HOST=timescale.example.com \
  -e ARCHIVE_DB_USER=archiver \
  -e ARCHIVE_DB_PASSWORD="${DB_PASSWORD}" \
  -e ARCHIVE_DB_NAME=metrics \
  -e ARCHIVE_S3_ENDPOINT=https://s3.amazonaws.com \
  -e ARCHIVE_S3_BUCKET=hourly-archives \
  -e ARCHIVE_S3_ACCESS_KEY="${AWS_ACCESS_KEY_ID}" \
  -e ARCHIVE_S3_SECRET_KEY="${AWS_SECRET_ACCESS_KEY}" \
  -e ARCHIVE_S3_REGION=us-east-1 \
  -e ARCHIVE_S3_PATH_TEMPLATE='metrics/{table}/{YYYY}/{MM}/{DD}/{HH}/{table}_{YYYY}{MM}{DD}_{HH}.jsonl.lz4' \
  -e ARCHIVE_TABLE=sensor_readings \
  -e ARCHIVE_OUTPUT_DURATION=hourly \
  -e ARCHIVE_COMPRESSION=lz4 \
  -e ARCHIVE_COMPRESSION_LEVEL=1 \
  -e ARCHIVE_WORKERS=8 \
  -e ARCHIVE_DATE_COLUMN=timestamp \
  ghcr.io/airframesio/data-archiver:latest
```

## Building Images

### Build Locally

```bash
# Build for your platform
docker build -t data-archiver:local --build-arg VERSION=dev .

# Build for specific platform
docker build --platform linux/amd64 -t data-archiver:amd64 .

# Build multi-platform image
docker buildx build \
  --platform linux/amd64,linux/arm64 \
  -t data-archiver:multi \
  --build-arg VERSION=1.1.1 \
  .
```

### Build with Docker Compose

```bash
# Build all services
docker compose -f docker-compose.dev.yaml build

# Build with specific version
VERSION=1.2.0 docker compose -f docker-compose.full.yaml build

# Rebuild without cache
docker compose -f docker-compose.dev.yaml build --no-cache
```

### GitHub Actions Build

Images are automatically built on:
- Push to `main` branch → `latest` tag
- Push to `develop` branch → `edge` tag
- Push of version tags → `vX.Y.Z`, `vX.Y`, `vX` tags

The workflow includes:
- Multi-platform builds (amd64, arm64)
- Build caching for faster builds
- SBOM (Software Bill of Materials) generation
- Build provenance attestation
- Automated testing of built images

## Troubleshooting

### Permission Issues

The image runs as non-root user (UID 65532). If you need to write files:

```bash
# Create cache directory with correct permissions
mkdir -p .cache
chmod 777 .cache

docker run --rm \
  -v $(pwd)/.cache:/tmp/.data-archiver \
  ...
```

### Network Issues

If the archiver can't connect to PostgreSQL or S3:

```bash
# Use host network mode (Linux only)
docker run --rm --network host ...

# Or create a shared network
docker network create archiver-net
docker run --rm --network archiver-net ...
```

### Debug Mode

Enable debug logging for troubleshooting:

```bash
docker run --rm \
  -e ARCHIVE_DEBUG=true \
  -e ARCHIVE_LOG_FORMAT=json \
  ...
```

### View Help

```bash
docker run --rm ghcr.io/airframesio/data-archiver:latest --help
```

### Check Version

```bash
docker run --rm ghcr.io/airframesio/data-archiver:latest --version
```

### Inspect Image

```bash
docker inspect ghcr.io/airframesio/data-archiver:latest
```

## Security Best Practices

1. **Use specific version tags** instead of `latest` in production
2. **Store secrets securely** using Docker secrets or environment variables from secure sources
3. **Run with non-root user** (default in our image)
4. **Use SSL/TLS** for database connections (`ARCHIVE_DB_SSLMODE=verify-full`)
5. **Limit network access** using Docker networks
6. **Set resource limits** in docker-compose.yaml
7. **Scan images** for vulnerabilities regularly
8. **Use minimal base images** (we use distroless)

## Additional Resources

- [Data Archiver GitHub Repository](https://github.com/airframesio/data-archiver)
- [GitHub Container Registry](https://github.com/airframesio/data-archiver/pkgs/container/data-archiver)
- [Configuration Examples](docker/config/example.yaml)
- [Sample Database Setup](docker/init-db/01-init.sql)

## Support

For issues, questions, or contributions:
- [GitHub Issues](https://github.com/airframesio/data-archiver/issues)
- [GitHub Discussions](https://github.com/airframesio/data-archiver/discussions)
