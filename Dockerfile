# syntax=docker/dockerfile:1

# Minify stage - runs on native build platform to avoid cross-compilation issues
# This ensures npm packages with native binaries work correctly
FROM --platform=$BUILDPLATFORM node:20-alpine AS minifier

WORKDIR /build

# Copy source files needed for minification
COPY package.json package-lock.json ./
COPY cmd/web ./cmd/web
COPY scripts/minify.sh ./scripts/

# Install dependencies and run minification on native platform
RUN npm install && \
    chmod +x ./scripts/minify.sh && \
    ./scripts/minify.sh

# Build stage - compiles Go binary for target platform
FROM golang:1.23-alpine AS builder

# Set working directory
WORKDIR /build

# Copy go mod files and download dependencies first (for layer caching)
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Copy minified assets from minifier stage
COPY --from=minifier /build/cmd/web/*.min.* ./cmd/web/

# Build the application for target platform
# - CGO_ENABLED=0: Build static binary without C dependencies
# - GOOS=linux: Target Linux OS
# - -ldflags: Inject version information and reduce binary size
ARG VERSION=dev
ARG TARGETOS
ARG TARGETARCH
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build \
    -ldflags="-X github.com/airframesio/data-archiver/cmd.Version=${VERSION} -w -s" \
    -o data-archiver \
    .

# Runtime stage - minimal distroless image for security
FROM gcr.io/distroless/static-debian12:nonroot

# Copy CA certificates for HTTPS connections to S3/GitHub
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

# Copy the binary from builder
COPY --from=builder /build/data-archiver /usr/local/bin/data-archiver

# Create cache directory structure
# Note: distroless runs as user 65532 (nonroot), uses /tmp/.data-archiver for cache
USER 65532:65532

# Expose web viewer port
EXPOSE 8080

# Set entrypoint
ENTRYPOINT ["/usr/local/bin/data-archiver"]

# Default to showing help
CMD ["--help"]
