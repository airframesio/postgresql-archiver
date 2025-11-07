# syntax=docker/dockerfile:1

# Build stage - includes Go and Node.js for asset minification
FROM golang:1.23-alpine AS builder

# Install Node.js and npm for web asset minification
RUN apk add --no-cache nodejs npm

# Set working directory
WORKDIR /build

# Copy package.json and install npm dependencies first (for layer caching)
COPY package.json package-lock.json ./
RUN npm install

# Copy go mod files and download dependencies (for layer caching)
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Minify web assets (required before building - assets are embedded via go:embed)
RUN chmod +x ./scripts/minify.sh && ./scripts/minify.sh

# Build the application
# - CGO_ENABLED=0: Build static binary without C dependencies
# - GOOS=linux: Target Linux OS
# - -ldflags: Inject version information and reduce binary size
ARG VERSION=dev
RUN CGO_ENABLED=0 GOOS=linux go build \
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
