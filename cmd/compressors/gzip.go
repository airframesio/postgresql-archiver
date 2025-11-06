package compressors

import (
	"bytes"
	"compress/gzip"
	"fmt"
)

// GzipCompressor handles Gzip compression
type GzipCompressor struct{}

// NewGzipCompressor creates a new Gzip compressor
func NewGzipCompressor() *GzipCompressor {
	return &GzipCompressor{}
}

// Compress compresses data using Gzip
func (c *GzipCompressor) Compress(data []byte, level int) ([]byte, error) {
	var buffer bytes.Buffer

	// Validate and normalize level (1-9, or -1 for default)
	if level < 1 || level > 9 {
		level = gzip.DefaultCompression
	}

	writer, err := gzip.NewWriterLevel(&buffer, level)
	if err != nil {
		return nil, fmt.Errorf("failed to create gzip writer: %w", err)
	}

	if _, err := writer.Write(data); err != nil {
		writer.Close()
		return nil, fmt.Errorf("failed to compress data: %w", err)
	}

	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("failed to close gzip writer: %w", err)
	}

	return buffer.Bytes(), nil
}

// Extension returns the file extension for Gzip compression
func (c *GzipCompressor) Extension() string {
	return ".gz"
}

// DefaultLevel returns the default compression level for Gzip
func (c *GzipCompressor) DefaultLevel() int {
	return 6 // gzip.DefaultCompression
}
