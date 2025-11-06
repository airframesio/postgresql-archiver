package compressors

import (
	"bytes"
	"fmt"

	"github.com/klauspost/compress/zstd"
)

// ZstdCompressor handles Zstandard compression
type ZstdCompressor struct {
	workers int
}

// NewZstdCompressor creates a new Zstandard compressor
func NewZstdCompressor() *ZstdCompressor {
	return &ZstdCompressor{
		workers: 4, // Default worker count
	}
}

// WithWorkers sets the number of workers for compression
func (c *ZstdCompressor) WithWorkers(workers int) *ZstdCompressor {
	c.workers = workers
	return c
}

// Compress compresses data using Zstandard
func (c *ZstdCompressor) Compress(data []byte, level int) ([]byte, error) {
	var buffer bytes.Buffer

	// Map level to zstd encoder level
	var encoderLevel zstd.EncoderLevel
	switch {
	case level <= 0:
		encoderLevel = zstd.SpeedFastest
	case level <= 3:
		encoderLevel = zstd.SpeedDefault
	case level <= 7:
		encoderLevel = zstd.SpeedBetterCompression
	default:
		encoderLevel = zstd.SpeedBestCompression
	}

	encoder, err := zstd.NewWriter(&buffer,
		zstd.WithEncoderLevel(encoderLevel),
		zstd.WithEncoderConcurrency(c.workers))
	if err != nil {
		return nil, fmt.Errorf("failed to create zstd encoder: %w", err)
	}
	defer encoder.Close()

	if _, err := encoder.Write(data); err != nil {
		return nil, fmt.Errorf("failed to compress data: %w", err)
	}

	if err := encoder.Close(); err != nil {
		return nil, fmt.Errorf("failed to close zstd encoder: %w", err)
	}

	return buffer.Bytes(), nil
}

// Extension returns the file extension for Zstandard compression
func (c *ZstdCompressor) Extension() string {
	return ".zst"
}

// DefaultLevel returns the default compression level for Zstandard
func (c *ZstdCompressor) DefaultLevel() int {
	return 3 // SpeedDefault
}
