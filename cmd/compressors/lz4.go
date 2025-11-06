package compressors

import (
	"bytes"
	"fmt"

	"github.com/pierrec/lz4/v4"
)

// LZ4Compressor handles LZ4 compression
type LZ4Compressor struct{}

// NewLZ4Compressor creates a new LZ4 compressor
func NewLZ4Compressor() *LZ4Compressor {
	return &LZ4Compressor{}
}

// Compress compresses data using LZ4
func (c *LZ4Compressor) Compress(data []byte, level int) ([]byte, error) {
	var buffer bytes.Buffer

	writer := lz4.NewWriter(&buffer)

	// Set compression level (1-9)
	if level >= 1 && level <= 9 {
		if err := writer.Apply(lz4.CompressionLevelOption(lz4.CompressionLevel(level))); err != nil {
			return nil, fmt.Errorf("failed to apply compression level: %w", err)
		}
	}

	if _, err := writer.Write(data); err != nil {
		return nil, fmt.Errorf("failed to compress data: %w", err)
	}

	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("failed to close lz4 writer: %w", err)
	}

	return buffer.Bytes(), nil
}

// Extension returns the file extension for LZ4 compression
func (c *LZ4Compressor) Extension() string {
	return ".lz4"
}

// DefaultLevel returns the default compression level for LZ4
func (c *LZ4Compressor) DefaultLevel() int {
	return 1 // Fast compression
}
