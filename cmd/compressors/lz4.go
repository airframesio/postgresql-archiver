package compressors

import (
	"bytes"
	"fmt"
	"io"

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

// NewWriter creates a streaming lz4 compression writer
func (c *LZ4Compressor) NewWriter(w io.Writer, level int) io.WriteCloser {
	writer := lz4.NewWriter(w)

	// Set compression level (1-9)
	if level >= 1 && level <= 9 {
		_ = writer.Apply(lz4.CompressionLevelOption(lz4.CompressionLevel(level)))
	}

	return writer
}

// DefaultLevel returns the default compression level for LZ4
func (c *LZ4Compressor) DefaultLevel() int {
	return 1 // Fast compression
}
