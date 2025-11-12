package compressors

import (
	"errors"
	"fmt"
	"io"
)

// ErrUnsupportedCompression is returned when an unsupported compression type is requested
var ErrUnsupportedCompression = errors.New("unsupported compression type")

// Compressor defines the interface for compression handlers
type Compressor interface {
	// Compress compresses the input data
	Compress(data []byte, level int) ([]byte, error)

	// NewWriter creates a streaming compression writer
	NewWriter(w io.Writer, level int) io.WriteCloser

	// NewReader creates a streaming decompression reader
	NewReader(r io.Reader) (io.ReadCloser, error)

	// Extension returns the file extension for this compression (e.g., ".zst", ".lz4", ".gz")
	Extension() string

	// DefaultLevel returns the default compression level
	DefaultLevel() int
}

// GetCompressor returns the appropriate compressor based on the compression string
func GetCompressor(compression string) (Compressor, error) {
	switch compression {
	case "zstd":
		return NewZstdCompressor(), nil
	case "lz4":
		return NewLZ4Compressor(), nil
	case "gzip":
		return NewGzipCompressor(), nil
	case "none":
		return NewNoneCompressor(), nil
	default:
		return nil, fmt.Errorf("%w: %s", ErrUnsupportedCompression, compression)
	}
}

// nopWriteCloser wraps a Writer to add a no-op Close method
type nopWriteCloser struct {
	io.Writer
}

func (nopWriteCloser) Close() error { return nil }
