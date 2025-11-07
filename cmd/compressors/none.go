package compressors

import "io"

// NoneCompressor is a no-op compressor that returns data unchanged
type NoneCompressor struct{}

// NewNoneCompressor creates a new no-op compressor
func NewNoneCompressor() *NoneCompressor {
	return &NoneCompressor{}
}

// Compress returns the data unchanged (no compression)
func (c *NoneCompressor) Compress(data []byte, _ int) ([]byte, error) {
	return data, nil
}

// Extension returns an empty string (no compression extension)
func (c *NoneCompressor) Extension() string {
	return ""
}

// NewWriter creates a no-op writer (passes through without compression)
func (c *NoneCompressor) NewWriter(w io.Writer, _ int) io.WriteCloser {
	return &nopWriteCloser{w}
}

// DefaultLevel returns 0 (no compression level needed)
func (c *NoneCompressor) DefaultLevel() int {
	return 0
}
