package compressors

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

// DefaultLevel returns 0 (no compression level needed)
func (c *NoneCompressor) DefaultLevel() int {
	return 0
}
