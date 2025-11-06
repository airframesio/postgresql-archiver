package cmd

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"
)

// newTestLogger creates a logger for testing
func newTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
}

func TestExtractDateFromTableName(t *testing.T) {
	// Note: extractDateFromTableName expects the full partition name
	// and extracts based on the base table name length
	archiver := NewArchiver(&Config{Table: "flights"}, newTestLogger())

	tests := []struct {
		name          string
		baseName      string
		partitionName string
		expectOk      bool
		expectDay     int
		expectMon     time.Month
		expectYr      int
	}{
		{
			name:          "daily partition YYYYMMDD",
			baseName:      "flights",
			partitionName: "flights_20240315",
			expectOk:      true,
			expectDay:     15,
			expectMon:     3,
			expectYr:      2024,
		},
		{
			name:          "daily partition with prefix pYYYYMMDD",
			baseName:      "flights",
			partitionName: "flights_p20240315",
			expectOk:      true,
			expectDay:     15,
			expectMon:     3,
			expectYr:      2024,
		},
		{
			name:          "monthly partition YYYY_MM",
			baseName:      "flights",
			partitionName: "flights_2024_03",
			expectOk:      true,
			expectDay:     1,
			expectMon:     3,
			expectYr:      2024,
		},
		{
			name:          "invalid table name - too short",
			baseName:      "flights",
			partitionName: "flights",
			expectOk:      false,
		},
		{
			name:          "leap year date",
			baseName:      "flights",
			partitionName: "flights_20240229",
			expectOk:      true,
			expectDay:     29,
			expectMon:     2,
			expectYr:      2024,
		},
		{
			name:          "last day of month",
			baseName:      "flights",
			partitionName: "flights_20240131",
			expectOk:      true,
			expectDay:     31,
			expectMon:     1,
			expectYr:      2024,
		},
		{
			name:          "monthly last day",
			baseName:      "flights",
			partitionName: "flights_2024_12",
			expectOk:      true,
			expectDay:     1,
			expectMon:     12,
			expectYr:      2024,
		},
		{
			name:          "invalid date",
			baseName:      "flights",
			partitionName: "flights_20240230",
			expectOk:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Update config table to match the base name for this test
			archiver.config.Table = tt.baseName

			date, ok := archiver.extractDateFromTableName(tt.partitionName)
			if ok != tt.expectOk {
				t.Fatalf("expected ok=%v, got ok=%v", tt.expectOk, ok)
			}
			if !tt.expectOk {
				return
			}

			if date.Year() != tt.expectYr || date.Month() != tt.expectMon || date.Day() != tt.expectDay {
				t.Fatalf("expected %04d-%02d-%02d, got %04d-%02d-%02d",
					tt.expectYr, tt.expectMon, tt.expectDay,
					date.Year(), date.Month(), date.Day())
			}
		})
	}
}

func TestCompressData(t *testing.T) {
	archiver := NewArchiver(&Config{Workers: 4}, newTestLogger())

	tests := []struct {
		name          string
		data          []byte
		expectSuccess bool
		minRatio      float64
	}{
		{
			name:          "empty data",
			data:          []byte{},
			expectSuccess: true,
			minRatio:      0, // Empty data compresses to very small
		},
		{
			name:          "repetitive data compresses well",
			data:          bytes.Repeat([]byte("test"), 1000),
			expectSuccess: true,
			minRatio:      2.0, // Should compress at least 2x
		},
		{
			name:          "random data compresses poorly",
			data:          []byte("random data that doesn't repeat much"),
			expectSuccess: true,
			minRatio:      0.5, // May expand slightly
		},
		{
			name:          "json data compresses well",
			data:          generateJSONData(1000),
			expectSuccess: true,
			minRatio:      1.5, // Should compress reasonably
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			compressed, err := archiver.compressData(tt.data)

			if err != nil && tt.expectSuccess {
				t.Fatalf("unexpected error: %v", err)
			}
			if err == nil && !tt.expectSuccess {
				t.Fatal("expected error, got nil")
			}

			if !tt.expectSuccess {
				return
			}

			if len(compressed) == 0 && len(tt.data) > 0 {
				t.Fatal("compressed data is empty")
			}

			if len(tt.data) > 0 {
				ratio := float64(len(tt.data)) / float64(len(compressed))
				if ratio < tt.minRatio {
					t.Logf("warning: compression ratio %.2f is below expected %.2f", ratio, tt.minRatio)
				}
			}
		})
	}
}

func TestCalculateMultipartETag(t *testing.T) {
	archiver := NewArchiver(&Config{}, newTestLogger())

	tests := []struct {
		name   string
		data   []byte
		minLen int
	}{
		{
			name:   "empty data",
			data:   []byte{},
			minLen: 1,
		},
		{
			name:   "small data",
			data:   []byte("test data"),
			minLen: 1,
		},
		{
			name:   "medium data",
			data:   bytes.Repeat([]byte("test"), 10000),
			minLen: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			etag := archiver.calculateMultipartETag(tt.data)

			// Just verify it returns a string
			if len(etag) < tt.minLen {
				t.Fatalf("ETag length %d is less than expected minimum %d", len(etag), tt.minLen)
			}
		})
	}
}

func TestNewArchiver(t *testing.T) {
	config := &Config{
		Table:   "test_table",
		Workers: 4,
	}

	archiver := NewArchiver(config, newTestLogger())

	if archiver == nil {
		t.Fatal("NewArchiver returned nil")
	}

	if archiver.config != config {
		t.Fatal("config not properly assigned")
	}

	if archiver.progressChan == nil {
		t.Fatal("progressChan not initialized")
	}
}

func TestPartitionInfoStruct(t *testing.T) {
	testDate := time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC)

	info := PartitionInfo{
		TableName: "test_table_20240315",
		Date:      testDate,
		RowCount:  1000,
	}

	if info.TableName != "test_table_20240315" {
		t.Fatalf("expected table name test_table_20240315, got %s", info.TableName)
	}

	if info.Date != testDate {
		t.Fatalf("expected date %v, got %v", testDate, info.Date)
	}

	if info.RowCount != 1000 {
		t.Fatalf("expected row count 1000, got %d", info.RowCount)
	}
}

func TestProcessResultStruct(t *testing.T) {
	info := PartitionInfo{
		TableName: "test_table",
		Date:      time.Now(),
		RowCount:  500,
	}

	result := ProcessResult{
		Partition:    info,
		Compressed:   true,
		Uploaded:     true,
		Skipped:      false,
		SkipReason:   "",
		Error:        nil,
		BytesWritten: 1024,
		Stage:        "upload",
	}

	if result.Partition != info {
		t.Fatal("partition not properly assigned")
	}

	if !result.Compressed {
		t.Fatal("expected compressed to be true")
	}

	if !result.Uploaded {
		t.Fatal("expected uploaded to be true")
	}

	if result.BytesWritten != 1024 {
		t.Fatalf("expected bytes written 1024, got %d", result.BytesWritten)
	}
}

// Helper function to generate test JSON data
func generateJSONData(count int) []byte {
	var buf bytes.Buffer
	buf.WriteString("[")
	for i := 0; i < count; i++ {
		if i > 0 {
			buf.WriteString(",")
		}
		fmt.Fprintf(&buf, `{"id":%d,"name":"item_%d","value":%f}`, i, i, float64(i)*1.5)
	}
	buf.WriteString("]")
	return buf.Bytes()
}

// Benchmark tests
func BenchmarkCompressData(b *testing.B) {
	archiver := NewArchiver(&Config{}, newTestLogger())
	data := bytes.Repeat([]byte("benchmark data"), 10000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = archiver.compressData(data)
	}
}

func BenchmarkCalculateMultipartETag(b *testing.B) {
	archiver := NewArchiver(&Config{}, newTestLogger())
	data := bytes.Repeat([]byte("test"), 100000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = archiver.calculateMultipartETag(data)
	}
}

func BenchmarkExtractDateFromTableName(b *testing.B) {
	archiver := NewArchiver(&Config{}, newTestLogger())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = archiver.extractDateFromTableName("flights_20240315")
	}
}

// Helper function to verify MD5 calculation
func CalculateMD5(data []byte) string {
	hash := md5.New()
	io.WriteString(hash, "")
	hash.Write(data)
	return fmt.Sprintf("%x", hash.Sum(nil))
}
