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

	"github.com/airframesio/data-archiver/cmd/compressors"
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
			// Use zstd compressor with default level
			compressor := archiver.config.Compression
			archiver.config.Compression = "zstd"
			archiver.config.CompressionLevel = 3

			cache := &PartitionCache{Entries: make(map[string]PartitionCacheEntry)}
			compressed, _, err := archiver.compressPartitionData(tt.data, PartitionInfo{TableName: "test"}, nil, cache, func(string) {})

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

			// Restore original
			archiver.config.Compression = compressor
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

func TestPartitionInfoHasCustomRange(t *testing.T) {
	info := PartitionInfo{
		TableName: "test_table",
		Date:      time.Now(),
	}

	if info.HasCustomRange() {
		t.Fatal("expected HasCustomRange to be false for zero range")
	}

	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	info.RangeStart = start
	info.RangeEnd = start.AddDate(0, 0, 1)

	if !info.HasCustomRange() {
		t.Fatal("expected HasCustomRange to be true when range bounds are set")
	}
}

func TestBuildDateRangePartition(t *testing.T) {
	t.Run("valid configuration", func(t *testing.T) {
		archiver := NewArchiver(&Config{
			Table:          "events",
			DateColumn:     "created_at",
			StartDate:      "2024-01-01",
			EndDate:        "2024-01-07",
			OutputDuration: "daily",
		}, newTestLogger())

		partitions, err := archiver.buildDateRangePartition()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(partitions) != 1 {
			t.Fatalf("expected 1 partition, got %d", len(partitions))
		}
		p := partitions[0]
		if !p.HasCustomRange() {
			t.Fatal("expected custom range to be set")
		}
		if !p.RangeStart.Equal(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)) {
			t.Fatalf("unexpected range start: %v", p.RangeStart)
		}
		if !p.RangeEnd.Equal(time.Date(2024, 1, 8, 0, 0, 0, 0, time.UTC)) {
			t.Fatalf("unexpected range end: %v", p.RangeEnd)
		}
	})

	t.Run("missing date column", func(t *testing.T) {
		archiver := NewArchiver(&Config{
			Table:     "events",
			StartDate: "2024-01-01",
			EndDate:   "2024-01-02",
		}, newTestLogger())

		if _, err := archiver.buildDateRangePartition(); err == nil {
			t.Fatal("expected error when date column is missing")
		}
	})
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
		BytesWritten: 1024,
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
	archiver := NewArchiver(&Config{Compression: "zstd", CompressionLevel: 3}, newTestLogger())
	data := bytes.Repeat([]byte("benchmark data"), 10000)

	compressor, err := compressors.GetCompressor(archiver.config.Compression)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = compressor.Compress(data, archiver.config.CompressionLevel)
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

func TestShouldSplitPartition(t *testing.T) {
	tests := []struct {
		name           string
		baseTable      string
		partition      string
		outputDuration string
		expectSplit    bool
		customRange    bool
	}{
		{
			name:           "monthly partition to daily output",
			baseTable:      "flights",
			partition:      "flights_2024_03",
			outputDuration: "daily",
			expectSplit:    true,
		},
		{
			name:           "monthly partition to weekly output",
			baseTable:      "flights",
			partition:      "flights_2024_03",
			outputDuration: "weekly",
			expectSplit:    true,
		},
		{
			name:           "monthly partition to monthly output",
			baseTable:      "flights",
			partition:      "flights_2024_03",
			outputDuration: "monthly",
			expectSplit:    false,
		},
		{
			name:           "daily partition to daily output",
			baseTable:      "flights",
			partition:      "flights_20240315",
			outputDuration: "daily",
			expectSplit:    false,
		},
		{
			name:           "compact monthly partition YYYYMM",
			baseTable:      "flights",
			partition:      "flights_202403",
			outputDuration: "daily",
			expectSplit:    true,
		},
		{
			name:           "custom range forces split",
			baseTable:      "flights",
			partition:      "flights",
			outputDuration: "daily",
			expectSplit:    true,
			customRange:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := &Config{
				Table:          tt.baseTable,
				OutputDuration: tt.outputDuration,
			}
			archiver := NewArchiver(config, newTestLogger())

			partition := PartitionInfo{
				TableName: tt.partition,
				Date:      time.Now(),
				RowCount:  1000,
			}
			if tt.customRange {
				start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
				partition.RangeStart = start
				partition.RangeEnd = start.AddDate(0, 0, 7)
			}

			result := archiver.shouldSplitPartition(partition)
			if result != tt.expectSplit {
				t.Errorf("expected split=%v, got split=%v", tt.expectSplit, result)
			}
		})
	}
}

func TestGenerateFilename(t *testing.T) {
	tests := []struct {
		name        string
		table       string
		date        time.Time
		duration    string
		format      string
		compression string
		wantPrefix  string
		wantSuffix  string
	}{
		{
			name:        "daily parquet with no compression",
			table:       "flights",
			date:        time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC),
			duration:    "daily",
			format:      ".parquet",
			compression: "",
			wantPrefix:  "flights-2024-03-15",
			wantSuffix:  ".parquet",
		},
		{
			name:        "monthly jsonl with gzip",
			table:       "flights",
			date:        time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC),
			duration:    "monthly",
			format:      ".jsonl",
			compression: ".gz",
			wantPrefix:  "flights-2024-03",
			wantSuffix:  ".jsonl.gz",
		},
		{
			name:        "weekly csv",
			table:       "events",
			date:        time.Date(2024, 1, 8, 0, 0, 0, 0, time.UTC),
			duration:    "weekly",
			format:      ".csv",
			compression: "",
			wantPrefix:  "events-2024-W02",
			wantSuffix:  ".csv",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GenerateFilename(tt.table, tt.date, tt.duration, tt.format, tt.compression)

			if !bytes.Contains([]byte(result), []byte(tt.wantPrefix)) {
				t.Errorf("filename %s doesn't contain prefix %s", result, tt.wantPrefix)
			}

			if !bytes.Contains([]byte(result), []byte(tt.wantSuffix)) {
				t.Errorf("filename %s doesn't contain suffix %s", result, tt.wantSuffix)
			}
		})
	}
}

func TestPathTemplate(t *testing.T) {
	tests := []struct {
		name     string
		template string
		table    string
		date     time.Time
		want     string
	}{
		{
			name:     "year month day template",
			template: "{table}/{YYYY}/{MM}/{DD}",
			table:    "flights",
			date:     time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC),
			want:     "flights/2024/03/15",
		},
		{
			name:     "compact template",
			template: "{table}/{YYYY}{MM}",
			table:    "events",
			date:     time.Date(2024, 12, 31, 0, 0, 0, 0, time.UTC),
			want:     "events/202412",
		},
		{
			name:     "default template",
			template: "",
			table:    "data",
			date:     time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			want:     "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pt := NewPathTemplate(tt.template)
			result := pt.Generate(tt.table, tt.date)

			if result != tt.want {
				t.Errorf("expected %s, got %s", tt.want, result)
			}
		})
	}
}

func TestSplitPartitionByDuration(t *testing.T) {
	start := time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2024, 4, 1, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name          string
		duration      string
		expectedCount int
		firstStart    time.Time
		firstEnd      time.Time
	}{
		{
			name:          "split monthly to daily",
			duration:      "daily",
			expectedCount: 31,
			firstStart:    time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC),
			firstEnd:      time.Date(2024, 3, 2, 0, 0, 0, 0, time.UTC),
		},
		{
			name:          "split monthly to weekly",
			duration:      "weekly",
			expectedCount: 5,
			firstStart:    time.Date(2024, 2, 26, 0, 0, 0, 0, time.UTC), // Monday before March 1
			firstEnd:      time.Date(2024, 3, 4, 0, 0, 0, 0, time.UTC),
		},
		{
			name:          "split monthly to monthly",
			duration:      "monthly",
			expectedCount: 1,
			firstStart:    time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC),
			firstEnd:      time.Date(2024, 4, 1, 0, 0, 0, 0, time.UTC),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ranges := SplitPartitionByDuration(start, end, tt.duration)

			if len(ranges) != tt.expectedCount {
				t.Errorf("expected %d ranges, got %d", tt.expectedCount, len(ranges))
			}

			if len(ranges) > 0 {
				if !ranges[0].Start.Equal(tt.firstStart) {
					t.Errorf("first range start: expected %v, got %v", tt.firstStart, ranges[0].Start)
				}
				if !ranges[0].End.Equal(tt.firstEnd) {
					t.Errorf("first range end: expected %v, got %v", tt.firstEnd, ranges[0].End)
				}
			}
		})
	}
}

func TestIsConnectionError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "connection reset error",
			err:      fmt.Errorf("connection reset by peer"), //nolint:err113 // test error
			expected: true,
		},
		{
			name:     "broken pipe error",
			err:      fmt.Errorf("broken pipe"), //nolint:err113 // test error
			expected: true,
		},
		{
			name:     "EOF error",
			err:      io.EOF,
			expected: false, // EOF is not checked by isConnectionError
		},
		{
			name:     "unexpected EOF error",
			err:      io.ErrUnexpectedEOF,
			expected: false, // unexpected EOF is not checked by isConnectionError
		},
		{
			name:     "bad connection error",
			err:      fmt.Errorf("bad connection"), //nolint:err113 // test error
			expected: true,
		},
		{
			name:     "other error",
			err:      fmt.Errorf("some other error"), //nolint:err113 // test error
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isConnectionError(tt.err)
			if result != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, result)
			}
		})
	}
}
