package cmd

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

//nolint:gocognit // test function with multiple sub-tests
func TestSafeSliceResults(t *testing.T) {
	t.Run("new safe slice results", func(t *testing.T) {
		sr := newSafeSliceResults()
		if sr == nil {
			t.Fatal("newSafeSliceResults returned nil")
		}

		if sr.len() != 0 {
			t.Errorf("expected length 0, got %d", sr.len())
		}

		if cap(sr.results) != maxSliceResults {
			t.Errorf("expected initial capacity %d, got %d", maxSliceResults, cap(sr.results))
		}
	})

	t.Run("append and len", func(t *testing.T) {
		sr := newSafeSliceResults()

		sr.append("2024-01-01", ProcessResult{
			BytesWritten: 100,
		})

		if sr.len() != 1 {
			t.Errorf("expected length 1, got %d", sr.len())
		}

		sr.append("2024-01-02", ProcessResult{
			BytesWritten: 200,
		})

		if sr.len() != 2 {
			t.Errorf("expected length 2, got %d", sr.len())
		}
	})

	t.Run("clear", func(t *testing.T) {
		sr := newSafeSliceResults()
		sr.append("2024-01-01", ProcessResult{})
		sr.append("2024-01-02", ProcessResult{})

		if sr.len() != 2 {
			t.Errorf("expected length 2 before clear, got %d", sr.len())
		}

		sr.clear()

		if sr.len() != 0 {
			t.Errorf("expected length 0 after clear, got %d", sr.len())
		}
	})

	t.Run("max 10 items", func(t *testing.T) {
		sr := newSafeSliceResults()

		// Add 15 items
		for i := 0; i < 15; i++ {
			sr.append("2024-01-01", ProcessResult{
				BytesWritten: int64(i),
			})
		}

		// Should only keep last 10
		if sr.len() != 10 {
			t.Errorf("expected length 10 (max items), got %d", sr.len())
		}
	})

	t.Run("getRecent", func(t *testing.T) {
		sr := newSafeSliceResults()

		// Add 8 items
		for i := 0; i < 8; i++ {
			sr.append("2024-01-01", ProcessResult{
				BytesWritten: int64(i * 100),
			})
		}

		// Get last 5
		recent := sr.getRecent(5)

		if len(recent) != 5 {
			t.Errorf("expected 5 recent items, got %d", len(recent))
		}

		// Check that we got the last 5 items (indices 3-7)
		if recent[0].result.BytesWritten != 300 {
			t.Errorf("expected first recent item to be 300, got %d", recent[0].result.BytesWritten)
		}

		if recent[4].result.BytesWritten != 700 {
			t.Errorf("expected last recent item to be 700, got %d", recent[4].result.BytesWritten)
		}
	})

	t.Run("getRecent with fewer items", func(t *testing.T) {
		sr := newSafeSliceResults()

		// Add only 3 items
		sr.append("2024-01-01", ProcessResult{BytesWritten: 100})
		sr.append("2024-01-02", ProcessResult{BytesWritten: 200})
		sr.append("2024-01-03", ProcessResult{BytesWritten: 300})

		// Request 5, should get all 3
		recent := sr.getRecent(5)

		if len(recent) != 3 {
			t.Errorf("expected 3 items, got %d", len(recent))
		}
	})

	t.Run("getRecent with zero or negative", func(t *testing.T) {
		sr := newSafeSliceResults()
		sr.append("2024-01-01", ProcessResult{BytesWritten: 100})

		// Request 0, should get nil
		recent := sr.getRecent(0)
		if recent != nil {
			t.Errorf("expected nil for n=0, got slice with %d items", len(recent))
		}

		// Request negative, should get nil
		recent = sr.getRecent(-1)
		if recent != nil {
			t.Errorf("expected nil for n=-1, got slice with %d items", len(recent))
		}
	})

	t.Run("clear preserves capacity", func(t *testing.T) {
		sr := newSafeSliceResults()
		for i := 0; i < 5; i++ {
			sr.append("2024-01-01", ProcessResult{BytesWritten: int64(i * 100)})
		}

		if sr.len() != 5 {
			t.Errorf("expected length 5 before clear, got %d", sr.len())
		}

		sr.clear()

		if sr.len() != 0 {
			t.Errorf("expected length 0 after clear, got %d", sr.len())
		}

		// Capacity should be preserved
		if cap(sr.results) == 0 {
			t.Error("expected non-zero capacity after clear")
		}
	})

	t.Run("concurrent access", func(t *testing.T) {
		sr := newSafeSliceResults()

		// Spawn goroutines to append concurrently
		done := make(chan bool)
		for i := 0; i < 10; i++ {
			go func(idx int) {
				sr.append("2024-01-01", ProcessResult{
					BytesWritten: int64(idx),
				})
				done <- true
			}(i)
		}

		// Wait for all goroutines
		for i := 0; i < 10; i++ {
			<-done
		}

		// Should have 10 items (or possibly less if some were trimmed)
		length := sr.len()
		if length > 10 {
			t.Errorf("expected max 10 items, got %d", length)
		}

		// Getting recent should not panic
		recent := sr.getRecent(5)
		if len(recent) > 10 {
			t.Errorf("getRecent returned too many items: %d", len(recent))
		}
	})
}

func TestFormatBytes(t *testing.T) {
	tests := []struct {
		name     string
		bytes    int64
		expected string
	}{
		{
			name:     "bytes",
			bytes:    500,
			expected: "500 B",
		},
		{
			name:     "kilobytes",
			bytes:    2048,
			expected: "2.0 KB",
		},
		{
			name:     "megabytes",
			bytes:    5242880,
			expected: "5.0 MB",
		},
		{
			name:     "gigabytes",
			bytes:    1073741824,
			expected: "1.0 GB",
		},
		{
			name:     "zero bytes",
			bytes:    0,
			expected: "0 B",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := formatBytes(tt.bytes)
			if result != tt.expected {
				t.Errorf("expected %s, got %s", tt.expected, result)
			}
		})
	}
}

func TestProcessResultWithDuration(t *testing.T) {
	duration := 5 * time.Second
	result := ProcessResult{
		Duration: duration,
		S3Key:    "test/path/file.parquet",
	}

	if result.Duration != duration {
		t.Errorf("expected duration %v, got %v", duration, result.Duration)
	}

	if result.Duration.Seconds() != 5.0 {
		t.Errorf("expected 5.0 seconds, got %f", result.Duration.Seconds())
	}

	if result.S3Key != "test/path/file.parquet" {
		t.Errorf("expected S3Key 'test/path/file.parquet', got %s", result.S3Key)
	}
}

func TestProcessResultSkipped(t *testing.T) {
	result := ProcessResult{
		Skipped:    true,
		SkipReason: "All slices skipped (no data in time ranges)",
	}

	if !result.Skipped {
		t.Error("expected result to be skipped")
	}

	if result.Uploaded {
		t.Error("skipped result should not be uploaded")
	}

	if result.BytesWritten != 0 {
		t.Errorf("skipped result should have 0 bytes written, got %d", result.BytesWritten)
	}

	if result.SkipReason != "All slices skipped (no data in time ranges)" {
		t.Errorf("unexpected skip reason: %s", result.SkipReason)
	}
}

func TestFormatResultLine(t *testing.T) {
	config := &Config{
		S3: S3Config{
			Bucket: "test-bucket",
		},
	}
	m := progressModel{
		config: config,
	}

	tests := []struct {
		name       string
		identifier string
		result     ProcessResult
		wantSubstr string
	}{
		{
			name:       "skipped result",
			identifier: "partition_2024_01",
			result: ProcessResult{
				Skipped:    true,
				SkipReason: "no data",
			},
			wantSubstr: "⏭",
		},
		{
			name:       "error result",
			identifier: "partition_2024_02",
			result: ProcessResult{
				Error: fmt.Errorf("database error"), //nolint:err113 // test error
			},
			wantSubstr: "❌",
		},
		{
			name:       "uploaded with S3 key",
			identifier: "partition_2024_03",
			result: ProcessResult{
				Uploaded:     true,
				S3Key:        "path/to/file.parquet",
				BytesWritten: 1024,
				Duration:     5 * time.Second,
			},
			wantSubstr: "s3://test-bucket",
		},
		{
			name:       "uploaded without S3 key",
			identifier: "partition_2024_04",
			result: ProcessResult{
				Uploaded:     true,
				BytesWritten: 2048,
				Duration:     3 * time.Second,
			},
			wantSubstr: "Uploaded 2048 bytes",
		},
		{
			name:       "in progress",
			identifier: "partition_2024_05",
			result:     ProcessResult{},
			wantSubstr: "In progress",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			line := m.formatResultLine(tt.identifier, tt.result)
			if !strings.Contains(line, tt.wantSubstr) {
				t.Errorf("expected line to contain %q, got: %s", tt.wantSubstr, line)
			}
			if !strings.Contains(line, tt.identifier) {
				t.Errorf("expected line to contain identifier %q, got: %s", tt.identifier, line)
			}
		})
	}
}
