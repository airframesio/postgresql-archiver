package cmd

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func TestPIDFile(t *testing.T) {
	// Create temporary directory
	tempDir, err := os.MkdirTemp("", "pid_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	// Override home directory
	originalHome := os.Getenv("HOME")
	os.Setenv("HOME", tempDir)
	defer os.Setenv("HOME", originalHome)

	t.Run("WritePIDFile", func(t *testing.T) {
		err := WritePIDFile()
		if err != nil {
			t.Fatal(err)
		}

		// Verify file exists
		pidPath := GetPIDFilePath()
		if _, err := os.Stat(pidPath); os.IsNotExist(err) {
			t.Fatal("PID file should exist")
		}

		// Verify content
		data, err := os.ReadFile(pidPath)
		if err != nil {
			t.Fatal(err)
		}

		pid := os.Getpid()
		expectedPID := strconv.Itoa(pid)
		if string(data) != expectedPID {
			t.Fatalf("expected PID %s, got %s", expectedPID, string(data))
		}
	})

	t.Run("ReadPIDFile", func(t *testing.T) {
		// Write PID file first
		err := WritePIDFile()
		if err != nil {
			t.Fatal(err)
		}

		// Read it back
		pid, err := ReadPIDFile()
		if err != nil {
			t.Fatal(err)
		}

		expectedPID := os.Getpid()
		if pid != expectedPID {
			t.Fatalf("expected PID %d, got %d", expectedPID, pid)
		}
	})

	t.Run("ReadPIDFileNotExist", func(t *testing.T) {
		// Remove PID file if it exists
		pidPath := GetPIDFilePath()
		os.Remove(pidPath)

		// Try to read
		_, err := ReadPIDFile()
		if err == nil {
			t.Fatal("expected error when PID file doesn't exist")
		}
	})

	t.Run("RemovePIDFile", func(t *testing.T) {
		// Write PID file
		err := WritePIDFile()
		if err != nil {
			t.Fatal(err)
		}

		// Remove it
		err = RemovePIDFile()
		if err != nil {
			t.Fatal(err)
		}

		// Verify it's gone
		pidPath := GetPIDFilePath()
		if _, err := os.Stat(pidPath); !os.IsNotExist(err) {
			t.Fatal("PID file should be removed")
		}
	})

	t.Run("IsProcessRunning", func(t *testing.T) {
		// Current process should be running
		currentPID := os.Getpid()
		if !IsProcessRunning(currentPID) {
			t.Fatal("current process should be running")
		}

		// Invalid PID should not be running
		// Use -1 as it's guaranteed to be invalid
		if IsProcessRunning(-1) {
			t.Fatal("invalid PID should not be running")
		}
	})
}

func TestTaskInfo(t *testing.T) {
	// Create temporary directory
	tempDir, err := os.MkdirTemp("", "task_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	// Override home directory
	originalHome := os.Getenv("HOME")
	os.Setenv("HOME", tempDir)
	defer os.Setenv("HOME", originalHome)

	t.Run("WriteTaskInfo", func(t *testing.T) {
		info := &TaskInfo{
			PID:            12345,
			StartTime:      time.Now(),
			Table:          "test_table",
			StartDate:      "2024-01-01",
			EndDate:        "2024-01-31",
			CurrentTask:    "Processing",
			Progress:       0.5,
			TotalItems:     100,
			CompletedItems: 50,
		}

		err := WriteTaskInfo(info)
		if err != nil {
			t.Fatal(err)
		}

		// Verify file exists
		taskPath := GetTaskFilePath()
		if _, err := os.Stat(taskPath); os.IsNotExist(err) {
			t.Fatal("task file should exist")
		}

		// Verify content
		data, err := os.ReadFile(taskPath)
		if err != nil {
			t.Fatal(err)
		}

		var saved TaskInfo
		err = json.Unmarshal(data, &saved)
		if err != nil {
			t.Fatal(err)
		}

		if saved.PID != info.PID {
			t.Fatalf("expected PID %d, got %d", info.PID, saved.PID)
		}
		if saved.Table != info.Table {
			t.Fatalf("expected table %s, got %s", info.Table, saved.Table)
		}
		if saved.CurrentTask != info.CurrentTask {
			t.Fatalf("expected task %s, got %s", info.CurrentTask, saved.CurrentTask)
		}
		if saved.Progress != info.Progress {
			t.Fatalf("expected progress %f, got %f", info.Progress, saved.Progress)
		}
		if saved.LastUpdate.IsZero() {
			t.Fatal("LastUpdate should be set")
		}
	})

	t.Run("ReadTaskInfo", func(t *testing.T) {
		// Write task info first
		info := &TaskInfo{
			PID:            54321,
			StartTime:      time.Now(),
			Table:          "another_table",
			CurrentTask:    "Extracting",
			Progress:       0.75,
			TotalItems:     200,
			CompletedItems: 150,
		}

		err := WriteTaskInfo(info)
		if err != nil {
			t.Fatal(err)
		}

		// Read it back
		read, err := ReadTaskInfo()
		if err != nil {
			t.Fatal(err)
		}

		if read.PID != info.PID {
			t.Fatalf("expected PID %d, got %d", info.PID, read.PID)
		}
		if read.Table != info.Table {
			t.Fatalf("expected table %s, got %s", info.Table, read.Table)
		}
		if read.CurrentTask != info.CurrentTask {
			t.Fatalf("expected task %s, got %s", info.CurrentTask, read.CurrentTask)
		}
		if read.TotalItems != info.TotalItems {
			t.Fatalf("expected total %d, got %d", info.TotalItems, read.TotalItems)
		}
		if read.CompletedItems != info.CompletedItems {
			t.Fatalf("expected completed %d, got %d", info.CompletedItems, read.CompletedItems)
		}
	})

	t.Run("ReadTaskInfoNotExist", func(t *testing.T) {
		// Remove task file if it exists
		taskPath := GetTaskFilePath()
		os.Remove(taskPath)

		// Try to read
		_, err := ReadTaskInfo()
		if err == nil {
			t.Fatal("expected error when task file doesn't exist")
		}
	})

	t.Run("RemoveTaskFile", func(t *testing.T) {
		// Write task file
		info := &TaskInfo{
			PID:         99999,
			CurrentTask: "Test",
		}
		err := WriteTaskInfo(info)
		if err != nil {
			t.Fatal(err)
		}

		// Remove it
		err = RemoveTaskFile()
		if err != nil {
			t.Fatal(err)
		}

		// Verify it's gone
		taskPath := GetTaskFilePath()
		if _, err := os.Stat(taskPath); !os.IsNotExist(err) {
			t.Fatal("task file should be removed")
		}
	})
}

func TestPathFunctions(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "path_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	originalHome := os.Getenv("HOME")
	os.Setenv("HOME", tempDir)
	defer os.Setenv("HOME", originalHome)

	t.Run("GetPIDFilePath", func(t *testing.T) {
		expected := filepath.Join(tempDir, ".postgresql-archiver", "archiver.pid")
		actual := GetPIDFilePath()
		if actual != expected {
			t.Fatalf("expected path %s, got %s", expected, actual)
		}
	})

	t.Run("GetTaskFilePath", func(t *testing.T) {
		expected := filepath.Join(tempDir, ".postgresql-archiver", "current_task.json")
		actual := GetTaskFilePath()
		if actual != expected {
			t.Fatalf("expected path %s, got %s", expected, actual)
		}
	})
}