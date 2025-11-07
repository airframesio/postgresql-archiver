package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
	"time"
)

// TaskInfo represents the current archiving task status
type TaskInfo struct {
	PID              int       `json:"pid"`
	StartTime        time.Time `json:"start_time"`
	Table            string    `json:"table"`
	StartDate        string    `json:"start_date"`
	EndDate          string    `json:"end_date"`
	CurrentTask      string    `json:"current_task"`
	CurrentPartition string    `json:"current_partition,omitempty"`
	CurrentStep      string    `json:"current_step,omitempty"`
	Progress         float64   `json:"progress"`
	TotalItems       int       `json:"total_items"`
	CompletedItems   int       `json:"completed_items"`
	LastUpdate       time.Time `json:"last_update"`
}

// GetPIDFilePath returns the path to the PID file
func GetPIDFilePath() string {
	homeDir, _ := os.UserHomeDir()
	return filepath.Join(homeDir, ".data-archiver", "archiver.pid")
}

// GetTaskFilePath returns the path to the task info file
func GetTaskFilePath() string {
	homeDir, _ := os.UserHomeDir()
	return filepath.Join(homeDir, ".data-archiver", "current_task.json")
}

// WritePIDFile writes the current process PID to a file
func WritePIDFile() error {
	pidPath := GetPIDFilePath()
	dir := filepath.Dir(pidPath)

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	pid := os.Getpid()
	return os.WriteFile(pidPath, []byte(strconv.Itoa(pid)), 0o600)
}

// RemovePIDFile removes the PID file
func RemovePIDFile() error {
	pidPath := GetPIDFilePath()
	return os.Remove(pidPath)
}

// ReadPIDFile reads the PID from file
func ReadPIDFile() (int, error) {
	pidPath := GetPIDFilePath()
	data, err := os.ReadFile(pidPath)
	if err != nil {
		return 0, err
	}

	pid, err := strconv.Atoi(string(data))
	if err != nil {
		return 0, fmt.Errorf("invalid PID in file: %w", err)
	}

	return pid, nil
}

// IsProcessRunning checks if a process with given PID is running
// Works on both Unix and Windows systems
func IsProcessRunning(pid int) bool {
	process, err := os.FindProcess(pid)
	if err != nil {
		return false
	}

	// On Unix systems, we can send signal 0 to check if process exists
	// On Windows, FindProcess always succeeds, so we need to try to send a signal
	err = process.Signal(syscall.Signal(0))

	// On Unix: err == nil means process exists
	// On Windows: err == nil also means process exists
	// Both systems return an error if the process doesn't exist
	return err == nil
}

// WriteTaskInfo writes current task information to file
func WriteTaskInfo(info *TaskInfo) error {
	taskPath := GetTaskFilePath()
	dir := filepath.Dir(taskPath)

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	info.LastUpdate = time.Now()

	data, err := json.MarshalIndent(info, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal task info: %w", err)
	}

	return os.WriteFile(taskPath, data, 0o600)
}

// ReadTaskInfo reads current task information from file
func ReadTaskInfo() (*TaskInfo, error) {
	taskPath := GetTaskFilePath()
	data, err := os.ReadFile(taskPath)
	if err != nil {
		return nil, err
	}

	var info TaskInfo
	if err := json.Unmarshal(data, &info); err != nil {
		return nil, fmt.Errorf("failed to unmarshal task info: %w", err)
	}

	return &info, nil
}

// RemoveTaskFile removes the task info file
func RemoveTaskFile() error {
	taskPath := GetTaskFilePath()
	return os.Remove(taskPath)
}
