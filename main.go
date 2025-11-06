package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/airframesio/postgresql-archiver/cmd"
	"github.com/charmbracelet/lipgloss"
)

var errorStyle = lipgloss.NewStyle().
	Foreground(lipgloss.Color("#FF0000")).
	Bold(true)

func main() {
	pid := os.Getpid()
	stopFile := filepath.Join(os.TempDir(), fmt.Sprintf("postgresql-archiver-%d.stop", pid))

	// Set up signal handling BEFORE any other initialization
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Workaround for Warp terminal bug: Watch for stop file
	// Warp has a known bug where CTRL-C doesn't send SIGINT to processes
	// GitHub issues: warpdotdev/Warp#6762, #7745, #4806
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				// Signal received, clean up stop file if it exists
				_ = os.Remove(stopFile)
				return
			case <-ticker.C:
				// Check if stop file exists
				if _, err := os.Stat(stopFile); err == nil {
					// Stop file detected - cancel context silently
					// The archiver will handle displaying cancellation messages
					cancel()
					return
				}
			}
		}
	}()

	// Clean up stop file on exit
	defer func() {
		_ = os.Remove(stopFile)
	}()

	// Store the context in cmd package for use by runArchive()
	cmd.SetSignalContext(ctx, stopFile)

	if err := cmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, errorStyle.Render("❌ Error: "+err.Error()))
		os.Exit(1)
	}
}
