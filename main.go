package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/airframesio/postgresql-archiver/cmd"
	"github.com/charmbracelet/lipgloss"
)

var errorStyle = lipgloss.NewStyle().
	Foreground(lipgloss.Color("#FF0000")).
	Bold(true)

func main() {
	// Set up signal handling BEFORE any other initialization
	// This ensures signals are registered before Cobra/Viper can interfere
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Store the context in cmd package for use by runArchive()
	cmd.SetSignalContext(ctx)

	if err := cmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, errorStyle.Render("❌ Error: "+err.Error()))
		os.Exit(1)
	}
}
