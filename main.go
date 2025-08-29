package main

import (
	"fmt"
	"os"

	"github.com/airframesio/postgresql-archiver/cmd"
	"github.com/charmbracelet/lipgloss"
)

var (
	errorStyle = lipgloss.NewStyle().
		Foreground(lipgloss.Color("#FF0000")).
		Bold(true)
)

func main() {
	if err := cmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, errorStyle.Render("❌ Error: "+err.Error()))
		os.Exit(1)
	}
}
