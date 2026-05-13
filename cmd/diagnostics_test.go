package cmd

import (
	"path/filepath"
	"testing"
	"time"
)

func TestDiagnosticsLogPathUsesOneFilePerDay(t *testing.T) {
	dir := t.TempDir()
	got := diagnosticsLogPath(dir, time.Date(2026, 5, 13, 15, 42, 0, 0, BrusselsTZ()))
	want := filepath.Join(dir, "20260513.log")
	if got != want {
		t.Fatalf("diagnosticsLogPath() = %q, want %q", got, want)
	}
}
