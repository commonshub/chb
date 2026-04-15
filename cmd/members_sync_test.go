package cmd

import (
	"path/filepath"
	"testing"
)

func TestGetMemberMonthsSupportsHistory(t *testing.T) {
	dataDir := t.TempDir()
	t.Setenv("DATA_DIR", dataDir)

	if err := writeDataFile(
		filepath.Join(dataDir, "2025", "01", "generated", "members.json"),
		[]byte(`{"members":[]}`),
	); err != nil {
		t.Fatalf("write cached members file: %v", err)
	}

	months := getMemberMonths([]string{"--history"})
	if len(months) == 0 {
		t.Fatal("expected history months")
	}
	if months[0].year != 2024 || months[0].month != 1 {
		t.Fatalf("expected history to start at 2024-01, got %04d-%02d", months[0].year, months[0].month)
	}
}

func TestGetMemberMonthsSupportsPositionalYearMonth(t *testing.T) {
	months := getMemberMonths([]string{"2025/03"})
	if len(months) != 1 {
		t.Fatalf("expected one month, got %d", len(months))
	}
	if months[0].year != 2025 || months[0].month != 3 {
		t.Fatalf("expected 2025-03, got %04d-%02d", months[0].year, months[0].month)
	}
}
