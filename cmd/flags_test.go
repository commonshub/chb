package cmd

import "testing"

func TestResolveSinceMonthHistoryAlwaysStartsFullHistory(t *testing.T) {
	got, ok := ResolveSinceMonth([]string{"--history"}, "finance")
	if !ok {
		t.Fatal("expected history mode")
	}
	if got != "2024-01" {
		t.Fatalf("expected 2024-01, got %q", got)
	}
}

func TestResolveSinceMonthSinceStillWinsOverHistory(t *testing.T) {
	got, ok := ResolveSinceMonth([]string{"--history", "--since", "2025/06"}, "finance")
	if !ok {
		t.Fatal("expected since/history mode")
	}
	if got != "2025-06" {
		t.Fatalf("expected 2025-06, got %q", got)
	}
}
