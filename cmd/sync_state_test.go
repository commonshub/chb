package cmd

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestSaveSyncStateWritesLatestSyncStatus(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	UpdateSyncSource("messages", true)

	if _, err := os.Stat(syncStatePath()); err != nil {
		t.Fatalf("expected sync state at %s: %v", syncStatePath(), err)
	}

	state := LoadSyncState()
	if state.Messages == nil || state.Messages.LastSync == "" || state.Messages.LastFullSync == "" {
		t.Fatalf("expected messages sync state to be recorded, got %+v", state.Messages)
	}
}

func TestUpdateSyncActivityTracksRecentAndHistoryRuns(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	UpdateSyncActivity(false)
	state := LoadSyncState()
	if state.Runs == nil || state.Runs.LastRecentSync == "" {
		t.Fatalf("expected recent sync timestamp, got %+v", state.Runs)
	}
	if state.Runs.LastHistorySync != "" {
		t.Fatalf("expected history sync to be empty initially, got %+v", state.Runs)
	}

	UpdateSyncActivity(true)
	state = LoadSyncState()
	if state.Runs.LastHistorySync == "" {
		t.Fatalf("expected history sync timestamp, got %+v", state.Runs)
	}
}

func TestLoadSyncStateReadsSyncStatusFileOnly(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	state := &SyncState{
		Runs: &SyncRunState{
			LastHistorySync: "2026-04-14T10:00:00Z",
		},
	}
	data, err := json.Marshal(state)
	if err != nil {
		t.Fatalf("marshal state: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(syncStatePath()), 0755); err != nil {
		t.Fatalf("mkdir sync state dir: %v", err)
	}
	if err := os.WriteFile(syncStatePath(), data, 0644); err != nil {
		t.Fatalf("write sync state: %v", err)
	}

	loaded := LoadSyncState()
	if loaded.Runs == nil || loaded.Runs.LastHistorySync != "2026-04-14T10:00:00Z" {
		t.Fatalf("expected sync status state to load, got %+v", loaded.Runs)
	}
}
