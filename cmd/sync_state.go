package cmd

import (
	"encoding/json"
	"os"
	"path/filepath"
	"time"
)

// SyncSourceState tracks when a source was last synced.
type SyncSourceState struct {
	LastFullSync string `json:"lastFullSync,omitempty"`
	LastSync     string `json:"lastSync,omitempty"`
}

// SyncState tracks sync timestamps per source.
type SyncState struct {
	Calendars    *SyncSourceState `json:"calendars,omitempty"`
	Transactions *SyncSourceState `json:"transactions,omitempty"`
	Messages     *SyncSourceState `json:"messages,omitempty"`
	Images       *SyncSourceState `json:"images,omitempty"`
}

func syncStatePath() string {
	return filepath.Join(DataDir(), "latest", ".sync-state.json")
}

// LoadSyncState reads the sync state from disk.
func LoadSyncState() *SyncState {
	data, err := os.ReadFile(syncStatePath())
	if err != nil {
		return &SyncState{}
	}
	var state SyncState
	if json.Unmarshal(data, &state) != nil {
		return &SyncState{}
	}
	return &state
}

// SaveSyncState writes the sync state to disk.
func SaveSyncState(state *SyncState) {
	data, _ := json.MarshalIndent(state, "", "  ")
	dir := filepath.Dir(syncStatePath())
	os.MkdirAll(dir, 0755)
	os.WriteFile(syncStatePath(), data, 0644)
}

// UpdateSyncSource updates the last sync time for a source and saves.
func UpdateSyncSource(source string, full bool) {
	state := LoadSyncState()
	now := time.Now().UTC().Format(time.RFC3339)

	get := func(s **SyncSourceState) *SyncSourceState {
		if *s == nil {
			*s = &SyncSourceState{}
		}
		return *s
	}

	var ss *SyncSourceState
	switch source {
	case "calendars":
		ss = get(&state.Calendars)
	case "transactions":
		ss = get(&state.Transactions)
	case "messages":
		ss = get(&state.Messages)
	case "images":
		ss = get(&state.Images)
	default:
		return
	}

	ss.LastSync = now
	if full {
		ss.LastFullSync = now
	}
	SaveSyncState(state)
}

// LastSyncMonth returns the YYYY-MM of the last sync for the given source,
// or empty string if never synced.
func LastSyncMonth(source string) string {
	state := LoadSyncState()
	var ss *SyncSourceState
	switch source {
	case "calendars":
		ss = state.Calendars
	case "transactions":
		ss = state.Transactions
	case "messages":
		ss = state.Messages
	case "images":
		ss = state.Images
	}
	if ss == nil || ss.LastSync == "" {
		return ""
	}
	t, err := time.Parse(time.RFC3339, ss.LastSync)
	if err != nil {
		return ""
	}
	return t.Format("2006-01")
}
