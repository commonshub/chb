package cmd

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// SyncSourceState tracks when a source was last synced.
type SyncSourceState struct {
	LastFullSync string `json:"lastFullSync,omitempty"`
	LastSync     string `json:"lastSync,omitempty"`
}

// SyncRunState tracks the last recent and history-oriented sync runs.
type SyncRunState struct {
	LastHistorySync string `json:"lastHistorySync,omitempty"`
	LastRecentSync  string `json:"lastRecentSync,omitempty"`
}

// SyncState tracks sync timestamps per source.
type SyncState struct {
	Calendars    *SyncSourceState            `json:"calendars,omitempty"`
	Transactions *SyncSourceState            `json:"transactions,omitempty"`
	Invoices     *SyncSourceState            `json:"invoices,omitempty"`
	Bills        *SyncSourceState            `json:"bills,omitempty"`
	Attachments  *SyncSourceState            `json:"attachments,omitempty"`
	Messages     *SyncSourceState            `json:"messages,omitempty"`
	Images       *SyncSourceState            `json:"images,omitempty"`
	Accounts     map[string]*SyncSourceState `json:"accounts,omitempty"`
	Runs         *SyncRunState               `json:"runs,omitempty"`
}

func syncStatePath() string {
	return filepath.Join(DataDir(), "latest", "sync-status.json")
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

func writeSyncStateFile(path string, data []byte) {
	dir := filepath.Dir(path)
	os.MkdirAll(dir, 0755)
	os.WriteFile(path, data, 0644)
}

// SaveSyncState writes the sync state to disk.
func SaveSyncState(state *SyncState) {
	data, _ := json.MarshalIndent(state, "", "  ")
	writeSyncStateFile(syncStatePath(), data)
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
	case "invoices":
		ss = get(&state.Invoices)
	case "bills":
		ss = get(&state.Bills)
	case "attachments":
		ss = get(&state.Attachments)
	case "messages":
		ss = get(&state.Messages)
	case "images":
		ss = get(&state.Images)
	default:
		if account, ok := strings.CutPrefix(source, "account:"); ok && account != "" {
			if state.Accounts == nil {
				state.Accounts = map[string]*SyncSourceState{}
			}
			account = strings.ToLower(account)
			ss = state.Accounts[account]
			if ss == nil {
				ss = &SyncSourceState{}
				state.Accounts[account] = ss
			}
		} else {
			return
		}
	}

	ss.LastSync = now
	if full {
		ss.LastFullSync = now
	}
	SaveSyncState(state)
}

// UpdateSyncActivity records the last recent or history sync run.
func UpdateSyncActivity(history bool) {
	state := LoadSyncState()
	now := time.Now().UTC().Format(time.RFC3339)
	if state.Runs == nil {
		state.Runs = &SyncRunState{}
	}
	if history {
		state.Runs.LastHistorySync = now
	} else {
		state.Runs.LastRecentSync = now
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
	case "invoices":
		ss = state.Invoices
	case "bills":
		ss = state.Bills
	case "attachments":
		ss = state.Attachments
	case "messages":
		ss = state.Messages
	case "images":
		ss = state.Images
	default:
		if account, ok := strings.CutPrefix(source, "account:"); ok && account != "" && state.Accounts != nil {
			ss = state.Accounts[strings.ToLower(account)]
		}
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

// LastSyncTime returns the last sync timestamp for the given source,
// or the zero time if never synced or malformed.
func LastSyncTime(source string) time.Time {
	state := LoadSyncState()
	var ss *SyncSourceState
	switch source {
	case "calendars":
		ss = state.Calendars
	case "transactions":
		ss = state.Transactions
	case "invoices":
		ss = state.Invoices
	case "bills":
		ss = state.Bills
	case "attachments":
		ss = state.Attachments
	case "messages":
		ss = state.Messages
	case "images":
		ss = state.Images
	default:
		if account, ok := strings.CutPrefix(source, "account:"); ok && account != "" && state.Accounts != nil {
			ss = state.Accounts[strings.ToLower(account)]
		}
	}
	if ss == nil || ss.LastSync == "" {
		return time.Time{}
	}
	t, err := time.Parse(time.RFC3339, ss.LastSync)
	if err != nil {
		return time.Time{}
	}
	return t
}
