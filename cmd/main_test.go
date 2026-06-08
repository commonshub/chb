package cmd

import (
	"os"
	"path/filepath"
	"testing"
)

// TestMain sandboxes the settings/data location for the whole cmd test suite.
//
// Several command paths call LoadSettings() → EnsureSettingsBootstrapped(),
// which force-overwrites accounts.json from the embedded default and runs
// settings migrations (e.g. migrateOdooJournalLinks). A test that forgets to
// isolate its environment would run those against the developer's real ~/.chb
// and silently rewrite their config.
//
// We pin HOME to a throwaway dir and clear APP_DATA_DIR so the default
// settings/data location (HOME/.chb) lands in the sandbox. Tests that set their
// own HOME or APP_DATA_DIR via t.Setenv still override this and restore to the
// sandbox afterwards.
func TestMain(m *testing.M) {
	os.Exit(func() int {
		sandbox, err := os.MkdirTemp("", "chb-cmd-test-*")
		if err != nil {
			panic(err)
		}
		defer os.RemoveAll(sandbox)
		_ = os.Setenv("HOME", sandbox)
		_ = os.Setenv("APP_DATA_DIR", "")
		if err := os.MkdirAll(filepath.Join(sandbox, ".chb", "settings"), 0755); err != nil {
			panic(err)
		}
		return m.Run()
	}())
}
