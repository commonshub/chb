package cmd

import (
	"os"
	"path/filepath"
	"testing"
)

// seedSettingsFixture writes a test fixture into APP_DATA_DIR/settings/<name>
// and marks it in the installed-defaults tracker so EnsureSettingsBootstrapped
// treats the fixture as user-edited (and does not overwrite it with the
// embedded default). Use this for any test that needs a specific settings
// payload to survive LoadSettings()'s bootstrap step.
func seedSettingsFixture(t *testing.T, name, payload string) {
	t.Helper()
	// A pinned fixture must survive bootstrap even for force-overwrite files
	// (e.g. accounts.json). Disable force-overwrite for the duration of the test.
	prev := forceOverwriteDefaultsEnabled
	forceOverwriteDefaultsEnabled = false
	t.Cleanup(func() { forceOverwriteDefaultsEnabled = prev })
	dir := AppSettingsDir()
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatalf("mkdir %s: %v", dir, err)
	}
	full := filepath.Join(dir, name)
	if err := os.WriteFile(full, []byte(payload), 0644); err != nil {
		t.Fatalf("write %s: %v", full, err)
	}
	tracker := loadInstalledDefaultsRecord(dir)
	// Any value that differs from the fixture's hash will do. Using a literal
	// marker keeps the tracker file readable when a test fails mid-run.
	tracker[name] = "test-fixture-user-edited"
	if err := saveInstalledDefaultsRecord(dir, tracker); err != nil {
		t.Fatalf("write installed-defaults tracker: %v", err)
	}
}
