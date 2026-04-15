package cmd

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWriteDataFileSetsPublicPermissions(t *testing.T) {
	dataDir := t.TempDir()
	t.Setenv("DATA_DIR", dataDir)

	path := filepath.Join(DataDir(), "2026", "04", "generated", "events.json")
	if err := writeDataFile(path, []byte(`{"ok":true}`)); err != nil {
		t.Fatalf("write data file: %v", err)
	}

	assertMode(t, DataDir(), 0755)
	assertMode(t, filepath.Join(dataDir, "2026"), 0755)
	assertMode(t, filepath.Join(dataDir, "2026", "04", "generated"), 0755)
	assertMode(t, path, 0644)
}

func TestDataDirDoesNotChmodRoot(t *testing.T) {
	dataDir := t.TempDir()
	if err := os.Chmod(dataDir, 0700); err != nil {
		t.Fatalf("chmod data dir: %v", err)
	}
	t.Setenv("DATA_DIR", dataDir)

	_ = DataDir()

	assertMode(t, dataDir, 0700)
}

func TestWriteDataFileSetsPrivateDirectoryPermissions(t *testing.T) {
	dataDir := t.TempDir()
	t.Setenv("DATA_DIR", dataDir)

	path := filepath.Join(DataDir(), "2026", "04", "finance", "stripe", "private", "customers", "customer-1.json")
	if err := writeDataFile(path, []byte(`{"ok":true}`)); err != nil {
		t.Fatalf("write private data file: %v", err)
	}

	assertMode(t, filepath.Join(dataDir, "2026", "04", "finance"), 0755)
	assertMode(t, filepath.Join(dataDir, "2026", "04", "finance", "stripe"), 0755)
	assertMode(t, filepath.Join(dataDir, "2026", "04", "finance", "stripe", "private"), 0700)
	assertMode(t, filepath.Join(dataDir, "2026", "04", "finance", "stripe", "private", "customers"), 0700)
	assertMode(t, path, 0644)
}

func TestDataDirNormalizesExistingPrivateDirectoryModes(t *testing.T) {
	dataDir := t.TempDir()
	privateDir := filepath.Join(dataDir, "2026", "04", "generated", "private")
	if err := os.MkdirAll(privateDir, 0755); err != nil {
		t.Fatalf("mkdir private dir: %v", err)
	}
	if err := os.Chmod(privateDir, 0755); err != nil {
		t.Fatalf("chmod private dir: %v", err)
	}

	t.Setenv("DATA_DIR", dataDir)
	_ = DataDir()

	assertMode(t, privateDir, 0700)
}

func TestWriteMonthFileCreatesNestedMessageDirectories(t *testing.T) {
	dataDir := t.TempDir()
	t.Setenv("DATA_DIR", dataDir)

	err := writeMonthFile(
		DataDir(),
		"2026",
		"03",
		filepath.Join("messages", "discord", "1443322243949137971", "messages.json"),
		[]byte(`{"messages":[]}`),
	)
	if err != nil {
		t.Fatalf("write month file: %v", err)
	}

	monthPath := filepath.Join(dataDir, "2026", "03", "messages", "discord", "1443322243949137971", "messages.json")
	latestPath := filepath.Join(dataDir, "latest", "messages", "discord", "1443322243949137971", "messages.json")

	assertMode(t, filepath.Dir(monthPath), 0755)
	assertMode(t, monthPath, 0644)
	assertMode(t, filepath.Dir(latestPath), 0755)
	assertMode(t, latestPath, 0644)
}

func assertMode(t *testing.T, path string, want os.FileMode) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	if got := info.Mode().Perm(); got != want {
		t.Fatalf("mode for %s = %o, want %o", path, got, want)
	}
}
