package cmd

import (
	"os"
	"path/filepath"
	"testing"
)

func TestAppDataDirDefaultsToHomeCHB(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("APP_DATA_DIR", "")

	want := filepath.Join(home, ".chb")
	got := AppDataDir()
	if got != want {
		t.Fatalf("AppDataDir() = %q, want %q", got, want)
	}
	assertMode(t, got, 0755)
}

func TestAppDataDirUsesEnvOverride(t *testing.T) {
	appDir := filepath.Join(t.TempDir(), "app")
	t.Setenv("APP_DATA_DIR", appDir)

	got := AppDataDir()
	if got != appDir {
		t.Fatalf("AppDataDir() = %q, want %q", got, appDir)
	}
	assertMode(t, got, 0755)
}

func TestDataDirDefaultsUnderAppDataDir(t *testing.T) {
	appDir := filepath.Join(t.TempDir(), "app")
	t.Setenv("APP_DATA_DIR", appDir)
	t.Setenv("DATA_DIR", "")

	want := filepath.Join(appDir, "data")
	got := DataDir()
	if got != want {
		t.Fatalf("DataDir() = %q, want %q", got, want)
	}
	assertMode(t, got, 0755)
}

func TestDataDirEnvOverridesAppDataDir(t *testing.T) {
	appDir := filepath.Join(t.TempDir(), "app")
	dataDir := filepath.Join(t.TempDir(), "data")
	t.Setenv("APP_DATA_DIR", appDir)
	t.Setenv("DATA_DIR", dataDir)

	got := DataDir()
	if got != dataDir {
		t.Fatalf("DataDir() = %q, want %q", got, dataDir)
	}
	if _, err := os.Stat(filepath.Join(appDir, "data")); !os.IsNotExist(err) {
		t.Fatalf("APP_DATA_DIR/data should not be created when DATA_DIR is set")
	}
	assertMode(t, dataDir, 0755)
}

func TestEnsureSettingsBootstrappedUsesAppDataDirOverride(t *testing.T) {
	appDir := filepath.Join(t.TempDir(), "app")
	t.Setenv("APP_DATA_DIR", appDir)
	settingsPath := filepath.Join(appDir, "settings")

	if err := os.MkdirAll(settingsPath, 0755); err != nil {
		t.Fatalf("mkdir settings dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(settingsPath, "settings.json"), []byte(`{}`), 0644); err != nil {
		t.Fatalf("write settings.json: %v", err)
	}

	got := EnsureSettingsBootstrapped()
	if got != settingsPath {
		t.Fatalf("EnsureSettingsBootstrapped() = %q, want %q", got, settingsPath)
	}
}

func TestConfigFilesUseAppSettingsDir(t *testing.T) {
	appDir := filepath.Join(t.TempDir(), "app")
	t.Setenv("APP_DATA_DIR", appDir)
	settingsPath := filepath.Join(appDir, "settings")

	// Settings dir holds the user-editable config files. Nostr keys
	// moved OUT of settings into a dedicated keys/ dir (SSH
	// convention; outside the rsync scope of mirror mode), so they
	// are tested separately below.
	cases := map[string]string{
		"accounts":    accountsConfigPath(),
		"categories":  categoriesPath(),
		"collectives": collectivesPath(),
		"config.env":  configEnvPath(),
		"rules":       rulesPath(),
	}

	for name, got := range cases {
		if !filepath.IsAbs(got) {
			t.Fatalf("%s path is not absolute: %q", name, got)
		}
		if filepath.Dir(got) != settingsPath {
			t.Fatalf("%s path dir = %q, want %q", name, filepath.Dir(got), settingsPath)
		}
	}
}

func TestNostrKeysUseDedicatedKeysDirWithLegacyFallback(t *testing.T) {
	appDir := filepath.Join(t.TempDir(), "app")
	t.Setenv("APP_DATA_DIR", appDir)

	if err := SaveNostrKeys(&NostrKeys{Npub: "npub-new", PubHex: "pub-new"}); err != nil {
		t.Fatalf("SaveNostrKeys: %v", err)
	}
	if _, err := os.Stat(filepath.Join(appDir, "keys", "nostr.json")); err != nil {
		t.Fatalf("expected keys/nostr.json: %v", err)
	}
	if got := LoadNostrKeys(); got == nil || got.Npub != "npub-new" {
		t.Fatalf("LoadNostrKeys() = %#v, want npub-new", got)
	}

	// Drop the new-location file and the legacy paths so the
	// next LoadNostrKeys read truly falls back rather than
	// auto-migrating from a previous run's leftovers.
	if err := os.Remove(nostrKeysPath()); err != nil {
		t.Fatalf("remove nostr.json: %v", err)
	}
	// Legacy path: ~/.chb/.nostr-keys.json (top-level dotfile).
	if err := os.WriteFile(legacyTopLevelNostrKeysPath(), []byte(`{"npub":"npub-legacy","pubHex":"pub-legacy"}`), 0600); err != nil {
		t.Fatalf("write legacy nostr keys: %v", err)
	}
	if got := LoadNostrKeys(); got == nil || got.Npub != "npub-legacy" {
		t.Fatalf("LoadNostrKeys() legacy = %#v, want npub-legacy", got)
	}
	// Auto-migration should have written the keys to the new
	// canonical location.
	if _, err := os.Stat(nostrKeysPath()); err != nil {
		t.Fatalf("migration to new path: %v", err)
	}
}
