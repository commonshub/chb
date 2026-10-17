package cmd

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// rooms.md prices rooms in tokens, so it must carry the token fine print
// (paying with tokens is not the euro service).
func TestRoomsMdTokenTerms(t *testing.T) {
	appDir := t.TempDir()
	dataDir := t.TempDir()
	t.Setenv("APP_DATA_DIR", appDir)
	t.Setenv("DATA_DIR", dataDir)
	if err := os.MkdirAll(filepath.Join(appDir, "settings"), 0755); err != nil {
		t.Fatal(err)
	}
	rooms := `{"rooms":[{"name":"Ostrom","slug":"ostrom","capacity":80,"pricePerHour":100,"tokensPerHour":10}]}`
	if err := os.WriteFile(filepath.Join(appDir, "settings", "rooms.json"), []byte(rooms), 0644); err != nil {
		t.Fatal(err)
	}

	generateRoomsMd(dataDir)

	data, err := os.ReadFile(filepath.Join(dataDir, "latest", "public", "rooms.md"))
	if err != nil {
		t.Fatal(err)
	}
	md := string(data)
	for _, want := range []string{"10 CHT/hour", "  - " + tokenTermsRooms, "- " + tokenTermsCoworking} {
		if !strings.Contains(md, want) {
			t.Errorf("rooms.md missing %q:\n%s", want, md)
		}
	}
}
