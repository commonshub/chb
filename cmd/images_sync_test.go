package cmd

import (
	"testing"
)

func TestCollectImageSyncScopesDefault(t *testing.T) {
	dataDir := t.TempDir()
	scopes := collectImageSyncScopes(dataDir, nil, "", "", false, "", false)
	if len(scopes) != 3 {
		t.Fatalf("expected 3 scopes, got %d", len(scopes))
	}
	if scopes[0].Year == "latest" || scopes[1].Year == "latest" || scopes[2].Year != "latest" {
		t.Fatalf("expected previous month, current month, then latest, got %+v", scopes)
	}
}

func TestCollectImageSyncScopesHistoryIncludesLatest(t *testing.T) {
	dataDir := t.TempDir()
	scopes := collectImageSyncScopes(dataDir, []string{"2024"}, "", "", false, "2024-01", true)
	if len(scopes) != 1 || scopes[0].Year != "latest" {
		t.Fatalf("expected latest scope when no month dirs exist, got %+v", scopes)
	}
}

func TestResolveDiscordImagePathUsesCanonicalFilePath(t *testing.T) {
	dataDir := t.TempDir()
	img := ImageEntry{
		ID:       "att-1",
		FilePath: "2024/12/messages/discord/images/att-1.jpeg",
	}
	got := resolveDiscordImagePath(dataDir, "latest", "", img)
	want := dataDir + "/2024/12/messages/discord/images/att-1.jpeg"
	if got != want {
		t.Fatalf("resolveDiscordImagePath() = %q, want %q", got, want)
	}
}
