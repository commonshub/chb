package cmd

import (
	"os"
	"path/filepath"
	"testing"
)

func TestGetDiscordChannelIDsIncludesRoomChannels(t *testing.T) {
	t.Helper()

	home := t.TempDir()
	t.Setenv("HOME", home)

	settingsJSON := []byte(`{
		"discord": {
			"guildId": "guild-1",
			"roles": {},
			"channels": {
				"general": "123",
				"nested": {
					"ops": "456"
				}
			}
		}
	}`)
	if err := writeTestFile(chbDir(), "settings.json", settingsJSON); err != nil {
		t.Fatalf("write settings.json: %v", err)
	}

	roomsJSON := []byte(`{
		"rooms": [
			{
				"id": "ostrom",
				"name": "Ostrom Room",
				"slug": "ostrom",
				"discordChannelId": "1443322327159803945"
			},
			{
				"id": "no-channel",
				"name": "No Channel",
				"slug": "no-channel"
			}
		]
	}`)
	if err := writeTestFile(chbDir(), "rooms.json", roomsJSON); err != nil {
		t.Fatalf("write rooms.json: %v", err)
	}

	settings, err := LoadSettings()
	if err != nil {
		t.Fatalf("LoadSettings: %v", err)
	}

	got := GetDiscordChannelIDs(settings)
	want := map[string]string{
		"general":     "123",
		"nested/ops":  "456",
		"room/ostrom": "1443322327159803945",
	}

	if len(got) != len(want) {
		t.Fatalf("unexpected channel count: got %d want %d (%v)", len(got), len(want), got)
	}
	for key, wantID := range want {
		if got[key] != wantID {
			t.Fatalf("channel %q: got %q want %q", key, got[key], wantID)
		}
	}
}

func TestLoadSettingsMergesCalendarsFile(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	settingsJSON := []byte(`{
		"discord": {"guildId": "guild-1", "roles": {}, "channels": {}},
		"calendars": {"google": "old-google", "luma": "old-luma"},
		"luma": {"calendarId": "old-calendar"}
	}`)
	if err := writeTestFile(AppSettingsDir(), "settings.json", settingsJSON); err != nil {
		t.Fatalf("write settings.json: %v", err)
	}

	calendarsJSON := []byte(`{
		"google": {"url": "new-google", "visibility": "auto"},
		"luma": {"url": "new-luma", "visibility": "public"}
	}`)
	if err := writeTestFile(AppSettingsDir(), "calendars.json", calendarsJSON); err != nil {
		t.Fatalf("write calendars.json: %v", err)
	}

	settings, err := LoadSettings()
	if err != nil {
		t.Fatalf("LoadSettings: %v", err)
	}

	if settings.Calendars.Google != "new-google" {
		t.Fatalf("google calendar = %q", settings.Calendars.Google)
	}
	if settings.Calendars.Luma != "new-luma" {
		t.Fatalf("luma calendar = %q", settings.Calendars.Luma)
	}
	if len(settings.Calendars.Sources) != 2 {
		t.Fatalf("sources length = %d", len(settings.Calendars.Sources))
	}
	if settings.Calendars.Sources[0].Slug != "google" || settings.Calendars.Sources[0].Provider != CalendarProviderICS || settings.Calendars.Sources[0].Visibility != CalendarVisibilityAuto {
		t.Fatalf("google source = %#v", settings.Calendars.Sources[0])
	}
	if settings.Calendars.Sources[1].Slug != "luma" || settings.Calendars.Sources[1].Provider != CalendarProviderICS || settings.Calendars.Sources[1].Visibility != CalendarVisibilityPublic {
		t.Fatalf("luma source = %#v", settings.Calendars.Sources[1])
	}
}

func TestLoadSettingsMigratesAddresslessTokenTrackersToTokens(t *testing.T) {
	appDir := t.TempDir()
	t.Setenv("APP_DATA_DIR", appDir)
	writeJSONFixture(t, filepath.Join(appDir, "settings", "settings.json"), `{
	  "discord": {"guildId": "guild-1", "roles": {}, "channels": {}}
	}`)
	writeJSONFixture(t, filepath.Join(appDir, "settings", "accounts.json"), `[
	  {
	    "name": "Commons Hub Token",
	    "slug": "cht",
	    "provider": "etherscan",
	    "chain": "celo",
	    "chainId": 42220,
	    "token": {"address": "0xcht", "name": "Commons Hub Token", "symbol": "CHT", "decimals": 18}
	  },
	  {
	    "name": "Treasury",
	    "slug": "treasury",
	    "provider": "etherscan",
	    "chain": "gnosis",
	    "chainId": 100,
	    "address": "0xabc",
	    "token": {"address": "0xeure", "name": "EURe", "symbol": "EURe", "decimals": 18}
	  }
	]`)

	settings, err := LoadSettings()
	if err != nil {
		t.Fatalf("LoadSettings: %v", err)
	}
	if len(settings.Tokens) != 1 || settings.Tokens[0].Slug != "cht" {
		t.Fatalf("tokens = %#v, want migrated CHT token", settings.Tokens)
	}
	if len(settings.Finance.Accounts) != 2 {
		t.Fatalf("finance accounts = %d, want treasury account plus token tracker: %#v", len(settings.Finance.Accounts), settings.Finance.Accounts)
	}

	accounts := LoadAccountConfigs()
	if len(accounts) != 1 || accounts[0].Slug != "treasury" {
		t.Fatalf("accounts.json was not stripped to real accounts: %#v", accounts)
	}
	tokens := LoadTokenConfigs()
	if len(tokens) != 1 || tokens[0].Symbol != "CHT" {
		t.Fatalf("tokens.json was not written: %#v", tokens)
	}
}

func writeTestFile(dir, name string, data []byte) error {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, name), data, 0644)
}
