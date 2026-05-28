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
	t.Setenv("APP_DATA_DIR", filepath.Join(home, ".chb"))

	seedSettingsFixture(t, "settings.json", `{
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
	seedSettingsFixture(t, "rooms.json", `{
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
	t.Setenv("APP_DATA_DIR", filepath.Join(home, ".chb"))

	seedSettingsFixture(t, "settings.json", `{
		"discord": {"guildId": "guild-1", "roles": {}, "channels": {}},
		"calendars": {"google": "old-google", "luma": "old-luma"},
		"luma": {"calendarId": "old-calendar"}
	}`)
	seedSettingsFixture(t, "calendars.json", `{
		"google": {"url": "new-google", "visibility": "auto"},
		"luma": {"url": "new-luma", "visibility": "public"}
	}`)

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

// TestLoadSettingsMergesTokensIntoFinanceAccounts verifies the post-v3 split
// where tokens.json (hand-maintained) and accounts.json (only real accounts)
// are loaded independently and merged into settings.Finance.Accounts. The
// earlier auto-migration that moved addressless token-tracker entries out of
// accounts.json was retired once tokens.json shipped as a default — see
// commit 6de6799 ("Prepare v3 account and settings release").
func TestLoadSettingsMergesTokensIntoFinanceAccounts(t *testing.T) {
	appDir := t.TempDir()
	t.Setenv("APP_DATA_DIR", appDir)
	seedSettingsFixture(t, "settings.json", `{
	  "discord": {"guildId": "guild-1", "roles": {}, "channels": {}}
	}`)
	seedSettingsFixture(t, "accounts.json", `[
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
	seedSettingsFixture(t, "tokens.json", `[
	  {
	    "name": "Commons Hub Token",
	    "slug": "cht",
	    "provider": "etherscan",
	    "chain": "celo",
	    "chainId": 42220,
	    "address": "0xcht",
	    "symbol": "CHT",
	    "decimals": 6,
	    "contribution": true
	  }
	]`)

	settings, err := LoadSettings()
	if err != nil {
		t.Fatalf("LoadSettings: %v", err)
	}

	if len(settings.Tokens) != 1 || settings.Tokens[0].Slug != "cht" {
		t.Fatalf("settings.Tokens = %#v, want one CHT entry", settings.Tokens)
	}

	if len(settings.Finance.Accounts) != 2 {
		t.Fatalf("Finance.Accounts = %d, want treasury + CHT projection: %#v", len(settings.Finance.Accounts), settings.Finance.Accounts)
	}
	if settings.Finance.Accounts[0].Slug != "treasury" {
		t.Fatalf("Finance.Accounts[0].Slug = %q, want treasury", settings.Finance.Accounts[0].Slug)
	}
	if settings.Finance.Accounts[1].Slug != "cht" || settings.Finance.Accounts[1].Token == nil {
		t.Fatalf("Finance.Accounts[1] should be CHT projection: %#v", settings.Finance.Accounts[1])
	}

	if settings.ContributionToken == nil || settings.ContributionToken.Symbol != "CHT" {
		t.Fatalf("ContributionToken not derived from tokens.json: %#v", settings.ContributionToken)
	}

	// accounts.json and tokens.json should remain independent — LoadSettings
	// never rewrites them.
	accounts := LoadAccountConfigs()
	if len(accounts) != 1 || accounts[0].Slug != "treasury" {
		t.Fatalf("accounts.json was rewritten unexpectedly: %#v", accounts)
	}
	tokens := LoadTokenConfigs()
	if len(tokens) != 1 || tokens[0].Symbol != "CHT" {
		t.Fatalf("tokens.json was rewritten unexpectedly: %#v", tokens)
	}
}

func writeTestFile(dir, name string, data []byte) error {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, name), data, 0644)
}
