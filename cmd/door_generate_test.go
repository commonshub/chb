package cmd

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	discordsource "github.com/CommonsHub/chb/providers/discord"
)

func doorMsg(id, ts, content string, mentions ...discordsource.Author) discordsource.Message {
	return discordsource.Message{
		ID:        id,
		Author:    discordsource.Author{ID: "999", Username: "door-bot"},
		Content:   content,
		Timestamp: ts,
		Mentions:  mentions,
	}
}

func writeDoorFixture(t *testing.T, dataDir, year, month, channelID string, msgs []discordsource.Message) {
	t.Helper()
	dir := filepath.Join(dataDir, year, month, "providers", "discord", channelID)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(discordsource.CacheFile{Messages: msgs, ChannelID: channelID})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "messages.json"), data, 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestGenerateMonthDoor(t *testing.T) {
	dataDir := t.TempDir()
	channelID := "1306678821751230514"

	alice := discordsource.Author{
		ID:         "111",
		Username:   "alice_dc",
		GlobalName: strPtr("Alice"),
		Avatar:     strPtr("abcdef"),
	}

	msgs := []discordsource.Message{
		// Alice, via shortcut: three openings across TWO distinct Brussels days.
		// The UTC timestamps straddle midnight: 22:30Z on Jul 1 is 00:30 Brussels
		// Jul 2 (CEST) — the day bucketing must be Brussels, not UTC.
		doorMsg("1", "2026-07-01T08:00:00.000000+00:00", "🚪 Door opened by <@111> via shortcut 📲", alice),
		doorMsg("2", "2026-07-01T15:00:00.000000+00:00", "🚪 Door opened by <@111> via shortcut 📲", alice),
		doorMsg("3", "2026-07-01T22:30:00.000000+00:00", "🚪 Door opened by <@111> via shortcut 📲", alice),
		// Bob via Citizen Wallet, one day, twice.
		doorMsg("4", "2026-07-03T09:00:00.000000+00:00", "🚪 Door opened by bob via Citizen Wallet"),
		doorMsg("5", "2026-07-03T18:00:00.000000+00:00", "🚪 Door opened by bob via Citizen Wallet"),
		// A guest through a signed event link.
		doorMsg("6", "2026-07-04T10:00:00.000000+00:00", "🚪 Jane Doe opened the door for [Repair Café](<https://lu.ma/x>) hosted by Ana"),
		// Anonymous token opening: counted, credited to no one.
		doorMsg("7", "2026-07-05T10:00:00.000000+00:00", "🚪 Door opened using today's token"),
		// Chatter in the channel is ignored.
		doorMsg("8", "2026-07-05T11:00:00.000000+00:00", "was the door left open?"),
	}
	writeDoorFixture(t, dataDir, "2026", "07", channelID, msgs)

	settings := &Settings{}
	settings.Discord.Channels = json.RawMessage(`{"door": "` + channelID + `"}`)

	if !generateMonthDoorGo(dataDir, "2026", "07", settings) {
		t.Fatal("expected door.json to be written")
	}

	data, err := os.ReadFile(filepath.Join(dataDir, "2026", "07", stewardsDirName, "door.json"))
	if err != nil {
		t.Fatal(err)
	}
	var out DoorMonthFile
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatal(err)
	}

	if out.Month != "2026-07" {
		t.Errorf("month = %q", out.Month)
	}
	if out.TokenOpens != 1 {
		t.Errorf("tokenOpens = %d, want 1", out.TokenOpens)
	}
	if out.TotalOpens != 7 {
		t.Errorf("totalOpens = %d, want 7 (3 alice + 2 bob + 1 guest + 1 token)", out.TotalOpens)
	}
	if len(out.Openers) != 3 {
		t.Fatalf("openers = %d, want 3 (alice, bob, jane)", len(out.Openers))
	}

	// Sorted by distinct days desc: alice (2 days) first.
	a := out.Openers[0]
	if a.ID != "111" || a.Username != "alice_dc" || a.Name != "Alice" {
		t.Errorf("alice identity wrong: %+v", a)
	}
	if a.Avatar != "https://cdn.discordapp.com/avatars/111/abcdef.png?size=128" {
		t.Errorf("alice avatar = %q", a.Avatar)
	}
	if a.Days != 2 {
		t.Errorf("alice days = %d, want 2 (22:30Z rolls into Jul 2 Brussels)", a.Days)
	}
	if a.Opens != 3 {
		t.Errorf("alice opens = %d, want 3", a.Opens)
	}
	if len(a.Dates) != 2 || a.Dates[0] != "2026-07-01" || a.Dates[1] != "2026-07-02" {
		t.Errorf("alice dates = %v", a.Dates)
	}

	var bob, jane *DoorOpener
	for i := range out.Openers {
		switch out.Openers[i].Name {
		case "bob":
			bob = &out.Openers[i]
		case "Jane Doe":
			jane = &out.Openers[i]
		}
	}
	if bob == nil {
		t.Fatal("bob missing")
	}
	if bob.ID != "" || bob.Username != "bob" || bob.Days != 1 || bob.Opens != 2 {
		t.Errorf("bob = %+v", *bob)
	}
	if len(bob.Via) != 1 || bob.Via[0] != "citizenwallet" {
		t.Errorf("bob via = %v", bob.Via)
	}
	if jane == nil {
		t.Fatal("jane missing")
	}
	if jane.ID != "" || jane.Days != 1 || jane.Opens != 1 {
		t.Errorf("jane = %+v", *jane)
	}
	if len(jane.Via) != 1 || jane.Via[0] != "event" {
		t.Errorf("jane via = %v", jane.Via)
	}
}

func TestGenerateMonthDoorNoChannelConfigured(t *testing.T) {
	settings := &Settings{}
	settings.Discord.Channels = json.RawMessage(`{"general": "123"}`)
	if generateMonthDoorGo(t.TempDir(), "2026", "07", settings) {
		t.Fatal("must be a no-op when the door channel is not configured")
	}
}

func TestGenerateMonthDoorNoMirrorYet(t *testing.T) {
	settings := &Settings{}
	settings.Discord.Channels = json.RawMessage(`{"door": "1306678821751230514"}`)
	if generateMonthDoorGo(t.TempDir(), "2026", "07", settings) {
		t.Fatal("must be a no-op when the channel has no cached messages for the month")
	}
}

func TestDoorOpenerIdentityFallbacks(t *testing.T) {
	// Raw mention in content but an empty mentions array: keep the id.
	key, opener, via := doorOpenerIdentity("🚪 Door opened by <@4242> via shortcut 📲", nil)
	if key != "id:4242" || opener.ID != "4242" || via != "shortcut" {
		t.Errorf("raw mention: key=%q opener=%+v via=%q", key, opener, via)
	}

	// Unrecognized door-ish content credits no one.
	key, _, _ = doorOpenerIdentity("🚪 Front door unlocked for deliveries", nil)
	if key != "" {
		t.Errorf("unrecognized content must not be credited, got key=%q", key)
	}
}
