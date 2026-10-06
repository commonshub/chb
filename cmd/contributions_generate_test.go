package cmd

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	discordsource "github.com/CommonsHub/chb/providers/discord"
)

func TestContributionIdentity(t *testing.T) {
	g := "Leen"
	if got := contributionIdentity(discordsource.Author{ID: "1", Username: "leen8610", GlobalName: &g}); got.DisplayName != "Leen" {
		t.Errorf("global name: %q", got.DisplayName)
	}
	mail := "someone@example.org"
	if got := contributionIdentity(discordsource.Author{ID: "2", Username: "someone", GlobalName: &mail}); got.DisplayName != "someone" {
		t.Errorf("an email as display name must fall back to the username: %q", got.DisplayName)
	}
	ms := []ContributionMessage{{ID: "1", Timestamp: "2026-08-01T10:00:00Z"}, {ID: "2", Timestamp: "2026-08-02T10:00:00Z"}}
	sortContributionsNewestFirst(ms)
	if ms[0].ID != "2" {
		t.Error("newest first")
	}
	if got := normaliseDiscordTimestamp("2025-08-01T13:42:08.773000+00:00"); got != "2025-08-01T13:42:08Z" {
		t.Errorf("timestamp: %q", got)
	}
}

func TestResolveDiscordContent(t *testing.T) {
	leen := "Leen"
	mentions := []discordsource.Author{{ID: "42", Username: "leen8610", GlobalName: &leen}}
	names := discordNames{roles: map[string]string{"7": "member"}, channels: map[string]string{"9": "general"}}
	in := "Thanks <@42> and <@!42> <@99>! <@&7> see <#9> <:chb:123> <a:dance:456> at <t:1790000000:R> BE68 5390 0754 7034"
	got := resolveDiscordContent(in, mentions, names)
	want := "Thanks @Leen and @Leen @someone! @member see #general :chb: :dance: at 2026-09-21 16:13 [IBAN]"
	if got != want {
		t.Errorf("\n got %q\nwant %q", got, want)
	}
	id := "123"
	rs := contributionReactions([]discordsource.Reaction{{Emoji: discordsource.Emoji{Name: "🙏"}, Count: 3}, {Emoji: discordsource.Emoji{ID: &id, Name: "chb"}, Count: 1}, {Emoji: discordsource.Emoji{Name: "x"}, Count: 0}})
	if len(rs) != 2 || rs[0].Emoji != "🙏" || rs[0].Count != 3 || rs[1].Emoji != ":chb:" || rs[1].ImageURL != "https://cdn.discordapp.com/emojis/123.png" {
		t.Errorf("reactions: %+v", rs)
	}
	av := "abc"
	if got := contributionIdentity(discordsource.Author{ID: "5", Username: "u", Avatar: &av}); got.AvatarURL != "https://cdn.discordapp.com/avatars/5/abc.png?size=128" {
		t.Errorf("avatar: %q", got.AvatarURL)
	}
}

func TestPraiseFeedSkipsBotsAndEmptyText(t *testing.T) {
	dir := t.TempDir()
	cf := discordsource.CacheFile{ChannelID: "c", Messages: []discordsource.Message{
		{ID: "1", Author: discordsource.Author{ID: "a", Username: "ann"}, Content: "merci!", Timestamp: "2026-10-01T10:00:00Z"},
		{ID: "2", Author: discordsource.Author{ID: "a", Username: "ann"}, Content: "  ", Timestamp: "2026-10-01T11:00:00Z"},
		{ID: "3", Author: discordsource.Author{ID: "b", Username: "bot", Bot: true}, Content: "hi", Timestamp: "2026-10-01T12:00:00Z"},
	}}
	p := discordsource.ChannelPath(dir, "2026", "10", "c")
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(cf)
	if err := os.WriteFile(p, data, 0o644); err != nil {
		t.Fatal(err)
	}
	got := praiseFeed.monthMessages(dir, "2026", "10", "c", discordNames{})
	if len(got) != 1 || got[0].ID != "1" || got[0].Content != "merci!" {
		t.Errorf("praise: %+v", got)
	}
	if got := contributionsFeed.monthMessages(dir, "2026", "10", "c", discordNames{}); len(got) != 2 {
		t.Errorf("contributions keeps photo-only posts: %d", len(got))
	}
}

func TestMaskEmailsKeepsJSONEscapes(t *testing.T) {
	for _, text := range []string{
		"thanks\n@Leen.v and\n@Inge",
		"line\nann@example.org end",
		"café bob@example.org",
		`back\slash x@example.org`,
	} {
		in, _ := json.Marshal(map[string]string{"content": text})
		out := maskEmails(in)
		var v map[string]string
		if err := json.Unmarshal(out, &v); err != nil {
			t.Fatalf("%q → invalid JSON %s: %v", text, out, err)
		}
		if strings.Contains(v["content"], "example.org") {
			t.Errorf("email not masked: %q", v["content"])
		}
	}
	in, _ := json.Marshal("hi\n@Leen.v")
	if out := maskEmails(in); string(out) != string(in) {
		t.Errorf("a mention after a newline is not an email: %s", out)
	}
}
