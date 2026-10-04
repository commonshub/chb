package cmd

import (
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
