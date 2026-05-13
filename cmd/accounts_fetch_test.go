package cmd

import (
	"strings"
	"testing"
	"time"
)

func TestAccountFetchArgsPinsSourceForStripe(t *testing.T) {
	args := accountFetchArgs(AccountConfig{Slug: "stripe", Provider: "stripe"}, []string{"--limit", "1"})
	want := []string{"--source", "stripe", "--account-sync", "--slug", "stripe", "--limit", "1"}
	if len(args) != len(want) {
		t.Fatalf("args = %#v, want %#v", args, want)
	}
	for i := range want {
		if args[i] != want[i] {
			t.Fatalf("args = %#v, want %#v", args, want)
		}
	}
}

func TestAccountFetchArgsPreservesExplicitSource(t *testing.T) {
	args := accountFetchArgs(AccountConfig{Slug: "stripe", Provider: "stripe"}, []string{"--source", "monerium"})
	want := []string{"--account-sync", "--slug", "stripe", "--source", "monerium"}
	if len(args) != len(want) {
		t.Fatalf("args = %#v, want %#v", args, want)
	}
	for i := range want {
		if args[i] != want[i] {
			t.Fatalf("args = %#v, want %#v", args, want)
		}
	}
}

func TestAccountFetchArgsNarrowsEtherscanSyncFromLastRecordedMonth(t *testing.T) {
	acc := AccountConfig{Slug: "checking", Provider: "etherscan", Chain: "gnosis"}
	checkpoint := accountSourceCheckpoint{Exists: true, Month: "2026-04"}

	args := accountFetchArgsForCheckpoint(acc, []string{"--no-nostr"}, checkpoint)
	want := []string{"--source", "gnosis", "--account-sync", "--slug", "checking", "--no-nostr", "--since", "2026-04"}
	if len(args) != len(want) {
		t.Fatalf("args = %#v, want %#v", args, want)
	}
	for i := range want {
		if args[i] != want[i] {
			t.Fatalf("args = %#v, want %#v", args, want)
		}
	}
}

func TestAccountFetchArgsPreservesExplicitRange(t *testing.T) {
	acc := AccountConfig{Slug: "checking", Provider: "etherscan", Chain: "gnosis"}
	checkpoint := accountSourceCheckpoint{Exists: true, Month: "2026-04"}

	args := accountFetchArgsForCheckpoint(acc, []string{"--since", "2025-11"}, checkpoint)
	want := []string{"--source", "gnosis", "--account-sync", "--slug", "checking", "--since", "2025-11"}
	if len(args) != len(want) {
		t.Fatalf("args = %#v, want %#v", args, want)
	}
	for i := range want {
		if args[i] != want[i] {
			t.Fatalf("args = %#v, want %#v", args, want)
		}
	}
}

func TestAccountSyncIsFullOnlyForHistoryOrSince(t *testing.T) {
	if !accountSyncIsFull([]string{"--history"}) {
		t.Fatal("--history account sync should update last full sync")
	}
	if !accountSyncIsFull([]string{"--since", "2024-01"}) {
		t.Fatal("--since account sync should update last full sync")
	}
	if accountSyncIsFull([]string{"2026/05"}) {
		t.Fatal("single-month account sync should not update last full sync")
	}
}

func TestAccountSyncPlanLinesShowsSourceAddressTokenAndSince(t *testing.T) {
	acc := &AccountConfig{
		Slug:     "savings",
		Provider: "etherscan",
		Chain:    "gnosis",
		Address:  "0xabc",
		Token: &struct {
			Address  string `json:"address"`
			Name     string `json:"name"`
			Symbol   string `json:"symbol"`
			Decimals int    `json:"decimals"`
		}{Address: "0xeure", Symbol: "EURe", Decimals: 18},
	}
	checkpoint := accountSourceCheckpoint{Exists: true, Timestamp: time.Date(2026, 5, 11, 9, 21, 45, 0, BrusselsTZ()).Unix()}

	lines := accountSyncPlanLines(acc, "gnosis", checkpoint, false)
	joined := strings.Join(lines, "\n")

	for _, want := range []string{
		"Source:  gnosis",
		"Address: 0xabc",
		"Token:   EURe (0xeure)",
		"Since:   2026-05-11 (last tx)",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("plan lines missing %q:\n%s", want, joined)
		}
	}
}
