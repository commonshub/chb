package cmd

import "testing"

func TestAccountFetchArgsPinsSourceForStripe(t *testing.T) {
	args := accountFetchArgs(AccountConfig{Slug: "stripe", Provider: "stripe"}, []string{"--limit", "1"})
	want := []string{"--source", "stripe", "--slug", "stripe", "--limit", "1"}
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
	want := []string{"--slug", "stripe", "--source", "monerium"}
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
	acc := AccountConfig{Slug: "checking", Provider: "etherscan"}
	checkpoint := accountSourceCheckpoint{Exists: true, Month: "2026-04"}

	args := accountFetchArgsForCheckpoint(acc, []string{"--no-nostr"}, checkpoint)
	want := []string{"--source", "etherscan", "--slug", "checking", "--no-nostr", "--since", "2026-04", "--force"}
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
	acc := AccountConfig{Slug: "checking", Provider: "etherscan"}
	checkpoint := accountSourceCheckpoint{Exists: true, Month: "2026-04"}

	args := accountFetchArgsForCheckpoint(acc, []string{"--since", "2025-11"}, checkpoint)
	want := []string{"--source", "etherscan", "--slug", "checking", "--since", "2025-11"}
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
