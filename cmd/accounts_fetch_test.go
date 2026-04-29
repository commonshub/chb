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
