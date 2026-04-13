package cmd

import (
	"testing"
)

func TestResolveWalletAddress(t *testing.T) {
	// Xavier's Discord ID → should resolve to a known wallet address
	xavierDiscordID := "689614876515237925"

	addr, err := resolveCardManagerAddress(
		xavierDiscordID,
		defaultCardManagerAddress,
		defaultInstanceID,
		defaultCeloRPC,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if addr == "" {
		t.Fatal("expected non-empty address")
	}
	t.Logf("Xavier's wallet: %s", addr)

	// Verify it matches the CardManager-derived address (used in on-chain CHT transactions)
	expected := "0xa6f29e8afdd08d518df119e08c1d1afb3730871d"
	if addr != expected {
		t.Errorf("address mismatch: got %s, want %s", addr, expected)
	}
}
