package cmd

import (
	"path/filepath"
	"testing"
)

// A freshly-fetched journal cache must satisfy the freshness check WITHOUT any
// Odoo round-trip (nil creds prove no RPC happens). This is what stops the
// KBC/journal push from hanging at "verifying cache freshness" during chb sync.
func TestVerifyJournalCacheFreshSkipsWhenRecentlyFetched(t *testing.T) {
	t.Setenv("DATA_DIR", filepath.Join(t.TempDir(), "data"))

	if _, err := writeOdooJournalLinesCacheFile(999, []OdooCacheLine{
		{ID: 1, Amount: 10, Date: "2024-01-01"},
	}); err != nil {
		t.Fatalf("write cache: %v", err)
	}
	// Recent FetchedAt → returns nil without touching Odoo (creds are nil).
	if err := verifyOdooJournalCacheFresh(nil, 0, 999); err != nil {
		t.Fatalf("fresh cache should skip the RPC and pass, got %v", err)
	}
	// Missing cache → a fast, clear error (not a hang).
	if err := verifyOdooJournalCacheFresh(nil, 0, 4242); err == nil {
		t.Fatal("expected an error for a journal with no local cache")
	}
}
