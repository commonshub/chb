package cmd

import (
	"os"
	"path/filepath"
	"testing"

	odoosource "github.com/CommonsHub/chb/providers/odoo"
)

// moveReadPath must prefer the namespaced path but fall back to the legacy
// non-namespaced path for data synced before per-database namespacing — else
// existing invoices/bills read as "none found".
func TestMoveReadPathLegacyFallback(t *testing.T) {
	odoosource.SetPathNamespace("citizenspring-test")
	defer odoosource.SetPathNamespace("")

	dir := t.TempDir()
	kind := moveKindInvoice

	// Nothing on disk → returns the namespaced path (unchanged not-exist semantics).
	got := moveReadPath(dir, "2025", "05", kind, false)
	wantNS := filepath.Join(dir, "2025", "05", kind.relPath())
	if got != wantNS {
		t.Fatalf("empty: got %q, want namespaced %q", got, wantNS)
	}

	// Only legacy exists → falls back to it.
	legacy := filepath.Join(dir, "2025", "05", kind.legacyRelPath())
	if err := os.MkdirAll(filepath.Dir(legacy), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(legacy, []byte(`{"invoices":[]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if got := moveReadPath(dir, "2025", "05", kind, false); got != legacy {
		t.Fatalf("legacy-only: got %q, want legacy %q", got, legacy)
	}

	// Namespaced now exists too → it wins.
	if err := os.MkdirAll(filepath.Dir(wantNS), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(wantNS, []byte(`{"invoices":[]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if got := moveReadPath(dir, "2025", "05", kind, false); got != wantNS {
		t.Fatalf("both: got %q, want namespaced %q", got, wantNS)
	}
}
