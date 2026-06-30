package odoo

import (
	"path/filepath"
	"testing"
)

func TestRelPathNamespace(t *testing.T) {
	defer SetPathNamespace("") // never leak into other tests

	SetPathNamespace("")
	if got := RelPath("invoices.json"); got != filepath.Join("providers", "odoo", "invoices.json") {
		t.Fatalf("un-namespaced RelPath = %q", got)
	}

	SetPathNamespace("citizenspring-test")
	if PathNamespace() != "citizenspring-test" {
		t.Fatalf("PathNamespace = %q", PathNamespace())
	}
	want := filepath.Join("providers", "odoo", "citizenspring-test", "invoices.json")
	if got := RelPath("invoices.json"); got != want {
		t.Fatalf("namespaced RelPath = %q, want %q", got, want)
	}

	// Private + full Path must inherit the namespace too.
	wantPriv := filepath.Join("providers", "odoo", "citizenspring-test", "private", "invoices.json")
	if got := PrivateRelPath("invoices.json"); got != wantPriv {
		t.Fatalf("namespaced PrivateRelPath = %q, want %q", got, wantPriv)
	}
	wantFull := filepath.Join("/data", "latest", "providers", "odoo", "citizenspring-test", "journals", "48.json")
	if got := Path("/data", "latest", "", "journals", "48.json"); got != wantFull {
		t.Fatalf("namespaced Path = %q, want %q", got, wantFull)
	}
}
