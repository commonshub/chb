package cmd

import (
	"os"
	"testing"
)

func TestOdooURIRoundTrip(t *testing.T) {
	uri := OdooURI("citizenspring.odoo.com", "citizenspring-test", "account.move", 42)
	want := "odoo:citizenspring.odoo.com:citizenspring-test:account.move:42"
	if uri != want {
		t.Fatalf("OdooURI = %q; want %q", uri, want)
	}
	ref, err := ParseOdooURI(uri)
	if err != nil {
		t.Fatalf("ParseOdooURI: %v", err)
	}
	if ref.Host != "citizenspring.odoo.com" || ref.DB != "citizenspring-test" ||
		ref.Model != "account.move" || ref.ID != 42 {
		t.Fatalf("round-trip lost data: %+v", ref)
	}
}

func TestParseOdooURIRejectsGarbage(t *testing.T) {
	bad := []string{
		"",
		"odoo:",
		"odoo:host:db:model",
		"stripe:txn:abc",
		"odoo:host:db:account.move:notanint",
	}
	for _, s := range bad {
		if _, err := ParseOdooURI(s); err == nil {
			t.Errorf("ParseOdooURI(%q) unexpectedly succeeded", s)
		}
	}
}

func TestOdooHost(t *testing.T) {
	cases := map[string]string{
		"https://citizenspring.odoo.com":    "citizenspring.odoo.com",
		"https://citizenspring.odoo.com/":   "citizenspring.odoo.com",
		"http://localhost:8069":             "localhost",
		"citizenspring-test.odoo.com":       "citizenspring-test.odoo.com",
		"https://erp.example.com/odoo/path": "erp.example.com",
	}
	for in, want := range cases {
		if got := OdooHost(in); got != want {
			t.Errorf("OdooHost(%q) = %q; want %q", in, got, want)
		}
	}
}

func TestHarmonizeOdooEnvDatabaseOverrideDerivesURL(t *testing.T) {
	t.Setenv("ODOO_DATABASE", "newdb")
	t.Setenv("ODOO_URL", "https://configured.odoo.com")

	HarmonizeOdooEnv(false, true)

	if got := os.Getenv("ODOO_URL"); got != "https://newdb.odoo.com" {
		t.Fatalf("ODOO_URL = %q, want derived URL", got)
	}
	if got := os.Getenv("ODOO_DATABASE"); got != "newdb" {
		t.Fatalf("ODOO_DATABASE = %q", got)
	}
}

func TestHarmonizeOdooEnvURLOverrideDerivesDatabase(t *testing.T) {
	t.Setenv("ODOO_URL", "https://newdb.odoo.com")
	t.Setenv("ODOO_DATABASE", "configured")

	HarmonizeOdooEnv(true, false)

	if got := os.Getenv("ODOO_DATABASE"); got != "newdb" {
		t.Fatalf("ODOO_DATABASE = %q, want DB derived from URL", got)
	}
	if got := os.Getenv("ODOO_URL"); got != "https://newdb.odoo.com" {
		t.Fatalf("ODOO_URL = %q", got)
	}
}

func TestResolveOdooCredentialsDerivesURLFromDatabase(t *testing.T) {
	t.Setenv("ODOO_DATABASE", "onlydb")
	t.Setenv("ODOO_URL", "")
	t.Setenv("ODOO_LOGIN", "user")
	t.Setenv("ODOO_PASSWORD", "secret")

	creds, err := ResolveOdooCredentials()
	if err != nil {
		t.Fatalf("ResolveOdooCredentials: %v", err)
	}
	if creds.URL != "https://onlydb.odoo.com" || creds.DB != "onlydb" {
		t.Fatalf("creds = %+v", creds)
	}
}
