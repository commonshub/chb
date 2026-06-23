package cmd

import (
	"path/filepath"
	"testing"
)

func TestAccountIdentityKeyPrefersStableIdentity(t *testing.T) {
	cases := []struct {
		name string
		acc  AccountConfig
		want string
	}{
		{"iban wins, normalized", AccountConfig{Slug: "kbc", IBAN: "be46 7340 7223 8636", Address: "0xabc"}, "iban:BE46734072238636"},
		{"chain-scoped address when chain id is present", AccountConfig{Slug: "savings", ChainID: 100, Address: "0xAbCdEf"}, "ethereum:100:address:0xabcdef"},
		{"legacy address when no chain is present", AccountConfig{Slug: "savings", Address: "0xAbCdEf"}, "address:0xabcdef"},
		{"stripe account id", AccountConfig{Slug: "stripe", AccountID: "acct_123"}, "stripe:acct_123"},
		{"slug fallback", AccountConfig{Slug: "Misc"}, "slug:misc"},
	}
	for _, c := range cases {
		if got := accountIdentityKey(c.acc); got != c.want {
			t.Errorf("%s: accountIdentityKey = %q, want %q", c.name, got, c.want)
		}
	}
}

// The link must survive both slug reassignment and Odoo instance switches: it is
// keyed by IBAN/address, not by slug or database name.
func TestApplyOdooJournalLinksByIdentityOnly(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("APP_DATA_DIR", filepath.Join(tmp, "app"))
	t.Setenv("ODOO_URL", "")
	t.Setenv("ODOO_DATABASE", "")

	if err := saveOdooJournalLinks(OdooJournalLinks{
		"iban:BE46734072238636":      28,
		"address:0xabc":              51,
		"ethereum:100:address:0xdef": 47,
		"ethereum:137:address:0xdef": 99,
		"stripe:acct_123":            48,
	}); err != nil {
		t.Fatalf("saveOdooJournalLinks: %v", err)
	}

	accounts := []AccountConfig{
		{Slug: "kbc", IBAN: "BE46 7340 7223 8636"},
		{Slug: "fridge", Address: "0xABC"},
		{Slug: "safe", ChainID: 100, Address: "0xDEF"},
		{Slug: "polygon", ChainID: 137, Address: "0xDEF"},
		{Slug: "stripe", AccountID: "acct_123"},
	}
	applyOdooJournalLinks(accounts)

	want := []int{28, 51, 47, 99, 48}
	for i, w := range want {
		if accounts[i].OdooJournalID != w {
			t.Fatalf("account %s: got journal %d, want %d", accounts[i].Slug, accounts[i].OdooJournalID, w)
		}
	}
}

func TestApplyOdooJournalLinksDoesNotApplyAmbiguousLegacyAddress(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("APP_DATA_DIR", filepath.Join(tmp, "app"))

	if err := saveOdooJournalLinks(OdooJournalLinks{"address:0xabc": 47}); err != nil {
		t.Fatalf("saveOdooJournalLinks: %v", err)
	}

	accounts := []AccountConfig{
		{Slug: "gnosis", ChainID: 100, Address: "0xABC"},
		{Slug: "polygon", ChainID: 137, Address: "0xABC"},
	}
	applyOdooJournalLinks(accounts)
	if accounts[0].OdooJournalID != 0 || accounts[1].OdooJournalID != 0 {
		t.Fatalf("ambiguous legacy link applied: %#v", accounts)
	}
}

func TestParseOdooJournalLinksMigratesInstanceScopedSchema(t *testing.T) {
	t.Setenv("ODOO_DATABASE", "current-instance")
	got := parseOdooJournalLinks([]byte(`{
		"other-instance": {"iban:BE1": 999, "address:0xabc": 51},
		"current-instance": {"iban:BE1": 28}
	}`))
	if got["iban:BE1"] != 28 {
		t.Fatalf("current instance should win for duplicate key, got %v", got)
	}
	if got["address:0xabc"] != 51 {
		t.Fatalf("non-conflicting legacy scoped key should be preserved, got %v", got)
	}
}

// Migration moves legacy accounts.json IDs into flat odoo-journals.json and is
// a no-op once the file exists.
func TestMigrateOdooJournalLinks(t *testing.T) {
	tmp := t.TempDir()
	dir := filepath.Join(tmp, "app", "settings")
	t.Setenv("APP_DATA_DIR", filepath.Join(tmp, "app"))

	if err := SaveAccountConfigs([]AccountConfig{
		{Slug: "kbc", IBAN: "BE46734072238636", OdooJournalID: 28},
		{Slug: "fridge", Address: "0xABC", OdooJournalID: 51},
		{Slug: "nolink", Address: "0xdef"},
	}); err != nil {
		t.Fatalf("SaveAccountConfigs: %v", err)
	}

	migrateOdooJournalLinks(dir)

	got := loadOdooJournalLinks()
	if got["iban:BE46734072238636"] != 28 || got["address:0xabc"] != 51 {
		t.Fatalf("migrated links = %v, want kbc=28 fridge=51", got)
	}
	if _, ok := got["address:0xdef"]; ok {
		t.Errorf("account without a journal id should not be migrated")
	}

	// Idempotent: a second run must not clobber an edited link.
	_ = setOdooJournalLink("ignored-instance", "iban:BE46734072238636", 99)
	migrateOdooJournalLinks(dir)
	if loadOdooJournalLinks()["iban:BE46734072238636"] != 99 {
		t.Errorf("migration overwrote an existing odoo-journals.json")
	}
}

// TestOdooJournalLinksSurviveBootstrap is the regression guard for the bug
// where odoo-journals.json was in forceOverwriteDefaults: every `chb` run
// reverted a locally-created link (`chb accounts <slug> link`) back to the
// embedded default. The file is local-mutable and must survive bootstrap.
func TestOdooJournalLinksSurviveBootstrap(t *testing.T) {
	appDir := filepath.Join(t.TempDir(), "app")
	t.Setenv("HOME", filepath.Join(t.TempDir(), "home"))
	t.Setenv("APP_DATA_DIR", appDir)

	// First bootstrap installs the embedded default + records the tracker.
	EnsureSettingsBootstrapped()

	linksPath := filepath.Join(appDir, "settings", odooJournalLinksFileName)
	// Simulate `chb accounts <slug> link` choosing a journal that is NOT the
	// embedded default — a sentinel id (never present in the embedded copy) so
	// a force-overwrite reversion would be unambiguous.
	const key = "ethereum:100:address:0xb01ccce2d75d517ee520de31eae6afca735aadc1"
	if err := saveOdooJournalLinks(OdooJournalLinks{key: 99999}); err != nil {
		t.Fatalf("save local link: %v", err)
	}

	// A subsequent bootstrap must NOT clobber the local link.
	EnsureSettingsBootstrapped()

	if got := loadOdooJournalLinks(); got[key] != 99999 {
		t.Fatalf("local link reverted by bootstrap: got %v (file %s)", got, linksPath)
	}
}
