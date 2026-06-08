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
		{"address when no iban", AccountConfig{Slug: "savings", Address: "0xAbCdEf"}, "address:0xabcdef"},
		{"stripe account id", AccountConfig{Slug: "stripe", AccountID: "acct_123"}, "stripe:acct_123"},
		{"slug fallback", AccountConfig{Slug: "Misc"}, "slug:misc"},
	}
	for _, c := range cases {
		if got := accountIdentityKey(c.acc); got != c.want {
			t.Errorf("%s: accountIdentityKey = %q, want %q", c.name, got, c.want)
		}
	}
}

// The link must survive a slug being reassigned to a different account: it is
// keyed by IBAN/address, not the slug.
func TestApplyOdooJournalLinksByInstanceAndIdentity(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("APP_DATA_DIR", filepath.Join(tmp, "app"))
	t.Setenv("ODOO_URL", "https://example.odoo.com")
	t.Setenv("ODOO_DATABASE", "test-instance")

	// kbc identified by IBAN; on-chain account identified by address.
	if err := saveOdooJournalLinks(OdooJournalLinks{
		"test-instance":  {"iban:BE46734072238636": 28, "address:0xabc": 51},
		"other-instance": {"iban:BE46734072238636": 999},
	}); err != nil {
		t.Fatalf("saveOdooJournalLinks: %v", err)
	}

	accounts := []AccountConfig{
		{Slug: "kbc", IBAN: "BE46 7340 7223 8636"},
		{Slug: "fridge", Address: "0xABC"},
		{Slug: "unlinked", Address: "0xdef"},
	}
	applyOdooJournalLinks(accounts)

	if accounts[0].OdooJournalID != 28 {
		t.Errorf("kbc: got journal %d, want 28 (from current instance, by IBAN)", accounts[0].OdooJournalID)
	}
	if accounts[1].OdooJournalID != 51 {
		t.Errorf("fridge: got journal %d, want 51 (by address)", accounts[1].OdooJournalID)
	}
	if accounts[2].OdooJournalID != 0 {
		t.Errorf("unlinked: got journal %d, want 0", accounts[2].OdooJournalID)
	}
}

func TestApplyOdooJournalLinksNoopWhenOdooUnconfigured(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("APP_DATA_DIR", filepath.Join(tmp, "app"))
	t.Setenv("ODOO_URL", "")
	t.Setenv("ODOO_DATABASE", "")

	_ = saveOdooJournalLinks(OdooJournalLinks{"x": {"iban:BE1": 5}})
	accounts := []AccountConfig{{Slug: "kbc", IBAN: "BE1"}}
	applyOdooJournalLinks(accounts)
	if accounts[0].OdooJournalID != 0 {
		t.Errorf("with no Odoo configured, overlay must be a no-op; got %d", accounts[0].OdooJournalID)
	}
}

// Migration moves legacy accounts.json IDs into odoo-journals.json (keyed by
// the configured instance) and is a no-op once the file exists.
func TestMigrateOdooJournalLinks(t *testing.T) {
	tmp := t.TempDir()
	dir := filepath.Join(tmp, "app", "settings")
	t.Setenv("APP_DATA_DIR", filepath.Join(tmp, "app"))
	t.Setenv("ODOO_URL", "https://example.odoo.com")
	t.Setenv("ODOO_DATABASE", "test-instance")

	if err := SaveAccountConfigs([]AccountConfig{
		{Slug: "kbc", IBAN: "BE46734072238636", OdooJournalID: 28},
		{Slug: "fridge", Address: "0xABC", OdooJournalID: 51},
		{Slug: "nolink", Address: "0xdef"},
	}); err != nil {
		t.Fatalf("SaveAccountConfigs: %v", err)
	}

	migrateOdooJournalLinks(dir)

	got := loadOdooJournalLinks()["test-instance"]
	if got["iban:BE46734072238636"] != 28 || got["address:0xabc"] != 51 {
		t.Fatalf("migrated links = %v, want kbc=28 fridge=51", got)
	}
	if _, ok := got["address:0xdef"]; ok {
		t.Errorf("account without a journal id should not be migrated")
	}

	// Idempotent: a second run must not clobber an edited link.
	_ = setOdooJournalLink("test-instance", "iban:BE46734072238636", 99)
	migrateOdooJournalLinks(dir)
	if loadOdooJournalLinks()["test-instance"]["iban:BE46734072238636"] != 99 {
		t.Errorf("migration overwrote an existing odoo-journals.json")
	}
}
