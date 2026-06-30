package cmd

import (
	"fmt"
	"os"
	"testing"
)

// TestDiscoverJournalGlAccounts is a live, read-only helper (gated on
// CHB_LIVE_GL=1) that prints each configured account's linked journal and its
// default GL account code. Used once to populate odooGlAccountCode in
// accounts.json. Run with:
//
//	CHB_LIVE_GL=1 go test ./cmd/ -run TestDiscoverJournalGlAccounts -v -count=1
func TestDiscoverJournalGlAccounts(t *testing.T) {
	if os.Getenv("CHB_LIVE_GL") == "" {
		t.Skip("set CHB_LIVE_GL=1 to run the live GL discovery")
	}
	if dir := os.Getenv("CHB_LIVE_APP_DIR"); dir != "" {
		t.Setenv("APP_DATA_DIR", dir)
	}
	creds, err := ResolveOdooCredentials()
	if err != nil {
		t.Fatalf("creds: %v", err)
	}
	uid, err := odooAuth(creds.URL, creds.DB, creds.Login, creds.Password)
	if err != nil || uid == 0 {
		t.Fatalf("auth: %v", err)
	}
	for _, acc := range LoadAccountConfigs() {
		if acc.OdooJournalID == 0 {
			fmt.Printf("%-24s journal=-    (no link)\n", acc.Slug)
			continue
		}
		gl, ok, err := fetchJournalDefaultAccountCode(creds, uid, acc.OdooJournalID)
		if err != nil {
			fmt.Printf("%-24s journal=%-4d ERROR %v\n", acc.Slug, acc.OdooJournalID, err)
			continue
		}
		if !ok {
			fmt.Printf("%-24s journal=%-4d (no default account)\n", acc.Slug, acc.OdooJournalID)
			continue
		}
		fmt.Printf("%-24s journal=%-4d  %-8s %s\n", acc.Slug, acc.OdooJournalID, gl.Code, gl.Name)
	}
}
