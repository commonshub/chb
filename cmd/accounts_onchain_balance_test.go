package cmd

import (
	"math"
	"os"
	"strings"
	"testing"
)

// TestOnchainBalanceMatchesJournal is an end-to-end integration test that
// exercises the new command shape:
//
//  1. chb accounts <slug> sync   (source → local cache)
//  2. chb odoo journals <id> sync (local → Odoo push)
//  3. Assert that the live on-chain balance equals the Odoo journal line sum.
//
// The test runs against whatever Etherscan + Odoo credentials are present in
// the environment. It picks the first linked etherscan+token account it finds
// (or the account named by the ONCHAIN_TEST_SLUG env var).
//
// Skips cleanly if credentials are missing, so normal `go test ./...` on a
// dev machine without an Odoo test instance won't fail.
//
// Run with:
//   go test -v -run TestOnchainBalanceMatchesJournal -timeout 600s ./cmd/
func TestOnchainBalanceMatchesJournal(t *testing.T) {
	LoadEnvFromConfig()

	if os.Getenv("ETHERSCAN_API_KEY") == "" && os.Getenv("GNOSISSCAN_API_KEY") == "" {
		t.Skip("ETHERSCAN_API_KEY / GNOSISSCAN_API_KEY not set")
	}

	creds, err := ResolveOdooCredentials()
	if err != nil {
		t.Skipf("Odoo credentials not configured: %v", err)
	}
	if !strings.Contains(creds.DB, "test") {
		t.Skipf("Safety check: Odoo database must contain 'test', got %q (point ODOO_DB at a test instance to enable)", creds.DB)
	}
	uid, err := odooAuth(creds.URL, creds.DB, creds.Login, creds.Password)
	if err != nil || uid == 0 {
		t.Fatalf("Odoo auth failed: %v", err)
	}

	// Pick the target account: honour ONCHAIN_TEST_SLUG, else first
	// etherscan+token account with a linked journal.
	configs := LoadAccountConfigs()
	want := strings.ToLower(os.Getenv("ONCHAIN_TEST_SLUG"))
	var acc *AccountConfig
	for i := range configs {
		a := &configs[i]
		if a.Provider != "etherscan" || a.Token == nil || a.OdooJournalID == 0 {
			continue
		}
		if want != "" && !strings.EqualFold(a.Slug, want) {
			continue
		}
		acc = a
		break
	}
	if acc == nil {
		t.Skip("no etherscan+token account with a linked Odoo journal found")
	}

	t.Logf("Target: slug=%s address=%s token=%s journal=#%d",
		acc.Slug, acc.Address, acc.Token.Symbol, acc.OdooJournalID)

	// Step 1 — source → local.
	t.Log("Step 1: AccountFetch (source → local cache)…")
	if err := AccountFetch(acc.Slug, []string{"--no-nostr"}); err != nil {
		t.Fatalf("AccountFetch: %v", err)
	}

	// Step 2 — local → Odoo.
	t.Log("Step 2: AccountOdooPush (local → Odoo journal)…")
	if err := AccountOdooPush(acc.Slug, nil); err != nil {
		t.Fatalf("AccountOdooPush: %v", err)
	}

	// Step 3 — compare balances.
	live, err := fetchTokenBalance(acc.ChainID, acc.Token.Address, acc.Address, acc.Token.Decimals)
	if err != nil {
		t.Fatalf("fetchTokenBalance: %v", err)
	}
	odooSum, err := odooJournalLineSum(creds, uid, acc.OdooJournalID)
	if err != nil {
		t.Fatalf("odooJournalLineSum: %v", err)
	}
	diff := odooSum - live
	t.Logf("on-chain balance: %.6f %s", live, acc.Token.Symbol)
	t.Logf("Odoo journal sum: %.6f %s", odooSum, acc.Token.Symbol)
	t.Logf("diff:            %+.6f %s", diff, acc.Token.Symbol)

	// Tolerance of 0.01 of the token's currency.
	if math.Abs(diff) > 0.01 {
		// On mismatch, dump some diagnostics to make iteration easier.
		localCount := len(loadAccountTransactions(acc))
		odooCount := odooJournalLineCount(creds, uid, acc.OdooJournalID)
		t.Errorf("balance mismatch: off by %+.6f %s (tolerance 0.01)\n"+
			"  local tx count: %d\n"+
			"  Odoo line count: %d",
			diff, acc.Token.Symbol, localCount, odooCount)
	}
}
