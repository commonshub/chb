package cmd

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	etherscansource "github.com/CommonsHub/chb/providers/etherscan"
)

func TestSafeBalancesURLForGnosis(t *testing.T) {
	acc := &AccountConfig{Chain: "gnosis", Address: "0x6fDF0AaE33E313d9C98D2Aa19Bcd8EF777912CBf", WalletType: "safe"}

	got := safeBalancesURL(acc)
	want := "https://app.safe.global/balances?safe=gno:0x6fDF0AaE33E313d9C98D2Aa19Bcd8EF777912CBf"
	if got != want {
		t.Fatalf("safeBalancesURL() = %q, want %q", got, want)
	}
}

func TestAccountDetailFallsBackToTxInfoWhenWalletTypeIsNotSafe(t *testing.T) {
	acc := AccountConfig{
		Slug:     "wallet",
		Provider: "etherscan",
		Chain:    "gnosis",
		Address:  "0x1111111111111111111111111111111111111111",
	}

	out := captureStdout(t, func() {
		printAccountOnlineLink(&acc, "  ")
	})

	assertContains(t, out, "https://txinfo.xyz/gnosis/address/0x1111111111111111111111111111111111111111")
	assertNotContains(t, out, "app.safe.global")
	if got := safeBalancesURL(&acc); got != "" {
		t.Fatalf("safeBalancesURL() = %q, want empty for missing walletType", got)
	}
}

func TestAccountDetailShowsOnchainAndLocalDiagnostics(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("APP_DATA_DIR", filepath.Join(tmp, "app"))
	t.Setenv("DATA_DIR", filepath.Join(tmp, "data"))
	t.Setenv("NO_COLOR", "1")

	token := &struct {
		Address  string `json:"address"`
		Name     string `json:"name"`
		Symbol   string `json:"symbol"`
		Decimals int    `json:"decimals"`
	}{
		Address:  "0xtoken",
		Name:     "EURe",
		Symbol:   "EURe",
		Decimals: 18,
	}
	acc := AccountConfig{
		Name:       "Savings",
		Slug:       "savings",
		Provider:   "etherscan",
		Chain:      "gnosis",
		ChainID:    100,
		Address:    "0x6fDF0AaE33E313d9C98D2Aa19Bcd8EF777912CBf",
		WalletType: "safe",
		Token:      token,
	}
	if err := SaveAccountConfigs([]AccountConfig{acc}); err != nil {
		t.Fatalf("SaveAccountConfigs: %v", err)
	}
	SaveSyncState(&SyncState{
		Accounts: map[string]*SyncSourceState{
			acc.Slug: {
				LastSync:     "2026-05-07T08:30:00Z",
				LastFullSync: "2026-05-01T07:00:00Z",
			},
		},
	})
	saveBalanceCache(&balanceCache{
		FetchedAt: "2026-05-07T08:00:00Z",
		Balances:  map[string]float64{strings.ToLower(acc.Address): 125.50},
	})

	raw := etherscansource.CacheFile{
		Account: acc.Address,
		Chain:   acc.Chain,
		Token:   token.Symbol,
		Transactions: []etherscansource.TokenTransfer{
			{Hash: "0x1", TimeStamp: "1704067200", From: "0xaaa", To: acc.Address, Value: "100000000000000000000", TokenSymbol: "EURe", TokenDecimal: "18"},
			{Hash: "0x2", TimeStamp: "1704153600", From: acc.Address, To: "0xbbb", Value: "29500000000000000000", TokenSymbol: "EURe", TokenDecimal: "18"},
		},
	}
	writeJSONFileForTest(t, etherscansource.Path(DataDir(), "2024", "01", acc.Chain, etherscansource.FileName(acc.Slug, acc.Address, token.Symbol)), raw)

	chain := acc.Chain
	local := TransactionsFile{
		Year:  "2024",
		Month: "01",
		Transactions: []TransactionEntry{
			{ID: "local-1", Provider: "etherscan", Chain: &chain, Account: acc.Address, AccountSlug: acc.Slug, Currency: "EURe", Amount: 100, NormalizedAmount: 100, Type: "CREDIT", Timestamp: 1704067200},
			{ID: "local-2", Provider: "etherscan", Chain: &chain, Account: acc.Address, AccountSlug: acc.Slug, Currency: "EURe", Amount: 29.5, NormalizedAmount: 29.5, Type: "DEBIT", Timestamp: 1704153600},
		},
	}
	writeJSONFileForTest(t, filepath.Join(DataDir(), "2024", "01", "generated", "transactions.json"), local)

	out := captureStdout(t, func() {
		printAccountDetailSummary(&acc, nil)
	})

	assertContains(t, out, "https://app.safe.global/balances?safe=gno:0x6fDF0AaE33E313d9C98D2Aa19Bcd8EF777912CBf")
	assertContains(t, out, "Account:")
	assertContains(t, out, "Savings")
	assertContains(t, out, "Address:")
	assertContains(t, out, "gnosis 0x6fDF0AaE33E313d9C98D2Aa19Bcd8EF777912CBf")
	assertContains(t, out, "URL:")
	// Three labelled balances, each with its own tx count + provenance.
	assertContains(t, out, "Live balance:")
	assertContains(t, out, "125.50 EURe  (2 txs · on-chain, cached 2026-05-07 10:00)")
	assertContains(t, out, "Local balance:")
	assertContains(t, out, "70.50 EURe  (2 txs · 2024-01-01 → 2024-01-02)")
	// Last sync and last full on a single line.
	assertContains(t, out, "Last sync: 2026-05-07 10:30")
	assertContains(t, out, "last full: 2026-05-01 09:00")
	assertContains(t, out, "Balance mismatch:")
	assertContains(t, out, "computed 70.50 EURe vs on-chain 125.50 EURe")
	assertContains(t, out, "Fix:")
	assertContains(t, out, "chb accounts savings sync --history")
	assertContains(t, out, "chb accounts savings --refresh")
	if strings.Index(out, "Balance mismatch:") < strings.Index(out, "Last sync:") {
		t.Fatalf("balance mismatch should be printed after summary:\n%s", out)
	}
}

func TestAccountDetailPromptsHistoryWhenNeverFullySynced(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("APP_DATA_DIR", filepath.Join(tmp, "app"))
	t.Setenv("DATA_DIR", filepath.Join(tmp, "data"))
	t.Setenv("NO_COLOR", "1")

	acc := AccountConfig{
		Name:       "Wallet",
		Slug:       "wallet",
		Provider:   "etherscan",
		Chain:      "gnosis",
		Address:    "0x1111111111111111111111111111111111111111",
		WalletType: "eoa",
	}
	if err := SaveAccountConfigs([]AccountConfig{acc}); err != nil {
		t.Fatalf("SaveAccountConfigs: %v", err)
	}

	out := captureStdout(t, func() {
		printAccountDetailSummary(&acc, nil)
	})

	assertContains(t, out, "https://txinfo.xyz/gnosis/address/0x1111111111111111111111111111111111111111")
	assertContains(t, out, "This account has never been fully synced yet, please run `chb accounts wallet sync --history`")
}

func TestAccountDetailShowsEOAAddressTxInfoLink(t *testing.T) {
	acc := AccountConfig{
		Slug:       "wallet",
		Provider:   "etherscan",
		Chain:      "gnosis",
		Address:    "0x1111111111111111111111111111111111111111",
		WalletType: "eoa",
	}

	out := captureStdout(t, func() {
		printAccountOnlineLink(&acc, "  ")
	})

	assertContains(t, out, "Address:")
	assertContains(t, out, "URL:")
	assertContains(t, out, "https://txinfo.xyz/gnosis/address/0x1111111111111111111111111111111111111111")
}

func TestAccountDetailShowsStripeDashboardLink(t *testing.T) {
	acc := AccountConfig{
		Slug:      "stripe",
		Provider:  "stripe",
		AccountID: "acct_123",
	}

	out := captureStdout(t, func() {
		printAccountOnlineLink(&acc, "  ")
	})

	assertContains(t, out, "URL:")
	assertContains(t, out, "https://dashboard.stripe.com")
}

func TestAccountSyncVerificationReportsMissingTransfersByMonth(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("APP_DATA_DIR", filepath.Join(tmp, "app"))
	t.Setenv("DATA_DIR", filepath.Join(tmp, "data"))
	t.Setenv("NO_COLOR", "1")

	acc := testSavingsAccountConfig()
	if err := SaveAccountConfigs([]AccountConfig{acc}); err != nil {
		t.Fatalf("SaveAccountConfigs: %v", err)
	}

	raw := etherscansource.CacheFile{
		Account: acc.Address,
		Chain:   acc.Chain,
		Token:   acc.Token.Symbol,
		Transactions: []etherscansource.TokenTransfer{
			{Hash: "0xpresent", TimeStamp: "1704067200", From: "0xaaa", To: acc.Address, Value: "100000000000000000000", TokenSymbol: "EURe", TokenDecimal: "18"},
			{Hash: "0xmissing-jan", TimeStamp: "1704153600", From: "0xbbb", To: acc.Address, Value: "25000000000000000000", TokenSymbol: "EURe", TokenDecimal: "18"},
		},
	}
	writeJSONFileForTest(t, etherscansource.Path(DataDir(), "2024", "01", acc.Chain, etherscansource.FileName(acc.Slug, acc.Address, acc.Token.Symbol)), raw)
	raw.Transactions = []etherscansource.TokenTransfer{
		{Hash: "0xmissing-feb", TimeStamp: "1706745600", From: acc.Address, To: "0xccc", Value: "10000000000000000000", TokenSymbol: "EURe", TokenDecimal: "18"},
	}
	writeJSONFileForTest(t, etherscansource.Path(DataDir(), "2024", "02", acc.Chain, etherscansource.FileName(acc.Slug, acc.Address, acc.Token.Symbol)), raw)

	chain := acc.Chain
	local := TransactionsFile{
		Year:  "2024",
		Month: "01",
		Transactions: []TransactionEntry{
			{ID: "local-1", TxHash: "0xpresent", Provider: "etherscan", Chain: &chain, Account: acc.Address, AccountSlug: acc.Slug, Currency: "EURe", Amount: 100, NormalizedAmount: 100, Type: "CREDIT", Timestamp: 1704067200},
		},
	}
	writeJSONFileForTest(t, filepath.Join(DataDir(), "2024", "01", "generated", "transactions.json"), local)

	live := 115.0
	result := verifyAccountLocalAgainstOnchainCache(&acc, &live)
	if result == nil {
		t.Fatal("verifyAccountLocalAgainstOnchainCache returned nil")
	}
	if result.OnchainTxCount != 3 || result.LocalTxCount != 1 {
		t.Fatalf("counts = onchain %d local %d, want 3/1", result.OnchainTxCount, result.LocalTxCount)
	}
	if len(result.Missing) != 2 {
		t.Fatalf("missing count = %d, want 2", len(result.Missing))
	}
	if len(result.MissingByMonth["2024-01"]) != 1 || len(result.MissingByMonth["2024-02"]) != 1 {
		t.Fatalf("missing by month = %#v", result.MissingByMonth)
	}

	out := captureStdout(t, func() {
		printAccountSyncVerification(result)
	})
	assertContains(t, out, "⚠ mismatch")
	assertContains(t, out, "Onchain data: 3 txs between 2024-01-01 and 2024-02-01, balance: 115.00 EURe")
	assertContains(t, out, "Local data:   1 tx between 2024-01-01 and 2024-01-01, balance: 100.00 EURe")
	assertContains(t, out, "2024-01: 1 missing")
	assertContains(t, out, "0xmissing-jan")
	assertContains(t, out, "2024-02: 1 missing")
	assertContains(t, out, "0xmissing-feb")
	assertContains(t, out, "chb accounts savings sync --history")
	assertContains(t, out, "chb generate transactions --since 2024-01")
}

func TestGeneratedAccountTotalsShowInternalTransfersAsBalanceContribution(t *testing.T) {
	acc := testSavingsAccountConfig()
	chain := acc.Chain
	totals := accountTotalsFromGeneratedTransactions(&acc, []TransactionEntry{
		{Provider: "etherscan", Chain: &chain, Account: acc.Address, AccountSlug: acc.Slug, Currency: "EURe", Amount: 100, NormalizedAmount: 100, Type: "CREDIT", Timestamp: 1704067200},
		{Provider: "etherscan", Chain: &chain, Account: acc.Address, AccountSlug: acc.Slug, Currency: "EURe", Amount: 20, NormalizedAmount: 20, Type: "DEBIT", Timestamp: 1704153600},
		{Provider: "etherscan", Chain: &chain, Account: acc.Address, AccountSlug: acc.Slug, Currency: "EURe", Amount: 500, NormalizedAmount: 500, Type: "INTERNAL", Timestamp: 1704240000, Metadata: map[string]interface{}{"direction": "CREDIT"}},
		{Provider: "etherscan", Chain: &chain, Account: acc.Address, AccountSlug: acc.Slug, Currency: "EURe", Amount: 50, NormalizedAmount: 50, Type: "INTERNAL", Timestamp: 1704326400, Metadata: map[string]interface{}{"direction": "DEBIT"}},
	})
	if totals == nil {
		t.Fatal("accountTotalsFromGeneratedTransactions returned nil")
	}
	if totals.InternalTransfers != 450 {
		t.Fatalf("InternalTransfers = %.2f, want 450.00", totals.InternalTransfers)
	}
	if totals.CurrentBalance != 530 {
		t.Fatalf("CurrentBalance = %.2f, want 530.00", totals.CurrentBalance)
	}
}

func TestFilterTransactionsAfterOdooCursorKeepsOnlyNewerLocalRows(t *testing.T) {
	acc := testSavingsAccountConfig()
	txs := []TransactionEntry{
		{TxHash: "0x1", AccountSlug: acc.Slug, Type: "CREDIT", Timestamp: 100},
		{TxHash: "0x2", AccountSlug: acc.Slug, Type: "CREDIT", Timestamp: 200},
		{TxHash: "0x3", AccountSlug: acc.Slug, Type: "CREDIT", Timestamp: 300},
	}
	cursor := odooImportCursor{Found: true, UniqueImportID: buildUniqueImportID(&acc, txs[1])}

	filtered, matched := filterTransactionsAfterOdooCursor(&acc, txs, cursor)
	if !matched {
		t.Fatal("expected cursor to match local transaction")
	}
	if len(filtered) != 1 || filtered[0].TxHash != "0x3" {
		t.Fatalf("filtered = %#v, want only 0x3", filtered)
	}
}

func TestFilterTransactionsAfterOdooCursorFallsBackToAllWhenNotMatched(t *testing.T) {
	acc := testSavingsAccountConfig()
	txs := []TransactionEntry{
		{TxHash: "0x1", AccountSlug: acc.Slug, Type: "CREDIT", Timestamp: 100},
		{TxHash: "0x2", AccountSlug: acc.Slug, Type: "CREDIT", Timestamp: 200},
	}
	cursor := odooImportCursor{Found: true, UniqueImportID: "gnosis:missing:0xmissing:0"}

	filtered, matched := filterTransactionsAfterOdooCursor(&acc, txs, cursor)
	if matched {
		t.Fatal("expected cursor not to match")
	}
	if len(filtered) != len(txs) {
		t.Fatalf("filtered len = %d, want %d", len(filtered), len(txs))
	}
}

func TestAccountLocalOdooSnapshotSummarizesTransactions(t *testing.T) {
	acc := testSavingsAccountConfig()
	chain := acc.Chain
	txs := []TransactionEntry{
		{TxHash: "0x1", Provider: "etherscan", Chain: &chain, Account: acc.Address, AccountSlug: acc.Slug, Currency: "EURe", Amount: 100, NormalizedAmount: 100, Type: "CREDIT", Timestamp: 100},
		{TxHash: "0x2", Provider: "etherscan", Chain: &chain, Account: acc.Address, AccountSlug: acc.Slug, Currency: "EURe", Amount: 25, NormalizedAmount: 25, Type: "DEBIT", Timestamp: 300},
	}
	snap := accountLocalOdooSnapshot(&acc, txs)
	if snap.TxCount != 2 {
		t.Fatalf("TxCount = %d, want 2", snap.TxCount)
	}
	if snap.Balance != 75 {
		t.Fatalf("Balance = %.2f, want 75", snap.Balance)
	}
	if snap.FirstTxAt.Unix() != 100 || snap.LastTxAt.Unix() != 300 {
		t.Fatalf("range = %v -> %v, want unix 100 -> 300", snap.FirstTxAt, snap.LastTxAt)
	}
}

func TestAccountSourceChangedMonthsUsesTransactionContentOnly(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("APP_DATA_DIR", filepath.Join(tmp, "app"))
	t.Setenv("DATA_DIR", filepath.Join(tmp, "data"))

	acc := testSavingsAccountConfig()
	before := map[string]string{}
	raw := etherscansource.CacheFile{
		Account: acc.Address,
		Chain:   acc.Chain,
		Token:   acc.Token.Symbol,
		Transactions: []etherscansource.TokenTransfer{
			{Hash: "0x1", TimeStamp: "1704067200", From: "0xaaa", To: acc.Address, Value: "100000000000000000000", TokenSymbol: "EURe", TokenDecimal: "18"},
		},
	}
	writeJSONFileForTest(t, etherscansource.Path(DataDir(), "2024", "01", acc.Chain, etherscansource.FileName(acc.Slug, acc.Address, acc.Token.Symbol)), raw)
	before = accountSourceMonthFingerprints(&acc)

	raw.CachedAt = "changed"
	writeJSONFileForTest(t, etherscansource.Path(DataDir(), "2024", "01", acc.Chain, etherscansource.FileName(acc.Slug, acc.Address, acc.Token.Symbol)), raw)
	if changed := accountChangedSourceMonths(&acc, before); len(changed) != 0 {
		t.Fatalf("changed months after cachedAt-only change = %#v, want none", changed)
	}

	raw.Transactions = append(raw.Transactions, etherscansource.TokenTransfer{
		Hash: "0x2", TimeStamp: "1704153600", From: acc.Address, To: "0xbbb", Value: "25000000000000000000", TokenSymbol: "EURe", TokenDecimal: "18",
	})
	writeJSONFileForTest(t, etherscansource.Path(DataDir(), "2024", "01", acc.Chain, etherscansource.FileName(acc.Slug, acc.Address, acc.Token.Symbol)), raw)
	changed := accountChangedSourceMonths(&acc, before)
	if len(changed) != 1 || changed[0] != "2024-01" {
		t.Fatalf("changed months = %#v, want [2024-01]", changed)
	}
}

func TestAccountSyncVerificationRowsAlignColumns(t *testing.T) {
	now := time.Date(2026, 5, 13, 12, 0, 0, 0, BrusselsTZ())
	v := &accountSyncVerification{
		AccountSlug:      "savings",
		Currency:         "EURe",
		OnchainBalance:   108454.41,
		OnchainBalanceOK: true,
		OnchainTxCount:   697,
		OnchainFirstTxAt: now.AddDate(-1, 0, 0),
		OnchainLastTxAt:  now,
		LocalBalance:     108454.41,
		LocalTxCount:     697,
		LocalFirstTxAt:   now.AddDate(-1, 0, 0),
		LocalLastTxAt:    now,
		MissingByMonth:   map[string][]missingOnchainTransfer{},
	}

	rows := accountSyncVerificationSummaryRows(v)
	if len(rows) != 2 {
		t.Fatalf("rows = %#v, want 2 rows", rows)
	}
	if rows[0][0] != "Onchain data" || rows[1][0] != "Local data" {
		t.Fatalf("unexpected labels: %#v", rows)
	}
	for _, row := range rows {
		if len(row) != 2 {
			t.Fatalf("row = %#v, want 2 columns", row)
		}
		if !strings.Contains(row[1], "697 txs between 2025-05-13 and 2026-05-13, balance: 108,454.41 EURe") {
			t.Fatalf("summary = %q, want tx range and balance", row[1])
		}
	}
}

func testSavingsAccountConfig() AccountConfig {
	token := &struct {
		Address  string `json:"address"`
		Name     string `json:"name"`
		Symbol   string `json:"symbol"`
		Decimals int    `json:"decimals"`
	}{
		Address:  "0xtoken",
		Name:     "EURe",
		Symbol:   "EURe",
		Decimals: 18,
	}
	return AccountConfig{
		Name:       "Savings",
		Slug:       "savings",
		Provider:   "etherscan",
		Chain:      "gnosis",
		ChainID:    100,
		Address:    "0x6fDF0AaE33E313d9C98D2Aa19Bcd8EF777912CBf",
		WalletType: "safe",
		Token:      token,
	}
}

func writeJSONFileForTest(t *testing.T, path string, v interface{}) {
	t.Helper()
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		t.Fatalf("json marshal: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func assertContains(t *testing.T, haystack, needle string) {
	t.Helper()
	if !strings.Contains(haystack, needle) {
		t.Fatalf("output missing %q:\n%s", needle, haystack)
	}
}

func assertNotContains(t *testing.T, haystack, needle string) {
	t.Helper()
	if strings.Contains(haystack, needle) {
		t.Fatalf("output unexpectedly contains %q:\n%s", needle, haystack)
	}
}
