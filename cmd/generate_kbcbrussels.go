package cmd

import (
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	kbcbrusselssource "github.com/CommonsHub/chb/providers/kbcbrussels"
)

// syncKBCAccount re-runs generate for every (year, month) that appears in
// the local CSV for this account. There is no upstream API to fetch — the
// CSV in latest/providers/kbcbrussels/ is the source of truth — so "sync"
// here means "(re)materialize the monthly transactions.json files so the
// account view and Odoo push see the full history".
//
// When there's nothing to read for this account it prints actionable
// guidance (and returns no error). That guidance is the only feedback the
// operator gets in this case, so it is always printed regardless of
// verbosity — only the per-month generate chatter is quietened when not
// verbose.
func syncKBCAccount(acc *AccountConfig, verbose bool) error {
	rows, err := kbcbrusselssource.LoadTransactionsForIBAN(DataDir(), acc.IBAN)
	if err != nil {
		return fmt.Errorf("load CSV: %v", err)
	}
	if len(rows) == 0 {
		printKBCNoDataGuidance(acc)
		return nil
	}
	months := kbcMonthsFromRows(rows)
	if verbose {
		fmt.Printf("  %sSource:%s   manual CSV (%s in latest/providers/%s/)\n",
			Fmt.Dim, Fmt.Reset, Pluralize(len(rows), "row", ""), kbcbrusselssource.Source)
		fmt.Printf("  %sRange:%s    %s → %s (%s)\n",
			Fmt.Dim, Fmt.Reset, rows[0].Date, rows[len(rows)-1].Date, Pluralize(len(months), "month", ""))
		fmt.Println()
		return GenerateTransactionsForMonths(months)
	}
	restore := silenceStdout()
	genErr := GenerateTransactionsForMonths(months)
	restore()
	return genErr
}

// printKBCNoDataGuidance explains why a KBC pull found nothing and tells the
// operator exactly what to do. It distinguishes the two real causes — no CSV
// export dropped at all, versus exports present but none matching this
// account's IBAN — because the fix differs.
func printKBCNoDataGuidance(acc *AccountConfig) {
	dir := kbcbrusselssource.LatestDir(DataDir())
	csvFiles := kbcCSVFileNames(dir)

	fmt.Println()
	if len(csvFiles) == 0 {
		// Make sure the drop folder exists so the path we point at is real.
		_ = os.MkdirAll(dir, 0o755)
		fmt.Printf("  %s⚠ No KBC Brussels CSV export found — nothing to import.%s\n", Fmt.Yellow, Fmt.Reset)
		fmt.Printf("  %sKBC Brussels has no API; a downloaded CSV export is the only data source.%s\n\n", Fmt.Dim, Fmt.Reset)
		fmt.Printf("  To import transactions for %s%s%s:\n", Fmt.Bold, acc.Slug, Fmt.Reset)
		fmt.Printf("    1. In KBC Brussels online banking, export the account's transactions as CSV.\n")
		fmt.Printf("    2. Drop the file into:\n       %s%s%s\n", Fmt.Cyan, dir, Fmt.Reset)
		fmt.Printf("       %s(any *.csv works; KBC's default name is export_<IBAN>_<YYYYMMDD>_<HHMM>.csv)%s\n", Fmt.Dim, Fmt.Reset)
		fmt.Printf("    3. Re-run %schb accounts %s pull --history%s\n\n", Fmt.Cyan, acc.Slug, Fmt.Reset)
		return
	}

	// CSV files are present but none carried rows for this account's IBAN.
	all, _ := kbcbrusselssource.LoadAllTransactions(DataDir())
	ibans := kbcDistinctIBANs(all)
	fmt.Printf("  %s⚠ Found %s in the drop folder, but none contain transactions for this account.%s\n",
		Fmt.Yellow, Pluralize(len(csvFiles), "CSV file", ""), Fmt.Reset)
	fmt.Printf("  %sFolder:%s %s%s%s\n", Fmt.Dim, Fmt.Reset, Fmt.Cyan, dir, Fmt.Reset)
	if acc.IBAN != "" {
		fmt.Printf("  %sConfigured IBAN for '%s':%s %s\n", Fmt.Dim, acc.Slug, Fmt.Reset, acc.IBAN)
	} else {
		fmt.Printf("  %sAccount '%s' has no IBAN configured.%s\n", Fmt.Yellow, acc.Slug, Fmt.Reset)
	}
	if len(ibans) > 0 {
		fmt.Printf("  %sIBANs present in the CSVs:%s %s\n", Fmt.Dim, Fmt.Reset, strings.Join(ibans, ", "))
	}
	fmt.Printf("\n  %sFix the IBAN configured for '%s' so it matches the export, or drop the right CSV into the folder above.%s\n\n",
		Fmt.Dim, acc.Slug, Fmt.Reset)
}

// kbcCSVFileNames lists the *.csv files currently in the drop folder.
// A missing folder yields an empty slice (no error) — that's the "no
// export dropped yet" case.
func kbcCSVFileNames(dir string) []string {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	var names []string
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(strings.ToLower(e.Name()), ".csv") {
			continue
		}
		names = append(names, e.Name())
	}
	return names
}

// kbcDistinctIBANs returns the unique account IBANs seen across the loaded
// CSV rows, sorted, so the guidance can show what the export actually
// contained when it doesn't match the configured account.
func kbcDistinctIBANs(rows []kbcbrusselssource.Transaction) []string {
	seen := map[string]bool{}
	var out []string
	for _, r := range rows {
		if r.AccountIBAN == "" || seen[r.AccountIBAN] {
			continue
		}
		seen[r.AccountIBAN] = true
		out = append(out, r.AccountIBAN)
	}
	sort.Strings(out)
	return out
}

// kbcMonthsFromRows returns the set of "YYYY-MM" labels covered by the
// supplied rows, deduplicated and sorted oldest-first — the shape that
// GenerateTransactionsForMonths expects.
func kbcMonthsFromRows(rows []kbcbrusselssource.Transaction) []string {
	seen := map[string]bool{}
	var months []string
	for _, row := range rows {
		if len(row.Date) < 7 {
			continue
		}
		ym := row.Date[:7]
		if seen[ym] {
			continue
		}
		seen[ym] = true
		months = append(months, ym)
	}
	sort.Strings(months)
	return months
}

// loadKBCAccountTransactions reads the CSV for this account directly and
// returns one TransactionEntry per row. This is the source-of-truth path
// used by every consumer that wants the *full* tx history (account
// detail, Odoo push, balance verification) without depending on which
// months have been generated.
func loadKBCAccountTransactions(acc *AccountConfig) []TransactionEntry {
	if acc == nil {
		return nil
	}
	// Odoo source of truth: the bootstrap CSV is ignored entirely — new entries
	// arrive in the Odoo journal via a separate plugin — so derive every
	// transaction from the pulled journal-lines cache.
	if acc.IsOdooSourceOfTruth() {
		return kbcTransactionsFromOdoo(acc)
	}
	rows, err := kbcbrusselssource.LoadTransactionsForIBAN(DataDir(), acc.IBAN)
	if err != nil || len(rows) == 0 {
		return nil
	}
	out := make([]TransactionEntry, 0, len(rows))
	for _, row := range rows {
		out = append(out, kbcRowToTransactionEntry(*acc, row))
	}
	return out
}

// kbcTransactionsFromOdoo derives KBC transactions from the pulled Odoo
// journal-lines cache. For an odooSourceOfTruth account the Odoo journal — not
// the bootstrap CSV — is authoritative, so this is the single source every
// consumer (balance, account view, generate's aggregate) reads.
func kbcTransactionsFromOdoo(acc *AccountConfig) []TransactionEntry {
	if acc == nil || acc.OdooJournalID == 0 {
		return nil
	}
	lines, ok := loadLatestOdooJournalLinesCache(acc.OdooJournalID)
	if !ok {
		return nil
	}
	out := make([]TransactionEntry, 0, len(lines))
	for _, ln := range lines {
		if isOdooSyntheticLine(ln) || ln.Amount == 0 {
			continue
		}
		out = append(out, odooLineToKBCTransaction(*acc, ln))
	}
	return out
}

// kbcMonthsFromOdoo lists the distinct YYYY-MM months present in the pulled
// journal-lines cache, so a pull can regenerate exactly those months.
func kbcMonthsFromOdoo(acc *AccountConfig) []string {
	if acc == nil || acc.OdooJournalID == 0 {
		return nil
	}
	lines, ok := loadLatestOdooJournalLinesCache(acc.OdooJournalID)
	if !ok {
		return nil
	}
	seen := map[string]bool{}
	var months []string
	for _, ln := range lines {
		if len(ln.Date) < 7 || seen[ln.Date[:7]] {
			continue
		}
		seen[ln.Date[:7]] = true
		months = append(months, ln.Date[:7])
	}
	sort.Strings(months)
	return months
}

// kbcEntryInYearMonth reports whether a derived KBC tx falls in the given year
// (and month, when set) — used to slice the full journal-cache history into the
// per-month generate buckets.
func kbcEntryInYearMonth(tx TransactionEntry, year, month string) bool {
	if tx.Timestamp == 0 {
		return false
	}
	t := time.Unix(tx.Timestamp, 0).In(BrusselsTZ())
	if t.Format("2006") != year {
		return false
	}
	return month == "" || t.Format("01") == month
}

// odooLineToKBCTransaction maps one Odoo bank-statement line to the canonical
// KBC TransactionEntry shape (mirrors kbcRowToTransactionEntry). The line's
// payment_ref becomes the description so the rules categorise it (e.g.
// cp-order-… → drinks). Identity is the stable Odoo statement-line id.
func odooLineToKBCTransaction(acc AccountConfig, ln OdooCacheLine) TransactionEntry {
	currency := strings.ToUpper(acc.Currency)
	if currency == "" {
		currency = "EUR"
	}
	txType := "CREDIT"
	if ln.Amount < 0 {
		txType = "DEBIT"
	}
	narration := strings.TrimSpace(firstNonEmptyStr(ln.PaymentRef, ln.Narration))
	counterparty := kbcbrusselssource.MerchantFromDescription(narration)
	if counterparty == "" {
		counterparty = kbcbrusselssource.CleanDescription(narration)
	}
	hash := strconv.Itoa(ln.ID)
	var ts int64
	if t, err := time.ParseInLocation("2006-01-02", ln.Date, BrusselsTZ()); err == nil {
		ts = t.Unix()
	}
	signed := roundCents(ln.Amount)
	metadata := map[string]interface{}{}
	if narration != "" {
		metadata["description"] = narration
		metadata["fullDescription"] = narration
	}
	if ln.ID > 0 {
		metadata["odooLineId"] = ln.ID
	}
	if ln.UniqueImportID != "" {
		metadata["odooImportId"] = ln.UniqueImportID
	}
	return TransactionEntry{
		ID:               BuildIBANTxURI(acc.IBAN, hash),
		ProviderID:       hash,
		AccountID:        BuildIBANAccountURI(acc.IBAN),
		TxHash:           hash,
		Provider:         kbcbrusselssource.Source,
		Account:          acc.IBAN,
		AccountSlug:      acc.Slug,
		AccountName:      acc.Name,
		Currency:         currency,
		Value:            fmt.Sprintf("%.2f", signed),
		Amount:           signed,
		NetAmount:        signed,
		GrossAmount:      roundCents(math.Abs(ln.Amount)),
		NormalizedAmount: signed,
		Type:             txType,
		Counterparty:     counterparty,
		Timestamp:        ts,
		Metadata:         metadata,
	}
}

// kbcAccountsByIBAN returns every configured kbcbrussels account, keyed
// by its normalized IBAN. Used by the generator to decide whether to run
// the CSV loader at all and to attach the correct slug/name to each row.
func kbcAccountsByIBAN() map[string]AccountConfig {
	out := map[string]AccountConfig{}
	for _, acc := range LoadAccountConfigs() {
		if !strings.EqualFold(acc.Provider, kbcbrusselssource.Source) {
			continue
		}
		iban := kbcbrusselssource.NormalizeIBAN(acc.IBAN)
		if iban == "" {
			continue
		}
		out[iban] = acc
	}
	return out
}

// kbcRowToTransactionEntry maps one CSV row to the canonical
// TransactionEntry shape. Amount is kept signed (positive = credit,
// negative = debit) and Type is derived from its sign — the same
// convention the Stripe / blockchain branches use.
func kbcRowToTransactionEntry(acc AccountConfig, row kbcbrusselssource.Transaction) TransactionEntry {
	currency := strings.ToUpper(row.Currency)
	if currency == "" {
		currency = strings.ToUpper(acc.Currency)
	}
	if currency == "" {
		currency = "EUR"
	}
	txType := "CREDIT"
	if row.Amount < 0 {
		txType = "DEBIT"
	}
	counterparty := row.CounterpartyName
	if counterparty == "" {
		// Card payments don't fill CounterpartyName — try to recover the
		// merchant from the description before falling back to IBAN /
		// raw text. Without this, the IBAN ends up as the partner name
		// in Odoo (or worse, the raw "PAYMENT VIA …" prefix).
		counterparty = kbcbrusselssource.MerchantFromDescription(row.Description)
	}
	if counterparty == "" {
		counterparty = row.CounterpartyIBAN
	}
	if counterparty == "" {
		counterparty = kbcbrusselssource.CleanDescription(row.Description)
	}

	desc := kbcbrusselssource.PreferredDescription(row)
	if desc == "" {
		desc = strings.TrimSpace(row.Description)
	}
	metadata := map[string]interface{}{
		"description":     desc,
		"fullDescription": strings.TrimSpace(row.Description),
		"statementNumber": row.StatementNumber,
		"balance":         roundCents(row.Balance),
	}
	if row.CounterpartyIBAN != "" {
		metadata["iban"] = row.CounterpartyIBAN
	}
	if row.CounterpartyBIC != "" {
		metadata["bic"] = row.CounterpartyBIC
	}
	if row.StandardReference != "" {
		metadata["reference"] = row.StandardReference
	}
	if row.FreeReference != "" {
		metadata["freeReference"] = row.FreeReference
	}
	if row.ValueDate != "" {
		metadata["valueDate"] = row.ValueDate
	}

	signed := roundCents(row.Amount)
	gross := roundCents(math.Abs(row.Amount))

	counterpartyURI := ""
	if row.CounterpartyIBAN != "" {
		counterpartyURI = BuildIBANAccountURI(row.CounterpartyIBAN)
	}

	return TransactionEntry{
		ID:               BuildIBANTxURI(row.AccountIBAN, row.Hash),
		ProviderID:       row.Hash,
		AccountID:        BuildIBANAccountURI(row.AccountIBAN),
		CounterpartyID:   counterpartyURI,
		TxHash:           row.Hash,
		Provider:         kbcbrusselssource.Source,
		Account:          row.AccountIBAN,
		AccountSlug:      acc.Slug,
		AccountName:      acc.Name,
		Currency:         currency,
		Value:            fmt.Sprintf("%.2f", signed),
		Amount:           signed,
		NetAmount:        signed,
		GrossAmount:      gross,
		NormalizedAmount: signed,
		Fee:              0,
		Type:             txType,
		Counterparty:     counterparty,
		Timestamp:        row.Timestamp,
		Metadata:         metadata,
	}
}
