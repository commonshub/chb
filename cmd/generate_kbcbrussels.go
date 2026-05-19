package cmd

import (
	"fmt"
	"math"
	"sort"
	"strings"

	kbcbrusselssource "github.com/CommonsHub/chb/providers/kbcbrussels"
)

// syncKBCAccount re-runs generate for every (year, month) that appears in
// the local CSV for this account. There is no upstream API to fetch — the
// CSV in latest/providers/kbcbrussels/ is the source of truth — so "sync"
// here means "(re)materialize the monthly transactions.json files so the
// account view and Odoo push see the full history".
func syncKBCAccount(acc *AccountConfig) error {
	rows, err := kbcbrusselssource.LoadTransactionsForIBAN(DataDir(), acc.IBAN)
	if err != nil {
		return fmt.Errorf("load CSV: %v", err)
	}
	if len(rows) == 0 {
		fmt.Printf("  %sNo CSV rows found for %s under %s%s\n",
			Fmt.Yellow, acc.IBAN, kbcbrusselssource.LatestDir(DataDir()), Fmt.Reset)
		fmt.Printf("  %sDownload an export from KBC Brussels and drop it there, then re-run sync.%s\n",
			Fmt.Dim, Fmt.Reset)
		return nil
	}
	months := kbcMonthsFromRows(rows)
	fmt.Printf("  %sSource:%s   manual CSV (%d %s in latest/providers/%s/)\n",
		Fmt.Dim, Fmt.Reset, len(rows), Pluralize(len(rows), "row", ""), kbcbrusselssource.Source)
	fmt.Printf("  %sRange:%s    %s → %s (%d %s)\n",
		Fmt.Dim, Fmt.Reset, rows[0].Date, rows[len(rows)-1].Date, len(months), Pluralize(len(months), "month", ""))
	fmt.Println()
	return GenerateTransactionsForMonths(months)
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
		counterparty = row.CounterpartyIBAN
	}
	if counterparty == "" {
		counterparty = row.Description
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
