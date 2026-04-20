package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// OdooCredentials holds resolved Odoo connection info.
type OdooCredentials struct {
	URL      string
	DB       string
	Login    string
	Password string
}

// ResolveOdooCredentials returns Odoo credentials from env vars (set in ~/.chb/config.env).
// Database is derived from the ODOO_URL hostname unless ODOO_DATABASE is set.
func ResolveOdooCredentials() (*OdooCredentials, error) {
	creds := &OdooCredentials{
		URL:      os.Getenv("ODOO_URL"),
		DB:       os.Getenv("ODOO_DATABASE"),
		Login:    os.Getenv("ODOO_LOGIN"),
		Password: os.Getenv("ODOO_PASSWORD"),
	}

	if creds.URL == "" || creds.Login == "" || creds.Password == "" {
		return nil, fmt.Errorf("ODOO_URL/ODOO_LOGIN/ODOO_PASSWORD not set (check ~/.chb/config.env)")
	}

	if creds.DB == "" {
		creds.DB = odooDBFromURL(creds.URL)
	}

	return creds, nil
}

// OdooAnalyticEnrichment is saved per month.
type OdooAnalyticEnrichment struct {
	FetchedAt string                `json:"fetchedAt"`
	Mappings  []OdooAnalyticMapping `json:"mappings"`
}

// OdooAnalyticMapping links a transaction reference to a category.
type OdooAnalyticMapping struct {
	StripeReference string  `json:"stripeReference,omitempty"` // ch_ or pi_ from Odoo
	BankRef         string  `json:"bankRef,omitempty"`         // bank statement ref
	Category        string  `json:"category"`
	OdooAccountName string  `json:"odooAccountName"`
	OdooAccountID   int     `json:"odooAccountId"`
	Amount          float64 `json:"amount,omitempty"`
	Date            string  `json:"date,omitempty"`
}

// OdooAnalyticSync fetches analytic data from Odoo and builds category enrichment.
func OdooAnalyticSync(args []string) (int, error) {
	if HasFlag(args, "--help", "-h", "help") {
		printOdooSyncHelp()
		return 0, nil
	}

	odooURL := os.Getenv("ODOO_URL")
	odooLogin := os.Getenv("ODOO_LOGIN")
	odooPassword := os.Getenv("ODOO_PASSWORD")

	if odooURL == "" || odooLogin == "" || odooPassword == "" {
		fmt.Printf("%s⚠ ODOO_URL/ODOO_LOGIN/ODOO_PASSWORD not set, skipping Odoo sync%s\n", Fmt.Yellow, Fmt.Reset)
		return 0, nil
	}

	db := odooDBFromURL(odooURL)

	fmt.Printf("\n%s🏢 Syncing Odoo analytics%s\n", Fmt.Bold, Fmt.Reset)
	fmt.Printf("%sURL: %s  DB: %s%s\n", Fmt.Dim, odooURL, db, Fmt.Reset)

	uid, err := odooAuth(odooURL, db, odooLogin, odooPassword)
	if err != nil || uid == 0 {
		return 0, fmt.Errorf("Odoo authentication failed: %v", err)
	}

	// Load category mapping from settings
	// TODO: read from settings.accounting.odoo.categoryMapping when persisted
	categoryMapping := map[int]string{} // Odoo analytic account ID -> our category slug

	// Fetch analytic accounts for name→ID lookup
	accountsResult, err := odooExec(odooURL, db, uid, odooPassword,
		"account.analytic.account", "search_read",
		[]interface{}{[]interface{}{
			[]interface{}{"active", "=", true},
		}},
		map[string]interface{}{"fields": []string{"id", "name", "code", "plan_id"}})
	if err != nil {
		return 0, fmt.Errorf("failed to fetch analytic accounts: %v", err)
	}

	type odooAcct struct {
		ID     int           `json:"id"`
		Name   string        `json:"name"`
		Code   string        `json:"code"`
		PlanID []interface{} `json:"plan_id"`
	}
	var analyticAccounts []odooAcct
	json.Unmarshal(accountsResult, &analyticAccounts)

	nameByID := map[int]string{}
	for _, a := range analyticAccounts {
		nameByID[a.ID] = a.Name
		// Auto-map by slugified code if present
		if a.Code != "" {
			categoryMapping[a.ID] = strings.ToLower(strings.ReplaceAll(a.Code, " ", "-"))
		}
	}
	fmt.Printf("  %sFetched %d analytic accounts%s\n", Fmt.Dim, len(analyticAccounts), Fmt.Reset)

	// Fetch analytic lines — these are the actual categorized postings
	// Each line links an amount to an analytic account, with a reference
	// back to the journal entry line (move_line_id).
	linesResult, err := odooExec(odooURL, db, uid, odooPassword,
		"account.analytic.line", "search_read",
		[]interface{}{[]interface{}{}},
		map[string]interface{}{
			"fields": []string{
				"id", "name", "date", "amount", "account_id",
				"general_account_id", "move_line_id", "product_id",
				"partner_id", "ref",
			},
			"limit":  10000,
			"order":  "date desc",
		})
	if err != nil {
		fmt.Printf("  %s⚠ Could not fetch analytic lines: %v%s\n", Fmt.Yellow, err, Fmt.Reset)
	}

	type analyticLine struct {
		ID               int         `json:"id"`
		Name             string      `json:"name"`
		Date             string      `json:"date"`
		Amount           float64     `json:"amount"`
		AccountID        interface{} `json:"account_id"`         // [id, name] or false
		GeneralAccountID interface{} `json:"general_account_id"` // [id, name] or false
		MoveLineID       interface{} `json:"move_line_id"`       // [id, name] or false
		ProductID        interface{} `json:"product_id"`         // [id, name] or false
		PartnerID        interface{} `json:"partner_id"`         // [id, name] or false
		Ref              string      `json:"ref"`
	}
	var lines []analyticLine
	if linesResult != nil {
		json.Unmarshal(linesResult, &lines)
	}
	fmt.Printf("  %sFetched %d analytic lines%s\n", Fmt.Dim, len(lines), Fmt.Reset)

	// Fetch payment transactions to match Stripe references
	paymentResult, err := odooExec(odooURL, db, uid, odooPassword,
		"payment.transaction", "search_read",
		[]interface{}{[]interface{}{
			[]interface{}{"provider_reference", "!=", false},
		}},
		map[string]interface{}{
			"fields": []string{"id", "provider_reference", "amount", "state"},
			"limit":  5000,
		})

	type paymentTx struct {
		ID                int     `json:"id"`
		ProviderReference string  `json:"provider_reference"`
		Amount            float64 `json:"amount"`
		State             string  `json:"state"`
	}
	var paymentTxs []paymentTx
	if paymentResult != nil {
		json.Unmarshal(paymentResult, &paymentTxs)
	}
	fmt.Printf("  %sFetched %d payment transactions with Stripe references%s\n", Fmt.Dim, len(paymentTxs), Fmt.Reset)

	// Fetch bank statement lines (for blockchain tx matching via ref)
	bslResult, err := odooExec(odooURL, db, uid, odooPassword,
		"account.bank.statement.line", "search_read",
		[]interface{}{[]interface{}{}},
		map[string]interface{}{
			"fields": []string{"id", "name", "ref", "amount", "date", "move_id"},
			"limit":  5000,
		})

	type bankStmtLine struct {
		ID     int         `json:"id"`
		Name   string      `json:"name"`
		Ref    string      `json:"ref"`
		Amount float64     `json:"amount"`
		Date   string      `json:"date"`
		MoveID interface{} `json:"move_id"` // [id, name] or false
	}
	var bslLines []bankStmtLine
	if bslResult != nil {
		json.Unmarshal(bslResult, &bslLines)
	}
	fmt.Printf("  %sFetched %d bank statement lines%s\n", Fmt.Dim, len(bslLines), Fmt.Reset)

	// Build move_line_id → analytic category mapping from analytic lines
	moveLineCategoryMap := map[int]OdooAnalyticMapping{} // move_line_id -> mapping
	for _, line := range lines {
		accountID := odooFieldID(line.AccountID)
		if accountID == 0 {
			continue
		}

		category := categoryMapping[accountID]
		accountName := nameByID[accountID]
		if category == "" {
			continue
		}

		moveLineID := odooFieldID(line.MoveLineID)
		if moveLineID == 0 {
			continue
		}

		moveLineCategoryMap[moveLineID] = OdooAnalyticMapping{
			Category:        category,
			OdooAccountName: accountName,
			OdooAccountID:   accountID,
			Amount:          line.Amount,
			Date:            line.Date,
		}
	}

	// Build final mappings
	var mappings []OdooAnalyticMapping
	totalMapped := 0

	// 1. Map Stripe payment references
	for _, ptx := range paymentTxs {
		if ptx.ProviderReference == "" {
			continue
		}
		// TODO: trace payment.transaction → account.move → account.move.line → moveLineCategoryMap
		// For now, store the Stripe reference for direct matching
		mappings = append(mappings, OdooAnalyticMapping{
			StripeReference: ptx.ProviderReference,
			Amount:          ptx.Amount,
		})
	}

	// 2. Map bank statement refs (blockchain tx matching)
	for _, bsl := range bslLines {
		if bsl.Ref == "" {
			continue
		}
		// Check if this ref matches the odoo-web3 format: {chain}:{account}:{txHash}:{logIndex}
		parts := strings.Split(bsl.Ref, ":")
		if len(parts) >= 3 {
			moveID := odooFieldID(bsl.MoveID)
			if moveID > 0 {
				if m, ok := moveLineCategoryMap[moveID]; ok {
					m.BankRef = bsl.Ref
					mappings = append(mappings, m)
					totalMapped++
				}
			}
		}
	}

	// Save enrichment grouped by month
	byMonth := map[string][]OdooAnalyticMapping{}
	for _, m := range mappings {
		ym := ""
		if m.Date != "" && len(m.Date) >= 7 {
			ym = m.Date[:4] + "-" + m.Date[5:7]
		}
		if ym == "" {
			now := time.Now()
			ym = fmt.Sprintf("%d-%02d", now.Year(), now.Month())
		}
		byMonth[ym] = append(byMonth[ym], m)
	}

	dataDir := DataDir()
	saved := 0
	for ym, monthMappings := range byMonth {
		parts := strings.Split(ym, "-")
		if len(parts) != 2 {
			continue
		}
		enrichment := OdooAnalyticEnrichment{
			FetchedAt: time.Now().UTC().Format(time.RFC3339),
			Mappings:  monthMappings,
		}
		data, _ := json.MarshalIndent(enrichment, "", "  ")
		relPath := filepath.Join("finance", "odoo", "analytic-enrichment.json")
		writeMonthFile(dataDir, parts[0], parts[1], relPath, data)
		saved++
	}

	fmt.Printf("\n  %s✓ %d mappings, saved %d months%s\n", Fmt.Green, totalMapped, saved, Fmt.Reset)
	fmt.Printf("  %s%d analytic accounts, %d analytic lines, %d payment refs, %d bank refs%s\n\n",
		Fmt.Dim, len(analyticAccounts), len(lines), len(paymentTxs), len(bslLines), Fmt.Reset)

	return totalMapped, nil
}

// odooFieldID extracts the integer ID from an Odoo many2one field (which is [id, name] or false).
func odooFieldID(v interface{}) int {
	if arr, ok := v.([]interface{}); ok && len(arr) >= 1 {
		if id, ok := arr[0].(float64); ok {
			return int(id)
		}
	}
	return 0
}

// odooFieldName extracts the string name from an Odoo many2one field.
func odooFieldName(v interface{}) string {
	if arr, ok := v.([]interface{}); ok && len(arr) >= 2 {
		if name, ok := arr[1].(string); ok {
			return name
		}
	}
	return ""
}

// OdooJournals lists Odoo journals linked to accounts, or resets a specific journal.
func OdooJournals(args []string) error {
	creds, err := ResolveOdooCredentials()
	if err != nil {
		return err
	}

	uid, err := odooAuth(creds.URL, creds.DB, creds.Login, creds.Password)
	if err != nil || uid == 0 {
		return fmt.Errorf("Odoo authentication failed: %v", err)
	}

	// Check for `chb odoo journals <id> [sync|--reset]`
	if len(args) >= 1 && !strings.HasPrefix(args[0], "-") {
		var journalID int
		fmt.Sscanf(args[0], "%d", &journalID)
		if journalID == 0 {
			return fmt.Errorf("invalid journal ID: %s", args[0])
		}
		if len(args) >= 2 && args[1] == "sync" {
			return odooJournalSync(journalID, args[2:])
		}
		if len(args) >= 2 && args[1] == "check" {
			return odooJournalCheck(creds, uid, journalID)
		}
		if len(args) >= 2 && args[1] == "fix" {
			return odooJournalFix(creds, uid, journalID, HasFlag(args, "--yes", "-y"), HasFlag(args, "--dry-run"))
		}
		if HasFlag(args, "--reset") {
			return odooJournalReset(creds, uid, journalID)
		}
		return odooJournalDetail(creds, uid, journalID)
	}

	// List journals linked to accounts
	configs := LoadAccountConfigs()

	fmt.Printf("\n%s🏢 Odoo Journals%s\n", Fmt.Bold, Fmt.Reset)
	fmt.Printf("  %s%s (db: %s)%s\n\n", Fmt.Dim, creds.URL, creds.DB, Fmt.Reset)

	hasLinked := false
	for _, acc := range configs {
		if acc.OdooJournalID == 0 {
			continue
		}
		hasLinked = true

		// Get line count and last date from Odoo
		countResult, _ := odooExec(creds.URL, creds.DB, uid, creds.Password,
			"account.bank.statement.line", "search_count",
			[]interface{}{[]interface{}{
				[]interface{}{"journal_id", "=", acc.OdooJournalID},
			}}, nil)
		var lineCount int
		json.Unmarshal(countResult, &lineCount)

		stmtResult, _ := odooExec(creds.URL, creds.DB, uid, creds.Password,
			"account.bank.statement", "search_count",
			[]interface{}{[]interface{}{
				[]interface{}{"journal_id", "=", acc.OdooJournalID},
			}}, nil)
		var stmtCount int
		json.Unmarshal(stmtResult, &stmtCount)

		fmt.Printf("  %s%s%s  %s#%d%s\n", Fmt.Bold, acc.OdooJournalName, Fmt.Reset, Fmt.Dim, acc.OdooJournalID, Fmt.Reset)
		fmt.Printf("    %sAccount: %s (%s)%s\n", Fmt.Dim, acc.Name, acc.Slug, Fmt.Reset)
		fmt.Printf("    %s%d statement lines, %d statements%s\n", Fmt.Dim, lineCount, stmtCount, Fmt.Reset)
		fmt.Println()
	}

	if !hasLinked {
		fmt.Printf("  %sNo accounts linked to Odoo journals.%s\n", Fmt.Dim, Fmt.Reset)
		fmt.Printf("  %sRun 'chb accounts <slug> link' to link one.%s\n\n", Fmt.Dim, Fmt.Reset)
		return nil
	}

	fmt.Printf("%sCOMMANDS%s\n\n", Fmt.Bold, Fmt.Reset)
	fmt.Printf("  %s%schb odoo journals <id>%s\n", Fmt.Bold, Fmt.Cyan, Fmt.Reset)
	fmt.Printf("    %sShow details for a specific journal%s\n\n", Fmt.Dim, Fmt.Reset)
	fmt.Printf("  %s%schb odoo journals <id> sync%s\n", Fmt.Bold, Fmt.Cyan, Fmt.Reset)
	fmt.Printf("    %sSync the linked account's transactions into the journal%s\n\n", Fmt.Dim, Fmt.Reset)
	fmt.Printf("  %s%schb odoo journals <id> check%s\n", Fmt.Bold, Fmt.Cyan, Fmt.Reset)
	fmt.Printf("    %sReport statements whose running balance is invalid%s\n\n", Fmt.Dim, Fmt.Reset)
	fmt.Printf("  %s%schb odoo journals <id> fix%s\n", Fmt.Bold, Fmt.Cyan, Fmt.Reset)
	fmt.Printf("    %sSet balance_end_real = running balance on invalid statements%s\n\n", Fmt.Dim, Fmt.Reset)
	fmt.Printf("  %s%schb odoo journals <id> --reset%s\n", Fmt.Bold, Fmt.Cyan, Fmt.Reset)
	fmt.Printf("    %sEmpty a journal (delete all statements and lines)%s\n\n", Fmt.Dim, Fmt.Reset)

	return nil
}

func odooJournalDetail(creds *OdooCredentials, uid int, journalID int) error {
	// Fetch journal info
	result, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.journal", "search_read",
		[]interface{}{[]interface{}{
			[]interface{}{"id", "=", journalID},
		}},
		map[string]interface{}{"fields": []string{"id", "name", "type"}, "limit": 1})
	if err != nil {
		return fmt.Errorf("failed to fetch journal: %v", err)
	}
	var journals []struct {
		ID   int    `json:"id"`
		Name string `json:"name"`
		Type string `json:"type"`
	}
	json.Unmarshal(result, &journals)
	if len(journals) == 0 {
		return fmt.Errorf("journal #%d not found", journalID)
	}
	j := journals[0]

	// Count lines and statements
	lineResult, _ := odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.bank.statement.line", "search_count",
		[]interface{}{[]interface{}{
			[]interface{}{"journal_id", "=", journalID},
		}}, nil)
	var lineCount int
	json.Unmarshal(lineResult, &lineCount)

	stmtResult, _ := odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.bank.statement", "search_read",
		[]interface{}{[]interface{}{
			[]interface{}{"journal_id", "=", journalID},
		}},
		map[string]interface{}{
			"fields": []string{"name", "line_ids"},
			"order":  "name desc",
			"limit":  0,
		})
	var stmts []struct {
		Name    string `json:"name"`
		LineIDs []int  `json:"line_ids"`
	}
	json.Unmarshal(stmtResult, &stmts)

	fmt.Printf("\n  %s%s%s  %s#%d  type: %s%s\n", Fmt.Bold, j.Name, Fmt.Reset, Fmt.Dim, j.ID, j.Type, Fmt.Reset)
	fmt.Printf("  %s%s (db: %s)%s\n\n", Fmt.Dim, creds.URL, creds.DB, Fmt.Reset)
	fmt.Printf("  %sStatement lines: %d%s\n", Fmt.Dim, lineCount, Fmt.Reset)
	fmt.Printf("  %sStatements: %d%s\n\n", Fmt.Dim, len(stmts), Fmt.Reset)

	if len(stmts) > 0 {
		fmt.Printf("  %-32s  %s\n", "Statement", "Lines")
		fmt.Printf("  %s%s%s\n", Fmt.Dim, strings.Repeat("─", 45), Fmt.Reset)
		for _, s := range stmts {
			fmt.Printf("  %-32s  %d\n", s.Name, len(s.LineIDs))
		}
		fmt.Println()
	}

	fmt.Printf("  %sTo reset: chb odoo journals %d --reset%s\n\n", Fmt.Dim, journalID, Fmt.Reset)
	return nil
}

// odooJournalCheck reports statements in the journal that violate Odoo's
// running-balance / chain-continuity invariants.
func odooJournalCheck(creds *OdooCredentials, uid int, journalID int) error {
	fmt.Printf("\n  %sChecking journal #%d…%s\n", Fmt.Dim, journalID, Fmt.Reset)
	issues, err := CheckOdooJournalStatements(creds, uid, journalID)
	if err != nil {
		return err
	}
	if len(issues) == 0 {
		fmt.Printf("  %s✓ All statements are valid%s\n\n", Fmt.Green, Fmt.Reset)
		return nil
	}
	PrintStatementIssues(issues)
	fmt.Printf("  %sTo fix: chb odoo journals %d fix%s\n\n", Fmt.Dim, journalID, Fmt.Reset)
	return nil
}

// odooJournalFix sets balance_end_real to the running balance for each
// statement with a balance_mismatch issue. chain_gap issues are reported but
// not auto-fixed (they usually indicate a missing/extra line).
func odooJournalFix(creds *OdooCredentials, uid int, journalID int, assumeYes, dryRun bool) error {
	issues, err := CheckOdooJournalStatements(creds, uid, journalID)
	if err != nil {
		return err
	}
	if len(issues) == 0 {
		fmt.Printf("\n  %s✓ Nothing to fix%s\n\n", Fmt.Green, Fmt.Reset)
		return nil
	}
	PrintStatementIssues(issues)

	var fixable []StatementBalanceIssue
	var chainGaps []StatementBalanceIssue
	for _, i := range issues {
		if i.Kind == "balance_mismatch" {
			fixable = append(fixable, i)
		} else {
			chainGaps = append(chainGaps, i)
		}
	}

	if len(chainGaps) > 0 {
		fmt.Printf("  %s⚠ %d chain gap(s) — often clear automatically after the balance fixes below.%s\n", Fmt.Yellow, len(chainGaps), Fmt.Reset)
		fmt.Printf("  %s  If any remain after fixing, a line is missing — re-run the account sync.%s\n\n", Fmt.Dim, Fmt.Reset)
	}

	if len(fixable) == 0 {
		return nil
	}

	if dryRun {
		fmt.Printf("  %s(dry-run) would update balance_end_real on %d statement(s)%s\n\n",
			Fmt.Dim, len(fixable), Fmt.Reset)
		return nil
	}

	if !assumeYes {
		fmt.Printf("  %sApply %d fix(es)? This sets balance_end_real = running balance.%s [y/N] ",
			Fmt.Bold, len(fixable), Fmt.Reset)
		var resp string
		fmt.Scanln(&resp)
		if resp != "y" && resp != "Y" && resp != "yes" {
			fmt.Println("  Aborted.")
			return nil
		}
	}

	okCount, errCount := 0, 0
	for _, i := range fixable {
		if err := FixOdooStatementBalance(creds, uid, i.StatementID, i.RunningBalance); err != nil {
			fmt.Printf("  %s✗ #%d %s: %v%s\n", Fmt.Red, i.StatementID, i.StatementName, err, Fmt.Reset)
			errCount++
			continue
		}
		fmt.Printf("  %s✓%s #%d %s → balance_end_real = %s\n",
			Fmt.Green, Fmt.Reset, i.StatementID, i.StatementName, fmtEUR(i.RunningBalance))
		okCount++
	}
	fmt.Printf("\n  %s✓ Fixed %d, errors %d%s\n\n", Fmt.Green, okCount, errCount, Fmt.Reset)
	return nil
}

// odooJournalSync resolves a journal ID to its linked account and runs the account sync.
func odooJournalSync(journalID int, args []string) error {
	for _, acc := range LoadAccountConfigs() {
		if acc.OdooJournalID == journalID {
			return AccountOdooSync(acc.Slug, args)
		}
	}
	return fmt.Errorf("no account linked to Odoo journal #%d. Run: chb accounts <slug> link", journalID)
}

func odooJournalReset(creds *OdooCredentials, uid int, journalID int) error {
	// Fetch journal name
	result, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.journal", "search_read",
		[]interface{}{[]interface{}{
			[]interface{}{"id", "=", journalID},
		}},
		map[string]interface{}{"fields": []string{"name"}, "limit": 1})
	if err != nil {
		return fmt.Errorf("failed to fetch journal: %v", err)
	}
	var journals []struct {
		Name string `json:"name"`
	}
	json.Unmarshal(result, &journals)
	if len(journals) == 0 {
		return fmt.Errorf("journal #%d not found", journalID)
	}

	return emptyOdooJournal(creds, uid, journalID, journals[0].Name)
}

// PrintOdooHelp shows the top-level odoo command help.
func PrintOdooHelp() {
	f := Fmt
	fmt.Printf("\n%schb odoo%s — Odoo integration\n\n", f.Bold, f.Reset)
	fmt.Printf("%sCOMMANDS%s\n\n", f.Bold, f.Reset)
	fmt.Printf("  %s%schb odoo sync%s\n", f.Bold, f.Cyan, f.Reset)
	fmt.Printf("    %sFetch analytic categories from Odoo%s\n\n", f.Dim, f.Reset)
	fmt.Printf("  %s%schb odoo journals%s\n", f.Bold, f.Cyan, f.Reset)
	fmt.Printf("    %sList Odoo journals linked to accounts%s\n\n", f.Dim, f.Reset)
	fmt.Printf("  %s%schb odoo journals <id>%s\n", f.Bold, f.Cyan, f.Reset)
	fmt.Printf("    %sShow journal details (statements, line counts)%s\n\n", f.Dim, f.Reset)
	fmt.Printf("  %s%schb odoo journals <id> sync%s\n", f.Bold, f.Cyan, f.Reset)
	fmt.Printf("    %sSync the linked account's transactions into the journal%s\n\n", f.Dim, f.Reset)
	fmt.Printf("  %s%schb odoo journals <id> check%s\n", f.Bold, f.Cyan, f.Reset)
	fmt.Printf("    %sReport statements whose running balance is invalid%s\n\n", f.Dim, f.Reset)
	fmt.Printf("  %s%schb odoo journals <id> fix%s\n", f.Bold, f.Cyan, f.Reset)
	fmt.Printf("    %sSet balance_end_real = running balance on invalid statements%s\n\n", f.Dim, f.Reset)
	fmt.Printf("  %s%schb odoo journals <id> --reset%s\n", f.Bold, f.Cyan, f.Reset)
	fmt.Printf("    %sEmpty a journal (delete all statements and lines)%s\n\n", f.Dim, f.Reset)
}

func printOdooSyncHelp() {
	f := Fmt
	fmt.Printf(`
%schb odoo sync%s — Fetch analytic categories from Odoo

%sUSAGE%s
  %schb odoo sync%s

%sDESCRIPTION%s
  Reads analytic data from your Odoo instance and builds category
  enrichment for transactions. It fetches:

  - Analytic plans and accounts (your categorization dimensions)
  - Analytic lines (actual categorized postings linked to journal entries)
  - Payment transactions (Stripe references for matching)
  - Bank statement lines (blockchain tx references for matching)

  The enrichment is used during 'chb generate' to automatically
  categorize transactions based on Odoo's analytic accounting.

  Run %schb setup odoo%s first to configure credentials and category mapping.

%sPRIORITY CHAIN%s
  Nostr annotations > Odoo analytics > Local rules > Uncategorized

%sENVIRONMENT%s
  %sODOO_URL%s            Odoo instance URL
  %sODOO_LOGIN%s          Odoo login email
  %sODOO_PASSWORD%s       Odoo password or API key
`,
		f.Bold, f.Reset,
		f.Bold, f.Reset,
		f.Cyan, f.Reset,
		f.Bold, f.Reset,
		f.Cyan, f.Reset,
		f.Bold, f.Reset,
		f.Bold, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
	)
}
