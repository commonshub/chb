package cmd

import (
	"encoding/json"
	"fmt"
	"math"
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

// ResolveOdooCredentials returns Odoo credentials from env vars (set in APP_DATA_DIR/config.env).
// Database is derived from the ODOO_URL hostname unless ODOO_DATABASE is set.
func ResolveOdooCredentials() (*OdooCredentials, error) {
	creds := &OdooCredentials{
		URL:      os.Getenv("ODOO_URL"),
		DB:       os.Getenv("ODOO_DATABASE"),
		Login:    os.Getenv("ODOO_LOGIN"),
		Password: os.Getenv("ODOO_PASSWORD"),
	}

	if creds.URL == "" || creds.Login == "" || creds.Password == "" {
		return nil, fmt.Errorf("ODOO_URL/ODOO_LOGIN/ODOO_PASSWORD not set (check APP_DATA_DIR/config.env)")
	}

	if creds.DB == "" {
		creds.DB = odooDBFromURL(creds.URL)
	}

	return creds, nil
}

// odooLog writes to stdout unless an aggregate caller (chb odoo sync) has set
// quietOdooContext. Used for verbose info that should collapse into a single
// summary line when running as part of the aggregate sync.
func odooLog(format string, args ...interface{}) {
	if quietOdooContext() {
		return
	}
	fmt.Printf(format, args...)
}

// odooSyncLine prints the single-line summary for a sync step when running
// under the aggregate `chb odoo sync` command. Format:
//
//	syncing <label>: <status>
func odooSyncLine(label, status string) {
	fmt.Printf("  %s%s%s: %s\n", Fmt.Bold, label, Fmt.Reset, status)
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
		if quietOdooContext() {
			odooSyncLine("categories", "ODOO env not set — skipped")
		} else {
			Warnf("%s⚠ ODOO_URL/ODOO_LOGIN/ODOO_PASSWORD not set, skipping Odoo sync%s", Fmt.Yellow, Fmt.Reset)
		}
		return 0, nil
	}

	db := odooDBFromURL(odooURL)

	odooLog("\n%s🏢 Syncing Odoo analytics%s\n", Fmt.Bold, Fmt.Reset)
	odooLog("%sURL: %s  DB: %s%s\n", Fmt.Dim, odooURL, db, Fmt.Reset)

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
	odooLog("  %sFetched %d analytic accounts%s\n", Fmt.Dim, len(analyticAccounts), Fmt.Reset)

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
			"limit": 10000,
			"order": "date desc",
		})
	if err != nil {
		Warnf("  %s⚠ Could not fetch analytic lines: %v%s", Fmt.Yellow, err, Fmt.Reset)
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
	odooLog("  %sFetched %d analytic lines%s\n", Fmt.Dim, len(lines), Fmt.Reset)

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
	odooLog("  %sFetched %d payment transactions with Stripe references%s\n", Fmt.Dim, len(paymentTxs), Fmt.Reset)

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
	odooLog("  %sFetched %d bank statement lines%s\n", Fmt.Dim, len(bslLines), Fmt.Reset)

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

	if quietOdooContext() {
		odooSyncLine("categories", fmt.Sprintf("%d mappings (%d accounts, %d lines, %d payment refs, %d bank refs)",
			totalMapped, len(analyticAccounts), len(lines), len(paymentTxs), len(bslLines)))
	} else {
		fmt.Printf("\n  %s✓ %d mappings, saved %d months%s\n", Fmt.Green, totalMapped, saved, Fmt.Reset)
		fmt.Printf("  %s%d analytic accounts, %d analytic lines, %d payment refs, %d bank refs%s\n\n",
			Fmt.Dim, len(analyticAccounts), len(lines), len(paymentTxs), len(bslLines), Fmt.Reset)
	}

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

	// `chb odoo journals sync` → push local → Odoo for every linked journal
	if len(args) >= 1 && args[0] == "sync" {
		return odooJournalsSyncAll(args[1:])
	}

	// Check for `chb odoo journals <id> [sync|--reset]`
	if len(args) >= 1 && !strings.HasPrefix(args[0], "-") {
		var journalID int
		fmt.Sscanf(args[0], "%d", &journalID)
		if journalID == 0 {
			return fmt.Errorf("invalid journal ID: %s", args[0])
		}
		if len(args) >= 2 && args[1] == "sync" {
			syncArgs := args[2:]
			if HasFlag(syncArgs, "--reset") {
				printOdooTargetLine(creds)
				if err := odooJournalReset(creds, uid, journalID); err != nil {
					return err
				}
				syncArgs = filterFlag(syncArgs, "--reset")
				wasPrinted := odooTargetAlreadyPrinted()
				setOdooTargetAlreadyPrinted(true)
				defer setOdooTargetAlreadyPrinted(wasPrinted)
			}
			return odooJournalSync(journalID, syncArgs)
		}
		if len(args) >= 2 && args[1] == "check" {
			return odooJournalCheck(creds, uid, journalID)
		}
		if len(args) >= 2 && args[1] == "fix" {
			return odooJournalFix(creds, uid, journalID, HasFlag(args, "--yes", "-y"), HasFlag(args, "--dry-run"))
		}
		if len(args) >= 2 && args[1] == "reconcile" {
			return odooJournalReconcile(creds, uid, journalID, HasFlag(args, "--yes", "-y"), HasFlag(args, "--dry-run"))
		}
		if HasFlag(args, "--reset") {
			printOdooTargetLine(creds)
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

		reconciledResult, _ := odooExec(creds.URL, creds.DB, uid, creds.Password,
			"account.bank.statement.line", "search_count",
			[]interface{}{[]interface{}{
				[]interface{}{"journal_id", "=", acc.OdooJournalID},
				[]interface{}{"is_reconciled", "=", true},
			}}, nil)
		var reconciledCount int
		json.Unmarshal(reconciledResult, &reconciledCount)

		fmt.Printf("  %s%s%s  %s#%d%s\n", Fmt.Bold, acc.OdooJournalName, Fmt.Reset, Fmt.Dim, acc.OdooJournalID, Fmt.Reset)
		fmt.Printf("    %sAccount: %s (%s)%s\n", Fmt.Dim, acc.Name, acc.Slug, Fmt.Reset)
		fmt.Printf("    %s%d statement lines, %d reconciled, %d statements%s\n", Fmt.Dim, lineCount, reconciledCount, stmtCount, Fmt.Reset)
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
	fmt.Printf("  %s%schb odoo journals <id> reconcile [--dry-run] [--yes]%s\n", Fmt.Bold, Fmt.Cyan, Fmt.Reset)
	fmt.Printf("    %sMatch unreconciled statement lines against open invoices and bills%s\n\n", Fmt.Dim, Fmt.Reset)
	fmt.Printf("  %s%schb odoo journals <id> --reset%s\n", Fmt.Bold, Fmt.Cyan, Fmt.Reset)
	fmt.Printf("    %sEmpty a journal (delete all statements and lines)%s\n\n", Fmt.Dim, Fmt.Reset)

	return nil
}

func odooJournalDetail(creds *OdooCredentials, uid int, journalID int) error {
	linkedAccount := linkedAccountForJournal(journalID)
	currency := "EUR"
	if linkedAccount != nil {
		currency = accountConfigCurrency(linkedAccount)
	}

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

	reconciledResult, _ := odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.bank.statement.line", "search_count",
		[]interface{}{[]interface{}{
			[]interface{}{"journal_id", "=", journalID},
			[]interface{}{"is_reconciled", "=", true},
		}}, nil)
	var reconciledCount int
	json.Unmarshal(reconciledResult, &reconciledCount)

	currentBalance, balanceErr := odooJournalLineSum(creds, uid, journalID)

	stmtResult, _ := odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.bank.statement", "search_read",
		[]interface{}{[]interface{}{
			[]interface{}{"journal_id", "=", journalID},
		}},
		map[string]interface{}{
			"fields": []string{"id", "name", "date", "line_ids", "balance_start", "balance_end_real", "reference"},
			"order":  "date desc, id desc",
			"limit":  0,
		})
	var stmts []struct {
		ID             int     `json:"id"`
		Name           odooStr `json:"name"`
		Date           odooStr `json:"date"`
		Reference      odooStr `json:"reference"`
		LineIDs        []int   `json:"line_ids"`
		BalanceStart   float64 `json:"balance_start"`
		BalanceEndReal float64 `json:"balance_end_real"`
	}
	json.Unmarshal(stmtResult, &stmts)

	journalURL := OdooWebURL(creds.URL, "account.journal", journalID)
	fmt.Printf("\n  %s%s%s  %s#%d  type: %s%s\n", Fmt.Bold, j.Name, Fmt.Reset, Fmt.Dim, j.ID, j.Type, Fmt.Reset)
	fmt.Printf("  %sOdoo: %s  db: %s%s\n", Fmt.Dim, creds.URL, creds.DB, Fmt.Reset)
	fmt.Printf("  %sJournal: %s%s\n", Fmt.Dim, hyperlink(journalURL, journalURL), Fmt.Reset)
	if lastSync := LastSyncTime(fmt.Sprintf("odoo:journal:%d", journalID)); !lastSync.IsZero() {
		fmt.Printf("  %sLast sync: %s  (%s)%s\n", Fmt.Dim, lastSync.In(BrusselsTZ()).Format("2006-01-02 15:04"), formatTimeAgo(lastSync), Fmt.Reset)
	} else {
		fmt.Printf("  %sLast sync: never%s\n", Fmt.Dim, Fmt.Reset)
	}
	fmt.Println()
	if balanceErr == nil {
		fmt.Printf("  %sCurrent balance: %s%s\n", Fmt.Dim, formatBalance(currentBalance, currency), Fmt.Reset)
	} else {
		fmt.Printf("  %sCurrent balance: unavailable (%v)%s\n", Fmt.Dim, balanceErr, Fmt.Reset)
	}
	fmt.Printf("  %sStatement lines: %d%s\n", Fmt.Dim, lineCount, Fmt.Reset)
	fmt.Printf("  %sReconciled lines: %d%s\n", Fmt.Dim, reconciledCount, Fmt.Reset)
	fmt.Printf("  %sStatements: %d%s\n\n", Fmt.Dim, len(stmts), Fmt.Reset)

	if len(stmts) > 0 {
		rows := [][]string{}
		for _, s := range stmts {
			name := string(s.Name)
			if string(s.Reference) == "open" && !strings.Contains(strings.ToLower(name), "open") {
				name += " (open)"
			}
			rows = append(rows, []string{
				string(s.Date),
				truncate(name, 36),
				fmt.Sprintf("%d", len(s.LineIDs)),
				formatBalancePlain(s.BalanceStart, currency),
				formatBalancePlain(s.BalanceEndReal, currency),
			})
		}
		printAlignedTable(
			[]string{"Date", "Statement", "Lines", "Start", "End"},
			rows,
			map[int]bool{2: true, 3: true, 4: true},
		)
		fmt.Println()
	}

	if linkedAccount != nil {
		fmt.Printf("  %sLinked account%s\n", Fmt.Bold, Fmt.Reset)
		printAccountDetailSummary(linkedAccount, nil)
		fmt.Println()
	} else {
		fmt.Printf("  %sNo local account is linked to this Odoo journal.%s\n\n", Fmt.Dim, Fmt.Reset)
	}

	fmt.Printf("  %sTo reset: chb odoo journals %d --reset%s\n\n", Fmt.Dim, journalID, Fmt.Reset)
	fmt.Printf("  %sTo reconcile: chb odoo journals %d reconcile --dry-run%s\n\n", Fmt.Dim, journalID, Fmt.Reset)
	return nil
}

func linkedAccountForJournal(journalID int) *AccountConfig {
	configs := LoadAccountConfigs()
	for i := range configs {
		if configs[i].OdooJournalID == journalID {
			return &configs[i]
		}
	}
	return nil
}

func accountConfigCurrency(acc *AccountConfig) string {
	if acc == nil {
		return "EUR"
	}
	if acc.Currency != "" {
		return acc.Currency
	}
	if acc.Token != nil && acc.Token.Symbol != "" {
		return acc.Token.Symbol
	}
	return "EUR"
}

func printAlignedTable(headers []string, rows [][]string, rightAlign map[int]bool) {
	widths := make([]int, len(headers))
	for i, h := range headers {
		widths[i] = displayWidth(h)
	}
	for _, row := range rows {
		for i, cell := range row {
			if i >= len(widths) {
				continue
			}
			if w := displayWidth(cell); w > widths[i] {
				widths[i] = w
			}
		}
	}

	printRow := func(row []string) {
		fmt.Print("  ")
		for i := range headers {
			cell := ""
			if i < len(row) {
				cell = row[i]
			}
			if i > 0 {
				fmt.Print("  ")
			}
			if rightAlign[i] {
				fmt.Print(padLeft(cell, widths[i]))
			} else {
				fmt.Print(padRight(cell, widths[i]))
			}
		}
		fmt.Println()
	}

	printRow(headers)
	fmt.Printf("  %s%s%s\n", Fmt.Dim, strings.Repeat("─", tableWidth(widths, len(headers))), Fmt.Reset)
	for _, row := range rows {
		printRow(row)
	}
}

func tableWidth(widths []int, cols int) int {
	total := 0
	for _, w := range widths {
		total += w
	}
	if cols > 1 {
		total += (cols - 1) * 2
	}
	return total
}

func padRight(s string, width int) string {
	if n := width - displayWidth(s); n > 0 {
		return s + strings.Repeat(" ", n)
	}
	return s
}

func padLeft(s string, width int) string {
	if n := width - displayWidth(s); n > 0 {
		return strings.Repeat(" ", n) + s
	}
	return s
}

func displayWidth(s string) int {
	return len([]rune(s))
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

// odooJournalFix walks every statement on the journal in chronological
// order and repairs the running-balance and chain invariants in one pass:
//
//   - Each statement's balance_end_real is rewritten to balance_start + Σ(lines).
//   - Each statement's balance_start (except the first) is rewritten to the
//     previous statement's balance_end_real so statements chain correctly.
//
// This is safe for the new chronological sync model because line amounts
// are authoritative — the starting and ending balances are derived from
// them, not asserted independently.
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

	if !assumeYes && !dryRun {
		fmt.Printf("  %sRepair journal? This rewrites balance_start and balance_end_real on affected statements.%s [y/N] ",
			Fmt.Bold, Fmt.Reset)
		var resp string
		fmt.Scanln(&resp)
		if resp != "y" && resp != "Y" && resp != "yes" {
			fmt.Println("  Aborted.")
			return nil
		}
	}

	stmts, err := fetchJournalStatementsOrdered(creds, uid, journalID)
	if err != nil {
		return err
	}

	var prevEnd float64
	hasPrev := false
	edits := 0
	errs := 0
	for _, s := range stmts {
		// The open (trailing) statement mirrors live Stripe state. Leave
		// both its balance_start and balance_end_real alone — they are
		// managed by the sync, not the repair tool.
		if s.Reference == "open" {
			fmt.Printf("  %s↷ Skipping open statement #%d %s (live-synced, not repaired)%s\n",
				Fmt.Dim, s.ID, s.Name, Fmt.Reset)
			continue
		}
		sum, err := statementLineSum(creds, uid, s.ID)
		if err != nil {
			fmt.Printf("  %s✗ #%d %s: line sum: %v%s\n", Fmt.Red, s.ID, s.Name, err, Fmt.Reset)
			errs++
			continue
		}
		bs := s.BalanceStart
		if hasPrev && math.Abs(bs-prevEnd) > 0.005 {
			if dryRun {
				fmt.Printf("  %s(dry-run)%s #%d %s  balance_start %s → %s\n",
					Fmt.Dim, Fmt.Reset, s.ID, s.Name, fmtEURSigned(bs), fmtEURSigned(prevEnd))
			} else if err := setStatementBalanceStart(creds, uid, s.ID, prevEnd); err != nil {
				fmt.Printf("  %s✗ #%d %s: balance_start: %v%s\n", Fmt.Red, s.ID, s.Name, err, Fmt.Reset)
				errs++
				continue
			} else {
				fmt.Printf("  %s✓%s #%d %s  balance_start %s → %s\n",
					Fmt.Green, Fmt.Reset, s.ID, s.Name, fmtEURSigned(bs), fmtEURSigned(prevEnd))
			}
			bs = prevEnd
			edits++
		}
		running := bs + sum
		if math.Abs(running-s.BalanceEndReal) > 0.005 {
			if dryRun {
				fmt.Printf("  %s(dry-run)%s #%d %s  balance_end_real %s → %s\n",
					Fmt.Dim, Fmt.Reset, s.ID, s.Name, fmtEURSigned(s.BalanceEndReal), fmtEURSigned(running))
			} else if err := setStatementBalanceEndReal(creds, uid, s.ID, running); err != nil {
				fmt.Printf("  %s✗ #%d %s: balance_end_real: %v%s\n", Fmt.Red, s.ID, s.Name, err, Fmt.Reset)
				errs++
				continue
			} else {
				fmt.Printf("  %s✓%s #%d %s  balance_end_real %s → %s\n",
					Fmt.Green, Fmt.Reset, s.ID, s.Name, fmtEURSigned(s.BalanceEndReal), fmtEURSigned(running))
			}
			edits++
		}
		prevEnd = running
		hasPrev = true
	}

	if dryRun {
		fmt.Printf("\n  %s(dry-run) would apply %d edit(s)%s\n\n", Fmt.Dim, edits, Fmt.Reset)
		return nil
	}
	fmt.Printf("\n  %s✓ Applied %d edit(s), %d error(s)%s\n", Fmt.Green, edits, errs, Fmt.Reset)

	// Verify
	after, err := CheckOdooJournalStatements(creds, uid, journalID)
	if err == nil {
		if len(after) == 0 {
			fmt.Printf("  %s✓ Journal is valid%s\n\n", Fmt.Green, Fmt.Reset)
		} else {
			Warnf("  %s⚠ %d issue(s) remain:%s", Fmt.Yellow, len(after), Fmt.Reset)
			PrintStatementIssues(after)
		}
	}
	return nil
}

// fetchJournalStatementsOrdered returns all statements for the journal
// ordered chronologically (date asc, id asc as tiebreaker).
func fetchJournalStatementsOrdered(creds *OdooCredentials, uid int, journalID int) ([]journalStatement, error) {
	result, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.bank.statement", "search_read",
		[]interface{}{[]interface{}{
			[]interface{}{"journal_id", "=", journalID},
		}},
		map[string]interface{}{
			"fields": []string{"id", "name", "date", "balance_start", "balance_end_real", "reference"},
			"order":  "date asc, id asc",
		})
	if err != nil {
		return nil, fmt.Errorf("fetch statements: %v", err)
	}
	var raw []struct {
		ID             int     `json:"id"`
		Name           odooStr `json:"name"`
		Date           odooStr `json:"date"`
		Reference      odooStr `json:"reference"`
		BalanceStart   float64 `json:"balance_start"`
		BalanceEndReal float64 `json:"balance_end_real"`
	}
	if err := json.Unmarshal(result, &raw); err != nil {
		return nil, fmt.Errorf("parse statements: %v", err)
	}
	out := make([]journalStatement, 0, len(raw))
	for _, r := range raw {
		out = append(out, journalStatement{
			ID:             r.ID,
			Name:           string(r.Name),
			Date:           string(r.Date),
			Reference:      string(r.Reference),
			BalanceStart:   r.BalanceStart,
			BalanceEndReal: r.BalanceEndReal,
		})
	}
	return out, nil
}

type journalStatement struct {
	ID             int
	Name           string
	Date           string
	Reference      string
	BalanceStart   float64
	BalanceEndReal float64
}

// setStatementBalanceStart overwrites the balance_start of an existing statement.
func setStatementBalanceStart(creds *OdooCredentials, uid int, stmtID int, value float64) error {
	_, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.bank.statement", "write",
		[]interface{}{[]interface{}{stmtID}, map[string]interface{}{
			"balance_start": value,
		}}, nil)
	return err
}

// OdooSyncAll is the meta-command behind `chb odoo sync`. It runs, in order:
//   - chb odoo categories sync  (fetch analytic categories from Odoo)
//   - chb odoo invoices sync    (fetch outgoing invoices from Odoo)
//   - chb odoo bills sync       (fetch vendor bills from Odoo)
//   - chb odoo journals sync    (push local transactions into every linked journal)
//
// Per-step failures are reported but do not abort the overall run.
func OdooSyncAll(args []string) error {
	if creds, err := ResolveOdooCredentials(); err == nil {
		fmt.Printf("\n%s🔄 Odoo sync%s  %s%s (db: %s)%s\n\n",
			Fmt.Bold, Fmt.Reset, Fmt.Dim, creds.URL, creds.DB, Fmt.Reset)
	}
	setQuietOdooContext(true)
	defer setQuietOdooContext(false)

	step := func(label string, fn func() error) {
		if err := fn(); err != nil {
			odooSyncLine(label, fmt.Sprintf("%s✗ %v%s", Fmt.Red, err, Fmt.Reset))
		}
	}
	step("categories", func() error { _, err := OdooAnalyticSync(args); return err })
	step("invoices", func() error { _, err := InvoicesSync(args); return err })
	step("bills", func() error { _, err := BillsSync(args); return err })
	step("journals", func() error { return odooJournalsSyncAll(args) })
	fmt.Println()
	return nil
}

// odooJournalSync resolves a journal ID to its linked account and runs the account sync.
func odooJournalSync(journalID int, args []string) error {
	for _, acc := range LoadAccountConfigs() {
		if acc.OdooJournalID == journalID {
			return AccountOdooPush(acc.Slug, args)
		}
	}
	return fmt.Errorf("no account linked to Odoo journal #%d. Run: chb accounts <slug> link", journalID)
}

// odooJournalsSyncAll pushes every linked account's local transactions into
// its Odoo journal. In aggregate (quiet) mode the accounts are processed
// serially so the per-account one-liners stay in order and can't interleave.
// In verbose mode, up to 4 journals run concurrently.
func odooJournalsSyncAll(args []string) error {
	// Print the Odoo URL once at the top, unless a wrapper already did it.
	wasQuiet := quietOdooContext()
	if !wasQuiet {
		if creds, err := ResolveOdooCredentials(); err == nil {
			fmt.Printf("\n%sOdoo: %s (db: %s)%s\n", Fmt.Dim, creds.URL, creds.DB, Fmt.Reset)
		}
	}
	setQuietOdooContext(true)
	if !wasQuiet {
		defer setQuietOdooContext(false)
	}

	configs := LoadAccountConfigs()
	type target struct {
		slug      string
		journalID int
	}
	var targets []target
	for _, acc := range configs {
		if acc.OdooJournalID > 0 {
			targets = append(targets, target{slug: acc.Slug, journalID: acc.OdooJournalID})
		}
	}
	if len(targets) == 0 {
		odooLog("\n  %sNo accounts are linked to an Odoo journal%s\n\n", Fmt.Dim, Fmt.Reset)
		return nil
	}

	failed := 0
	if wasQuiet {
		// Serial: preserve one-line-per-item ordering, avoid interleaving.
		for _, t := range targets {
			if err := AccountOdooPush(t.slug, args); err != nil {
				failed++
			}
		}
	} else {
		type result struct {
			slug string
			err  error
		}
		sem := make(chan struct{}, 4)
		resultsCh := make(chan result, len(targets))
		for _, t := range targets {
			t := t
			sem <- struct{}{}
			go func() {
				defer func() { <-sem }()
				err := AccountOdooPush(t.slug, args)
				resultsCh <- result{slug: t.slug, err: err}
			}()
		}
		for i := 0; i < cap(sem); i++ {
			sem <- struct{}{}
		}
		close(resultsCh)

		for r := range resultsCh {
			if r.err != nil {
				fmt.Printf("  %s✗ %s: %v%s\n", Fmt.Red, r.slug, r.err, Fmt.Reset)
				failed++
			}
		}
	}
	if failed > 0 {
		return fmt.Errorf("%d journal(s) failed", failed)
	}
	return nil
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

func printOdooTargetLine(creds *OdooCredentials) {
	if quietOdooContext() || creds == nil {
		return
	}
	fmt.Printf("\n%sOdoo: %s (db: %s)%s\n", Fmt.Dim, creds.URL, creds.DB, Fmt.Reset)
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
	fmt.Printf("  %s%schb odoo backup%s\n", f.Bold, f.Cyan, f.Reset)
	fmt.Printf("    %sDownload a full database backup (zip of SQL dump + filestore)%s\n\n", f.Dim, f.Reset)
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
