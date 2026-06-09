package cmd

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sort"
	"strings"
	"time"

	odoosource "github.com/CommonsHub/chb/providers/odoo"
)

// OdooCredentials holds resolved Odoo connection info.
type OdooCredentials struct {
	URL      string
	DB       string
	Login    string
	Password string
}

// ResolveOdooCredentials returns Odoo credentials from env vars (set in APP_DATA_DIR/settings/config.env).
// Database is derived from the ODOO_URL hostname unless ODOO_DATABASE is set.
func ResolveOdooCredentials() (*OdooCredentials, error) {
	creds := &OdooCredentials{
		URL:      os.Getenv("ODOO_URL"),
		DB:       os.Getenv("ODOO_DATABASE"),
		Login:    os.Getenv("ODOO_LOGIN"),
		Password: os.Getenv("ODOO_PASSWORD"),
	}

	if creds.URL == "" || creds.Login == "" || creds.Password == "" {
		return nil, fmt.Errorf("ODOO_URL/ODOO_LOGIN/ODOO_PASSWORD not set (check APP_DATA_DIR/settings/config.env)")
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
//	<label>: <status>
func odooSyncLine(label, status string) {
	fmt.Printf("  %s%s%s: %s\n", Fmt.Bold, label, Fmt.Reset, status)
}

func odooItemSyncStatus(count int, singular, detail string) string {
	status := Pluralize(count, singular, "")
	if strings.TrimSpace(detail) != "" {
		status += " (" + strings.TrimSpace(detail) + ")"
	}
	return status
}

// odooSyncHeader prints a parent label (e.g. "journals:") for a group of
// nested items rendered with odooSyncSubLine.
func odooSyncHeader(label string) {
	fmt.Printf("  %s%s:%s\n", Fmt.Bold, label, Fmt.Reset)
}

// odooSyncSubLine prints a summary line nested one level under a header
// printed by odooSyncHeader.
func odooSyncSubLine(label, status string) {
	fmt.Printf("    %s%s%s: %s\n", Fmt.Bold, label, Fmt.Reset, status)
}

// journalSyncRow is one row in the journals summary table.
type journalSyncRow struct {
	JournalID int
	Slug      string
	TxCount   int
	Balance   string // pre-formatted (e.g. "1,234.56 EUR")
	Status    string // "already in sync", "5 new", "✗ <error>", ...
	Mismatch  string // optional multi-line detail to print after the row
	HasError  bool
}

// journalRowLayout holds the pre-computed prefix column widths used by the
// incremental row renderer. Set while odooJournalsSyncAll is iterating, so
// each per-journal sync can finalize its own row.
type journalRowLayout struct {
	JIDWidth  int
	SlugWidth int
	IsTTY     bool
}

var journalRowLayoutActive *journalRowLayout

// journalRowSink, when set, replaces finalizeJournalRow's direct print
// with a write to *journalRowSink. Used by the compact `chb push`
// driver: stdout is silenced around AccountOdooPush, the row data
// arrives here instead, and the driver renders one clean line per
// journal on stderr.
var journalRowSink *journalSyncRow

// pushAttentionHint is one closing action item collected during the
// compact push — surfaced after all journals finish so the operator
// has a clear "what to do next" list.
type pushAttentionHint struct {
	JournalID int
	Slug      string
	Message   string // human-readable suggestion
	Suggested string // shell command the operator can run
}

var pushAttentionHints []pushAttentionHint

// printJournalRowPrefix prints the "    #48  stripe" prefix of a journal row,
// followed by a newline so it stays visible while the per-journal sync prints
// its progress messages on the line below.
func printJournalRowPrefix(journalID int, slug string) {
	if journalRowLayoutActive == nil {
		return
	}
	w := journalRowLayoutActive
	fmt.Printf("    %s%-*s  %-*s%s\n",
		Fmt.Bold, w.JIDWidth, fmt.Sprintf("#%d", journalID),
		w.SlugWidth, slug, Fmt.Reset)
}

// finalizeJournalRow completes a row previously started with
// printJournalRowPrefix: on a TTY it walks the cursor back up to the prefix
// line and rewrites it with the full row (txs, balance, status). On non-TTY
// streams it just emits the full row as a new line. Returns true when it
// handled the output, false to signal the caller should fall back to the
// regular sub-line printer.
func finalizeJournalRow(row journalSyncRow) bool {
	// Compact-mode capture: row data is collected by the caller (the
	// `chb push` / `chb sync` driver) instead of printed inline. The
	// driver renders one clean line per journal on stderr after the
	// silenced section returns.
	// Snapshot the global into a local so a per-journal timeout that nils the
	// sink between this nil-check and the deref can't cause a panic.
	if sink := journalRowSink; sink != nil {
		*sink = row
		return true
	}
	w := journalRowLayoutActive
	if w == nil {
		return false
	}
	idStr := fmt.Sprintf("#%d", row.JournalID)
	txsStr := formatThousands(row.TxCount)
	status := row.Status
	if row.HasError {
		status = fmt.Sprintf("%s%s%s", Fmt.Red, status, Fmt.Reset)
	}
	if w.IsTTY {
		fmt.Print("\033[A\r\033[K")
	}
	fmt.Printf("    %s%-*s  %-*s%s  %10s txs  balance: %15s  (%s)\n",
		Fmt.Bold, w.JIDWidth, idStr, w.SlugWidth, row.Slug, Fmt.Reset,
		txsStr, row.Balance, status)
	if row.Mismatch != "" {
		Warnf("%s", strings.TrimRight(row.Mismatch, "\n"))
	}
	return true
}

// formatThousands renders an integer with thousands separators (1234 → "1,234").
func formatThousands(n int) string {
	neg := false
	if n < 0 {
		neg = true
		n = -n
	}
	s := fmt.Sprintf("%d", n)
	if len(s) <= 3 {
		if neg {
			return "-" + s
		}
		return s
	}
	var b strings.Builder
	pre := len(s) % 3
	if pre > 0 {
		b.WriteString(s[:pre])
		if len(s) > pre {
			b.WriteByte(',')
		}
	}
	for i := pre; i < len(s); i += 3 {
		b.WriteString(s[i : i+3])
		if i+3 < len(s) {
			b.WriteByte(',')
		}
	}
	if neg {
		return "-" + b.String()
	}
	return b.String()
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
			odooSyncLine("categories", odooItemSyncStatus(0, "category", "issue: ODOO env not set"))
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

	// Fetch bank statement lines whose ref looks like the odoo-web3 format
	// "{chain}:{account}:{txHash}:{logIndex}". Filtering server-side avoids
	// pulling every statement line in the database (the previous client-side
	// 5000-row cap silently truncated journals with more lines than that).
	bslResult, err := odooExec(odooURL, db, uid, odooPassword,
		"account.bank.statement.line", "search_read",
		[]interface{}{[]interface{}{
			[]interface{}{"ref", "ilike", "%:%:%:%"},
		}},
		map[string]interface{}{
			"fields": []string{"id", "name", "ref", "amount", "date", "move_id"},
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
		relPath := odoosource.RelPath(odoosource.AnalyticEnrichmentFile)
		writeMonthFile(dataDir, parts[0], parts[1], relPath, data)
		saved++
	}

	if quietOdooContext() {
		odooSyncLine("categories", odooItemSyncStatus(len(analyticAccounts), "category", ""))
	} else {
		fmt.Printf("\n  %s✓ %d mappings, saved %d months%s\n", Fmt.Green, totalMapped, saved, Fmt.Reset)
		fmt.Printf("  %s%d categories (%d with code-based slug), %d analytic lines, %d payment refs, %d bank refs%s\n\n",
			Fmt.Dim, len(analyticAccounts), len(categoryMapping), len(lines), len(paymentTxs), len(bslLines), Fmt.Reset)
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

// resolveJournalIDArg accepts either a numeric Odoo journal ID or an
// account slug. When given a slug, it looks up the configured account
// in accounts.json and returns its linked OdooJournalID. Errors clearly
// if the slug isn't known or isn't linked to a journal.
func resolveJournalIDArg(arg string) (int, error) {
	var journalID int
	if _, err := fmt.Sscanf(arg, "%d", &journalID); err == nil && journalID > 0 {
		return journalID, nil
	}
	acc := findAccountConfigBySlug(arg)
	if acc == nil {
		return 0, fmt.Errorf("invalid journal ID or unknown account slug: %s", arg)
	}
	if acc.OdooJournalID == 0 {
		return 0, fmt.Errorf("account %q is not linked to an Odoo journal", acc.Slug)
	}
	return acc.OdooJournalID, nil
}

// OdooJournals lists Odoo journals linked to accounts, or resets a specific journal.
func OdooJournals(args []string) error {
	// `--help` (anywhere in args) always wins — print contextual help
	// and never trigger the underlying side-effecting subcommand.
	if HasFlag(args, "--help", "-h") {
		printOdooJournalsContextualHelp(args)
		return nil
	}

	creds, err := ResolveOdooCredentials()
	if err != nil {
		return err
	}

	uid, err := odooAuth(creds.URL, creds.DB, creds.Login, creds.Password)
	if err != nil || uid == 0 {
		return fmt.Errorf("Odoo authentication failed: %v", err)
	}

	// `chb odoo journals push` (alias: `sync`) → push local → Odoo
	// for every linked journal.
	if len(args) >= 1 && (args[0] == "push" || args[0] == "sync") {
		if args[0] == "sync" {
			Warnf("%s'chb odoo journals sync' is deprecated — use 'chb odoo journals push' instead%s", Fmt.Dim, Fmt.Reset)
		}
		return odooJournalsSyncAll(args[1:])
	}

	// Check for `chb odoo journals <id|slug> [sync|--reset]`
	if len(args) >= 1 && !strings.HasPrefix(args[0], "-") {
		journalID, err := resolveJournalIDArg(args[0])
		if err != nil {
			return err
		}
		if targetArg := GetOption(args, "--merge-with"); targetArg != "" {
			if HasFlag(args, "--convert-invoice-payments") {
				return odooJournalConvertInvoicePayments(creds, uid, journalID, targetArg,
					HasFlag(args, "--dry-run"), HasFlag(args, "--verbose", "-v"), HasFlag(args, "--yes", "-y"))
			}
			return odooJournalMerge(creds, uid, journalID, targetArg, HasFlag(args, "--dry-run"), HasFlag(args, "--verbose", "-v"), HasFlag(args, "--yes", "-y"))
		}
		// `push` pushes local → Odoo only. `sync` is the full round-trip:
		// pull source → local, push local → Odoo, then refresh the local
		// journal cache, so afterwards live/local/journal all agree.
		if len(args) >= 2 && args[1] == "push" {
			return odooJournalPushWithReset(creds, uid, journalID, args[2:])
		}
		if len(args) >= 2 && args[1] == "sync" {
			return odooJournalFullSync(creds, uid, journalID, args[2:])
		}
		if len(args) >= 2 && args[1] == "check" {
			return odooJournalCheck(creds, uid, journalID)
		}
		if len(args) >= 2 && args[1] == "fix" {
			return odooJournalFix(creds, uid, journalID, HasFlag(args, "--yes", "-y"), HasFlag(args, "--dry-run"))
		}
		if len(args) >= 2 && (args[1] == "fix-amounts" || args[1] == "repair-amounts") {
			return repairOdooJournalLineAmounts(creds, uid, journalID, HasFlag(args, "--yes", "-y"), HasFlag(args, "--dry-run"))
		}
		if len(args) >= 2 && args[1] == "merge" {
			acc := linkedAccountForJournal(journalID)
			if acc == nil {
				return fmt.Errorf("journal #%d has no linked account; merge is only defined for accounts that own a local source-of-truth (e.g. kbcbrussels)", journalID)
			}
			if acc.Provider != "kbcbrussels" {
				return fmt.Errorf("`merge` is only implemented for kbcbrussels journals — for provider '%s' use `sync`", acc.Provider)
			}
			_, err := mergeKBCJournalWithCSV(creds, uid, journalID, acc,
				HasFlag(args, "--dry-run"),
				HasFlag(args, "--yes", "-y"),
				HasFlag(args, "--verbose", "-v"),
			)
			return err
		}
		if len(args) >= 2 && args[1] == "categorize" {
			acc := linkedAccountForJournal(journalID)
			if acc == nil {
				return fmt.Errorf("journal #%d has no linked account", journalID)
			}
			return categorizeOdooJournal(creds, uid, journalID, acc,
				HasFlag(args, "--dry-run"),
				HasFlag(args, "--yes", "-y"),
				HasFlag(args, "--verbose", "-v"),
			)
		}
		if len(args) >= 2 && args[1] == "reconcile" {
			if HasFlag(args[2:], "--from-journal") {
				reconcileArgs := append([]string{args[0]}, args[2:]...)
				return OdooReconcileCommand(reconcileArgs)
			}
			return odooJournalReconcileInteractive(creds, uid, journalID,
				HasFlag(args, "--yes", "-y"),
				HasFlag(args, "--dry-run"),
				HasFlag(args, "--verbose", "-v"),
				HasFlag(args, "--interactive", "-i"),
			)
		}
		if len(args) >= 2 && args[1] == "lines" {
			return odooJournalLines(creds, uid, journalID, args[2:])
		}
		if len(args) >= 2 && args[1] == "statements" {
			return odooJournalStatements(creds, uid, journalID, args[2:])
		}
		if len(args) >= 2 && args[1] == "pull" {
			return odooJournalPull(creds, uid, journalID)
		}
		if HasFlag(args, "--reset") {
			printOdooTargetLine(creds)
			return odooJournalReset(creds, uid, journalID, HasFlag(args, "--yes", "-y") || HasFlag(args, "--force"))
		}
		odooJournalDetailVerbose = HasFlag(args, "--verbose", "-v")
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
		acc := acc // capture by value for &acc below

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

		odooBalance, odooBalErr := odooJournalCurrentBalance(creds, uid, acc.OdooJournalID)
		currency := accCurrency(&acc)

		var localBalance float64
		var localCount int
		if totals := accountTotalsFromGeneratedTransactions(&acc, loadAccountTransactionsForOdoo(&acc)); totals != nil {
			localBalance = totals.CurrentBalance
			localCount = totals.TxCount
		}
		// In-sync = balance match (within rounding). Counts may diverge
		// legitimately for Stripe (per-BT local rows vs. aggregated fee
		// lines in Odoo), so balance is the load-bearing signal.
		inSync := odooBalErr == nil && math.Abs(odooBalance-localBalance) < 0.01

		journalName := OdooJournalName(acc.OdooJournalID)
		if journalName == "" {
			journalName = fmt.Sprintf("journal #%d", acc.OdooJournalID)
		}
		fmt.Printf("  %s%s%s  %s#%d%s\n", Fmt.Bold, journalName, Fmt.Reset, Fmt.Dim, acc.OdooJournalID, Fmt.Reset)
		fmt.Printf("    %sAccount: %s (%s)%s\n", Fmt.Dim, acc.Name, acc.Slug, Fmt.Reset)
		if inSync {
			fmt.Printf("    %sBalance: %s  %d lines (%d reconciled, %d statements)%s\n",
				Fmt.Dim, formatAccountDataBalance(odooBalance, currency), lineCount, reconciledCount, stmtCount, Fmt.Reset)
			fmt.Printf("    %s✓ in sync%s\n", Fmt.Green, Fmt.Reset)
		} else {
			fmt.Printf("    %sOdoo:    %s  %d lines (%d reconciled, %d statements)%s\n",
				Fmt.Dim, formatAccountDataBalance(odooBalance, currency), lineCount, reconciledCount, stmtCount, Fmt.Reset)
			fmt.Printf("    %sLocal:   %s  %d tx%s\n",
				Fmt.Dim, formatAccountDataBalance(localBalance, currency), localCount, Fmt.Reset)
			var parts []string
			if countDiff := localCount - lineCount; countDiff != 0 {
				switch {
				case countDiff > 0:
					parts = append(parts, Pluralize(countDiff, "tx", "")+" missing on Odoo")
				default:
					parts = append(parts, Pluralize(-countDiff, "tx", "")+" missing locally")
				}
			}
			balDiff := localBalance - odooBalance
			if math.Abs(balDiff) >= 0.01 {
				switch {
				case balDiff > 0:
					parts = append(parts, fmt.Sprintf("local balance is %s higher", formatAccountDataBalance(balDiff, currency)))
				default:
					parts = append(parts, fmt.Sprintf("Odoo balance is %s higher", formatAccountDataBalance(-balDiff, currency)))
				}
			}
			fmt.Printf("    %s⚠ not in sync%s  %s%s%s\n", Fmt.Yellow, Fmt.Reset, Fmt.Dim, strings.Join(parts, " — "), Fmt.Reset)
		}
		fmt.Println()
	}

	if !hasLinked {
		fmt.Printf("  %sNo accounts linked to Odoo journals.%s\n", Fmt.Dim, Fmt.Reset)
		fmt.Printf("  %sRun 'chb accounts <slug> link' to link one.%s\n\n", Fmt.Dim, Fmt.Reset)
		return nil
	}

	fmt.Printf("  %sRun 'chb odoo journals --help' for commands.%s\n\n", Fmt.Dim, Fmt.Reset)

	return nil
}

// odooJournalLines lists the most recent statement lines for a journal.
// Pagination via -n / --skip; --csv switches the output format.
// Columns: date, description, partner, account (CoA counterpart), amount.
func odooJournalLines(creds *OdooCredentials, uid int, journalID int, args []string) error {
	limit := GetNumber(args, []string{"-n", "--limit"}, 30)
	skip := GetNumber(args, []string{"--skip"}, 0)
	if limit <= 0 {
		limit = 30
	}
	if skip < 0 {
		skip = 0
	}
	asCSV := HasFlag(args, "--csv")

	currency := "EUR"
	if linked := linkedAccountForJournal(journalID); linked != nil {
		currency = accountConfigCurrency(linked)
	}

	result, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.bank.statement.line", "search_read",
		[]interface{}{[]interface{}{
			[]interface{}{"journal_id", "=", journalID},
		}},
		map[string]interface{}{
			"fields": []string{"id", "date", "payment_ref", "narration", "partner_id", "amount", "move_id"},
			"order":  "date desc, id desc",
			"limit":  limit,
			"offset": skip,
		})
	if err != nil {
		return fmt.Errorf("failed to fetch statement lines: %v", err)
	}

	type line struct {
		ID         int         `json:"id"`
		Date       odooStr     `json:"date"`
		PaymentRef odooStr     `json:"payment_ref"`
		Narration  odooStr     `json:"narration"`
		PartnerID  interface{} `json:"partner_id"`
		Amount     float64     `json:"amount"`
		MoveID     interface{} `json:"move_id"`
	}
	var lines []line
	if err := json.Unmarshal(result, &lines); err != nil {
		return fmt.Errorf("failed to decode statement lines: %v", err)
	}

	// Resolve the counterpart chart-of-accounts account via the line's
	// move (account.move.line.account_id), excluding the bank/cash side.
	accountByMove := map[int]string{}
	var moveIDs []int
	for _, l := range lines {
		if mid := odooFieldID(l.MoveID); mid > 0 {
			moveIDs = append(moveIDs, mid)
		}
	}
	if len(moveIDs) > 0 {
		mlResult, mlErr := odooExec(creds.URL, creds.DB, uid, creds.Password,
			"account.move.line", "search_read",
			[]interface{}{[]interface{}{
				[]interface{}{"move_id", "in", intsToInterfaces(moveIDs)},
			}},
			map[string]interface{}{"fields": []string{"move_id", "account_id", "account_type"}})
		if mlErr == nil {
			var mlines []struct {
				MoveID      interface{} `json:"move_id"`
				AccountID   interface{} `json:"account_id"`
				AccountType string      `json:"account_type"`
			}
			json.Unmarshal(mlResult, &mlines)
			for _, ml := range mlines {
				mid := odooFieldID(ml.MoveID)
				name := odooFieldName(ml.AccountID)
				if mid <= 0 || name == "" {
					continue
				}
				// Skip the bank/cash leg — we want the counterpart.
				if ml.AccountType == "asset_cash" || ml.AccountType == "liability_credit_card" {
					continue
				}
				if _, dup := accountByMove[mid]; !dup {
					accountByMove[mid] = name
				}
			}
		}
	}

	cellFor := func(l line) (date, desc, partner, account, amount string) {
		date = string(l.Date)
		desc = string(l.PaymentRef)
		if desc == "" {
			desc = string(l.Narration)
		}
		partner = odooFieldName(l.PartnerID)
		account = accountByMove[odooFieldID(l.MoveID)]
		amount = formatBalancePlain(l.Amount, currency)
		return
	}

	if asCSV {
		fmt.Println("date,description,partner,account,amount,currency")
		for _, l := range lines {
			date, desc, partner, account, _ := cellFor(l)
			fmt.Printf("%s,%s,%s,%s,%.2f,%s\n",
				csvCell(date), csvCell(desc), csvCell(partner), csvCell(account), l.Amount, csvCell(currency))
		}
		return nil
	}

	if len(lines) == 0 {
		fmt.Printf("\n%sNo statement lines found%s\n\n", Fmt.Dim, Fmt.Reset)
		return nil
	}

	journalName := OdooJournalName(journalID)
	if journalName == "" {
		journalName = fmt.Sprintf("journal #%d", journalID)
	}
	fmt.Printf("\n%s%s%s  %sshowing %d line(s), skip %d%s\n\n",
		Fmt.Bold, journalName, Fmt.Reset, Fmt.Dim, len(lines), skip, Fmt.Reset)

	headers := []string{"Date", "Description", "Partner", "Account", "Amount"}
	rows := make([][]string, 0, len(lines))
	var total float64
	for _, l := range lines {
		date, desc, partner, account, amount := cellFor(l)
		rows = append(rows, []string{
			date,
			Truncate(desc, 40),
			Truncate(partner, 24),
			Truncate(account, 28),
			amount,
		})
		total += l.Amount
	}
	totalRow := []string{
		"",
		Pluralize(len(lines), "line", ""),
		"",
		"",
		formatBalancePlain(total, currency),
	}
	renderTicketsTable(headers, rows, totalRow, map[int]bool{4: true})
	return nil
}

// printOdooJournalsContextualHelp inspects the args and prints help
// for whatever subcommand the user was invoking. Falls back to the
// generic `chb odoo journals` listing-help when the subcommand isn't
// recognized. Always silent on side effects — never hits Odoo.
func printOdooJournalsContextualHelp(args []string) {
	// Strip the help flag so the rest of args can be used for subcommand
	// detection.
	clean := make([]string, 0, len(args))
	for _, a := range args {
		if a == "--help" || a == "-h" {
			continue
		}
		clean = append(clean, a)
	}

	sub := ""
	if len(clean) >= 2 {
		sub = clean[1]
	} else if len(clean) == 1 {
		// `chb odoo journals push --help` — no <id> in front.
		if clean[0] == "push" || clean[0] == "sync" {
			sub = "sync-all"
		}
	}

	f := Fmt
	switch sub {
	case "push", "sync":
		fmt.Printf(`
%schb odoo journals <id|slug> push%s — Push local transactions for the linked
account into the Odoo journal. Reads the resolved category /
collective / accountCode / partnerId from each tx (written by
%schb generate%s) and applies them to created Odoo lines.

%schb odoo journals <id|slug> sync%s — Full round-trip: pull the linked
account's source → local (refreshes the live balance + regenerates the
local view), push local → Odoo, then refresh the journal cache. After it
the live, local and journal balances agree. Pass --reset once to rebuild
a journal whose existing lines pre-date a fix.

%sOPTIONS%s
  %s--dry-run%s              Preview only; no writes to Odoo
  %s-n N%s, %s--limit N%s        In dry-run, number of planned lines to show
  %s--force%s                Empty the journal first, then re-import
  %s--reset%s                Empty the journal first, then sync
  %s-y%s, %s--yes%s              With --reset, skip the confirmation prompt
  %s--history%s              Re-fetch the full Odoo import-id set (slow)
  %s--since YYYYMMDD%s       Only push transactions at/after this date
  %s--months N%s             Limit to the last N months
  %s--until YYYYMMDD%s       Only push transactions up to (inclusive) this date
  %s--skip-reconciliation%s  Don't reconcile created lines (use 'reconcile' later)
  %s--transactions%s         Stripe-only: import statement lines/statements/fees
  %s--partners%s             Stripe-only: link/create partners and collective tags
  %s--accounts%s             Stripe-only: apply account rules to journal lines
  %s--metadata%s             Stripe & blockchain: refresh payment_ref + narration on existing lines
  %s--payout <id>%s          Stripe-only: limit to one payout
`,
			f.Bold, f.Reset, f.Cyan, f.Reset,
			f.Bold, f.Reset, // sync header
			f.Bold, f.Reset, // OPTIONS
			f.Yellow, f.Reset, // --dry-run
			f.Yellow, f.Reset, f.Yellow, f.Reset, // -n, --limit
			f.Yellow, f.Reset, // --force
			f.Yellow, f.Reset, // --reset
			f.Yellow, f.Reset, f.Yellow, f.Reset, // -y, --yes
			f.Yellow, f.Reset, // --history
			f.Yellow, f.Reset, // --since
			f.Yellow, f.Reset, // --months
			f.Yellow, f.Reset, // --until
			f.Yellow, f.Reset, // --skip-reconciliation
			f.Yellow, f.Reset, // --transactions
			f.Yellow, f.Reset, // --partners
			f.Yellow, f.Reset, // --accounts
			f.Yellow, f.Reset, // --metadata
			f.Yellow, f.Reset, // --payout
		)
	case "sync-all":
		fmt.Printf(`
%schb odoo journals push%s — Push every linked account's local transactions
into its Odoo journal. Alias: 'sync' (deprecated). Same flags as the
per-journal form.

%sOPTIONS%s
  %s--dry-run%s              Preview only; no writes to Odoo
  %s--months N%s             Limit to the last N months
`,
			f.Bold, f.Reset, f.Bold, f.Reset,
			f.Yellow, f.Reset,
			f.Yellow, f.Reset,
		)
	case "lines":
		fmt.Printf(`
%schb odoo journals <id|slug> lines%s — List recent statement lines from Odoo
(date, description, partner, account, amount).

%sOPTIONS%s
  %s-n N%s, %s--limit N%s    Number of lines to show (default 30)
  %s--skip N%s               Skip the first N lines (pagination)
  %s--csv%s                  Emit CSV instead of a formatted table
`,
			f.Bold, f.Reset, f.Bold, f.Reset,
			f.Yellow, f.Reset, f.Yellow, f.Reset,
			f.Yellow, f.Reset,
			f.Yellow, f.Reset,
		)
	case "statements":
		fmt.Printf(`
%schb odoo journals <id|slug> statements%s — List statements (date, name,
# lines, start/end balance), newest first.

%sOPTIONS%s
  %s-n N%s, %s--limit N%s    Number of statements to show (default: all)
  %s--skip N%s               Skip the first N statements
  %s--csv%s                  Emit CSV instead of a formatted table
`,
			f.Bold, f.Reset, f.Bold, f.Reset,
			f.Yellow, f.Reset, f.Yellow, f.Reset,
			f.Yellow, f.Reset,
			f.Yellow, f.Reset,
		)
	case "check":
		fmt.Printf(`
%schb odoo journals <id|slug> check%s — Report statements whose running
balance is invalid (start + sum(lines) ≠ end). Read-only.
`, f.Bold, f.Reset)
	case "fix":
		fmt.Printf(`
%schb odoo journals <id|slug> fix%s — Repair a journal: rewrite orphan
import-ids, remove unmatched lines, correct line amounts that disagree with
local (e.g. mis-signed transfers), and recompute balances. Also diffs the
journal against local: offers to push local txs missing from Odoo, and reports
any balance gap from manual (non-chb) entries. Each repair is previewed and
confirmed separately. See also '%sfix-amounts%s' to run only the amount step.

%sOPTIONS%s
  %s--dry-run%s              Preview only; no writes to Odoo
  %s-y%s, %s--yes%s             Skip the interactive confirmation
`,
			f.Bold, f.Reset, f.Cyan, f.Reset, f.Bold, f.Reset,
			f.Yellow, f.Reset,
			f.Yellow, f.Reset, f.Yellow, f.Reset,
		)
	case "fix-amounts", "repair-amounts":
		fmt.Printf(`
%schb odoo journals <id|slug> fix-amounts%s — Correct, in place, statement lines
whose amount in Odoo disagrees with the locally-computed (correctly-signed)
amount for the same transaction — matched by unique_import_id. For each wrong
line it unreconciles (if needed), drafts the move, writes the corrected amount,
then re-posts. Use this instead of '%ssync --reset%s' to repair a few mis-signed
lines without wiping and rebuilding the whole journal. Re-reconcile afterwards
with '%sreconcile%s'.

%sOPTIONS%s
  %s--dry-run%s              Preview the wrong lines and the net balance change; no writes
  %s-y%s, %s--yes%s             Skip the interactive confirmation
`,
			f.Bold, f.Reset, f.Cyan, f.Reset, f.Cyan, f.Reset,
			f.Bold, f.Reset,
			f.Yellow, f.Reset,
			f.Yellow, f.Reset, f.Yellow, f.Reset,
		)
	case "reconcile":
		fmt.Printf(`
%schb odoo journals <id|slug> reconcile%s — Match unreconciled statement
lines against open invoices and bills, or move reconciliations from another
journal onto equivalent lines in this journal.

%sUSAGE%s
  %schb odoo journals 53 reconcile --dry-run%s
  %schb odoo journals 53 reconcile --from-journal 30 --dry-run%s
  %schb odoo journals 53 reconcile --from-journal 30 --yes%s

%sOPTIONS%s
  %s--from-journal <id|slug>%s  Source journal whose reconciled lines are moved
  %s--dry-run%s                 Preview only; no writes to Odoo
  %s-v%s, %s--verbose%s            Also list skipped/no-match lines
  %s-y%s, %s--yes%s                Skip the interactive confirmation
`,
			f.Bold, f.Reset,
			f.Bold, f.Reset,
			f.Cyan, f.Reset,
			f.Cyan, f.Reset,
			f.Cyan, f.Reset,
			f.Bold, f.Reset,
			f.Yellow, f.Reset,
			f.Yellow, f.Reset,
			f.Yellow, f.Reset, f.Yellow, f.Reset,
			f.Yellow, f.Reset, f.Yellow, f.Reset,
		)
	case "--merge-with":
		fmt.Printf(`
%schb odoo journals <sourceId|slug> --merge-with <targetId|slug>%s — Merge an
old/source journal into a target journal: move matching reconciliations, report
which source lines are already present or missing in the target, then empty and
delete the source journal when applied. Standalone accounting entries still
referencing the source journal are listed and can be moved too.

%sUSAGE%s
  %schb odoo journals 48 --merge-with 53 --dry-run --verbose%s
  %schb odoo journals 48 --merge-with 53 --yes%s

%sOPTIONS%s
  %s--dry-run%s       Preview only; no writes to Odoo and no confirmation prompt
  %s-v%s, %s--verbose%s  List skipped, missing, and already-present source lines
  %s-y%s, %s--yes%s      Skip move confirmation and per-line override prompts
                 Adding missing lines and deleting the source still ask
  %s--convert-invoice-payments%s
                 Instead of moving reconciliation records (which assumes
                 both journals use the A/R-via-partial-reconcile pattern),
                 walk every reconciled source line that pays a real
                 invoice/bill, find the equivalent target line by
                 (date, amount), and reconcile that target line against
                 the same invoice. The reconcile path rewrites the
                 target's revenue counterpart to A/R first — so a j48
                 line that was previously direct-posted to revenue
                 (e.g. 700150 VENTE TICKETS) ends up paying the same
                 invoice the j30 line was paying. Stripe fees and lines
                 reconciled to non-invoice moves are skipped. Same-
                 date+amount ambiguity is resolved by chronological line
                 id pairing within each (date, amount) bucket.
`,
			f.Bold, f.Reset,
			f.Bold, f.Reset,
			f.Cyan, f.Reset,
			f.Cyan, f.Reset,
			f.Bold, f.Reset,
			f.Yellow, f.Reset,
			f.Yellow, f.Reset, f.Yellow, f.Reset,
			f.Yellow, f.Reset, f.Yellow, f.Reset,
			f.Yellow, f.Reset,
		)
	default:
		// `chb odoo journals --help` or `chb odoo journals <id> --help`.
		if len(clean) >= 1 {
			fmt.Printf(`
%schb odoo journals <id|slug>%s — Show details for one journal.

%sSUBCOMMANDS%s
  %slines%s        Recent statement lines  (chb odoo journals <id> lines --help)
  %sstatements%s   Bank statements         (chb odoo journals <id> statements --help)
  %spush%s         Push local → Odoo       (chb odoo journals <id> push --help)
  %scheck%s        Report invalid balances
  %sfix%s          Repair the journal      (chb odoo journals <id> fix --help)
  %sreconcile%s    Match lines vs. invoices/bills (chb odoo journals <id> reconcile --help)
  %s--merge-with%s Move reconciliations into another journal, then delete this one
               (chb odoo journals <id> --merge-with --help)
               Supports %s--convert-invoice-payments%s for j30 A/R → j48 revenue merges
  %s--reset%s      Empty the journal (delete all statements + lines)
`,
				f.Bold, f.Reset,
				f.Bold, f.Reset,
				f.Cyan, f.Reset,
				f.Cyan, f.Reset,
				f.Cyan, f.Reset,
				f.Cyan, f.Reset,
				f.Cyan, f.Reset,
				f.Cyan, f.Reset,
				f.Yellow, f.Reset,
				f.Yellow, f.Reset,
				f.Yellow, f.Reset,
			)
		} else {
			fmt.Printf(`
%schb odoo journals%s — List Odoo journals linked to accounts.

%sCOMMANDS%s
  %schb odoo journals <id|slug>%s              Show details for one journal
  %schb odoo journals <id|slug> <subcommand>%s See per-subcommand --help
  %schb odoo journals push%s                   Push every linked journal (alias: sync)
`,
				f.Bold, f.Reset,
				f.Bold, f.Reset,
				f.Cyan, f.Reset,
				f.Cyan, f.Reset,
				f.Cyan, f.Reset,
			)
		}
	}
}

// odooJournalStatements lists every account.bank.statement bound to
// the journal, newest first. Date / Statement / # Lines / Start / End.
// Pagination via -n / --skip; --csv switches the output format.
func odooJournalStatements(creds *OdooCredentials, uid int, journalID int, args []string) error {
	limit := GetNumber(args, []string{"-n", "--limit"}, 0)
	skip := GetNumber(args, []string{"--skip"}, 0)
	if skip < 0 {
		skip = 0
	}
	asCSV := HasFlag(args, "--csv")

	currency := "EUR"
	if linked := linkedAccountForJournal(journalID); linked != nil {
		currency = accountConfigCurrency(linked)
	}

	opts := map[string]interface{}{
		"fields": []string{"id", "name", "date", "line_ids", "balance_start", "balance_end_real", "reference"},
		"order":  "date desc, id desc",
		"offset": skip,
	}
	if limit > 0 {
		opts["limit"] = limit
	} else {
		opts["limit"] = 0 // Odoo: no cap
	}
	stmtResult, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.bank.statement", "search_read",
		[]interface{}{[]interface{}{
			[]interface{}{"journal_id", "=", journalID},
		}}, opts)
	if err != nil {
		return fmt.Errorf("failed to fetch statements: %v", err)
	}
	var stmts []struct {
		ID             int     `json:"id"`
		Name           odooStr `json:"name"`
		Date           odooStr `json:"date"`
		Reference      odooStr `json:"reference"`
		LineIDs        []int   `json:"line_ids"`
		BalanceStart   float64 `json:"balance_start"`
		BalanceEndReal float64 `json:"balance_end_real"`
	}
	if err := json.Unmarshal(stmtResult, &stmts); err != nil {
		return fmt.Errorf("failed to decode statements: %v", err)
	}

	if asCSV {
		fmt.Println("date,statement,reference,lines,balance_start,balance_end,currency")
		for _, s := range stmts {
			fmt.Printf("%s,%s,%s,%d,%.2f,%.2f,%s\n",
				csvCell(string(s.Date)),
				csvCell(string(s.Name)),
				csvCell(string(s.Reference)),
				len(s.LineIDs),
				s.BalanceStart, s.BalanceEndReal,
				csvCell(currency),
			)
		}
		return nil
	}

	if len(stmts) == 0 {
		fmt.Printf("\n%sNo statements found%s\n\n", Fmt.Dim, Fmt.Reset)
		return nil
	}

	journalName := OdooJournalName(journalID)
	if journalName == "" {
		journalName = fmt.Sprintf("journal #%d", journalID)
	}
	fmt.Printf("\n%s%s%s  %sshowing %d statement(s), skip %d%s\n\n",
		Fmt.Bold, journalName, Fmt.Reset, Fmt.Dim, len(stmts), skip, Fmt.Reset)

	headers := []string{"Date", "Statement", "Lines", "Start", "End"}
	rows := make([][]string, 0, len(stmts))
	for _, s := range stmts {
		name := string(s.Name)
		if string(s.Reference) == "open" && !strings.Contains(strings.ToLower(name), "open") {
			name += " (open)"
		}
		rows = append(rows, []string{
			string(s.Date),
			Truncate(name, 36),
			fmt.Sprintf("%d", len(s.LineIDs)),
			formatBalancePlain(s.BalanceStart, currency),
			formatBalancePlain(s.BalanceEndReal, currency),
		})
	}
	totalRow := []string{"", Pluralize(len(stmts), "statement", ""), "", "", ""}
	renderTicketsTable(headers, rows, totalRow, map[int]bool{2: true, 3: true, 4: true})
	return nil
}

// odooJournalPull refreshes the local journal-lines cache for one
// journal. Lighter than the full `chb odoo pull` when the operator just
// wants to re-sync state for a specific journal — e.g. after a manual
// edit in the Odoo UI or after `chb odoo journals <id> --reset`.
func odooJournalPull(creds *OdooCredentials, uid int, journalID int) error {
	printOdooTargetLine(creds)
	fmt.Printf("\n  %sRefreshing local cache for journal #%d...%s\n", Fmt.Dim, journalID, Fmt.Reset)
	count, err := writeOdooJournalLinesCache(creds, uid, journalID)
	if err != nil {
		return fmt.Errorf("refresh journal #%d cache: %v", journalID, err)
	}
	fmt.Printf("  %s✓ Cached %d statement line%s for journal #%d%s\n\n",
		Fmt.Green, count, plural(count), journalID, Fmt.Reset)
	return nil
}

// odooJournalDetailVerbose is set by the caller of odooJournalDetail to
// switch the missing-tx preview between truncated (default) and full list.
// Threaded as a package-level flag rather than threading it through every
// helper signature — narrow scope, single read.
var odooJournalDetailVerbose bool

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

	currentBalance, balanceErr := odooJournalCurrentBalance(creds, uid, journalID)

	stmtCountResult, _ := odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.bank.statement", "search_count",
		[]interface{}{[]interface{}{
			[]interface{}{"journal_id", "=", journalID},
		}}, nil)
	var stmtCount int
	json.Unmarshal(stmtCountResult, &stmtCount)

	journalURL := OdooBankReconciliationURL(creds.URL, journalID)
	if j.Type != "bank" && j.Type != "cash" {
		journalURL = OdooWebURL(creds.URL, "account.journal", journalID)
	}
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
	fmt.Printf("  %sStatements: %d  %s(see: chb odoo journals %d statements)%s\n\n",
		Fmt.Dim, stmtCount, Fmt.Dim, journalID, Fmt.Reset)

	if linkedAccount != nil {
		if missing, err := findLocalTxsMissingFromOdoo(journalID, linkedAccount); err == nil && len(missing) > 0 {
			printMissingLocalTxs(missing, linkedAccount, journalID, odooJournalDetailVerbose)
		}
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

// missingLocalTx is a local transaction whose unique_import_id has no
// corresponding statement line in the Odoo journal.
type missingLocalTx struct {
	Date         string
	Amount       float64
	Counterparty string
	ImportID     string
	TxHash       string
}

// findLocalTxsMissingFromOdoo returns every local transaction for the
// linked account whose unique_import_id is absent from the Odoo journal
// — computed purely from local cache (no RPC). Reads the journal-lines
// cache populated by `chb odoo pull`; if it's missing, the operator gets
// pointed at the right command instead of a silent fallback.
func findLocalTxsMissingFromOdoo(journalID int, acc *AccountConfig) ([]missingLocalTx, error) {
	cached, ok := loadLatestOdooJournalLinesCache(acc.OdooJournalID)
	if !ok {
		return nil, fmt.Errorf("no local cache for journal #%d — run `chb odoo pull` first", journalID)
	}
	odooIDs := map[string]bool{}
	for _, ln := range cached {
		if isOdooSyntheticLine(ln) {
			continue
		}
		if ln.UniqueImportID != "" {
			odooIDs[ln.UniqueImportID] = true
		}
	}
	localTxs := loadAccountTransactionsForOdoo(acc)
	var missing []missingLocalTx
	for _, tx := range localTxs {
		importID := buildUniqueImportID(acc, tx)
		if importID == "" || odooIDs[importID] {
			continue
		}
		missing = append(missing, missingLocalTx{
			Date:         time.Unix(tx.Timestamp, 0).In(BrusselsTZ()).Format("2006-01-02"),
			Amount:       signedOdooAmountForTransaction(acc, tx),
			Counterparty: tx.Counterparty,
			ImportID:     importID,
			TxHash:       tx.TxHash,
		})
	}
	sort.Slice(missing, func(i, j int) bool { return missing[i].Date < missing[j].Date })
	return missing, nil
}

// printMissingLocalTxs prints a summary count + a preview of local txs not
// yet in Odoo. Default shows the first previewN (most recent first since
// they're typically the ones the operator's looking for); --verbose shows
// the full list.
func printMissingLocalTxs(missing []missingLocalTx, acc *AccountConfig, journalID int, verbose bool) {
	const previewN = 5
	currency := accCurrency(acc)
	total := len(missing)
	fmt.Printf("  %s⚠ %s missing from Odoo:%s\n", Fmt.Yellow, Pluralize(total, "local tx", ""), Fmt.Reset)

	// Show most recent first — most likely to be the operator's interest.
	display := make([]missingLocalTx, total)
	copy(display, missing)
	sort.Slice(display, func(i, j int) bool { return display[i].Date > display[j].Date })
	limit := total
	if !verbose && limit > previewN {
		limit = previewN
	}
	for _, m := range display[:limit] {
		ref := m.Counterparty
		if ref == "" {
			ref = m.TxHash
		}
		fmt.Printf("    %s%s%s  %14s  %s\n",
			Fmt.Dim, m.Date, Fmt.Reset,
			formatAccountDataBalance(m.Amount, currency),
			truncate(ref, 60))
	}
	if !verbose && total > previewN {
		fmt.Printf("    %s… and %d more (run with --verbose to see all)%s\n",
			Fmt.Dim, total-previewN, Fmt.Reset)
	}
	fmt.Printf("\n  %sPush them: chb odoo journals %d push%s\n\n", Fmt.Dim, journalID, Fmt.Reset)
}

type odooOwnedLineSync struct {
	LineID         int
	Date           string
	Amount         float64
	OldPaymentRef  string
	NewPaymentRef  string
	UniqueImportID string
	Update         map[string]interface{}
}

func planOdooOwnedLineSync(creds *OdooCredentials, uid int, journalID int, acc *AccountConfig) ([]odooOwnedLineSync, error) {
	if acc == nil {
		return nil, nil
	}
	fmt.Printf("  %sPlanning Monerium line metadata repairs for journal #%d...%s\n", Fmt.Dim, journalID, Fmt.Reset)
	localTxs := loadAccountTransactionsForOdoo(acc)
	byImportID := map[string]TransactionEntry{}
	moneriumImportIDs := []interface{}{}
	for _, tx := range localTxs {
		importID := buildUniqueImportID(acc, tx)
		if importID == "" {
			continue
		}
		byImportID[importID] = tx
		if isMoneriumTransaction(tx) {
			moneriumImportIDs = append(moneriumImportIDs, importID)
		}
	}
	if len(byImportID) == 0 {
		return nil, nil
	}
	// Two Odoo-side filters union'd via the leading `|`:
	//   1. payment_ref starts with "0x0000…" — legacy raw-blockchain
	//      placeholder, the original target of this refresh pass.
	//   2. unique_import_id is one of our known Monerium lines — catches
	//      the wider "counterparty leaked into payment_ref" case where
	//      the Monerium memo wasn't yet enriched at first upload.
	//
	// The local builder (buildMoneriumLineSyncUpdate) is the safety gate;
	// it returns nil for any line whose current payment_ref isn't "safe
	// to replace" (zero-address / empty / equals tx.Counterparty), so
	// widening the Odoo query can't accidentally rewrite a manual edit.
	fmt.Printf("  %sChecking Monerium lines for stale payment_refs...%s\n", Fmt.Dim, Fmt.Reset)
	domain := []interface{}{
		[]interface{}{"journal_id", "=", journalID},
	}
	if len(moneriumImportIDs) > 0 {
		domain = append(domain,
			"|",
			[]interface{}{"payment_ref", "ilike", "0x0000"},
			[]interface{}{"unique_import_id", "in", moneriumImportIDs},
		)
	} else {
		domain = append(domain, []interface{}{"payment_ref", "ilike", "0x0000"})
	}
	rows, err := odooSearchReadAllMaps(creds, uid, "account.bank.statement.line",
		domain,
		[]string{"id", "date", "amount", "payment_ref", "narration", "partner_id", "partner_bank_id", "unique_import_id"},
		"date asc, id asc",
	)
	if err != nil {
		return nil, err
	}
	fmt.Printf("  %sMatching %s against %s...%s\n",
		Fmt.Dim, Pluralize(len(rows), "zero-address Odoo line", ""), Pluralize(len(byImportID), "local transaction id", "local transaction ids"), Fmt.Reset)
	var out []odooOwnedLineSync
	for _, row := range rows {
		importID := odooString(row["unique_import_id"])
		tx, ok := byImportID[importID]
		if !ok {
			continue
		}
		update := buildMoneriumLineSyncUpdate(acc, tx, row)
		if len(update) == 0 {
			continue
		}
		out = append(out, odooOwnedLineSync{
			LineID:         odooInt(row["id"]),
			Date:           odooString(row["date"]),
			Amount:         odooFloat(row["amount"]),
			OldPaymentRef:  odooString(row["payment_ref"]),
			NewPaymentRef:  buildOdooPaymentRef(tx),
			UniqueImportID: importID,
			Update:         update,
		})
	}
	fmt.Printf("  %sFound %s to refresh.%s\n", Fmt.Dim, Pluralize(len(out), "line", ""), Fmt.Reset)
	return out, nil
}

func printOdooOwnedLineSyncPlan(plan []odooOwnedLineSync) {
	fmt.Printf("\n  %s%s can be refreshed from local enriched tx data:%s\n",
		Fmt.Yellow, Pluralize(len(plan), "Odoo line", ""), Fmt.Reset)
	for _, item := range plan {
		fmt.Printf("    %s%s%s  %12s  #%d  %s → %s\n",
			Fmt.Dim, item.Date, Fmt.Reset,
			fmtEURSigned(item.Amount),
			item.LineID,
			truncate(item.OldPaymentRef, 36),
			truncate(item.NewPaymentRef, 36))
	}
	fmt.Println()
}

func applyOdooOwnedLineSync(creds *OdooCredentials, uid int, plan []odooOwnedLineSync) (int, int) {
	ok, errs := 0, 0
	for _, item := range plan {
		if item.LineID == 0 || len(item.Update) == 0 {
			continue
		}
		_, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
			"account.bank.statement.line", "write",
			[]interface{}{[]interface{}{item.LineID}, item.Update}, odooStatementLineMetadataWriteContext())
		if err != nil {
			fmt.Printf("  %s✗ #%d: %v%s\n", Fmt.Red, item.LineID, err, Fmt.Reset)
			errs++
			continue
		}
		ok++
	}
	return ok, errs
}

func odooStatementLineMetadataWriteContext() map[string]interface{} {
	return map[string]interface{}{
		"context": map[string]interface{}{
			"skip_account_move_synchronization": true,
			"check_move_validity":               false,
		},
	}
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
// running-balance / chain-continuity invariants. For journals linked to
// manual / CSV providers (kbcbrussels), it additionally compares Odoo
// against the CSV source-of-truth: missing rows, count, balance.
func odooJournalCheck(creds *OdooCredentials, uid int, journalID int) error {
	fmt.Printf("\n  %sChecking journal #%d…%s\n", Fmt.Dim, journalID, Fmt.Reset)
	issues, err := CheckOdooJournalStatements(creds, uid, journalID)
	if err != nil {
		return err
	}
	if len(issues) == 0 {
		fmt.Printf("  %s✓ All statements are valid%s\n", Fmt.Green, Fmt.Reset)
	} else {
		PrintStatementIssues(issues)
		fmt.Printf("  %sTo fix: chb odoo journals %d fix%s\n", Fmt.Dim, journalID, Fmt.Reset)
	}

	if acc := linkedAccountForJournal(journalID); acc != nil && acc.Provider == "kbcbrussels" {
		if err := checkKBCBrusselsJournalAgainstCSV(creds, uid, journalID, acc); err != nil {
			return err
		}
	}
	fmt.Println()
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
	if !dryRun {
		if err := RequireOdooWriteCapability(); err != nil {
			return err
		}
	}
	fmt.Printf("\n  %sFixing Odoo journal #%d%s\n", Fmt.Bold, journalID, Fmt.Reset)
	// Detection is wrapped in a closure so it can be re-run after a push:
	// creating the missing lines changes the orphan/duplicate/balance
	// picture, so the remaining steps must plan against fresh state.
	var res *odooOrphanFindResult
	var linePlan []odooOwnedLineSync
	var loosePlan *looseLinesPlan
	var issues []StatementBalanceIssue
	var amountFixes []odooAmountFix
	detect := func() error {
		fmt.Printf("  %sChecking orphan/duplicate statement lines...%s\n", Fmt.Dim, Fmt.Reset)
		var err error
		res, err = findOdooOrphanStatementLines(creds, uid, journalID)
		if err != nil {
			return err
		}
		fmt.Printf("  %sOrphan check complete: %s, %s, %s.%s\n",
			Fmt.Dim,
			Pluralize(len(res.Repairs), "repair", ""),
			Pluralize(len(res.Duplicates), "duplicate pair", ""),
			Pluralize(len(res.Orphans), "orphan", ""),
			Fmt.Reset)
		var linePlanErr error
		linePlan, linePlanErr = planOdooOwnedLineSync(creds, uid, journalID, res.Account)
		if linePlanErr != nil {
			Warnf("  %s⚠ Could not plan line metadata repair: %v%s", Fmt.Yellow, linePlanErr, Fmt.Reset)
			linePlan = nil
		}

		fmt.Printf("  %sChecking line amounts...%s\n", Fmt.Dim, Fmt.Reset)
		var amountErr error
		amountFixes, amountErr = detectOdooJournalAmountFixes(creds, uid, journalID, res.Account)
		if amountErr != nil {
			Warnf("  %s⚠ Could not check line amounts: %v%s", Fmt.Yellow, amountErr, Fmt.Reset)
			amountFixes = nil
		}

		fmt.Printf("  %sChecking loose statement lines...%s\n", Fmt.Dim, Fmt.Reset)
		var looseErr error
		loosePlan, looseErr = planAttachLooseLines(creds, uid, journalID)
		if looseErr != nil {
			Warnf("  %s⚠ Could not plan loose-line attachment: %v%s", Fmt.Yellow, looseErr, Fmt.Reset)
			loosePlan = &looseLinesPlan{}
		}

		fmt.Printf("  %sChecking statement balances...%s\n", Fmt.Dim, Fmt.Reset)
		issues, err = CheckOdooJournalStatements(creds, uid, journalID)
		return err
	}
	if err := detect(); err != nil {
		return err
	}

	// Balance diagnostic: explains a local↔journal total gap that no
	// structural repair touches — typically Odoo lines with no
	// unique_import_id (opening balances, accountant adjustments). Skipped
	// when the account was never fully synced (local totals unreliable).
	var diag journalBalanceDiagnostic
	diagOK := false
	if res.SkippedReason == "" {
		if d, ok := computeJournalBalanceDiagnostic(res); ok {
			diag, diagOK = d, true
		}
	}

	if res.SkippedReason != "" {
		fmt.Printf("\n  %s⚠ Skipping orphan check: %s%s\n", Fmt.Yellow, res.SkippedReason, Fmt.Reset)
	}

	// A balance gap backed by deletable manual lines is itself an
	// actionable finding (the delete step below), so it must not trip the
	// early "nothing to fix" return.
	deletableUnowned := diagOK && diag.hasGap && len(res.Unowned) > 0

	if len(res.Repairs) == 0 && len(res.Duplicates) == 0 && len(res.Orphans) == 0 && len(res.PostLatestLocal) == 0 && len(res.Missing) == 0 && len(linePlan) == 0 && len(amountFixes) == 0 && len(loosePlan.Assign) == 0 && len(issues) == 0 && !deletableUnowned {
		if diagOK && diag.hasGap {
			printJournalBalanceDiagnostic(diag, res)
			fmt.Printf("  %s✓ No chb-owned issues to fix — the %s difference reported by sync is not from chb-owned lines (see above).%s\n\n",
				Fmt.Green, fmtEURSigned(diag.balanceDelta), Fmt.Reset)
		} else {
			fmt.Printf("\n  %s✓ Nothing to fix%s\n\n", Fmt.Green, Fmt.Reset)
		}
		return nil
	}

	if diagOK && diag.hasGap {
		printJournalBalanceDiagnostic(diag, res)
	}

	if len(res.Missing) > 0 {
		printMissingLines(res.Missing, res.Account)
		proceed := assumeYes
		if !assumeYes && !dryRun {
			fmt.Printf("  %sPush %s to Odoo now? Runs the standard push for this journal.%s [y/N] ",
				Fmt.Bold, Pluralize(len(res.Missing), "missing tx", ""), Fmt.Reset)
			var resp string
			fmt.Scanln(&resp)
			proceed = resp == "y" || resp == "Y" || resp == "yes"
		}
		switch {
		case dryRun:
			fmt.Printf("  %s(dry-run) would push %s%s\n\n", Fmt.Dim, Pluralize(len(res.Missing), "missing tx", ""), Fmt.Reset)
		case proceed && res.Account == nil:
			Warnf("  %s⚠ No linked account for journal #%d — cannot push%s", Fmt.Yellow, journalID, Fmt.Reset)
		case proceed:
			if err := AccountOdooPush(res.Account.Slug, nil); err != nil {
				Warnf("  %s⚠ Push failed: %v%s", Fmt.Yellow, err, Fmt.Reset)
			} else {
				// Re-detect so the steps below plan against the journal as
				// it now stands (new lines reconciled, balances shifted).
				if err := detect(); err != nil {
					return err
				}
				if d, ok := computeJournalBalanceDiagnostic(res); ok {
					diag, diagOK = d, true
				}
			}
		default:
			fmt.Printf("  %sPush skipped.%s\n\n", Fmt.Dim, Fmt.Reset)
		}
	}

	if len(res.PostLatestLocal) > 0 {
		printPostLatestOrphanLines(res.PostLatestLocal, res.Account, res.LatestLocal)
	}

	if len(res.Duplicates) > 0 {
		printOrphanDuplicates(res.Duplicates)
		proceed := assumeYes
		if !assumeYes && !dryRun {
			fmt.Printf("  %sConsolidate %s? Keeps the reconciled side, deletes the duplicate.%s [y/N] ",
				Fmt.Bold, Pluralize(len(res.Duplicates), "duplicate pair", ""), Fmt.Reset)
			var resp string
			fmt.Scanln(&resp)
			proceed = resp == "y" || resp == "Y" || resp == "yes"
		}
		switch {
		case dryRun:
			fmt.Printf("  %s(dry-run) would consolidate %s%s\n\n", Fmt.Dim, Pluralize(len(res.Duplicates), "pair", ""), Fmt.Reset)
		case proceed:
			ok, conflicts := consolidateOrphanDuplicates(creds, uid, res.Duplicates)
			fmt.Printf("  %s✓ Consolidated %s%s", Fmt.Green, Pluralize(ok, "pair", ""), Fmt.Reset)
			if len(conflicts) > 0 {
				fmt.Printf("  %s(%s had both sides reconciled — manual review needed)%s", Fmt.Yellow, Pluralize(len(conflicts), "pair", ""), Fmt.Reset)
				for _, c := range conflicts {
					fmt.Printf("\n    %s#%d (legacy) and #%d (canonical)%s",
						Fmt.Yellow, c.Broken.ID, c.CleanLineID, Fmt.Reset)
				}
			}
			fmt.Println()
			fmt.Println()
		default:
			fmt.Printf("  %sConsolidation skipped.%s\n\n", Fmt.Dim, Fmt.Reset)
		}
	}

	if len(res.Repairs) > 0 {
		printOrphanRepairs(res.Repairs)
		proceed := assumeYes
		if !assumeYes && !dryRun {
			fmt.Printf("  %sRewrite unique_import_id on %s? Reconciliation, partners and narration are preserved.%s [y/N] ",
				Fmt.Bold, Pluralize(len(res.Repairs), "line", ""), Fmt.Reset)
			var resp string
			fmt.Scanln(&resp)
			proceed = resp == "y" || resp == "Y" || resp == "yes"
		}
		switch {
		case dryRun:
			fmt.Printf("  %s(dry-run) would rewrite %s%s\n\n", Fmt.Dim, Pluralize(len(res.Repairs), "line", ""), Fmt.Reset)
		case proceed:
			ok, failed := repairOrphanImportIDs(creds, uid, res.Repairs)
			fmt.Printf("  %s✓ Rewrote %s%s", Fmt.Green, Pluralize(ok, "line", ""), Fmt.Reset)
			if len(failed) > 0 {
				fmt.Printf("  %s(%d failed — falling back to delete)%s", Fmt.Yellow, len(failed), Fmt.Reset)
				res.Orphans = append(res.Orphans, failed...)
			}
			fmt.Println()
			fmt.Println()
		default:
			fmt.Printf("  %sRepair skipped.%s\n\n", Fmt.Dim, Fmt.Reset)
		}
	}

	if len(linePlan) > 0 {
		printOdooOwnedLineSyncPlan(linePlan)
		proceed := assumeYes
		if !assumeYes && !dryRun {
			fmt.Printf("  %sUpdate %s from local enriched transaction data?%s [y/N] ",
				Fmt.Bold, Pluralize(len(linePlan), "Odoo line", ""), Fmt.Reset)
			var resp string
			fmt.Scanln(&resp)
			proceed = resp == "y" || resp == "Y" || resp == "yes"
		}
		switch {
		case dryRun:
			fmt.Printf("  %s(dry-run) would update %s%s\n\n", Fmt.Dim, Pluralize(len(linePlan), "line", ""), Fmt.Reset)
		case proceed:
			ok, errs := applyOdooOwnedLineSync(creds, uid, linePlan)
			fmt.Printf("  %s✓ Updated %s%s", Fmt.Green, Pluralize(ok, "line", ""), Fmt.Reset)
			if errs > 0 {
				fmt.Printf("  %s(%s)%s", Fmt.Yellow, Pluralize(errs, "error", ""), Fmt.Reset)
			}
			fmt.Println()
			fmt.Println()
		default:
			fmt.Printf("  %sLine metadata repair skipped.%s\n\n", Fmt.Dim, Fmt.Reset)
		}
	}

	if len(amountFixes) > 0 {
		printOdooJournalAmountFixes(amountFixes)
		proceed := assumeYes
		if !assumeYes && !dryRun {
			fmt.Printf("  %sCorrect %s? Each line is unreconciled if needed, then drafted, rewritten and re-posted.%s [y/N] ",
				Fmt.Bold, Pluralize(len(amountFixes), "wrong amount", ""), Fmt.Reset)
			var resp string
			fmt.Scanln(&resp)
			proceed = resp == "y" || resp == "Y" || resp == "yes"
		}
		switch {
		case dryRun:
			fmt.Printf("  %s(dry-run) would correct %s%s\n\n", Fmt.Dim, Pluralize(len(amountFixes), "line", ""), Fmt.Reset)
		case proceed:
			reconciledN := countReconciledAmountFixes(amountFixes)
			ok, failed := applyOdooJournalAmountFixes(creds, uid, journalID, amountFixes)
			printOdooJournalAmountFixResult(journalID, len(amountFixes), ok, failed, reconciledN)
		default:
			fmt.Printf("  %sAmount repair skipped.%s\n\n", Fmt.Dim, Fmt.Reset)
		}
	}

	if len(res.Orphans) > 0 {
		printOrphanStatementLines(res.Orphans)
		proceed := assumeYes
		if !assumeYes && !dryRun {
			fmt.Printf("  %sDelete %s from Odoo? This cannot be undone.%s [y/N] ",
				Fmt.Bold, Pluralize(len(res.Orphans), "orphan line", ""), Fmt.Reset)
			var resp string
			fmt.Scanln(&resp)
			proceed = resp == "y" || resp == "Y" || resp == "yes"
		}
		switch {
		case dryRun:
			fmt.Printf("  %s(dry-run) would delete %s%s\n\n", Fmt.Dim, Pluralize(len(res.Orphans), "orphan line", ""), Fmt.Reset)
		case proceed:
			ids := make([]int, len(res.Orphans))
			for i, o := range res.Orphans {
				ids[i] = o.ID
			}
			if err := deleteStatementLines(creds, uid, ids); err != nil {
				Warnf("  %s⚠ Failed to delete orphan lines: %v%s", Fmt.Red, err, Fmt.Reset)
			} else {
				fmt.Printf("  %s✓ Deleted %s%s\n\n", Fmt.Green, Pluralize(len(res.Orphans), "orphan line", ""), Fmt.Reset)
			}
		default:
			fmt.Printf("  %sOrphan removal skipped.%s\n\n", Fmt.Dim, Fmt.Reset)
		}
	}

	// Non-chb (manual) lines that throw off the balance vs local. Offered
	// only when there's an actual gap AND such lines exist — listed in full
	// first, then a destructive delete behind explicit confirmation. These
	// are typically opening balances or accountant adjustments, so the
	// prompt is deliberately cautious.
	deletedUnowned := 0
	if diagOK && diag.hasGap && len(res.Unowned) > 0 {
		printUnownedLines(res.Unowned, res.Account, diag)
		proceed := assumeYes
		if !assumeYes && !dryRun {
			fmt.Printf("  %sDelete %s not owned by chb? Removes manual entries (opening balances, adjustments) to close the balance gap. This cannot be undone.%s [y/N] ",
				Fmt.Bold, Pluralize(len(res.Unowned), "line", ""), Fmt.Reset)
			var resp string
			fmt.Scanln(&resp)
			proceed = resp == "y" || resp == "Y" || resp == "yes"
		}
		switch {
		case dryRun:
			fmt.Printf("  %s(dry-run) would delete %s%s\n\n", Fmt.Dim, Pluralize(len(res.Unowned), "non-chb line", ""), Fmt.Reset)
		case proceed:
			ids := make([]int, len(res.Unowned))
			for i, o := range res.Unowned {
				ids[i] = o.ID
			}
			if err := deleteStatementLines(creds, uid, ids); err != nil {
				Warnf("  %s⚠ Failed to delete non-chb lines: %v%s", Fmt.Red, err, Fmt.Reset)
			} else {
				deletedUnowned = len(res.Unowned)
				fmt.Printf("  %s✓ Deleted %s%s\n\n", Fmt.Green, Pluralize(deletedUnowned, "non-chb line", ""), Fmt.Reset)
			}
		default:
			fmt.Printf("  %sNon-chb line removal skipped.%s\n\n", Fmt.Dim, Fmt.Reset)
		}
	}

	// Re-plan loose-line attachment after the orphan/duplicate/repair
	// mutations above, since some previously-loose lines may have been
	// deleted (or are now attached as a side-effect of consolidation).
	if !dryRun && (len(res.Repairs) > 0 || len(res.Duplicates) > 0 || len(res.Orphans) > 0 || len(linePlan) > 0 || deletedUnowned > 0) {
		if replanned, err := planAttachLooseLines(creds, uid, journalID); err == nil {
			loosePlan = replanned
		}
	}

	if len(loosePlan.Assign) > 0 {
		printLooseLinesPlan(loosePlan)
		proceed := assumeYes
		if !assumeYes && !dryRun {
			fmt.Printf("  %sAttach %s to their target statement?%s [y/N] ",
				Fmt.Bold, Pluralize(len(loosePlan.Assign), "loose line", ""), Fmt.Reset)
			var resp string
			fmt.Scanln(&resp)
			proceed = resp == "y" || resp == "Y" || resp == "yes"
		}
		switch {
		case dryRun:
			fmt.Printf("  %s(dry-run) would attach %s%s\n\n", Fmt.Dim, Pluralize(len(loosePlan.Assign), "line", ""), Fmt.Reset)
		case proceed:
			ok := attachLooseLines(creds, uid, loosePlan.Assign)
			fmt.Printf("  %s✓ Attached %s%s\n\n", Fmt.Green, Pluralize(ok, "line", ""), Fmt.Reset)
		default:
			fmt.Printf("  %sLoose-line attachment skipped.%s\n\n", Fmt.Dim, Fmt.Reset)
		}
	}

	// Balance invariants are checked LAST: every mutation above (orphan
	// deletes, duplicate consolidation, import_id repair, loose-line
	// attachment) can shift line totals or move lines between statements,
	// so the balance_start/balance_end_real walk has to come after them.
	if !dryRun && (len(res.Repairs) > 0 || len(res.Duplicates) > 0 || len(res.Orphans) > 0 || len(loosePlan.Assign) > 0 || deletedUnowned > 0) {
		if rechecked, err := CheckOdooJournalStatements(creds, uid, journalID); err == nil {
			issues = rechecked
		}
	}

	if len(issues) == 0 {
		return nil
	}
	PrintStatementIssues(issues)

	if !assumeYes && !dryRun {
		fmt.Printf("  %sRepair journal? This rewrites balance_start and balance_end_real on affected statements, and collapses duplicate \"Stripe fees for open statement\" lines.%s [y/N] ",
			Fmt.Bold, Fmt.Reset)
		var resp string
		fmt.Scanln(&resp)
		if resp != "y" && resp != "Y" && resp != "yes" {
			fmt.Println("  Aborted.")
			return nil
		}
	}

	edits := 0
	errs := 0

	// Step 1: collapse duplicate "Stripe fees for open statement" lines.
	// Sums are preserved (we keep one line whose amount = Σ duplicates),
	// so the running-balance walk that follows sees correct line totals.
	for _, issue := range issues {
		if issue.Kind != "duplicate_open_fee_lines" {
			continue
		}
		applied, err := mergeDuplicateOpenFeeLines(creds, uid, issue, dryRun)
		if err != nil {
			fmt.Printf("  %s✗ #%d %s: merge fee duplicates: %v%s\n",
				Fmt.Red, issue.StatementID, issue.StatementName, err, Fmt.Reset)
			errs++
			continue
		}
		edits += applied
	}

	stmts, err := fetchJournalStatementsOrdered(creds, uid, journalID)
	if err != nil {
		return err
	}

	var prevEnd float64
	hasPrev := false
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
		fmt.Printf("\n  %s(dry-run) would apply %s%s\n\n", Fmt.Dim, Pluralize(edits, "edit", ""), Fmt.Reset)
		return nil
	}
	fmt.Printf("\n  %s✓ Applied %s, %s%s\n", Fmt.Green, Pluralize(edits, "edit", ""), Pluralize(errs, "error", ""), Fmt.Reset)

	// Verify
	after, err := CheckOdooJournalStatements(creds, uid, journalID)
	if err == nil {
		if len(after) == 0 {
			fmt.Printf("  %s✓ Journal is valid%s\n\n", Fmt.Green, Fmt.Reset)
		} else {
			Warnf("  %s⚠ %s remain:%s", Fmt.Yellow, Pluralize(len(after), "issue", ""), Fmt.Reset)
			PrintStatementIssues(after)
		}
	}
	return nil
}

// odooOrphanLine is an account.bank.statement.line in Odoo whose
// unique_import_id has no matching local transaction — typically a stale
// artifact from an earlier sync (a local tx that was deleted or rebuilt).
// Lines with no unique_import_id are excluded: those are manual entries
// (opening balances, accountant adjustments) and chb does not own them.
type odooOrphanLine struct {
	ID             int
	Date           string
	Amount         float64
	PaymentRef     string
	UniqueImportID string
}

// odooOrphanFindResult holds the categorized output of orphan detection.
//   - Repairs: broken-form lines whose canonical form isn't yet in Odoo;
//     a simple in-place unique_import_id rewrite is enough.
//   - Duplicates: broken-form lines whose canonical form ALSO exists in
//     the journal (two lines for the same tx). Needs reconciliation-aware
//     consolidation: delete the unreconciled side, rewrite the keeper.
//   - Orphans: no canonical form found in local; safe to delete.
//   - PostLatestLocal: dated after the latest local tx — likely a missed
//     local sync, not stale data.
//   - Missing: local txs with no Odoo line at all (the local→Odoo
//     direction). The journal is behind local; the remedy is `push`,
//     not a structural repair. Not populated for Stripe journals, where
//     local balance-txs don't map 1:1 to statement lines.
type odooOrphanFindResult struct {
	Account         *AccountConfig
	Repairs         []odooOrphanRepair
	Duplicates      []odooOrphanDuplicate
	Orphans         []odooOrphanLine
	PostLatestLocal []odooOrphanLine
	Missing         []TransactionEntry
	LatestLocal     time.Time
	SkippedReason   string

	// OdooImportedCount/Sum describe the chb-owned side of the journal
	// (lines carrying a unique_import_id). Unowned holds the complement —
	// statement lines with NO unique_import_id (manual opening balances,
	// accountant adjustments). chb never creates these; they're surfaced so
	// the balance diagnostic can explain a local↔journal gap no structural
	// repair touches, and so `fix` can offer to delete them when the user
	// confirms they're spurious.
	OdooImportedCount int
	OdooImportedSum   float64
	Unowned           []odooOrphanLine
	UnownedSum        float64
}

// odooOrphanRepair pairs a broken Odoo line with its canonical
// unique_import_id. Rewriting in place avoids the unreconcile/draft/
// unlink dance and keeps all downstream Odoo state intact.
type odooOrphanRepair struct {
	Line        odooOrphanLine
	CanonicalID string
}

// odooOrphanDuplicate describes a (legacy, canonical) pair of lines for
// the same on-chain transaction. The broken-form line is typically the
// older, reconciled one; the clean-form line is typically a recent
// re-push that hasn't been reconciled yet.
type odooOrphanDuplicate struct {
	Broken      odooOrphanLine
	CleanID     string
	CleanLineID int
}

func findOdooOrphanStatementLines(creds *OdooCredentials, uid int, journalID int) (*odooOrphanFindResult, error) {
	acc := linkedAccountForJournal(journalID)
	if acc == nil {
		return &odooOrphanFindResult{}, nil
	}
	res := &odooOrphanFindResult{Account: acc}

	if LastFullSyncTime("account:" + strings.ToLower(acc.Slug)).IsZero() {
		res.SkippedReason = fmt.Sprintf("account %q has never been fully synced (run: chb accounts %s sync --history)", acc.Slug, acc.Slug)
		return res, nil
	}

	localTxs := loadAccountTransactionsForOdoo(acc)
	localIDs := map[string]bool{}
	// Legacy index: some imports stored `<chain>:<addr>:<chain>:<14-hex
	// prefix>:<n>` (pre-NIP-73 tx.ID was `<chain>:<short_hash>`). Map the
	// truncated form back to a unique full-length canonical id by prefix.
	shortHashIndex := map[string]string{}
	shortHashAmbiguous := map[string]bool{}
	var latest time.Time
	for _, tx := range localTxs {
		cleanID := buildUniqueImportID(acc, tx)
		if cleanID != "" {
			localIDs[cleanID] = true
		}
		hash := strings.ToLower(tx.TxHash)
		if cleanID != "" && strings.HasPrefix(hash, "0x") && len(hash) >= 16 {
			prefix := hash[:16] // "0x" + 14 hex
			if existing, ok := shortHashIndex[prefix]; ok && existing != cleanID {
				shortHashAmbiguous[prefix] = true
			} else {
				shortHashIndex[prefix] = cleanID
			}
		}
		if t := time.Unix(tx.Timestamp, 0); t.After(latest) {
			latest = t
		}
	}
	res.LatestLocal = latest

	// Fetch ALL lines (no unique_import_id filter) so we can partition the
	// journal into chb-owned (import_id set) and unowned (manual entries:
	// opening balances, accountant adjustments). The unowned set drives the
	// balance diagnostic and the optional delete step.
	data, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.bank.statement.line", "search_read",
		[]interface{}{[]interface{}{
			[]interface{}{"journal_id", "=", journalID},
		}},
		map[string]interface{}{
			"fields": []string{"id", "date", "amount", "payment_ref", "unique_import_id"},
			"order":  "date asc, id asc",
		})
	if err != nil {
		return nil, fmt.Errorf("fetch lines: %v", err)
	}
	var rows []struct {
		ID             int     `json:"id"`
		Date           odooStr `json:"date"`
		Amount         float64 `json:"amount"`
		PaymentRef     odooStr `json:"payment_ref"`
		UniqueImportID odooStr `json:"unique_import_id"`
	}
	if err := json.Unmarshal(data, &rows); err != nil {
		return nil, fmt.Errorf("parse lines: %v", err)
	}

	// Index every unique_import_id currently in this journal → its line
	// ID, so we can detect whether a repaired canonical form would
	// collide with an existing clean-form line (a duplicate pair).
	// Alongside, accumulate the chb-owned count/sum and peel off the
	// unowned (no import_id) lines for the fix-time balance diagnostic.
	odooLineByID := make(map[string]int, len(rows))
	var unownedSum float64
	for _, r := range rows {
		id := string(r.UniqueImportID)
		if id == "" {
			res.Unowned = append(res.Unowned, odooOrphanLine{
				ID:         r.ID,
				Date:       string(r.Date),
				Amount:     r.Amount,
				PaymentRef: string(r.PaymentRef),
			})
			unownedSum += r.Amount
			continue
		}
		odooLineByID[id] = r.ID
		res.OdooImportedCount++
		res.OdooImportedSum += r.Amount
	}
	res.OdooImportedSum = roundCents(res.OdooImportedSum)
	res.UnownedSum = roundCents(unownedSum)

	// covered tracks the local canonical import IDs that some Odoo line
	// already accounts for — by exact match, or via a broken-form line
	// classified below as a Repair or Duplicate. Local txs NOT in this set
	// have no Odoo line at all (the Missing set), so the journal is behind
	// local. Tracking it here avoids double-counting: a tx present in Odoo
	// only under a broken import_id would otherwise look both "repairable"
	// (Odoo side) and "missing" (local side).
	covered := make(map[string]bool, len(rows))

	for _, r := range rows {
		importID := string(r.UniqueImportID)
		if importID == "" || localIDs[importID] {
			if importID != "" {
				covered[importID] = true
			}
			continue
		}
		if isStripeAggregateFeeImportID(importID) {
			continue
		}
		line := odooOrphanLine{
			ID:             r.ID,
			Date:           string(r.Date),
			Amount:         r.Amount,
			PaymentRef:     string(r.PaymentRef),
			UniqueImportID: importID,
		}
		lineDate, _ := time.Parse("2006-01-02", line.Date)
		if !latest.IsZero() && lineDate.After(latest) {
			res.PostLatestLocal = append(res.PostLatestLocal, line)
			continue
		}
		// Try to find the canonical form. First the URI-form
		// canonicalize (TxHash-stripping bug); then the legacy
		// short-hash form (<chain>:<addr>:<chain>:<14-hex prefix>:<n>)
		// matched by hash prefix against local txs.
		canonical := CanonicalizeImportID(importID)
		if canonical == "" {
			if parts := strings.Split(importID, ":"); len(parts) == 5 && parts[0] == parts[2] && strings.HasPrefix(parts[3], "0x") {
				if prefix := parts[3]; !shortHashAmbiguous[prefix] {
					canonical = shortHashIndex[prefix]
				}
			}
		}
		if canonical == "" || !localIDs[canonical] {
			res.Orphans = append(res.Orphans, line)
			continue
		}
		covered[canonical] = true
		if cleanLineID, alreadyInOdoo := odooLineByID[canonical]; alreadyInOdoo {
			// A clean-form line for the same tx already exists in the
			// journal. Needs reconciliation-aware consolidation, not a
			// blind rewrite (which would hit a UNIQUE-constraint error).
			res.Duplicates = append(res.Duplicates, odooOrphanDuplicate{
				Broken:      line,
				CleanID:     canonical,
				CleanLineID: cleanLineID,
			})
			continue
		}
		res.Repairs = append(res.Repairs, odooOrphanRepair{Line: line, CanonicalID: canonical})
	}

	// Local → Odoo direction: txs with no covering Odoo line. This is the
	// "journal is behind local" discrepancy `sync` surfaces but the
	// structural checks above never see. Skipped for Stripe, where local
	// balance-txs are aggregated into statement lines rather than mapped
	// 1:1, so a naive diff would massively over-report.
	if !strings.EqualFold(acc.Provider, "stripe") {
		for _, tx := range localTxs {
			id := buildUniqueImportID(acc, tx)
			if id == "" || covered[id] {
				continue
			}
			res.Missing = append(res.Missing, tx)
		}
	}
	return res, nil
}

// isStripeAggregateFeeImportID matches both the canonical
// stripe:<acc>:open:<stmtID>:fees and the per-payout
// stripe:<acc>:<payoutID>:fees aggregate-fee import IDs created in
// stripe_odoo_sync.go. These lines summarize hundreds of Stripe balance
// transactions, so no individual local tx will ever map to them.
func isStripeAggregateFeeImportID(id string) bool {
	return strings.HasPrefix(id, "stripe:") && strings.HasSuffix(id, ":fees")
}

type orphanMonthSummary struct {
	Month string
	Count int
	Sum   float64
}

func summarizeOrphansByMonth(orphans []odooOrphanLine) []orphanMonthSummary {
	byMonth := map[string]*orphanMonthSummary{}
	for _, o := range orphans {
		key := o.Date
		if len(key) >= 7 {
			key = key[:7]
		}
		if byMonth[key] == nil {
			byMonth[key] = &orphanMonthSummary{Month: key}
		}
		byMonth[key].Count++
		byMonth[key].Sum += o.Amount
	}
	out := make([]orphanMonthSummary, 0, len(byMonth))
	for _, s := range byMonth {
		out = append(out, *s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Month < out[j].Month })
	return out
}

func printOrphanStatementLines(orphans []odooOrphanLine) {
	fmt.Printf("\n  %s%s in Odoo (no matching local tx):%s\n",
		Fmt.Bold, Pluralize(len(orphans), "orphan line", ""), Fmt.Reset)
	fmt.Printf("\n    %sby month:%s\n", Fmt.Dim, Fmt.Reset)
	for _, m := range summarizeOrphansByMonth(orphans) {
		fmt.Printf("      %s   %18s  %12s\n", m.Month, Pluralize(m.Count, "line", ""), fmtEURSigned(m.Sum))
	}
	// Diagnostic: show whether the importID has a recognized broken form.
	// If canonicalize returns non-empty for any orphan, the line should
	// have been classified as Repair instead — which means the canonical
	// form wasn't in the local set (regenerate local) or was already
	// present in Odoo (stale duplicate of a clean line).
	fmt.Printf("\n    %sdetail:%s\n", Fmt.Dim, Fmt.Reset)
	for _, o := range orphans {
		canonical := CanonicalizeImportID(o.UniqueImportID)
		canonicalNote := ""
		if canonical != "" {
			canonicalNote = fmt.Sprintf("  %scanon=%s (not matched locally)%s", Fmt.Dim, canonical, Fmt.Reset)
		}
		fmt.Printf("      %s#%d%s  %s  %12s  %s  %s%s%s%s\n",
			Fmt.Dim, o.ID, Fmt.Reset,
			o.Date, fmtEURSigned(o.Amount), o.PaymentRef,
			Fmt.Dim, o.UniqueImportID, Fmt.Reset,
			canonicalNote)
	}
	fmt.Println()
}

func printOrphanRepairs(repairs []odooOrphanRepair) {
	fmt.Printf("\n  %s%s repairable by rewriting unique_import_id:%s\n",
		Fmt.Bold, Pluralize(len(repairs), "line", ""), Fmt.Reset)
	for _, r := range repairs {
		fmt.Printf("    %s#%d%s  %s  %12s  %s\n      %sfrom:%s %s\n      %s  to:%s %s\n",
			Fmt.Dim, r.Line.ID, Fmt.Reset,
			r.Line.Date, fmtEURSigned(r.Line.Amount), r.Line.PaymentRef,
			Fmt.Dim, Fmt.Reset, r.Line.UniqueImportID,
			Fmt.Dim, Fmt.Reset, r.CanonicalID)
	}
	fmt.Println()
}

func printOrphanDuplicates(dupes []odooOrphanDuplicate) {
	fmt.Printf("\n  %s%s — two lines exist for the same tx, in different import_id formats:%s\n",
		Fmt.Bold, Pluralize(len(dupes), "duplicate pair", ""), Fmt.Reset)
	for _, d := range dupes {
		brokenAge, cleanAge := "newer", "older"
		if d.Broken.ID < d.CleanLineID {
			brokenAge, cleanAge = "older", "newer"
		}
		fmt.Printf("    %s%s  %12s  %s%s\n      %s%s #%d (broken format):%s %s\n      %s%s #%d (clean format):%s  %s\n",
			Fmt.Dim, d.Broken.Date, fmtEURSigned(d.Broken.Amount), d.Broken.PaymentRef, Fmt.Reset,
			Fmt.Dim, brokenAge, d.Broken.ID, Fmt.Reset, d.Broken.UniqueImportID,
			Fmt.Dim, cleanAge, d.CleanLineID, Fmt.Reset, d.CleanID)
	}
	fmt.Println()
}

// looseLineAssignment describes one orphaned-from-statement line that
// can be attached to a specific statement based on its date.
type looseLineAssignment struct {
	LineID        int
	Date          string
	Amount        float64
	StatementID   int
	StatementName string
}

// looseLinesPlan groups the result of planAttachLooseLines into lines we
// can attach to an existing statement and lines that should stay loose
// (their date is after every existing statement — typically the
// current/open period).
type looseLinesPlan struct {
	Assign       []looseLineAssignment
	StayLooseN   int
	StayLooseSum float64
}

// planAttachLooseLines surveys account.bank.statement.line for lines
// with no statement_id and decides where each belongs. A loose line
// joins the earliest statement whose date is >= the line's date — the
// "containing" monthly statement. Lines newer than every statement
// stay loose (they're awaiting the next statement to be created).
func planAttachLooseLines(creds *OdooCredentials, uid int, journalID int) (*looseLinesPlan, error) {
	stmtRes, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.bank.statement", "search_read",
		[]interface{}{[]interface{}{[]interface{}{"journal_id", "=", journalID}}},
		map[string]interface{}{
			"fields": []string{"id", "date", "name"},
			"order":  "date asc, id asc",
		})
	if err != nil {
		return nil, fmt.Errorf("fetch statements: %v", err)
	}
	var stmts []struct {
		ID   int     `json:"id"`
		Date odooStr `json:"date"`
		Name odooStr `json:"name"`
	}
	if err := json.Unmarshal(stmtRes, &stmts); err != nil {
		return nil, fmt.Errorf("parse statements: %v", err)
	}

	looseRes, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.bank.statement.line", "search_read",
		[]interface{}{[]interface{}{
			[]interface{}{"journal_id", "=", journalID},
			[]interface{}{"statement_id", "=", false},
		}},
		map[string]interface{}{
			"fields": []string{"id", "date", "amount"},
		})
	if err != nil {
		return nil, fmt.Errorf("fetch loose lines: %v", err)
	}
	var loose []struct {
		ID     int     `json:"id"`
		Date   odooStr `json:"date"`
		Amount float64 `json:"amount"`
	}
	if err := json.Unmarshal(looseRes, &loose); err != nil {
		return nil, fmt.Errorf("parse loose lines: %v", err)
	}

	plan := &looseLinesPlan{}
	for _, l := range loose {
		lineDate := string(l.Date)
		targetID := 0
		targetName := ""
		for _, s := range stmts {
			if string(s.Date) >= lineDate {
				targetID = s.ID
				targetName = string(s.Name)
				break
			}
		}
		if targetID == 0 {
			plan.StayLooseN++
			plan.StayLooseSum += l.Amount
			continue
		}
		plan.Assign = append(plan.Assign, looseLineAssignment{
			LineID:        l.ID,
			Date:          lineDate,
			Amount:        l.Amount,
			StatementID:   targetID,
			StatementName: targetName,
		})
	}
	return plan, nil
}

func printLooseLinesPlan(plan *looseLinesPlan) {
	fmt.Printf("\n  %s%s can be attached to a statement:%s\n",
		Fmt.Bold, Pluralize(len(plan.Assign), "loose line", ""), Fmt.Reset)

	type stmtBucket struct {
		Name  string
		Count int
		Sum   float64
	}
	bucketByID := map[int]*stmtBucket{}
	for _, a := range plan.Assign {
		b := bucketByID[a.StatementID]
		if b == nil {
			b = &stmtBucket{Name: a.StatementName}
			bucketByID[a.StatementID] = b
		}
		b.Count++
		b.Sum += a.Amount
	}
	stmtIDs := make([]int, 0, len(bucketByID))
	for id := range bucketByID {
		stmtIDs = append(stmtIDs, id)
	}
	sort.Ints(stmtIDs)
	fmt.Printf("\n    %sby target statement:%s\n", Fmt.Dim, Fmt.Reset)
	for _, id := range stmtIDs {
		b := bucketByID[id]
		fmt.Printf("      #%-4d %-24s %18s  %12s\n", id, truncate(b.Name, 24), Pluralize(b.Count, "line", ""), fmtEURSigned(b.Sum))
	}
	if plan.StayLooseN > 0 {
		fmt.Printf("\n    %s%s stay loose (newer than every statement, %s)%s\n",
			Fmt.Dim, Pluralize(plan.StayLooseN, "line", ""), fmtEURSigned(plan.StayLooseSum), Fmt.Reset)
	}
	fmt.Println()
}

// attachLooseLines applies a looseLinesPlan, batching writes per target
// statement (one Odoo RPC per statement instead of per line).
func attachLooseLines(creds *OdooCredentials, uid int, plan []looseLineAssignment) int {
	byStmt := map[int][]int{}
	stmtName := map[int]string{}
	for _, a := range plan {
		byStmt[a.StatementID] = append(byStmt[a.StatementID], a.LineID)
		stmtName[a.StatementID] = a.StatementName
	}
	stmtIDs := make([]int, 0, len(byStmt))
	for id := range byStmt {
		stmtIDs = append(stmtIDs, id)
	}
	sort.Ints(stmtIDs)

	assigned := 0
	for i, stmtID := range stmtIDs {
		ids := byStmt[stmtID]
		prefix := fmt.Sprintf("  %s[%d/%d]%s", Fmt.Dim, i+1, len(stmtIDs), Fmt.Reset)
		fmt.Printf("%s attach %s → statement #%d %s\n", prefix, Pluralize(len(ids), "line", ""), stmtID, stmtName[stmtID])
		_, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
			"account.bank.statement.line", "write",
			[]interface{}{intsToInterfaces(ids), map[string]interface{}{
				"statement_id": stmtID,
			}}, nil)
		if err != nil {
			Warnf("%s ✗ attach to #%d failed: %v", prefix, stmtID, err)
			continue
		}
		assigned += len(ids)
	}
	return assigned
}

// fetchStatementLinesReconciledState returns is_reconciled for each
// account.bank.statement.line ID in `ids`, keyed by id.
func fetchStatementLinesReconciledState(creds *OdooCredentials, uid int, ids []int) (map[int]bool, error) {
	out := map[int]bool{}
	if len(ids) == 0 {
		return out, nil
	}
	data, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.bank.statement.line", "read",
		[]interface{}{intsToInterfaces(ids), []string{"id", "is_reconciled"}}, nil)
	if err != nil {
		return nil, fmt.Errorf("read is_reconciled: %v", err)
	}
	var rows []struct {
		ID           int  `json:"id"`
		IsReconciled bool `json:"is_reconciled"`
	}
	if err := json.Unmarshal(data, &rows); err != nil {
		return nil, fmt.Errorf("parse is_reconciled: %v", err)
	}
	for _, r := range rows {
		out[r.ID] = r.IsReconciled
	}
	return out, nil
}

// consolidateOrphanDuplicates resolves each (legacy, canonical) pair by
// keeping the line that carries the accountant's reconciliation work.
// Rules:
//   - legacy reconciled, clean not → delete clean, rewrite legacy → clean
//   - clean reconciled, legacy not → delete legacy (clean already correct)
//   - neither reconciled            → delete legacy (clean already correct)
//   - both reconciled               → skip with warning (manual review)
//
// Returns (consolidated, conflicts) where conflicts are pairs where both
// sides are reconciled and need a human to choose.
func consolidateOrphanDuplicates(creds *OdooCredentials, uid int, dupes []odooOrphanDuplicate) (int, []odooOrphanDuplicate) {
	if len(dupes) == 0 {
		return 0, nil
	}
	ids := make([]int, 0, 2*len(dupes))
	for _, d := range dupes {
		ids = append(ids, d.Broken.ID, d.CleanLineID)
	}
	reconciled, err := fetchStatementLinesReconciledState(creds, uid, ids)
	if err != nil {
		Warnf("  %s⚠ Could not read reconciliation state: %v%s", Fmt.Yellow, err, Fmt.Reset)
		return 0, dupes
	}

	consolidated := 0
	var conflicts []odooOrphanDuplicate
	total := len(dupes)
	for i, d := range dupes {
		brokenRec := reconciled[d.Broken.ID]
		cleanRec := reconciled[d.CleanLineID]
		prefix := fmt.Sprintf("  %s[%d/%d]%s", Fmt.Dim, i+1, total, Fmt.Reset)

		// Age labels: Postgres serial IDs grow with creation time, so
		// the lower id is the older line.
		brokenAge, cleanAge := "newer", "older"
		if d.Broken.ID < d.CleanLineID {
			brokenAge, cleanAge = "older", "newer"
		}

		switch {
		case brokenRec && cleanRec:
			// Keep the older, delete the newer. Deleting a reconciled
			// line undoes its match, so the invoice/bill that the
			// duplicate was attached to will flag back to unpaid —
			// that's the desired signal for the accountant to
			// investigate the double-reconciliation.
			keepID, deleteID := d.CleanLineID, d.Broken.ID
			keepFmt, deleteFmt := "clean", "broken"
			if d.Broken.ID < d.CleanLineID {
				keepID, deleteID = d.Broken.ID, d.CleanLineID
				keepFmt, deleteFmt = "broken", "clean"
			}
			fmt.Printf("%s keep older #%d (%s format), delete newer #%d (%s format) — both reconciled, undoes newer's match\n",
				prefix, keepID, keepFmt, deleteID, deleteFmt)
			if err := deleteStatementLines(creds, uid, []int{deleteID}); err != nil {
				Warnf("%s ✗ delete #%d failed: %v", prefix, deleteID, err)
				continue
			}
			if keepFmt == "broken" {
				if err := updateStatementLineFields(creds, uid, keepID, map[string]interface{}{
					"unique_import_id": d.CleanID,
				}); err != nil {
					Warnf("%s ✗ rewrite #%d failed: %v", prefix, keepID, err)
					continue
				}
			}
		case brokenRec && !cleanRec:
			fmt.Printf("%s keep %s #%d (broken format, reconciled), delete %s #%d (clean format, unreconciled) — rewriting kept line → canonical\n",
				prefix, brokenAge, d.Broken.ID, cleanAge, d.CleanLineID)
			if err := deleteStatementLines(creds, uid, []int{d.CleanLineID}); err != nil {
				Warnf("%s ✗ delete #%d failed: %v", prefix, d.CleanLineID, err)
				continue
			}
			if err := updateStatementLineFields(creds, uid, d.Broken.ID, map[string]interface{}{
				"unique_import_id": d.CleanID,
			}); err != nil {
				Warnf("%s ✗ rewrite #%d failed: %v", prefix, d.Broken.ID, err)
				continue
			}
		default:
			reason := "neither reconciled"
			if cleanRec {
				reason = "clean line is reconciled"
			}
			fmt.Printf("%s keep %s #%d (clean format), delete %s #%d (broken format) — %s\n",
				prefix, cleanAge, d.CleanLineID, brokenAge, d.Broken.ID, reason)
			if err := deleteStatementLines(creds, uid, []int{d.Broken.ID}); err != nil {
				Warnf("%s ✗ delete #%d failed: %v", prefix, d.Broken.ID, err)
				continue
			}
		}
		consolidated++
	}
	return consolidated, conflicts
}

// repairOrphanImportIDs rewrites unique_import_id in place using Odoo's
// `write` method. This preserves the underlying account.move, partner_id,
// statement_id and reconciliation links — exactly the state that the
// delete-and-re-push path would otherwise unwind. Returns the count of
// successful rewrites and the list of repairs that failed (e.g. due to
// a unique-constraint race) so the caller can fall back to delete.
func repairOrphanImportIDs(creds *OdooCredentials, uid int, repairs []odooOrphanRepair) (int, []odooOrphanLine) {
	ok := 0
	var failed []odooOrphanLine
	total := len(repairs)
	for i, r := range repairs {
		prefix := fmt.Sprintf("  %s[%d/%d]%s", Fmt.Dim, i+1, total, Fmt.Reset)
		fmt.Printf("%s rewriting #%d → %s\n", prefix, r.Line.ID, r.CanonicalID)
		err := updateStatementLineFields(creds, uid, r.Line.ID, map[string]interface{}{
			"unique_import_id": r.CanonicalID,
		})
		if err != nil {
			Warnf("%s ✗ rewrite #%d failed: %v", prefix, r.Line.ID, err)
			failed = append(failed, r.Line)
			continue
		}
		ok++
	}
	return ok, failed
}

func printPostLatestOrphanLines(post []odooOrphanLine, acc *AccountConfig, latestLocal time.Time) {
	latestStr := "no local txs"
	if !latestLocal.IsZero() {
		latestStr = latestLocal.Format("2006-01-02")
	}
	slug := ""
	if acc != nil {
		slug = acc.Slug
	}
	fmt.Printf("\n  %s⚠ %s dated after last local tx (%s) — likely a missed local sync, not orphans:%s\n",
		Fmt.Yellow, Pluralize(len(post), "Odoo line", ""), latestStr, Fmt.Reset)
	if slug != "" {
		fmt.Printf("    %sRun: chb accounts %s sync%s\n", Fmt.Dim, slug, Fmt.Reset)
	}
	for _, o := range post {
		fmt.Printf("    %s#%d%s  %s  %12s  %s\n",
			Fmt.Dim, o.ID, Fmt.Reset,
			o.Date, fmtEURSigned(o.Amount), o.PaymentRef)
	}
	fmt.Println()
}

// journalBalanceDiagnostic decomposes a local↔journal total gap. The
// manual fields capture the part of the journal that carries no
// unique_import_id (opening balances, accountant adjustments) — lines chb
// does not own and never touches, which is the usual reason `sync` reports
// a balance difference that `fix` cannot itself repair.
type journalBalanceDiagnostic struct {
	localCount     int
	localBalance   float64
	journalCount   int
	journalBalance float64
	manualCount    int     // journal lines with no unique_import_id
	manualSum      float64 // their contribution to the journal balance
	balanceDelta   float64 // journalBalance - localBalance
	hasGap         bool
}

// computeJournalBalanceDiagnostic derives the local↔journal gap from the
// partition already gathered during orphan detection (chb-owned vs manual
// lines) plus the local snapshot — no extra RPC. Returns ok=false when
// there's no linked account.
func computeJournalBalanceDiagnostic(res *odooOrphanFindResult) (journalBalanceDiagnostic, bool) {
	if res == nil || res.Account == nil {
		return journalBalanceDiagnostic{}, false
	}
	local := accountLocalOdooSyncSnapshot(res.Account)
	journalCount := res.OdooImportedCount + len(res.Unowned)
	journalBalance := roundCents(res.OdooImportedSum + res.UnownedSum)
	d := journalBalanceDiagnostic{
		localCount:     local.TxCount,
		localBalance:   local.Balance,
		journalCount:   journalCount,
		journalBalance: journalBalance,
		manualCount:    len(res.Unowned),
		manualSum:      res.UnownedSum,
		balanceDelta:   roundCents(journalBalance - local.Balance),
	}
	d.hasGap = math.Abs(d.balanceDelta) > 0.005
	return d, true
}

func printJournalBalanceDiagnostic(d journalBalanceDiagnostic, res *odooOrphanFindResult) {
	cur := ""
	if res != nil && res.Account != nil {
		cur = accCurrency(res.Account)
	}
	fmt.Printf("\n  %sBalance diagnostic%s\n", Fmt.Bold, Fmt.Reset)
	fmt.Printf("    %sLocal:%s    %s, %s\n",
		Fmt.Dim, Fmt.Reset, Pluralize(d.localCount, "tx", ""), formatAccountDataBalance(d.localBalance, cur))
	fmt.Printf("    %sJournal:%s  %s, %s  %s(gap %s)%s\n",
		Fmt.Dim, Fmt.Reset, Pluralize(d.journalCount, "line", ""), formatAccountDataBalance(d.journalBalance, cur),
		Fmt.Dim, fmtEURSigned(d.balanceDelta), Fmt.Reset)
	if d.manualCount > 0 {
		fmt.Printf("    %s↳ %s with no unique_import_id (manual entries, %s) — not chb-owned; fix leaves them alone.%s\n",
			Fmt.Dim, Pluralize(d.manualCount, "journal line", ""), fmtEURSigned(d.manualSum), Fmt.Reset)
	}
	fmt.Println()
}

// printUnownedLines lists the journal's manual entries (no
// unique_import_id) — the lines a delete step would remove to close the
// balance gap. Listed in full, per line, so the user reviews exactly what
// they're confirming before any destructive write.
func printUnownedLines(unowned []odooOrphanLine, acc *AccountConfig, d journalBalanceDiagnostic) {
	cur := ""
	if acc != nil {
		cur = accCurrency(acc)
	}
	fmt.Printf("\n  %s%s not owned by chb (no unique_import_id), total %s:%s\n",
		Fmt.Bold, Pluralize(len(unowned), "manual line", ""), fmtEURSigned(d.manualSum), Fmt.Reset)
	fmt.Printf("    %sThese are the likely cause of the %s gap vs local. Review before deleting.%s\n",
		Fmt.Dim, formatAccountDataBalance(d.balanceDelta, cur), Fmt.Reset)
	for _, o := range unowned {
		ref := o.PaymentRef
		if strings.TrimSpace(ref) == "" {
			ref = Fmt.Dim + "(no label)" + Fmt.Reset
		}
		fmt.Printf("    %s#%d%s  %s  %12s  %s\n",
			Fmt.Dim, o.ID, Fmt.Reset, o.Date, fmtEURSigned(o.Amount), ref)
	}
	fmt.Println()
}

// printMissingLines reports local txs that have no Odoo line at all — the
// journal is behind local. Grouped by month like orphans, with a per-line
// detail block. The signed amount mirrors the local snapshot's balance
// convention (signedOdooAmountForTransaction) so the totals here reconcile
// with the count/balance gap `sync` shows.
func printMissingLines(missing []TransactionEntry, acc *AccountConfig) {
	fmt.Printf("\n  %s%s in local, not in Odoo (journal is behind):%s\n",
		Fmt.Bold, Pluralize(len(missing), "tx", ""), Fmt.Reset)

	type monthAgg struct {
		count int
		sum   float64
	}
	byMonth := map[string]*monthAgg{}
	var total float64
	for _, tx := range missing {
		amt := signedOdooAmountForTransaction(acc, tx)
		total += amt
		month := "unknown"
		if tx.Timestamp > 0 {
			month = time.Unix(tx.Timestamp, 0).In(BrusselsTZ()).Format("2006-01")
		}
		if byMonth[month] == nil {
			byMonth[month] = &monthAgg{}
		}
		byMonth[month].count++
		byMonth[month].sum += amt
	}
	months := make([]string, 0, len(byMonth))
	for m := range byMonth {
		months = append(months, m)
	}
	sort.Strings(months)

	fmt.Printf("\n    %sby month:%s\n", Fmt.Dim, Fmt.Reset)
	for _, m := range months {
		fmt.Printf("      %s   %18s  %12s\n", m, Pluralize(byMonth[m].count, "tx", ""), fmtEURSigned(byMonth[m].sum))
	}
	fmt.Printf("      %stotal: %s, %s%s\n", Fmt.Dim, Pluralize(len(missing), "tx", ""), fmtEURSigned(total), Fmt.Reset)
	fmt.Println()
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

// mergeDuplicateOpenFeeLines collapses the duplicate "Stripe fees for open
// statement" lines on a single statement into one. The earliest line (by
// date asc, id asc — which is the order the issue's slice already uses) is
// retained; its amount is rewritten to the sum of all duplicates and its
// unique_import_id is normalized to the current canonical form
// (stripe:<accountID>:open:<stmtID>:fees) so future sync runs match it.
// The other lines are deleted.
//
// Returns the number of edits applied (or that would be applied in dry-run).
func mergeDuplicateOpenFeeLines(creds *OdooCredentials, uid int, issue StatementBalanceIssue, dryRun bool) (int, error) {
	if len(issue.DuplicateLines) < 2 {
		return 0, nil
	}
	keeper := issue.DuplicateLines[0]
	var sum float64
	deleteIDs := make([]int, 0, len(issue.DuplicateLines)-1)
	for i, d := range issue.DuplicateLines {
		sum += d.Amount
		if i > 0 {
			deleteIDs = append(deleteIDs, d.ID)
		}
	}
	canonicalImport := keeper.ImportID
	if accountID := parseStripeAccountIDFromOpenFeeImportID(keeper.ImportID); accountID != "" {
		canonicalImport = openStatementFeeImportID(accountID, issue.StatementID)
	}

	if dryRun {
		fmt.Printf("  %s(dry-run)%s #%d %s  collapse %s → 1 line of %s (keep #%d, delete %v)\n",
			Fmt.Dim, Fmt.Reset, issue.StatementID, issue.StatementName,
			Pluralize(len(issue.DuplicateLines), "fee line", ""), fmtEURSigned(sum), keeper.ID, deleteIDs)
		return len(issue.DuplicateLines), nil
	}

	// Delete the duplicates first so the keeper's importID rename doesn't
	// hit a uniqueness constraint with one of them.
	if len(deleteIDs) > 0 {
		if err := deleteStatementLines(creds, uid, deleteIDs); err != nil {
			return 0, fmt.Errorf("delete duplicates: %v", err)
		}
	}
	if err := updateStatementLineFields(creds, uid, keeper.ID, map[string]interface{}{
		"amount":           sum,
		"unique_import_id": canonicalImport,
	}); err != nil {
		return len(deleteIDs), fmt.Errorf("update keeper #%d: %v", keeper.ID, err)
	}
	fmt.Printf("  %s✓%s #%d %s  collapsed %s into #%d (%s, importID=%s)\n",
		Fmt.Green, Fmt.Reset, issue.StatementID, issue.StatementName,
		Pluralize(len(issue.DuplicateLines), "fee line", ""), keeper.ID, fmtEURSigned(sum), canonicalImport)
	return len(issue.DuplicateLines), nil
}

// parseStripeAccountIDFromOpenFeeImportID extracts the Stripe account id
// segment from either the canonical importID
// (stripe:<accountID>:open:<stmtID>:fees) or the legacy buggy form
// (stripe:<accountID>:open:<bt1>:<bt2>:fees). Returns "" if the importID
// isn't recognizable.
func parseStripeAccountIDFromOpenFeeImportID(importID string) string {
	parts := strings.Split(importID, ":")
	if len(parts) < 4 || parts[0] != "stripe" || parts[2] != "open" {
		return ""
	}
	return parts[1]
}

// deleteStatementLines removes the given statement lines. Each statement
// line auto-creates a journal entry (account.move) when its statement is
// validated; that entry is what actually persists. Direct unlink of the
// statement line therefore fails if the move is posted or reconciled. The
// supported recipe (same one used by emptyOdooJournal) is:
//
//  1. Unreconcile any reconciled move lines.
//  2. Reset the moves to draft.
//  3. Unlink the moves — this cascades to the statement lines.
//
// Lines whose move_id is unset (rare — only if Odoo hasn't yet generated
// the move) are unlinked directly as a fallback.
func deleteStatementLines(creds *OdooCredentials, uid int, ids []int) error {
	if len(ids) == 0 {
		return nil
	}
	idsAny := intsToInterfaces(ids)

	linesData, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.bank.statement.line", "read",
		[]interface{}{idsAny, []string{"move_id"}}, nil)
	if err != nil {
		return fmt.Errorf("read lines: %v", err)
	}
	var rows []struct {
		ID     int         `json:"id"`
		MoveID interface{} `json:"move_id"`
	}
	if err := json.Unmarshal(linesData, &rows); err != nil {
		return fmt.Errorf("parse lines: %v", err)
	}
	var moveIDs []int
	var lineIDsWithoutMove []int
	for _, r := range rows {
		if mid := odooFieldID(r.MoveID); mid > 0 {
			moveIDs = append(moveIDs, mid)
		} else {
			lineIDsWithoutMove = append(lineIDsWithoutMove, r.ID)
		}
	}

	if len(moveIDs) > 0 {
		moveIDs = uniquePositiveInts(moveIDs)
		movesIface := intsToInterfaces(moveIDs)

		reconRaw, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
			"account.move.line", "search",
			[]interface{}{[]interface{}{
				[]interface{}{"move_id", "in", movesIface},
				[]interface{}{"reconciled", "=", true},
			}}, nil)
		if err == nil {
			var reconIDs []int
			json.Unmarshal(reconRaw, &reconIDs)
			if len(reconIDs) > 0 {
				if _, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
					"account.move.line", "remove_move_reconcile",
					[]interface{}{intsToInterfaces(reconIDs)}, nil); err != nil {
					return fmt.Errorf("unreconcile move lines: %v", err)
				}
			}
		}

		toDraft, alreadyDraft, err := partitionOdooMovesForDeletion(creds, uid, moveIDs)
		if err != nil {
			return fmt.Errorf("read move states: %v", err)
		}
		deleteMoveIDs := alreadyDraft
		if len(toDraft) > 0 {
			if _, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
				"account.move", "button_draft",
				[]interface{}{intsToInterfaces(toDraft)}, nil); err != nil {
				return fmt.Errorf("reset moves to draft: %v", err)
			}
			deleteMoveIDs = append(deleteMoveIDs, toDraft...)
		}

		if _, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
			"account.move", "unlink",
			[]interface{}{intsToInterfaces(uniquePositiveInts(deleteMoveIDs))}, nil); err != nil {
			return fmt.Errorf("delete moves: %v", err)
		}
	}

	if len(lineIDsWithoutMove) > 0 {
		if _, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
			"account.bank.statement.line", "unlink",
			[]interface{}{intsToInterfaces(lineIDsWithoutMove)}, nil); err != nil {
			return fmt.Errorf("unlink lines without moves: %v", err)
		}
	}
	return nil
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
//   - chb odoo partners sync    (fetch partner snapshot from Odoo)
//   - chb odoo invoices sync    (fetch outgoing invoices from Odoo)
//   - chb odoo bills sync       (fetch vendor bills from Odoo)
//   - chb odoo journals sync    (push local transactions into every linked journal)
//
// Per-step failures are reported but do not abort the overall run.
func OdooSyncAll(args []string) error {
	if HasFlag(args, "--help", "-h", "help") {
		printOdooSyncHelp()
		return nil
	}
	if creds, err := ResolveOdooCredentials(); err == nil {
		fmt.Printf("\n%s🔄 Odoo sync%s  %s%s (db: %s)%s\n\n",
			Fmt.Bold, Fmt.Reset, Fmt.Dim, creds.URL, creds.DB, Fmt.Reset)
	}
	BeginDeferredWarnings()
	defer func() {
		warnings := EndDeferredWarnings()
		if len(warnings) == 0 {
			return
		}
		fmt.Printf("\n  %sWarnings%s\n", Fmt.Bold, Fmt.Reset)
		for _, warning := range warnings {
			fmt.Printf("%s\n", warning)
		}
	}()
	setQuietOdooContext(true)
	defer setQuietOdooContext(false)

	step := func(label string, fn func() error) {
		if err := fn(); err != nil {
			odooSyncLine(label, fmt.Sprintf("%s✗ %v%s", Fmt.Red, err, Fmt.Reset))
		}
	}
	// `chb odoo sync` is strictly Odoo→local fetch. The push side
	// (local → Odoo journals) lives under `chb odoo journals sync` /
	// `chb odoo journals <id> sync`. Keeping the two phases on
	// different commands is the same contract as `chb sync` (fetch
	// only) vs `chb generate` (local transform) vs `chb odoo journals
	// sync` (push). It also lets the operator inspect the freshly
	// fetched data (and re-run `chb generate`) before deciding to push.
	step("categories", func() error { _, err := OdooAnalyticSync(args); return err })
	step("analytic plans", func() error { _, err := OdooAnalyticPlansSync(args); return err })
	step("partners", func() error { _, err := OdooPartnersSync(args); return err })
	step("invoices", func() error { _, err := InvoicesSync(args); return err })
	step("bills", func() error { _, err := BillsSync(args); return err })
	step("journal lines", func() error { return refreshAllOdooJournalLineCaches() })
	printPendingMergesSummary()
	fmt.Printf("\n  %sTo push local changes to Odoo: chb odoo journals push%s", Fmt.Dim, Fmt.Reset)
	if len(pendingPartnerMerges()) > 0 {
		fmt.Printf("%s + chb odoo contacts apply%s", Fmt.Dim, Fmt.Reset)
	}
	fmt.Printf("\n\n")
	return nil
}

// OdooProviderSync is the provider-registry sync for Odoo. It only fetches
// local provider archives; journal pushes remain under `chb odoo sync` and
// `chb odoo journals ... sync`.
func OdooProviderSync(args []string) error {
	if HasFlag(args, "--help", "-h", "help") {
		printOdooSyncHelp()
		return nil
	}
	if creds, err := ResolveOdooCredentials(); err == nil {
		fmt.Printf("\n%s🔄 Odoo provider sync%s  %s%s (db: %s)%s\n\n",
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
	step("analytic plans", func() error { _, err := OdooAnalyticPlansSync(args); return err })
	step("partners", func() error { _, err := OdooPartnersSync(args); return err })
	step("invoices", func() error { _, err := InvoicesSync(args); return err })
	step("bills", func() error { _, err := BillsSync(args); return err })
	step("journal lines", func() error { return refreshAllOdooJournalLineCaches() })
	fmt.Println()
	return nil
}

// refreshAllOdooJournalLineCaches walks every linked-journal account and
// refreshes its providers/odoo/journals/<id>.json cache. Moves the previously
// lazy-fetch-on-first-push read into pull, so `chb odoo journals push` can
// rely on local cache and never has to fetch journal lines itself. The
// per-write target-state read (fetchOdooImportIDs to dedupe) is small and
// stays at push time — it has to reflect concurrent edits to be safe.
func refreshAllOdooJournalLineCaches() error {
	creds, err := ResolveOdooCredentials()
	if err != nil {
		return err
	}
	uid, err := odooAuth(creds.URL, creds.DB, creds.Login, creds.Password)
	if err != nil || uid == 0 {
		return fmt.Errorf("authenticate: %v", err)
	}
	seen := map[int]bool{}
	var failures []string
	for _, acc := range LoadAccountConfigs() {
		if acc.OdooJournalID == 0 || seen[acc.OdooJournalID] {
			continue
		}
		seen[acc.OdooJournalID] = true
		if _, err := writeOdooJournalLinesCache(creds, uid, acc.OdooJournalID); err != nil {
			failures = append(failures, fmt.Sprintf("#%d %s: %v", acc.OdooJournalID, acc.Slug, err))
		}
	}
	if len(failures) > 0 {
		return fmt.Errorf("%d of %d cache refresh(es) failed: %s", len(failures), len(seen), strings.Join(failures, "; "))
	}
	odooSyncLine("journal lines", fmt.Sprintf("%d journals cached", len(seen)))
	return nil
}

// odooJournalSync resolves a journal ID to its linked account and runs the account sync.
func odooJournalSync(journalID int, args []string) error {
	if err := RequireOdooWriteCapability(); err != nil {
		return err
	}
	for _, acc := range LoadAccountConfigs() {
		if acc.OdooJournalID == journalID {
			return AccountOdooPush(acc.Slug, args)
		}
	}
	return fmt.Errorf("no account linked to Odoo journal #%d. Run: chb accounts <slug> link", journalID)
}

// odooJournalPushWithReset pushes local → Odoo for one journal, honouring
// `--reset` (wipe the journal, then re-push full history). Extracted so both
// `journals <id> push` and the push step of `journals <id> sync` share the
// exact same reset semantics.
func odooJournalPushWithReset(creds *OdooCredentials, uid int, journalID int, syncArgs []string) error {
	if HasFlag(syncArgs, "--reset") {
		if odooSyncStageFlagsExplicit(syncArgs) && !HasFlag(syncArgs, "--transactions") {
			return fmt.Errorf("--reset can only be used with --transactions, or with no stage flags")
		}
		if HasFlag(syncArgs, "--dry-run") {
			// In dry-run, --reset means "simulate an empty journal" without
			// prompting or deleting anything. AccountOdooPush already treats
			// --force as reset/rebuild mode.
			syncArgs = append(filterFlag(syncArgs, "--reset"), "--force")
		} else {
			printOdooTargetLine(creds)
			if err := odooJournalReset(creds, uid, journalID, HasFlag(syncArgs, "--yes", "-y") || HasFlag(syncArgs, "--force")); err != nil {
				return err
			}
			syncArgs = filterFlag(syncArgs, "--reset")
			if !HasFlag(syncArgs, "--history") {
				syncArgs = append(syncArgs, "--history")
			}
			wasPrinted := odooTargetAlreadyPrinted()
			setOdooTargetAlreadyPrinted(true)
			defer setOdooTargetAlreadyPrinted(wasPrinted)
		}
	}
	return odooJournalSync(journalID, syncArgs)
}

// odooJournalFullSync makes all three balances for a journal agree in one
// command:
//
//  1. pull the linked account's source → local (refreshes the live on-chain /
//     provider balance and re-generates the local transaction view),
//  2. push local → Odoo and reconcile (so the Odoo journal carries every local
//     transaction), then
//  3. refresh the local Odoo journal-lines cache (so the journal balance the
//     account view prints reflects the just-pushed state).
//
// After it, `chb accounts <slug>` shows live == local == journal. Pass
// `--reset` to wipe and rebuild the journal first (needed once to correct
// lines pushed before a signing fix).
func odooJournalFullSync(creds *OdooCredentials, uid int, journalID int, syncArgs []string) error {
	if err := RequireOdooWriteCapability(); err != nil {
		return err
	}
	acc := linkedAccountForJournal(journalID)
	if acc == nil {
		return fmt.Errorf("no account linked to Odoo journal #%d. Run: chb accounts <slug> link", journalID)
	}

	printOdooTargetLine(creds)
	wasPrinted := odooTargetAlreadyPrinted()
	setOdooTargetAlreadyPrinted(true)
	defer setOdooTargetAlreadyPrinted(wasPrinted)

	// 1. Pull source → local (live balance + regenerated local view). Strip
	// push-only flags the fetch doesn't understand; keep --verbose/--debug.
	fmt.Printf("\n  %s① Pulling %s source → local…%s\n", Fmt.Dim, acc.Slug, Fmt.Reset)
	fetchArgs := []string{}
	if HasFlag(syncArgs, "--verbose", "-v") {
		fetchArgs = append(fetchArgs, "--verbose")
	}
	if HasFlag(syncArgs, "--history") || HasFlag(syncArgs, "--reset") {
		fetchArgs = append(fetchArgs, "--history")
	}
	if err := AccountFetch(acc.Slug, fetchArgs); err != nil {
		return fmt.Errorf("pull %s: %v", acc.Slug, err)
	}

	// 2. Push local → Odoo (+ reconcile), honouring --reset.
	fmt.Printf("\n  %s② Pushing local → Odoo journal #%d…%s\n", Fmt.Dim, journalID, Fmt.Reset)
	if err := odooJournalPushWithReset(creds, uid, journalID, syncArgs); err != nil {
		return err
	}

	// 3. Refresh the local journal cache so the journal balance is post-push.
	if !HasFlag(syncArgs, "--dry-run") {
		fmt.Printf("\n  %s③ Refreshing journal #%d cache…%s\n", Fmt.Dim, journalID, Fmt.Reset)
		if _, err := writeOdooJournalLinesCache(creds, uid, journalID); err != nil {
			return fmt.Errorf("refresh journal #%d cache: %v", journalID, err)
		}
	}
	return nil
}

// odooJournalsSyncAll pushes every linked account's local transactions into
// its Odoo journal. In aggregate (quiet) mode the accounts are processed
// serially so the per-account one-liners stay in order and can't interleave.
// In verbose mode, up to 4 journals run concurrently.
func odooJournalsSyncAll(args []string) error {
	if err := RequireOdooWriteCapability(); err != nil {
		return err
	}
	verbose := HasFlag(args, "--verbose", "-v") || HasFlag(args, "--debug")
	wasQuiet := quietOdooContext()
	// Note: the Odoo DB is already shown by the surrounding "Pushing
	// changes — Odoo: <db>" banner (PushAllTargets), so we no longer
	// reprint it here.
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

	// Pre-compute prefix widths so each row's "#<id>  <slug>" column lines
	// up. Each per-journal sync prints its prefix immediately, runs (with
	// progress shown on the line below), and on completion rewrites its
	// prefix line with the full row (txs, balance, status).
	wJID, wSlug := 0, 0
	for _, t := range targets {
		if n := len(fmt.Sprintf("#%d", t.journalID)); n > wJID {
			wJID = n
		}
		if n := len(t.slug); n > wSlug {
			wSlug = n
		}
	}
	info, _ := os.Stdout.Stat()
	isTTY := info != nil && (info.Mode()&os.ModeCharDevice) != 0
	journalRowLayoutActive = &journalRowLayout{JIDWidth: wJID, SlugWidth: wSlug, IsTTY: isTTY}
	defer func() { journalRowLayoutActive = nil }()
	pushAttentionHints = nil
	defer func() { pushAttentionHints = nil }()

	failed := 0
	if !verbose {
		// Compact mode: serial loop with per-journal status line +
		// silenced stdout. Sub-step chatter goes to /dev/null; the
		// final row is captured via journalRowSink and rendered as a
		// single status-line Final.
		for _, t := range targets {
			label := fmt.Sprintf("#%-*d %s", wJID-1, t.journalID, t.slug)
			diag := BeginStepDiagnostics(label)
			sl := NewStatusLine(label)
			SetActiveStatusLine(sl)
			var row journalSyncRow
			journalRowSink = &row
			restore := silenceStdout()
			err := runWithTimeout(fmt.Sprintf("journal #%d (%s)", t.journalID, t.slug), syncStepTimeout,
				func() error { return AccountOdooPush(t.slug, args) })
			restore()
			journalRowSink = nil
			SetActiveStatusLine(nil)
			if err != nil {
				diag.Errors = append(diag.Errors, err.Error())
			}
			if row.HasError && row.Status != "" {
				diag.Errors = append(diag.Errors, row.Status)
			}
			if row.Mismatch != "" {
				// Balance-mismatch warning was previously printed inline
				// via Warnf, breaking the per-row layout. Route it to the
				// footer instead — the row already shows ⚠ via diag.
				diag.Warnings = append(diag.Warnings, strings.TrimRight(row.Mismatch, "\n"))
			}
			EndStepDiagnostics()
			if err != nil || row.HasError {
				failed++
			}
			summary := formatCompactJournalRow(row, err)
			sl.Final(StepMark(err, diag), summary)
		}
	} else if wasQuiet {
		// Serial: preserve one-line-per-item ordering, avoid interleaving.
		for _, t := range targets {
			printJournalRowPrefix(t.journalID, t.slug)
			if err := runWithTimeout(fmt.Sprintf("journal #%d (%s)", t.journalID, t.slug), syncStepTimeout,
				func() error { return AccountOdooPush(t.slug, args) }); err != nil {
				fmt.Printf("  %s✗ #%d %s: %v%s\n", Fmt.Red, t.journalID, t.slug, err, Fmt.Reset)
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
				err := runWithTimeout(fmt.Sprintf("journal #%d (%s)", t.journalID, t.slug), syncStepTimeout,
					func() error { return AccountOdooPush(t.slug, args) })
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

	// Closing summary — attention items only, in compact mode. Things
	// the operator needs to act on (reconcile threshold exceeded,
	// out-of-sync balances, …) are collected during the loop and
	// listed here with the exact command to run.
	if !verbose && len(pushAttentionHints) > 0 {
		fmt.Printf("\n  %s⚠ %d journal%s need%s attention:%s\n",
			Fmt.Yellow, len(pushAttentionHints),
			plural(len(pushAttentionHints)),
			attentionVerb(len(pushAttentionHints)),
			Fmt.Reset)
		for _, h := range pushAttentionHints {
			fmt.Printf("    %s#%d %s — %s%s\n", Fmt.Dim, h.JournalID, h.Slug, h.Message, Fmt.Reset)
			if h.Suggested != "" {
				fmt.Printf("      %s$ %s%s\n", Fmt.Cyan, h.Suggested, Fmt.Reset)
			}
		}
	}

	if failed > 0 {
		return fmt.Errorf("%s failed", Pluralize(failed, "journal", ""))
	}
	return nil
}

// formatCompactJournalRow turns a captured journalSyncRow into a
// single status-line summary string. Falls back to the error message
// when the push errored out.
func formatCompactJournalRow(row journalSyncRow, err error) string {
	if err != nil {
		return Fmt.Red + truncErr(err) + Fmt.Reset
	}
	var parts []string
	if row.Status != "" {
		parts = append(parts, strings.TrimSpace(row.Status))
	}
	if row.TxCount > 0 {
		parts = append(parts, fmt.Sprintf("%s txs", formatThousands(row.TxCount)))
	}
	if row.Balance != "" {
		parts = append(parts, "balance "+row.Balance)
	}
	if len(parts) == 0 {
		return "ok"
	}
	return strings.Join(parts, " · ")
}

// attentionVerb is a tiny grammar helper: "needs" / "need" depending
// on count, used in the closing attention list header.
func attentionVerb(n int) string {
	if n == 1 {
		return "s"
	}
	return ""
}

func odooJournalReset(creds *OdooCredentials, uid int, journalID int, yes bool) error {
	if err := RequireOdooWriteCapability(); err != nil {
		return err
	}
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

	CacheOdooJournalName(journalID, journals[0].Name)
	if err := emptyOdooJournal(creds, uid, journalID, yes); err != nil {
		return err
	}
	// Refresh the local journal-lines cache so the next push's freshness
	// check sees the now-empty Odoo state instead of pre-reset cached lines
	// (which would otherwise abort with "out of sync"). Soft-fail with a
	// warning — the reset itself succeeded.
	if _, err := writeOdooJournalLinesCache(creds, uid, journalID); err != nil {
		Warnf("  %s⚠ Could not refresh local cache for journal #%d after reset: %v%s", Fmt.Yellow, journalID, err, Fmt.Reset)
	}
	return nil
}

func printOdooTargetLine(creds *OdooCredentials) {
	if quietOdooContext() || creds == nil {
		return
	}
	fmt.Printf("\n%sOdoo: %s (db: %s)%s\n", Fmt.Dim, creds.URL, creds.DB, Fmt.Reset)
}

// PrintOdooHelp shows the top-level odoo command help.
// PrintOdooHint is the one-line pointer shown for a bare `chb odoo` with no
// subcommand; the full command list stays behind `chb odoo --help`.
func PrintOdooHint() {
	fmt.Printf("  %sRun 'chb odoo --help' for commands.%s\n", Fmt.Dim, Fmt.Reset)
}

func PrintOdooHelp() {
	f := Fmt
	fmt.Printf("\n%schb odoo%s — Odoo integration (Odoo is a target: pull + push + pending)\n\n", f.Bold, f.Reset)
	fmt.Printf("%sCOMMANDS%s\n\n", f.Bold, f.Reset)
	fmt.Printf("  %s%schb odoo pull%s\n", f.Bold, f.Cyan, f.Reset)
	fmt.Printf("    %sFetch Odoo data into local provider archives (categories, partners, invoices, bills, journal lines)%s\n\n", f.Dim, f.Reset)
	fmt.Printf("  %s%schb odoo push%s\n", f.Bold, f.Cyan, f.Reset)
	fmt.Printf("    %sPush local transactions into every linked Odoo journal (mirror of pull; = chb odoo journals push)%s\n\n", f.Dim, f.Reset)
	fmt.Printf("  %s%schb odoo mapping%s\n", f.Bold, f.Cyan, f.Reset)
	fmt.Printf("    %sList / add / edit the category → Odoo account+partner mapping (odoo_mapping.json)%s\n\n", f.Dim, f.Reset)
	fmt.Printf("  %s%schb odoo journals%s\n", f.Bold, f.Cyan, f.Reset)
	fmt.Printf("    %sList Odoo journals linked to accounts%s\n\n", f.Dim, f.Reset)
	fmt.Printf("  %s%schb odoo journals <id>%s\n", f.Bold, f.Cyan, f.Reset)
	fmt.Printf("    %sShow journal details + count of local txs not yet pushed (--verbose lists them all)%s\n\n", f.Dim, f.Reset)
	fmt.Printf("  %s%schb odoo journals <id> push%s\n", f.Bold, f.Cyan, f.Reset)
	fmt.Printf("    %sPush local transactions into one journal. Reconcile runs automatically on small batches (≤%d new lines).%s\n", f.Dim, reconcileAutoThreshold, f.Reset)
	fmt.Printf("    %sUse --dry-run to preview, --history for a full duplicate check%s\n\n", f.Dim, f.Reset)
	fmt.Printf("  %s%schb odoo journals <id> pull%s\n", f.Bold, f.Cyan, f.Reset)
	fmt.Printf("    %sRefresh the local journal-lines cache for one journal (cheaper than full chb odoo pull)%s\n\n", f.Dim, f.Reset)
	fmt.Printf("  %s%schb odoo journals <id> categorize%s\n", f.Bold, f.Cyan, f.Reset)
	fmt.Printf("    %sRe-apply the OdooMapping account_code + analytic distribution onto existing lines%s\n\n", f.Dim, f.Reset)
	fmt.Printf("  %s%schb odoo journals <id> reconcile%s\n", f.Bold, f.Cyan, f.Reset)
	fmt.Printf("    %sReconcile this journal's lines against open A/R / A/P move lines%s\n\n", f.Dim, f.Reset)
	fmt.Printf("  %s%schb odoo journals <id> check%s\n", f.Bold, f.Cyan, f.Reset)
	fmt.Printf("    %sReport statements whose running balance is invalid%s\n\n", f.Dim, f.Reset)
	fmt.Printf("  %s%schb odoo journals <id> fix%s\n", f.Bold, f.Cyan, f.Reset)
	fmt.Printf("    %sSet balance_end_real = running balance on invalid statements%s\n\n", f.Dim, f.Reset)
	fmt.Printf("  %s%schb odoo journals <id> --reset%s\n", f.Bold, f.Cyan, f.Reset)
	fmt.Printf("    %sEmpty a journal (delete all statements and lines)%s\n\n", f.Dim, f.Reset)
	fmt.Printf("  %s%schb odoo journals <src> --merge-with <target>%s\n", f.Bold, f.Cyan, f.Reset)
	fmt.Printf("    %sMerge one journal into another (move reconciliations + entries, then delete the source)%s\n\n", f.Dim, f.Reset)
	fmt.Printf("  %s%schb odoo get <ref>%s\n", f.Bold, f.Cyan, f.Reset)
	fmt.Printf("    %sInspect an Odoo statement line by unique_import_id%s\n\n", f.Dim, f.Reset)
	fmt.Printf("  %s%schb odoo backup%s\n", f.Bold, f.Cyan, f.Reset)
	fmt.Printf("    %sDownload a full database backup (zip of SQL dump + filestore)%s\n\n", f.Dim, f.Reset)
	fmt.Printf("%sGLOBAL FLAGS%s\n\n", f.Bold, f.Reset)
	fmt.Printf("  %s--odoo-db <slug>%s   Override the Odoo DB (auto-derives URL: <slug>.odoo.com)\n", f.Yellow, f.Reset)
	fmt.Printf("  %s--odoo-url <url>%s   Override the Odoo URL (auto-derives DB from hostname)\n\n", f.Yellow, f.Reset)
	fmt.Printf("  %sNote%s: for the full loop, use the top-level 'chb sync' (= chb pull && chb push).\n\n", f.Bold, f.Reset)
}

func printOdooSyncHelp() {
	f := Fmt
	fmt.Printf(`
%schb odoo pull%s — Fetch data from Odoo (alias: sync, deprecated)

%sUSAGE%s
  %schb odoo pull%s

%sDESCRIPTION%s
  Pulls every Odoo-side dataset chb cares about into local provider
  archives. Strictly read-only on the Odoo side. Fetches:

  - Analytic plans and accounts (categorization dimensions, ensures
    plans 3/8/Income exist and creates one analytic.account per
    category and per collective)
  - Partners (cached for IBAN/name lookups during merge/push)
  - Invoices and bills (with private attachments)
  - Analytic enrichment lines (Odoo-side categorized postings)

  The pulled data feeds %schb generate%s, which writes the resolved
  category / collective / accountCode / partnerId onto each
  transactions.json. To push that local data into Odoo journals, use
  %schb odoo journals push%s.

  Run %schb setup odoo%s first to configure credentials.

%sENVIRONMENT%s
  %sODOO_URL%s            Odoo instance URL
  %sODOO_LOGIN%s          Odoo login email
  %sODOO_PASSWORD%s       Odoo password or API key
`,
		f.Bold, f.Reset, // title
		f.Bold, f.Reset, // USAGE
		f.Cyan, f.Reset, // chb odoo pull
		f.Bold, f.Reset, // DESCRIPTION
		f.Cyan, f.Reset, // chb generate
		f.Cyan, f.Reset, // chb odoo journals push
		f.Cyan, f.Reset, // chb setup odoo
		f.Bold, f.Reset, // ENVIRONMENT
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
	)
}
