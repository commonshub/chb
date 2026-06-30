package cmd

import (
	"bufio"
	"fmt"
	"math"
	"os"
	"sort"
	"strings"
)

// reviewSuspenseItem is one posted move line sitting on a holding/suspense
// account (e.g. 499000 "Comptes d'attente"), paired with the account its
// underlying transaction *should* book to.
type reviewSuspenseItem struct {
	MoveLineID    int     // the suspense move.line itself
	MoveID        int     // its account.move
	JournalID     int     //
	Date          string  //
	Label         string  // payment_ref (preferred) or the move line name
	Amount        float64 // signed balance on the suspense line
	StatementLine int     // bank statement line id (0 = manual entry, can't auto-move)
	ImportID      string  // unique_import_id of the statement line ("" = no local match)
	ProposedCode  string  // category → account mapping result ("" = unresolved)
}

// OdooAccountReview lists every posted line booked on a suspense/holding account
// and proposes where each belongs, so the operator can sweep them out of
// suspense in one pass — a reconcile-style allocation. Read-only by default;
// --apply moves the lines whose underlying transaction resolves to a real
// account. Lines with no local match (manual journal entries, or transactions
// chb doesn't track) are surfaced for manual handling and never touched.
//
//	chb odoo accounts 499000 review
//	chb odoo accounts 499000 review --journal 48
//	chb odoo accounts 499000 review --journal 48 --apply
func OdooAccountReview(creds *OdooCredentials, uid int, code string, args []string) error {
	if HasFlag(args, "--help", "-h", "help") {
		printOdooAccountReviewHelp()
		return nil
	}
	apply := HasFlag(args, "--apply")
	journalFilter := GetNumber(args, []string{"--journal", "-j"}, 0)

	// --apply may carry an explicit target code (e.g. `--apply 700150`): move
	// every in-scope item to THAT account, overriding the auto-resolved
	// proposal. Narrow the scope first with --since/--until/--journal so the
	// override only touches the items you mean.
	applyTarget := ""
	if apply {
		if v := strings.TrimSpace(GetOption(args, "--apply")); allDigits(v) {
			applyTarget = v
		}
	}

	acc, ok, err := fetchOdooAccountByCode(creds, uid, code)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("no Odoo account with code %q", code)
	}

	// 1. Every posted journal item on the suspense account (optionally one
	//    journal, optionally a date window).
	domain := []interface{}{
		[]interface{}{"account_id", "=", acc.ID},
		[]interface{}{"parent_state", "=", "posted"},
	}
	if journalFilter > 0 {
		domain = append(domain, []interface{}{"journal_id", "=", journalFilter})
	}
	if s := GetOption(args, "--since"); s != "" {
		t, ok := ParseSinceDate(s)
		if !ok {
			return fmt.Errorf("invalid --since %q (expected %s)", s, DateFormatHelp)
		}
		domain = append(domain, []interface{}{"date", ">=", t.Format("2006-01-02")})
	}
	if u := GetOption(args, "--until"); u != "" {
		t, ok := ParseDateEndExclusive(u)
		if !ok {
			return fmt.Errorf("invalid --until %q (expected %s)", u, DateFormatHelp)
		}
		domain = append(domain, []interface{}{"date", "<", t.Format("2006-01-02")})
	}
	rows, err := odooSearchReadAllMaps(creds, uid, "account.move.line",
		domain,
		[]string{"id", "move_id", "journal_id", "date", "name", "balance"},
		"date asc, id asc")
	if err != nil {
		return fmt.Errorf("read suspense lines: %v", err)
	}
	if len(rows) == 0 {
		scope := ""
		if journalFilter > 0 {
			scope = fmt.Sprintf(" in journal #%d", journalFilter)
		}
		fmt.Printf("\n%s✓ %s — %s%s is empty%s\n\n", Fmt.Green, acc.Code, acc.Name, scope, Fmt.Reset)
		return nil
	}

	items := make([]reviewSuspenseItem, 0, len(rows))
	for _, r := range rows {
		items = append(items, reviewSuspenseItem{
			MoveLineID: odooInt(r["id"]),
			MoveID:     odooFieldID(r["move_id"]),
			JournalID:  odooFieldID(r["journal_id"]),
			Date:       odooString(r["date"]),
			Label:      odooString(r["name"]),
			Amount:     roundCents(odooFloat(r["balance"])),
		})
	}

	// Page the (date-sorted) items with --skip / -n before any further work, so
	// the preview, resolution, and --apply all act on the same window.
	total := len(items)
	skip := GetNumber(args, []string{"--skip"}, 0)
	if skip > 0 {
		if skip >= len(items) {
			items = nil
		} else {
			items = items[skip:]
		}
	}
	if n := GetNumber(args, []string{"-n", "--limit"}, 0); n > 0 && n < len(items) {
		items = items[:n]
	}

	moveIDs := make([]int, 0, len(items))
	for _, it := range items {
		if it.MoveID > 0 {
			moveIDs = append(moveIDs, it.MoveID)
		}
	}

	// 2. Map each move back to its bank statement line (import id + payment_ref).
	//    Manual journal entries have none — those stay unresolved.
	stmtByMove := map[int]map[string]interface{}{}
	if len(moveIDs) > 0 {
		stmtRows, err := odooSearchReadAllMaps(creds, uid, "account.bank.statement.line",
			[]interface{}{[]interface{}{"move_id", "in", intsToInterfaces(uniquePositiveInts(moveIDs))}},
			[]string{"id", "move_id", "unique_import_id", "payment_ref", "amount"}, "id asc")
		if err != nil {
			return fmt.Errorf("read statement lines: %v", err)
		}
		for _, r := range stmtRows {
			stmtByMove[odooFieldID(r["move_id"])] = r
		}
	}

	targetIDs := map[string]bool{}
	for i := range items {
		if sl, ok := stmtByMove[items[i].MoveID]; ok {
			items[i].StatementLine = odooInt(sl["id"])
			items[i].ImportID = odooString(sl["unique_import_id"])
			if ref := odooString(sl["payment_ref"]); ref != "" {
				items[i].Label = ref
			}
			if items[i].ImportID != "" {
				targetIDs[items[i].ImportID] = true
			}
		}
	}

	// 3. Resolve each import id to the account its category maps to (no
	//    sync-since cutoff — we want to clear the *whole* suspense account).
	expected := resolveExpectedAccountCodesForImportIDs(targetIDs)
	for i := range items {
		if items[i].ImportID != "" {
			items[i].ProposedCode = expected[items[i].ImportID]
		}
	}

	// Fallback for accounts whose source of truth is Odoo (e.g. KBC): the
	// bootstrap CSV is no longer authoritative, so derive the proposal straight
	// from the pulled journal data — run the local rules on the statement line's
	// payment_ref (the bank narration) and map the resulting category. Only fills
	// items the local-mirror pass above left unresolved.
	if cz := newCategorizerOrNil(); cz != nil {
		mappings, _ := LoadOdooMappings()
		providerByJournal := providerByOdooJournalID()
		for i := range items {
			if items[i].ProposedCode != "" {
				continue
			}
			sl, ok := stmtByMove[items[i].MoveID]
			if !ok {
				continue
			}
			items[i].ProposedCode = proposeAccountFromOdooNarration(
				cz, mappings, providerByJournal[items[i].JournalID],
				odooString(sl["payment_ref"]), odooFloat(sl["amount"]))
		}
	}

	// `--apply <code>` overrides every movable item to that one account. Only
	// items with a statement line can be re-accounted; manual entries stay put.
	if applyTarget != "" {
		for i := range items {
			if items[i].StatementLine > 0 {
				items[i].ProposedCode = applyTarget
			}
		}
	}

	names := fetchAccountNamesByCode(creds, uid, distinctProposedCodes(items))
	printSuspenseReview(acc, journalFilter, items, names)
	if skip > 0 || len(items) < total {
		fmt.Printf("  %sShowing items %d–%d of %d (page with --skip / -n).%s\n",
			Fmt.Dim, skip+1, skip+len(items), total, Fmt.Reset)
	}
	if applyTarget != "" {
		fmt.Printf("  %s↳ --apply %s: overriding every item above to %s%s\n",
			Fmt.Yellow, applyTarget, codeLabel(applyTarget, names), Fmt.Reset)
	}

	if !apply {
		return nil
	}
	return applySuspenseReview(creds, uid, acc, items, names)
}

// resolveExpectedAccountCodesForImportIDs scans every account's local
// transactions and resolves the Odoo account code each of the wanted import ids
// maps to (generate-time AccountCode, else a live odoo_mapping lookup). The same
// resolution `chb odoo journals <id> fix` uses, but account-centric and without
// the per-account sync-since cutoff.
func resolveExpectedAccountCodesForImportIDs(targetIDs map[string]bool) map[string]string {
	out := map[string]string{}
	if len(targetIDs) == 0 {
		return out
	}
	mappings, _ := LoadOdooMappings()
	for _, a := range LoadAccountConfigs() {
		acc := a
		for _, tx := range loadAccountTransactionsForOdoo(&acc) {
			importID := buildUniqueImportID(&acc, tx)
			if importID == "" || !targetIDs[importID] {
				continue
			}
			code := tx.AccountCode
			if m := LookupOdooMapping(mappings, tx); m != nil && m.Set.AccountCode != "" {
				code = m.Set.AccountCode
			}
			if code != "" {
				out[importID] = code
			}
		}
		// Stripe refunds/chargebacks carry the original charge's category only
		// after the BT-level reversal inheritance runs — generate writes just the
		// collective as the description, so transactions.json leaves them
		// uncategorised. Re-resolve those here (same path push/`fix` use) so a
		// ticket/membership refund is allocated, not left in suspense.
		if acc.Provider == "stripe" {
			for importID, dl := range localStripeOdooDesiredLinesByImportID(&acc, targetIDs) {
				if out[importID] != "" {
					continue
				}
				cat := metaString(dl.Metadata, "category")
				if cat == "" {
					continue
				}
				dir := "CREDIT"
				if metaFloat(dl.Metadata, "amount") < 0 {
					dir = "DEBIT"
				}
				tx := TransactionEntry{Type: dir, Category: cat, Collective: metaString(dl.Metadata, "collective")}
				if m := LookupOdooMapping(mappings, tx); m != nil && m.Set.AccountCode != "" {
					out[importID] = m.Set.AccountCode
				}
			}
		}
	}
	return out
}

// newCategorizerOrNil builds a rules categorizer from the active settings, or
// returns nil if settings can't be loaded (the caller then skips the
// narration-based proposal rather than failing the whole review).
func newCategorizerOrNil() *Categorizer {
	s, err := LoadSettings()
	if err != nil || s == nil {
		return nil
	}
	return NewCategorizer(s)
}

// providerByOdooJournalID maps each configured account's Odoo bank journal id to
// its chb provider (e.g. journal 28 → "kbcbrussels"). Used to stamp the right
// provider on a synthetic narration tx so provider-gated rules (e.g. the KBC
// "*proximus*" / "*electrabel*" vendor rules) fire on the suspense-review path.
func providerByOdooJournalID() map[int]string {
	out := map[int]string{}
	for _, acc := range LoadAccountConfigs() {
		if acc.OdooJournalID > 0 && acc.Provider != "" {
			out[acc.OdooJournalID] = acc.Provider
		}
	}
	return out
}

// proposeAccountFromOdooNarration categorizes a bank statement line's narration
// (payment_ref) and maps the resulting category to an Odoo account code. The
// direction comes from the signed bank-line amount (negative = money out), which
// is what the directional odoo_mapping rules key off; provider stamps the
// owning journal's provider so provider-gated vendor rules match. Returns "" when
// nothing matches. This is the Odoo-source-of-truth path: no local CSV/mirror
// lookup, just the data pulled for the journal.
func proposeAccountFromOdooNarration(cz *Categorizer, mappings []OdooMapping, provider, narration string, amount float64) string {
	narration = strings.TrimSpace(narration)
	if narration == "" || cz == nil {
		return ""
	}
	txType := "CREDIT"
	if amount < 0 {
		txType = "DEBIT"
	}
	tx := TransactionEntry{
		Type:     txType,
		Amount:   amount,
		Provider: provider,
		Metadata: map[string]interface{}{
			"description":     narration,
			"fullDescription": narration,
		},
	}
	cz.Apply(&tx)
	if m := LookupOdooMapping(mappings, tx); m != nil && m.Set.AccountCode != "" {
		return m.Set.AccountCode
	}
	return ""
}

func distinctProposedCodes(items []reviewSuspenseItem) []string {
	seen := map[string]bool{}
	var out []string
	for _, it := range items {
		if it.ProposedCode != "" && !seen[it.ProposedCode] {
			seen[it.ProposedCode] = true
			out = append(out, it.ProposedCode)
		}
	}
	return out
}

// fetchAccountNamesByCode resolves chart-of-accounts names for display, keyed by
// code. Best-effort: a code that doesn't resolve is simply absent.
func fetchAccountNamesByCode(creds *OdooCredentials, uid int, codes []string) map[string]string {
	out := map[string]string{}
	if len(codes) == 0 {
		return out
	}
	in := make([]interface{}, len(codes))
	for i, c := range codes {
		in[i] = c
	}
	rows, err := odooSearchReadAllMaps(creds, uid, "account.account",
		[]interface{}{[]interface{}{"code", "in", in}},
		[]string{"code", "name"}, "code asc")
	if err != nil {
		return out
	}
	for _, r := range rows {
		out[odooString(r["code"])] = odooString(r["name"])
	}
	return out
}

func codeLabel(code string, names map[string]string) string {
	if code == "" {
		return "?"
	}
	if n := names[code]; n != "" {
		return code + " " + n
	}
	return code
}

func printSuspenseReview(acc odooGlAccount, journalFilter int, items []reviewSuspenseItem, names map[string]string) {
	scope := ""
	if journalFilter > 0 {
		scope = fmt.Sprintf("  %s(journal #%d)%s", Fmt.Dim, journalFilter, Fmt.Reset)
	}
	fmt.Printf("\n%s🧹 %s — %s%s%s\n", Fmt.Bold, acc.Code, acc.Name, Fmt.Reset, scope)
	fmt.Printf("  %s%s on this account; proposing where each belongs%s\n\n",
		Fmt.Dim, Pluralize(len(items), "item", ""), Fmt.Reset)

	headers := []string{"Date", "Jrnl", "Description", "Amount", "→ Proposed account"}
	out := make([][]string, 0, len(items))
	var total float64
	for _, it := range items {
		proposed := codeLabel(it.ProposedCode, names)
		if it.ProposedCode == "" {
			if it.ImportID == "" {
				proposed = Fmt.Dim + "manual — no local match" + Fmt.Reset
			} else {
				proposed = Fmt.Yellow + "unresolved (no mapping)" + Fmt.Reset
			}
		}
		out = append(out, []string{
			it.Date,
			fmt.Sprintf("%d", it.JournalID),
			Truncate(it.Label, 46),
			signPrefix(it.Amount) + fmtNumber(math.Abs(it.Amount)),
			Truncate(proposed, 40),
		})
		total += it.Amount
	}
	totalRow := []string{"", "", Pluralize(len(items), "item", ""), signPrefix(total) + fmtNumber(math.Abs(total)), ""}
	renderTicketsTable(headers, out, totalRow, map[int]bool{3: true})

	// Grouped summary by proposed target.
	type grp struct {
		n   int
		sum float64
	}
	byCode := map[string]*grp{}
	var resolvable int
	for _, it := range items {
		key := it.ProposedCode
		if key == "" {
			if it.ImportID == "" {
				key = "__manual"
			} else {
				key = "__unresolved"
			}
		} else if it.StatementLine > 0 {
			resolvable++
		}
		g := byCode[key]
		if g == nil {
			g = &grp{}
			byCode[key] = g
		}
		g.n++
		g.sum += it.Amount
	}
	codes := make([]string, 0, len(byCode))
	for c := range byCode {
		codes = append(codes, c)
	}
	sort.Strings(codes)
	fmt.Printf("\n  %sProposed allocation:%s\n", Fmt.Bold, Fmt.Reset)
	for _, c := range codes {
		g := byCode[c]
		label := codeLabel(c, names)
		switch c {
		case "__manual":
			label = Fmt.Dim + "manual entries — no local match (left as-is)" + Fmt.Reset
		case "__unresolved":
			label = Fmt.Yellow + "no category mapping (left as-is)" + Fmt.Reset
		}
		fmt.Printf("    %s%-44s%s %s  %s%s%s\n",
			Fmt.Cyan, label, Fmt.Reset,
			Pluralize(g.n, "item", ""),
			Fmt.Dim, signPrefix(g.sum)+fmtNumber(math.Abs(g.sum))+" EUR", Fmt.Reset)
	}

	if resolvable > 0 {
		fmt.Printf("\n  %s%s can be allocated automatically. Re-run with %s--apply%s%s to move them.%s\n\n",
			Fmt.Dim, Pluralize(resolvable, "item", ""), Fmt.Reset+Fmt.Cyan, Fmt.Reset+Fmt.Dim, "", Fmt.Reset)
	} else {
		fmt.Printf("\n  %sNothing can be allocated automatically — all items need manual handling.%s\n\n", Fmt.Dim, Fmt.Reset)
	}
}

func applySuspenseReview(creds *OdooCredentials, uid int, acc odooGlAccount, items []reviewSuspenseItem, names map[string]string) error {
	// Only items with a resolved target AND a statement line can be moved by the
	// counterpart-rewrite path; manual entries are left for the operator.
	byCode := map[string][]int{} // proposed code -> statement line ids
	moved := 0
	for _, it := range items {
		if it.ProposedCode == "" || it.StatementLine == 0 {
			continue
		}
		byCode[it.ProposedCode] = append(byCode[it.ProposedCode], it.StatementLine)
		moved++
	}
	if moved == 0 {
		fmt.Printf("  %sNothing to allocate automatically.%s\n\n", Fmt.Dim, Fmt.Reset)
		return nil
	}

	fmt.Printf("  %sAllocate %s out of %s (%s) to their proposed accounts?%s [y/N] ",
		Fmt.Bold, Pluralize(moved, "item", ""), acc.Code, acc.Name, Fmt.Reset)
	resp, _ := bufio.NewReader(os.Stdin).ReadString('\n')
	if r := strings.TrimSpace(strings.ToLower(resp)); r != "y" && r != "yes" {
		fmt.Printf("  %sCancelled.%s\n\n", Fmt.Dim, Fmt.Reset)
		return nil
	}

	status := newStatusLine()
	codes := make([]string, 0, len(byCode))
	for c := range byCode {
		codes = append(codes, c)
	}
	sort.Strings(codes)
	done := 0
	for _, c := range codes {
		ids := byCode[c]
		status.Update("Allocating %s → %s…", Pluralize(len(ids), "item", ""), codeLabel(c, names))
		if err := applyOdooMappingAccount(creds, uid, ids, c, status); err != nil {
			status.Clear()
			Warnf("  %s⚠ Could not allocate %s to %s: %v%s", Fmt.Yellow, Pluralize(len(ids), "item", ""), c, err, Fmt.Reset)
			continue
		}
		done += len(ids)
	}
	status.Clear()
	fmt.Printf("\n  %s✓ Allocated %s out of %s.%s\n\n", Fmt.Green, Pluralize(done, "item", ""), acc.Code, Fmt.Reset)
	return nil
}

func printOdooAccountReviewHelp() {
	f := Fmt
	fmt.Printf(`
%schb odoo accounts <code> review%s — Review a suspense/holding account and allocate its items.

Lists every posted line on the account (e.g. 499000 "Comptes d'attente") and
proposes the income/expense account each underlying transaction maps to. Read-only
by default; %s--apply%s moves the lines whose transaction resolves to a real account.
Manual journal entries (no local match) are surfaced but never touched.

Pass a code to %s--apply%s (e.g. %s--apply 700150%s) to force every in-scope item to that
account — narrow the scope first with --since/--until/--journal so you only move
the items you mean.

%sUSAGE%s
  %schb odoo accounts 499000 review%s                 Review every item on 499000
  %schb odoo accounts 499000 review --journal 48%s    Only items posted via journal #48
  %schb odoo accounts 499000 review --journal 48 --apply%s          Allocate auto-resolved items
  %schb odoo accounts 499000 review --since 2025 --until 2026 --journal 48 --apply 700150%s
                                                    Force the remaining items to 700150

%sOPTIONS%s
  %s--journal N%s, %s-j N%s     Only review items posted via that journal
  %s--since D%s, %s--until D%s   Date window (%s)
  %s--skip N%s, %s-n N%s        Page the date-sorted items: skip N, then show at most N
  %s--apply [code]%s        Move items; with a code, force them all to that account
`,
		f.Bold, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset, f.Cyan, f.Reset,
		f.Bold, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Bold, f.Reset,
		f.Yellow, f.Reset, f.Yellow, f.Reset,
		f.Yellow, f.Reset, f.Yellow, f.Reset, DateFormatHelp,
		f.Yellow, f.Reset, f.Yellow, f.Reset,
		f.Yellow, f.Reset,
	)
}
