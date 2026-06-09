package cmd

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sort"
	"strings"
	"time"

	kbcbrusselssource "github.com/CommonsHub/chb/providers/kbcbrussels"
)

// mergeKBCJournalWithCSV reconciles the CSV (source of truth) against an
// existing Odoo journal that was populated by an earlier import tool.
// Matched lines are left untouched on purpose — they already carry a
// unique_import_id from the original importer and that link is what other
// tools may rely on for reconciliation. Merge only:
//
//   - creates Odoo lines for CSV rows with no match (the "missing" set);
//   - deletes orphan Odoo lines (no CSV counterpart, not reconciled);
//   - prints a compact preview (default) or a full table (--verbose) so
//     the operator can verify before committing.
//
// In --dry-run, no writes happen.
// mergeKBCJournalWithCSV pushes the local KBC CSV into the linked Odoo
// journal. Returns the number of newly-created statement lines so the
// caller can decide whether to auto-reconcile (see
// shouldReconcileAfterPush). Returns 0 on no-op / dry-run / error.
func mergeKBCJournalWithCSV(creds *OdooCredentials, uid int, journalID int, acc *AccountConfig, dryRun, assumeYes, verbose bool) (int, error) {
	createdLines := 0
	iban := kbcbrusselssource.NormalizeIBAN(acc.IBAN)
	if iban == "" {
		return 0, fmt.Errorf("account '%s' has no IBAN configured", acc.Slug)
	}
	rows, err := kbcbrusselssource.LoadTransactionsForIBAN(DataDir(), iban)
	if err != nil {
		return 0, fmt.Errorf("load CSV: %v", err)
	}
	if len(rows) == 0 {
		return 0, fmt.Errorf("no CSV rows for %s under %s",
			iban, kbcbrusselssource.LatestDir(DataDir()))
	}

	odooLines, err := loadKBCOdooLines(creds, uid, journalID)
	if err != nil {
		return 0, fmt.Errorf("fetch Odoo lines: %v", err)
	}

	// Build the set of unique_import_id values already in Odoo so we can
	// short-circuit both sides of the planning: a CSV row whose canonical
	// import_id is already in Odoo is by definition matched (nothing to
	// add); the corresponding Odoo line is by definition kept (nothing to
	// delete). Skipping them symmetrically is what makes the merge
	// idempotent — without it, repeated runs would push the same rows
	// again because the Odoo lines get excluded from the key-based map.
	odooImportIDs := map[string]bool{}
	for _, line := range odooLines {
		if line.ImportID != "" {
			odooImportIDs[line.ImportID] = true
		}
	}
	csvImportIDs := map[string]bool{}
	for _, row := range rows {
		csvImportIDs[buildKBCImportID(iban, row.Hash)] = true
	}

	csvByKey := map[mergeKey][]kbcMergeCSVRow{}
	for _, row := range rows {
		importID := buildKBCImportID(iban, row.Hash)
		if odooImportIDs[importID] {
			continue
		}
		key := mergeKeyFromCSV(row)
		csvByKey[key] = append(csvByKey[key], kbcMergeCSVRow{
			Row:      row,
			ImportID: importID,
		})
	}
	odooByKey := map[mergeKey][]kbcMergeOdooLine{}
	alreadyCanonical := 0
	for _, line := range odooLines {
		if csvImportIDs[line.ImportID] {
			alreadyCanonical++
			continue
		}
		key := mergeKeyFromOdoo(line)
		odooByKey[key] = append(odooByKey[key], line)
	}

	plan := buildKBCMergePlan(csvByKey, odooByKey)

	// Pre-compute the rule-driven category / account / partner choices
	// for each ToAdd row so --verbose previews and the apply step share
	// a single source of truth. These are all local lookups (no Odoo
	// roundtrip) so they're cheap.
	ctx := newKBCMergeContext(acc)
	for i := range plan.ToAdd {
		annotateKBCMergeRowWithMapping(&plan.ToAdd[i], acc, &ctx)
	}

	// Pre-compute the partner create/update split using the local Odoo
	// partner cache (from `chb odoo sync`) plus one batched lookup
	// against res.partner.bank for every unique IBAN in ToAdd.
	partnerIdx := loadLatestOdooPartnerIndex(DataDir())
	partnerSummary, err := computeKBCPartnerSummary(creds, uid, plan.ToAdd, partnerIdx)
	if err != nil {
		Warnf("  %s⚠ partner pre-flight: %v%s", Fmt.Yellow, err, Fmt.Reset)
	}

	// Short-circuit when there's nothing to push. Skips the full merge
	// summary + preview tables that would otherwise scroll past every
	// run of `chb odoo journals push` even on a no-op KBC journal. The
	// dry-run is still allowed to short-circuit — the answer is "nothing
	// would happen" and the empty table doesn't add information. --verbose
	// still gets the detailed view in case the user is debugging matching.
	noop := len(plan.ToAdd) == 0 && len(plan.ToDelete) == 0
	if noop && !verbose {
		if !quietOdooContext() {
			suffix := ""
			if dryRun {
				suffix = " (dry-run)"
			}
			fmt.Printf("  %s✓ kbc: nothing to push%s%s\n", Fmt.Green, suffix, Fmt.Reset)
		}
		return createdLines, nil
	}

	printKBCMergeSummary(plan, len(rows), len(odooLines), alreadyCanonical, iban, journalID, partnerSummary, partnerIdx != nil)

	if verbose {
		printKBCMergeTables(plan, acc, journalID, creds.URL)
	} else {
		printKBCMergePreviews(plan, acc, journalID, creds.URL)
	}

	if dryRun {
		fmt.Printf("\n  %s(dry-run — no writes)%s\n\n", Fmt.Dim, Fmt.Reset)
		return createdLines, nil
	}

	if noop {
		fmt.Printf("\n  %s✓ Nothing to do%s\n\n", Fmt.Green, Fmt.Reset)
		return createdLines, nil
	}

	// Confirm interactively — but ONLY when we can actually read a reply.
	// Under the aggregate `chb odoo journals push` (or any non-TTY run) stdout
	// is silenced, so this prompt would be invisible yet still block on stdin —
	// the hang reported as "stuck at #28 kbc: verifying cache freshness", which
	// a blind Enter releases. isInteractiveTTY() is false in quiet/aggregate or
	// non-TTY contexts; there we proceed (the operator explicitly invoked a
	// write — use --dry-run to preview, or run the journal on its own to be
	// prompted).
	if !assumeYes && isInteractiveTTY() {
		fmt.Println()
		fmt.Printf("  Apply: create %s%s? Matched lines left untouched. [y/N] ",
			Pluralize(len(plan.ToAdd), "Odoo line", ""),
			deleteOrphansSuffix(len(plan.ToDelete)))
		reader := bufio.NewReader(os.Stdin)
		resp, _ := reader.ReadString('\n')
		if strings.ToLower(strings.TrimSpace(resp)) != "y" {
			return 0, fmt.Errorf("cancelled")
		}
	}

	created, err := applyKBCMerge(creds, uid, journalID, acc, plan)
	if err != nil {
		return created, err
	}
	createdLines = created
	fmt.Printf("\n  %s✓ Merge complete%s\n\n", Fmt.Green, Fmt.Reset)
	return createdLines, nil
}

func deleteOrphansSuffix(n int) string {
	if n == 0 {
		return ""
	}
	return fmt.Sprintf(", delete %s", Pluralize(n, "orphan Odoo line", ""))
}

// mergeKey is the reconciliation tuple. amountCents avoids float-equality
// fragility; cpIBAN is lowercase-normalized.
type mergeKey struct {
	date        string
	cpIBAN      string
	amountCents int64
}

type kbcMergeCSVRow struct {
	Row      kbcbrusselssource.Transaction
	ImportID string
	// Category / AccountCode / AccountName are filled in by the planning
	// step using the same categorizer + OdooMapping lookup that the
	// create step uses, so --dry-run / --verbose can preview the
	// assignments without hitting Odoo.
	Category    string
	Collective  string
	AccountCode string
	AccountName string
	// MappingPartnerID is the partner id baked into a matching OdooMapping
	// (e.g. "all donations go to Anonymous Donor"). Zero when no mapping
	// pins a partner; the dynamic IBAN→partner lookup happens in apply.
	MappingPartnerID   int
	MappingPartnerName string
}

type kbcMergeOdooLine struct {
	ID               int
	Date             string
	AmountCents      int64
	Amount           float64
	CounterpartyIBAN string
	CounterpartyName string
	PaymentRef       string
	Narration        string
	ImportID         string
	IsReconciled     bool
}

type kbcMergePair struct {
	CSV  kbcMergeCSVRow
	Odoo kbcMergeOdooLine
}

type kbcMergePlan struct {
	Pairs    []kbcMergePair
	ToAdd    []kbcMergeCSVRow
	ToDelete []kbcMergeOdooLine
	// Ambiguous: keys where one side has more candidates than the other.
	// The extras land in ToAdd / ToDelete, but we surface the count so the
	// operator knows greedy pairing might have mis-paired siblings.
	Ambiguous int
}

func mergeKeyFromCSV(row kbcbrusselssource.Transaction) mergeKey {
	return mergeKey{
		date:        row.Date,
		cpIBAN:      strings.ToLower(strings.ReplaceAll(row.CounterpartyIBAN, " ", "")),
		amountCents: int64(math.Round(row.Amount * 100)),
	}
}

func mergeKeyFromOdoo(line kbcMergeOdooLine) mergeKey {
	return mergeKey{
		date:        line.Date,
		cpIBAN:      strings.ToLower(strings.ReplaceAll(line.CounterpartyIBAN, " ", "")),
		amountCents: line.AmountCents,
	}
}

func buildKBCMergePlan(csvByKey map[mergeKey][]kbcMergeCSVRow, odooByKey map[mergeKey][]kbcMergeOdooLine) kbcMergePlan {
	plan := kbcMergePlan{}

	// Flatten into single working slices, indexed by which CSV/Odoo line
	// is still unmatched. This keeps the multi-pass logic readable.
	var allCSV []kbcMergeCSVRow
	for _, csvs := range csvByKey {
		allCSV = append(allCSV, csvs...)
	}
	var allOdoo []kbcMergeOdooLine
	for _, odoos := range odooByKey {
		allOdoo = append(allOdoo, odoos...)
	}
	// Stable ordering before pairing — and reconciled Odoo lines come
	// FIRST so any per-bucket greedy pick prefers preserving the
	// reconciled side of a duplicate.
	sort.SliceStable(allCSV, func(i, j int) bool {
		if allCSV[i].Row.Timestamp != allCSV[j].Row.Timestamp {
			return allCSV[i].Row.Timestamp < allCSV[j].Row.Timestamp
		}
		return allCSV[i].Row.Hash < allCSV[j].Row.Hash
	})
	sort.SliceStable(allOdoo, func(i, j int) bool {
		if allOdoo[i].IsReconciled != allOdoo[j].IsReconciled {
			return allOdoo[i].IsReconciled
		}
		if allOdoo[i].Date != allOdoo[j].Date {
			return allOdoo[i].Date < allOdoo[j].Date
		}
		return allOdoo[i].ID < allOdoo[j].ID
	})

	csvUsed := make([]bool, len(allCSV))
	odooUsed := make([]bool, len(allOdoo))

	// Run the passes from strictest to loosest. Each pass only consumes
	// CSV / Odoo entries that no earlier pass paired.
	passes := []struct {
		needIBAN bool
		maxDays  int
	}{
		{needIBAN: true, maxDays: 0},  // strict
		{needIBAN: false, maxDays: 0}, // exact date, IBAN-agnostic
		{needIBAN: true, maxDays: 1},  // ±1 day with IBAN
		{needIBAN: false, maxDays: 3}, // ±3 days, IBAN-agnostic — catches debit-card posting delays
		{needIBAN: false, maxDays: 7}, // ±7 days, last resort for laggy postings
	}
	for _, p := range passes {
		for i := range allCSV {
			if csvUsed[i] {
				continue
			}
			c := allCSV[i]
			cIBAN := strings.ToLower(strings.ReplaceAll(c.Row.CounterpartyIBAN, " ", ""))
			cAmt := int64(math.Round(c.Row.Amount * 100))
			matchIdx := -1
			for j := range allOdoo {
				if odooUsed[j] {
					continue
				}
				o := allOdoo[j]
				if o.AmountCents != cAmt {
					continue
				}
				if p.needIBAN {
					oIBAN := strings.ToLower(strings.ReplaceAll(o.CounterpartyIBAN, " ", ""))
					if cIBAN == "" || oIBAN == "" || cIBAN != oIBAN {
						continue
					}
				}
				if !datesWithinNDays(c.Row.Date, o.Date, p.maxDays) {
					continue
				}
				matchIdx = j
				break
			}
			if matchIdx >= 0 {
				plan.Pairs = append(plan.Pairs, kbcMergePair{CSV: c, Odoo: allOdoo[matchIdx]})
				csvUsed[i] = true
				odooUsed[matchIdx] = true
			}
		}
	}

	for i, used := range csvUsed {
		if !used {
			plan.ToAdd = append(plan.ToAdd, allCSV[i])
		}
	}
	for j, used := range odooUsed {
		if !used {
			plan.ToDelete = append(plan.ToDelete, allOdoo[j])
		}
	}

	// Sort outputs for stable presentation.
	sort.SliceStable(plan.Pairs, func(i, j int) bool {
		return plan.Pairs[i].CSV.Row.Timestamp < plan.Pairs[j].CSV.Row.Timestamp
	})
	sort.SliceStable(plan.ToAdd, func(i, j int) bool {
		return plan.ToAdd[i].Row.Timestamp < plan.ToAdd[j].Row.Timestamp
	})
	sort.SliceStable(plan.ToDelete, func(i, j int) bool {
		return plan.ToDelete[i].Date < plan.ToDelete[j].Date
	})
	return plan
}

// datesWithinNDays returns true when two YYYY-MM-DD strings are at most
// N calendar days apart. N=0 means dates must match exactly. Either side
// empty or unparseable returns false.
func datesWithinNDays(a, b string, n int) bool {
	if a == "" || b == "" {
		return false
	}
	ta, err := time.Parse("2006-01-02", a)
	if err != nil {
		return false
	}
	tb, err := time.Parse("2006-01-02", b)
	if err != nil {
		return false
	}
	diff := ta.Sub(tb)
	if diff < 0 {
		diff = -diff
	}
	return diff <= time.Duration(n)*24*time.Hour
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// datesWithinOneDay returns true when two YYYY-MM-DD strings are at most
// one calendar day apart (in either direction). Either side being empty
// or unparseable counts as "no match".
func datesWithinOneDay(a, b string) bool {
	if a == "" || b == "" {
		return false
	}
	ta, err := time.Parse("2006-01-02", a)
	if err != nil {
		return false
	}
	tb, err := time.Parse("2006-01-02", b)
	if err != nil {
		return false
	}
	diff := ta.Sub(tb)
	if diff < 0 {
		diff = -diff
	}
	return diff <= 24*time.Hour
}

// kbcPartnerSummary classifies each ToAdd row by what would happen to the
// counterparty partner record in Odoo if the merge were applied. Counts
// add up to len(plan.ToAdd).
type kbcPartnerSummary struct {
	Matched  int // res.partner.bank already exists for the IBAN
	ToUpdate int // partner found by name; we'd attach the IBAN
	ToCreate int // no IBAN match, no name match → new res.partner
	Skipped  int // row has neither IBAN nor name
}

// computeKBCPartnerSummary batches one res.partner.bank search across every
// unique IBAN in the ToAdd set, then uses the locally-cached partner index
// (from `chb odoo sync`) for name lookups. Zero or one Odoo RPC, no per-row
// queries — safe to call from the dry-run path.
func computeKBCPartnerSummary(creds *OdooCredentials, uid int, toAdd []kbcMergeCSVRow, partnerIdx *odooPartnerIndex) (kbcPartnerSummary, error) {
	var summary kbcPartnerSummary
	if len(toAdd) == 0 {
		return summary, nil
	}

	ibanSet := map[string]bool{}
	for _, r := range toAdd {
		iban := normalizeBankAccountNumber(r.Row.CounterpartyIBAN)
		if iban != "" {
			ibanSet[iban] = true
		}
	}

	ibanToPartner := map[string]int{}
	if len(ibanSet) > 0 {
		ibanList := make([]interface{}, 0, len(ibanSet))
		for iban := range ibanSet {
			ibanList = append(ibanList, iban)
		}
		bankRows, err := odooSearchReadAllMaps(creds, uid, "res.partner.bank",
			[]interface{}{[]interface{}{"sanitized_acc_number", "in", ibanList}},
			[]string{"id", "partner_id", "sanitized_acc_number", "acc_number"},
			"id asc",
		)
		if err != nil {
			return summary, err
		}
		for _, row := range bankRows {
			iban := normalizeBankAccountNumber(odooString(row["sanitized_acc_number"]))
			if iban == "" {
				iban = normalizeBankAccountNumber(odooString(row["acc_number"]))
			}
			partnerID := odooFieldID(row["partner_id"])
			if iban != "" && partnerID > 0 {
				ibanToPartner[iban] = partnerID
			}
		}
	}

	for _, r := range toAdd {
		iban := normalizeBankAccountNumber(r.Row.CounterpartyIBAN)
		name := strings.TrimSpace(r.Row.CounterpartyName)
		if iban == "" && name == "" {
			summary.Skipped++
			continue
		}
		if iban != "" {
			if _, ok := ibanToPartner[iban]; ok {
				summary.Matched++
				continue
			}
		}
		if name != "" && partnerIdx != nil {
			normName := normalizePartnerName(name)
			if matches := partnerIdx.byName[normName]; len(matches) > 0 {
				if iban != "" {
					summary.ToUpdate++
				} else {
					summary.Matched++
				}
				continue
			}
		}
		summary.ToCreate++
	}
	return summary, nil
}

func printKBCMergeSummary(plan kbcMergePlan, csvCount, odooCount, alreadyCanonical int, iban string, journalID int, partners kbcPartnerSummary, partnerIdxLoaded bool) {
	fmt.Printf("\n  %sPush plan for KBC journal #%d (%s)%s\n", Fmt.Bold, journalID, iban, Fmt.Reset)
	fmt.Printf("  %sCSV-to-Odoo match passes: strict (date, IBAN, amount) → exact-date by amount → ±1/3/7 day fuzzy by amount; reconciled Odoo lines win on duplicate%s\n\n",
		Fmt.Dim, Fmt.Reset)
	fmt.Printf("    %sCSV rows:%s            %d\n", Fmt.Dim, Fmt.Reset, csvCount)
	fmt.Printf("    %sOdoo lines:%s          %d\n", Fmt.Dim, Fmt.Reset, odooCount)
	if alreadyCanonical > 0 {
		fmt.Printf("    %sAlready canonical:%s   %d (skipped)\n", Fmt.Dim, Fmt.Reset, alreadyCanonical)
	}
	fmt.Println()
	fmt.Printf("    %sMatched:%s             %d %s(left untouched)%s\n",
		Fmt.Green, Fmt.Reset, len(plan.Pairs), Fmt.Dim, Fmt.Reset)
	fmt.Printf("    %sTo add to Odoo:%s      %d %s(missing CSV rows)%s\n",
		Fmt.Yellow, Fmt.Reset, len(plan.ToAdd), Fmt.Dim, Fmt.Reset)
	fmt.Printf("    %sTo delete from Odoo:%s %d %s(no CSV match)%s\n",
		Fmt.Red, Fmt.Reset, len(plan.ToDelete), Fmt.Dim, Fmt.Reset)
	if plan.Ambiguous > 0 {
		fmt.Printf("    %sAmbiguous keys:%s      %d %s(greedy pairing — sample rows may need manual review)%s\n",
			Fmt.Yellow, Fmt.Reset, plan.Ambiguous, Fmt.Dim, Fmt.Reset)
	}
	if len(plan.ToAdd) > 0 {
		fmt.Println()
		fmt.Printf("    %sPartners — matched:%s   %d %s(IBAN already linked in Odoo)%s\n",
			Fmt.Green, Fmt.Reset, partners.Matched, Fmt.Dim, Fmt.Reset)
		fmt.Printf("    %sPartners — to update:%s %d %s(existing partner, link new IBAN)%s\n",
			Fmt.Yellow, Fmt.Reset, partners.ToUpdate, Fmt.Dim, Fmt.Reset)
		fmt.Printf("    %sPartners — to create:%s %d %s(no IBAN, no name match)%s\n",
			Fmt.Yellow, Fmt.Reset, partners.ToCreate, Fmt.Dim, Fmt.Reset)
		if partners.Skipped > 0 {
			fmt.Printf("    %sPartners — skipped:%s   %d %s(no IBAN, no name)%s\n",
				Fmt.Dim, Fmt.Reset, partners.Skipped, Fmt.Dim, Fmt.Reset)
		}
		if !partnerIdxLoaded {
			fmt.Printf("    %s↪ Run `chb odoo sync` first for accurate update/create counts (no local partner cache)%s\n", Fmt.Dim, Fmt.Reset)
		}
	}
}

// kbcShortPreviewTag returns a compact " [category | collective | account]"
// suffix for the short-preview lines. Skips empty fields and returns ""
// when nothing was resolved.
func kbcShortPreviewTag(p kbcMergeCSVRow) string {
	parts := make([]string, 0, 3)
	if p.Category != "" {
		parts = append(parts, p.Category)
	}
	if p.Collective != "" {
		parts = append(parts, "@"+p.Collective)
	}
	if p.AccountCode != "" {
		parts = append(parts, p.AccountCode)
	}
	if len(parts) == 0 {
		return ""
	}
	return fmt.Sprintf(" %s[%s]%s", Fmt.Cyan, strings.Join(parts, " · "), Fmt.Reset)
}

func printKBCMergePreviews(plan kbcMergePlan, acc *AccountConfig, journalID int, baseURL string) {
	renderKBCMergeTables(plan, acc, journalID, baseURL, 15)
}

func kbcMergePreviewLimit(n int) int {
	if n > 15 {
		return 15
	}
	return n
}

// printKBCMergeTables renders the full add / delete sets as tables.
// Used by --verbose; the short preview path calls renderKBCMergeTables
// with a row limit instead.
func printKBCMergeTables(plan kbcMergePlan, acc *AccountConfig, journalID int, baseURL string) {
	renderKBCMergeTables(plan, acc, journalID, baseURL, 0)
}

// renderKBCMergeTables prints the to-add and to-delete tables. limit=0
// shows all rows; limit>0 caps each table at that many rows and adds a
// "first N of M" header. Totals always reflect the full plan, not the
// truncated preview, so the operator can spot a balance mismatch even
// in the short preview.
func renderKBCMergeTables(plan kbcMergePlan, acc *AccountConfig, journalID int, baseURL string, limit int) {
	currency := accCurrency(acc)
	if len(plan.ToAdd) > 0 {
		title := fmt.Sprintf("To add (%d)", len(plan.ToAdd))
		preview := plan.ToAdd
		if limit > 0 && len(preview) > limit {
			preview = preview[:limit]
			title = fmt.Sprintf("To add (first %d of %d)", limit, len(plan.ToAdd))
		}
		fmt.Printf("\n  %s%s:%s\n", Fmt.Bold, title, Fmt.Reset)
		headers := []string{"Date", "Amount", "Counterparty", "IBAN", "Description", "Category", "Collective", "Account"}
		rows := make([][]string, 0, len(preview))
		var previewTotal, fullTotal float64
		for _, p := range preview {
			r := p.Row
			cp := r.CounterpartyName
			if cp == "" {
				cp = "-"
			}
			cat := p.Category
			if cat == "" {
				cat = "-"
			}
			coll := p.Collective
			if coll == "" {
				coll = "-"
			}
			acct := p.AccountCode
			if acct == "" {
				acct = "-"
			} else if p.AccountName != "" {
				acct = fmt.Sprintf("%s %s", acct, Truncate(p.AccountName, 18))
			}
			rows = append(rows, []string{
				r.Date,
				formatBalancePlain(r.Amount, currency),
				Truncate(cp, 22),
				Truncate(r.CounterpartyIBAN, 22),
				Truncate(r.Description, 30),
				Truncate(cat, 16),
				Truncate(coll, 14),
				Truncate(acct, 26),
			})
			previewTotal += r.Amount
		}
		for _, p := range plan.ToAdd {
			fullTotal += p.Row.Amount
		}
		total := []string{
			"",
			formatBalancePlain(fullTotal, currency),
			"", "", "", "", "", "",
		}
		renderTicketsTable(headers, rows, total, map[int]bool{1: true})
		if limit > 0 && len(plan.ToAdd) > limit {
			fmt.Printf("  %s(preview total: %s of %s)%s\n",
				Fmt.Dim,
				formatBalancePlain(previewTotal, currency),
				formatBalancePlain(fullTotal, currency),
				Fmt.Reset)
		}
		fmt.Println()
	}
	if len(plan.ToDelete) > 0 {
		title := fmt.Sprintf("To delete (%d)", len(plan.ToDelete))
		preview := plan.ToDelete
		if limit > 0 && len(preview) > limit {
			preview = preview[:limit]
			title = fmt.Sprintf("To delete (first %d of %d)", limit, len(plan.ToDelete))
		}
		fmt.Printf("\n  %s%s:%s\n", Fmt.Bold, title, Fmt.Reset)
		headers := []string{"Odoo #", "Date", "Amount", "Counterparty", "IBAN", "Description", "Reconciled"}
		rows := make([][]string, 0, len(preview))
		var previewTotal, fullTotal float64
		reconciled := 0
		for _, line := range preview {
			cp := line.CounterpartyName
			if cp == "" {
				cp = "-"
			}
			rec := ""
			if line.IsReconciled {
				rec = "yes"
			}
			rows = append(rows, []string{
				fmt.Sprintf("#%d", line.ID),
				line.Date,
				formatBalancePlain(line.Amount, currency),
				Truncate(cp, 28),
				Truncate(line.CounterpartyIBAN, 22),
				Truncate(line.PaymentRef, 50),
				rec,
			})
			previewTotal += line.Amount
		}
		for _, line := range plan.ToDelete {
			fullTotal += line.Amount
			if line.IsReconciled {
				reconciled++
			}
		}
		recLabel := ""
		if reconciled > 0 {
			recLabel = fmt.Sprintf("%d reconciled", reconciled)
		}
		total := []string{
			"",
			"",
			formatBalancePlain(fullTotal, currency),
			"", "", "", recLabel,
		}
		renderTicketsTable(headers, rows, total, map[int]bool{2: true})
		if limit > 0 && len(plan.ToDelete) > limit {
			fmt.Printf("  %s(preview total: %s of %s)%s\n",
				Fmt.Dim,
				formatBalancePlain(previewTotal, currency),
				formatBalancePlain(fullTotal, currency),
				Fmt.Reset)
		}
		if baseURL != "" {
			fmt.Printf("\n    %sInspect: %s%s\n", Fmt.Dim, OdooBankReconciliationURL(baseURL, journalID), Fmt.Reset)
		}
	}
	if plan.Ambiguous > 0 {
		fmt.Printf("\n  %s%d ambiguous key%s — multiple txs share the same (date, IBAN, amount). Greedy pairing may have paired the wrong sibling.%s\n",
			Fmt.Yellow, plan.Ambiguous, plural(plan.Ambiguous), Fmt.Reset)
	}
}

// loadKBCOdooLines fetches every statement line on the journal and joins
// in counterparty IBAN data (either from the inline `account_number`
// field or from the linked res.partner.bank).
func loadKBCOdooLines(creds *OdooCredentials, uid int, journalID int) ([]kbcMergeOdooLine, error) {
	data, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.bank.statement.line", "search_read",
		[]interface{}{[]interface{}{
			[]interface{}{"journal_id", "=", journalID},
		}},
		map[string]interface{}{
			"fields": []string{
				"id", "date", "amount", "unique_import_id",
				"payment_ref", "narration",
				"account_number", "partner_bank_id", "partner_name",
				"is_reconciled",
			},
			"order": "date asc, id asc",
			"limit": 0,
		})
	if err != nil {
		return nil, err
	}
	var raw []map[string]interface{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("parse lines: %v", err)
	}

	// Collect bank IDs we'll need to resolve to IBANs.
	bankIDs := map[int]bool{}
	for _, row := range raw {
		if id := odooFieldID(row["partner_bank_id"]); id > 0 {
			bankIDs[id] = true
		}
	}
	bankIBAN := map[int]string{}
	if len(bankIDs) > 0 {
		ids := make([]interface{}, 0, len(bankIDs))
		for id := range bankIDs {
			ids = append(ids, id)
		}
		bankData, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
			"res.partner.bank", "read",
			[]interface{}{ids},
			map[string]interface{}{"fields": []string{"id", "acc_number"}})
		if err == nil {
			var banks []map[string]interface{}
			_ = json.Unmarshal(bankData, &banks)
			for _, b := range banks {
				id := int(odooFloat(b["id"]))
				if id == 0 {
					continue
				}
				bankIBAN[id] = kbcbrusselssource.NormalizeIBAN(odooString(b["acc_number"]))
			}
		}
	}

	out := make([]kbcMergeOdooLine, 0, len(raw))
	for _, row := range raw {
		amount := odooFloat(row["amount"])
		iban := kbcbrusselssource.NormalizeIBAN(odooString(row["account_number"]))
		if iban == "" {
			if id := odooFieldID(row["partner_bank_id"]); id > 0 {
				iban = bankIBAN[id]
			}
		}
		// Odoo dates come back as "YYYY-MM-DD" or full datetime; keep the
		// date prefix only to align with the CSV's date format.
		date := odooString(row["date"])
		if len(date) >= 10 {
			date = date[:10]
		}
		out = append(out, kbcMergeOdooLine{
			ID:               int(odooFloat(row["id"])),
			Date:             date,
			Amount:           amount,
			AmountCents:      int64(math.Round(amount * 100)),
			CounterpartyIBAN: iban,
			CounterpartyName: odooString(row["partner_name"]),
			PaymentRef:       odooString(row["payment_ref"]),
			Narration:        odooString(row["narration"]),
			ImportID:         odooString(row["unique_import_id"]),
			IsReconciled:     odooBool(row["is_reconciled"]),
		})
	}
	return out, nil
}

func buildKBCImportID(iban, hash string) string {
	return fmt.Sprintf("kbcbrussels:%s:%s",
		strings.ToLower(strings.ReplaceAll(iban, " ", "")),
		strings.ToLower(hash))
}

// applyKBCMerge performs the writes for a real (non-dry-run) merge.
// Matched lines are intentionally not modified — they already carry an
// import-id from the original importer and other tools may rely on it.
// Errors abort early; merge is idempotent so re-running picks up from
// wherever the previous attempt stopped.
//
// Returns the number of statement lines actually created (used by the
// caller to decide whether to auto-reconcile based on batch size).
func applyKBCMerge(creds *OdooCredentials, uid int, journalID int, acc *AccountConfig, plan kbcMergePlan) (int, error) {
	created := 0
	if len(plan.ToAdd) > 0 {
		fmt.Printf("\n  %sCreating %s in Odoo…%s\n", Fmt.Dim, Pluralize(len(plan.ToAdd), "line", ""), Fmt.Reset)
		partnerCache := map[string]int{}
		batch := make([]map[string]interface{}, 0, len(plan.ToAdd))
		// Map importID → account code so we can apply rule-driven
		// account writes after the batch create returns IDs.
		ruleAccountByImportID := map[string]string{}
		for _, p := range plan.ToAdd {
			bankID, partnerID, err := resolveOrCreateKBCPartnerBank(creds, uid, p.Row, &partnerCache)
			if err != nil {
				Warnf("    %s⚠ partner lookup for %s: %v%s", Fmt.Yellow, kbcMergePaymentRef(p.Row), err, Fmt.Reset)
			}
			if p.MappingPartnerID > 0 {
				partnerID = p.MappingPartnerID
			}

			line := map[string]interface{}{
				"journal_id":       journalID,
				"date":             p.Row.Date,
				"amount":           p.Row.Amount,
				"payment_ref":      kbcMergePaymentRef(p.Row),
				"narration":        kbcMergeNarration(p.Row),
				"unique_import_id": p.ImportID,
			}
			if p.Row.CounterpartyIBAN != "" {
				line["account_number"] = p.Row.CounterpartyIBAN
			}
			name := strings.TrimSpace(p.Row.CounterpartyName)
			if name == "" {
				name = kbcbrusselssource.MerchantFromDescription(p.Row.Description)
			}
			if name != "" {
				line["partner_name"] = name
			}
			if partnerID > 0 {
				line["partner_id"] = partnerID
			}
			if bankID > 0 {
				line["partner_bank_id"] = bankID
			}
			batch = append(batch, line)
			if p.AccountCode != "" {
				ruleAccountByImportID[p.ImportID] = p.AccountCode
			}
		}
		result, err := batchCreateStatementLinesWithProgressReport(creds, uid, batch, "kbc merge")
		if err != nil {
			return created, fmt.Errorf("create new lines: %v", err)
		}
		created = len(result.IDs)
		fmt.Printf("  %s✓ Created %d/%d line%s%s\n", Fmt.Green, len(result.IDs), len(batch),
			plural(len(batch)), Fmt.Reset)
		if len(result.Failures) > 0 {
			for _, f := range result.Failures {
				Warnf("    %s⚠ %s: %s%s", Fmt.Yellow, f.ImportID, f.Reason, Fmt.Reset)
			}
		}
		// Apply rule-driven account codes. We re-fetch line IDs by
		// unique_import_id so failed creates don't shift the mapping.
		if len(ruleAccountByImportID) > 0 && len(result.IDs) > 0 {
			if err := applyKBCRuleAccounts(creds, uid, journalID, ruleAccountByImportID); err != nil {
				Warnf("  %s⚠ apply rule accounts: %v%s", Fmt.Yellow, err, Fmt.Reset)
			}
		}
	}

	if len(plan.ToDelete) > 0 {
		fmt.Printf("\n  %sDeleting %s from Odoo…%s\n", Fmt.Dim, Pluralize(len(plan.ToDelete), "orphan line", ""), Fmt.Reset)
		ids := make([]int, 0, len(plan.ToDelete))
		for _, line := range plan.ToDelete {
			if line.IsReconciled {
				Warnf("    %s⚠ skip reconciled line #%d (%s %.2f)%s",
					Fmt.Yellow, line.ID, line.Date, line.Amount, Fmt.Reset)
				continue
			}
			ids = append(ids, line.ID)
		}
		if len(ids) > 0 {
			ifaceIDs := make([]interface{}, len(ids))
			for i, id := range ids {
				ifaceIDs[i] = id
			}
			if _, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
				"account.bank.statement.line", "unlink",
				[]interface{}{ifaceIDs}, nil); err != nil {
				return created, fmt.Errorf("unlink orphan lines: %v", err)
			}
		}
		fmt.Printf("  %s✓ Deleted %d/%d orphan line%s%s\n",
			Fmt.Green, len(ids), len(plan.ToDelete), plural(len(plan.ToDelete)), Fmt.Reset)
	}

	UpdateSyncSource(fmt.Sprintf("odoo:journal:%d", journalID), false)
	_ = time.Now()
	return created, nil
}

func kbcMergePaymentRef(row kbcbrusselssource.Transaction) string {
	if s := kbcbrusselssource.PreferredDescription(row); s != "" {
		return s
	}
	if row.CounterpartyName != "" {
		return row.CounterpartyName
	}
	if row.CounterpartyIBAN != "" {
		return row.CounterpartyIBAN
	}
	return ""
}

func kbcMergeNarration(row kbcbrusselssource.Transaction) string {
	details := map[string]interface{}{
		"description":     row.Description,
		"statementNumber": row.StatementNumber,
		"balance":         row.Balance,
	}
	if row.CounterpartyIBAN != "" {
		details["counterpartyIban"] = row.CounterpartyIBAN
	}
	if row.CounterpartyName != "" {
		details["counterpartyName"] = row.CounterpartyName
	}
	if row.StandardReference != "" {
		details["reference"] = row.StandardReference
	}
	if row.FreeReference != "" {
		details["freeReference"] = row.FreeReference
	}
	data, _ := json.Marshal(details)
	return string(data)
}

func plural(n int) string {
	if n == 1 {
		return ""
	}
	return "s"
}

// kbcMergeContext bundles the per-merge shared state (mappings,
// categorizer, partner cache, internal-account set) so each row processing
// call has everything it needs without re-loading the same files.
type kbcMergeContext struct {
	odooMappings []OdooMapping
	categorizer  *Categorizer
	partnerCache map[string]int
	// internalIBANs holds normalized IBANs of every CHB-linked account
	// other than the one we're merging into. A CSV row whose counterparty
	// IBAN appears here is a transfer between two accounts the operator
	// owns — category becomes "internal_transfer" so the matching Odoo
	// mapping routes it to account 580001.
	internalIBANs map[string]bool
}

func newKBCMergeContext(acc *AccountConfig) kbcMergeContext {
	mappings, _ := LoadOdooMappings()
	ctx := kbcMergeContext{
		odooMappings:  mappings,
		categorizer:   NewCategorizer(nil),
		partnerCache:  map[string]int{},
		internalIBANs: map[string]bool{},
	}
	selfIBAN := strings.ToLower(strings.ReplaceAll(strings.TrimSpace(acc.IBAN), " ", ""))
	for _, other := range LoadAccountConfigs() {
		iban := strings.ToLower(strings.ReplaceAll(strings.TrimSpace(other.IBAN), " ", ""))
		if iban == "" || iban == selfIBAN {
			continue
		}
		ctx.internalIBANs[iban] = true
	}
	return ctx
}

// annotateKBCMergeRowWithMapping resolves the matched OdooMapping for a
// row and writes the resulting category/account/partner onto the row in
// place. Mirrors what `chb generate` does for transactions.json — the
// load-bearing fields (Category/Collective/AccountCode/PartnerID) go
// through applyOdooMapping so KBC merge produces the same values
// generate would write. The matched lookup that follows is only used to
// pull the cosmetic AccountName / PartnerName labels for the preview
// table.
func annotateKBCMergeRowWithMapping(row *kbcMergeCSVRow, acc *AccountConfig, ctx *kbcMergeContext) {
	ruleTx := kbcRowToTransactionEntry(*acc, row.Row)
	ctx.categorizer.Apply(&ruleTx)
	if ctx.isInternalTransfer(row.Row) {
		ruleTx.Category = "internal_transfer"
	}
	applyOdooMapping(ctx.odooMappings, &ruleTx)
	row.Category = ruleTx.Category
	row.Collective = ruleTx.Collective
	row.AccountCode = ruleTx.AccountCode
	row.MappingPartnerID = ruleTx.PartnerID
	// Cosmetic labels for the --verbose table only.
	if matched := LookupOdooMapping(ctx.odooMappings, ruleTx); matched != nil {
		row.AccountName = matched.Set.AccountName
		row.MappingPartnerName = matched.Set.PartnerName
	}
}

func (c *kbcMergeContext) isInternalTransfer(row kbcbrusselssource.Transaction) bool {
	if len(c.internalIBANs) == 0 {
		return false
	}
	iban := strings.ToLower(strings.ReplaceAll(row.CounterpartyIBAN, " ", ""))
	return iban != "" && c.internalIBANs[iban]
}

// resolveOrCreateKBCPartnerBank returns (partnerBankID, partnerID) for
// the row's counterparty, creating Odoo records as needed:
//
//  1. If a res.partner.bank already exists for this IBAN, use it (and
//     its linked partner).
//  2. Else find/create the partner by name via resolveOdooPartner, then
//     create the res.partner.bank record linking that partner to this
//     IBAN. This "updates" an existing partner by adding the IBAN.
//
// Returns (0, 0, nil) if the row has no IBAN AND no name — we can't
// reasonably attach a partner.
func resolveOrCreateKBCPartnerBank(creds *OdooCredentials, uid int, row kbcbrusselssource.Transaction, cache *map[string]int) (int, int, error) {
	iban := kbcbrusselssource.NormalizeIBAN(row.CounterpartyIBAN)
	name := strings.TrimSpace(row.CounterpartyName)
	if name == "" {
		// Card-payment rows arrive with no counterparty name. Recover the
		// merchant from the description so we can still link/create a
		// real partner instead of dropping the line on the floor.
		name = kbcbrusselssource.MerchantFromDescription(row.Description)
	}
	if iban == "" && name == "" {
		return 0, 0, nil
	}

	// 1. IBAN already in Odoo → reuse the partner_bank and its partner.
	// This is the canonical path: when the bank account is known, the
	// linked partner WINS over name-matching. Names are noisy (KBC
	// shortens them, capitalises differently, etc.); the IBAN is stable.
	if iban != "" {
		bankID, partnerID, err := findOdooPartnerBankByAccountNumber(creds, uid, iban)
		if err != nil {
			return 0, 0, err
		}
		if bankID > 0 && partnerID > 0 {
			return bankID, partnerID, nil
		}
		// Bank row exists but with no partner — extremely rare. Fall
		// through to name resolution but skip the create-bank step so
		// we don't trip the unique (partner, acc_number) constraint.
		if bankID > 0 && partnerID == 0 {
			return bankID, 0, nil
		}
	}

	// 2. No bank by IBAN — find or create the partner by name.
	if name == "" {
		return 0, 0, nil
	}
	cacheMap := *cache
	if cacheMap == nil {
		cacheMap = map[string]int{}
		*cache = cacheMap
	}
	partnerID := resolveOdooPartner(creds, uid, name, "", "", "", false, cacheMap)
	if partnerID == 0 {
		return 0, 0, nil
	}
	if iban == "" {
		return 0, partnerID, nil
	}
	// 3. Attach the IBAN to that partner so future lookups hit step (1).
	bankID, err := createOdooPartnerBank(creds, uid, partnerID, iban)
	if err != nil {
		return 0, partnerID, fmt.Errorf("link IBAN %s to partner #%d: %v", iban, partnerID, err)
	}
	return bankID, partnerID, nil
}

// applyKBCRuleAccounts rewrites the counterpart account.move.line for
// each created line whose CSV row matched an Odoo rule with a non-empty
// AccountCode. Uses unique_import_id to map back from creation order to
// the right line — robust against partial-failure shifts.
func applyKBCRuleAccounts(creds *OdooCredentials, uid int, journalID int, accountByImportID map[string]string) error {
	importIDs := make([]string, 0, len(accountByImportID))
	for id := range accountByImportID {
		importIDs = append(importIDs, id)
	}
	rows, err := fetchOdooStatementLinesByImportID(creds, uid, importIDs)
	if err != nil {
		return err
	}
	// Group lines by account code so we batch the per-code account writes.
	byCode := map[string][]int{}
	for importID, code := range accountByImportID {
		if code == "" {
			continue
		}
		row := rows[importID]
		if row == nil {
			continue
		}
		lineID := odooInt(row["id"])
		if lineID == 0 {
			continue
		}
		// Confirm the journal — defensive in case importIDs collide.
		if jid := odooFieldID(row["journal_id"]); jid > 0 && jid != journalID {
			continue
		}
		byCode[code] = append(byCode[code], lineID)
	}
	// applyOdooMappingAccount now batches the draft → write → post pass
	// per account-code group, so this step is ~3 RPCs per code regardless
	// of how many lines share that code (down from the old per-line
	// ~5 RPCs per line). On a 188-line KBC journal with ~10 distinct
	// codes that's ~30 RPCs instead of ~940.
	totalLines := 0
	codes := make([]string, 0, len(byCode))
	for code, ids := range byCode {
		totalLines += len(ids)
		codes = append(codes, code)
	}
	sort.Strings(codes)
	if totalLines == 0 {
		return nil
	}
	fmt.Printf("  %sApplying rule-driven account codes to %d line%s across %d code%s…%s\n",
		Fmt.Dim, totalLines, plural(totalLines), len(codes), plural(len(codes)), Fmt.Reset)
	status := newStatusLine()
	defer status.Clear()
	done := 0
	for _, code := range codes {
		ids := byCode[code]
		status.Update("  account %s — line %d/%d total", code, done, totalLines)
		if err := applyOdooMappingAccount(creds, uid, ids, code, status); err != nil {
			return fmt.Errorf("set account %s on %d line(s): %v", code, len(ids), err)
		}
		done += len(ids)
		status.Update("  account %s done (%d/%d total)", code, done, totalLines)
	}
	return nil
}
