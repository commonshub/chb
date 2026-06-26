package cmd

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"

	stripesource "github.com/CommonsHub/chb/providers/stripe"
)

// noPartnerLine is a journal statement line that has no partner linked, with the
// counterparty signals (name + "bank account number") used to resolve one. For
// bank journals these come from the imported partner_name / account_number
// fields; for Stripe they're the customer name and the Stripe customer id (which
// is stored as a res.partner.bank acc_number so future lines match on it).
type noPartnerLine struct {
	ID            int
	Name          string
	AccountNumber string
	IsReconciled  bool
}

// existingLink is a no-partner line resolved to an existing partner, plus an
// optional bank account to attach to that partner (set when the match was by
// name and the line carries an account the partner doesn't have yet — e.g. a
// Stripe customer id, or a counterparty IBAN).
type existingLink struct {
	PartnerID     int
	AttachAccount string
}

// partnerLinkNewGroup is one new partner to create, plus the lines that will
// link to it. Lines are grouped by counterparty name (or bank account when the
// name is blank) so the same counterparty yields a single new partner carrying
// all of its bank accounts.
type partnerLinkNewGroup struct {
	Name     string
	Accounts map[string]bool // raw account numbers to attach
	LineIDs  []int
}

type partnerLinkPlan struct {
	NoPartner   int
	ToExisting  map[int]existingLink            // lineID -> existing partner (+ account to attach)
	NewGroups   map[string]*partnerLinkNewGroup // group key -> group
	Unmatchable []int                           // lineIDs with neither name nor account
}

func (p *partnerLinkPlan) existingPartnerCount() int {
	seen := map[int]bool{}
	for _, link := range p.ToExisting {
		seen[link.PartnerID] = true
	}
	return len(seen)
}

func (p *partnerLinkPlan) newLineCount() int {
	n := 0
	for _, g := range p.NewGroups {
		n += len(g.LineIDs)
	}
	return n
}

func (p *partnerLinkPlan) hasWork() bool {
	return len(p.ToExisting) > 0 || len(p.NewGroups) > 0
}

// fetchNoPartnerLines returns the journal's statement lines with no partner_id.
// For a Stripe journal the counterparty isn't in the bank fields, so it derives
// the name + "account" (the Stripe customer id) from the line's narration.
func fetchNoPartnerLines(creds *OdooCredentials, uid int, journalID int, acc *AccountConfig) ([]noPartnerLine, error) {
	isStripe := acc != nil && acc.Provider == "stripe"
	fields := []string{"id", "partner_name", "account_number", "is_reconciled"}
	if isStripe {
		fields = append(fields, "narration")
	}
	rows, err := odooSearchReadAllMaps(creds, uid, "account.bank.statement.line",
		[]interface{}{
			[]interface{}{"journal_id", "=", journalID},
			[]interface{}{"partner_id", "=", false},
		},
		fields, "date asc, id asc")
	if err != nil {
		return nil, err
	}
	out := make([]noPartnerLine, 0, len(rows))
	for _, r := range rows {
		ln := noPartnerLine{
			ID:            odooInt(r["id"]),
			Name:          strings.TrimSpace(odooString(r["partner_name"])),
			AccountNumber: strings.TrimSpace(odooString(r["account_number"])),
			IsReconciled:  odooBool(r["is_reconciled"]),
		}
		if isStripe {
			if name, custID := stripeCustomerFromNarration(odooString(r["narration"])); name != "" || custID != "" {
				if name != "" {
					ln.Name = name
				}
				// The Stripe customer id is the "bank account number" we match
				// on and store on the partner.
				if custID != "" {
					ln.AccountNumber = custID
				}
			}
		}
		out = append(out, ln)
	}
	return out, nil
}

// attachStripeCustomerIDs ensures every Stripe charge line's partner carries the
// charge's Stripe customer id as a res.partner.bank acc_number — so the customer
// is durably linked to the partner and future lines match on the id. It joins
// the local charge archive's customer-ids to Odoo partner-ids by import-id, then
// batch-checks existing banks and creates only the missing ones.
func attachStripeCustomerIDs(creds *OdooCredentials, uid int, journalID int, acc *AccountConfig) error {
	if acc == nil || acc.Provider != "stripe" {
		return nil
	}
	bts, err := stripesource.LoadTransactionsSince(DataDir(), acc.AccountID, 0)
	if err != nil {
		return err
	}
	chargeIndex := loadArchivedStripeCharges(DataDir())
	custByImport := map[string]string{} // lowercased importID -> customerID
	for _, bt := range bts {
		bt = enrichStripeBTFromCharge(bt, chargeIndex)
		if cid := stringMetadata(bt.Metadata, "stripeCustomerId"); cid != "" {
			if id := stripeBTImportID(acc, bt); id != "" {
				custByImport[strings.ToLower(id)] = cid
			}
		}
	}
	if len(custByImport) == 0 {
		return nil
	}
	rows, err := odooSearchReadAllMaps(creds, uid, "account.bank.statement.line",
		[]interface{}{[]interface{}{"journal_id", "=", journalID}},
		[]string{"id", "unique_import_id", "partner_id"}, "id asc")
	if err != nil {
		return err
	}
	wantByPartner := map[int]map[string]bool{} // partnerID -> customer-ids to ensure
	allCust := map[string]bool{}
	for _, r := range rows {
		pid := odooFieldID(r["partner_id"])
		if pid <= 0 {
			continue
		}
		cid := custByImport[strings.ToLower(odooString(r["unique_import_id"]))]
		if cid == "" {
			continue
		}
		if wantByPartner[pid] == nil {
			wantByPartner[pid] = map[string]bool{}
		}
		wantByPartner[pid][cid] = true
		allCust[cid] = true
	}
	if len(allCust) == 0 {
		return nil
	}
	// Batch-check existing res.partner.bank rows for these customer ids.
	custList := make([]interface{}, 0, len(allCust))
	for c := range allCust {
		custList = append(custList, c)
	}
	existingByAcct := map[string]int{} // acc_number -> partnerID
	for start := 0; start < len(custList); start += 200 {
		end := start + 200
		if end > len(custList) {
			end = len(custList)
		}
		brows, berr := odooSearchReadAllMaps(creds, uid, "res.partner.bank",
			[]interface{}{[]interface{}{"acc_number", "in", custList[start:end]}},
			[]string{"acc_number", "partner_id"}, "id asc")
		if berr != nil {
			continue
		}
		for _, br := range brows {
			existingByAcct[odooString(br["acc_number"])] = odooFieldID(br["partner_id"])
		}
	}
	pids := make([]int, 0, len(wantByPartner))
	for pid := range wantByPartner {
		pids = append(pids, pid)
	}
	sort.Ints(pids)
	created, present := 0, 0
	for _, pid := range pids {
		for cid := range wantByPartner[pid] {
			if _, ok := existingByAcct[cid]; ok {
				present++ // already linked (to this or another partner) — leave it
				continue
			}
			if _, err := createOdooPartnerBank(creds, uid, pid, cid); err != nil {
				continue
			}
			existingByAcct[cid] = pid
			created++
		}
	}
	if created > 0 || present > 0 {
		fmt.Printf("    %s→ Stripe customer ids: attached %d, %d already linked%s\n", Fmt.Dim, created, present, Fmt.Reset)
	}
	return nil
}

// stripeCustomerFromNarration extracts the customer name and Stripe customer id
// from a statement line's JSON narration (written by buildStripeOdooNarration).
// Odoo stores narration as HTML, so it arrives wrapped in <p>…</p>; strip the
// tags before parsing the JSON.
func stripeCustomerFromNarration(narration string) (name, customerID string) {
	narration = stripHTMLTags(narration)
	if strings.TrimSpace(narration) == "" {
		return "", ""
	}
	var meta map[string]interface{}
	if err := json.Unmarshal([]byte(narration), &meta); err != nil {
		return "", ""
	}
	return metaString(meta, "customerName"), stripeCustomerIDFromLineMetadata(meta)
}

var htmlTagPattern = regexp.MustCompile(`<[^>]+>`)

// stripHTMLTags removes HTML tags (e.g. Odoo's <p> wrapper around a JSON
// narration) and trims surrounding whitespace.
func stripHTMLTags(s string) string {
	return strings.TrimSpace(htmlTagPattern.ReplaceAllString(s, ""))
}

// findOdooPartnerByExactName returns the lowest-id partner whose name matches
// case-insensitively (=ilike), plus the number of matches, or 0 when none.
func findOdooPartnerByExactName(creds *OdooCredentials, uid int, name string) (int, int, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return 0, 0, nil
	}
	rows, err := odooSearchReadAllMaps(creds, uid, "res.partner",
		[]interface{}{[]interface{}{"name", "=ilike", name}},
		[]string{"id"}, "id asc")
	if err != nil {
		return 0, 0, err
	}
	if len(rows) == 0 {
		return 0, 0, nil
	}
	return odooInt(rows[0]["id"]), len(rows), nil
}

// planPartnerLinks resolves every no-partner line: a perfect bank-account match
// or a case-insensitive name match links to an existing partner; otherwise the
// line is grouped under a new partner to create. bankResolver maps a raw
// account number to an existing partner id (0 = none); nameResolver maps a name
// likewise. Both results are cached by normalized account / lowercased name so a
// journal with many lines but few counterparties issues each lookup once.
func planPartnerLinks(lines []noPartnerLine, bankResolver, nameResolver func(string) int) partnerLinkPlan {
	plan := partnerLinkPlan{
		NoPartner:  len(lines),
		ToExisting: map[int]existingLink{},
		NewGroups:  map[string]*partnerLinkNewGroup{},
	}
	acctCache := map[string]int{} // normalized account -> partnerID (0 = looked up, none)
	nameCache := map[string]int{} // lowercased name -> partnerID

	for _, ln := range lines {
		acct := normalizeBankAccountNumber(ln.AccountNumber)
		name := ln.Name

		// 1. Perfect bank-account match.
		pid := 0
		matchedByBank := false
		if acct != "" {
			if v, ok := acctCache[acct]; ok {
				pid = v
			} else {
				pid = bankResolver(ln.AccountNumber)
				acctCache[acct] = pid
			}
			matchedByBank = pid > 0
		}
		// 2. Case-insensitive name match.
		if pid == 0 && name != "" {
			key := strings.ToLower(name)
			if v, ok := nameCache[key]; ok {
				pid = v
			} else {
				pid = nameResolver(name)
				nameCache[key] = pid
			}
		}
		if pid > 0 {
			link := existingLink{PartnerID: pid}
			// Matched by name but the line carries an account the partner is
			// not yet known to hold (the bank lookup missed) → attach it, so a
			// Stripe customer id / counterparty IBAN sticks to the partner.
			if !matchedByBank && ln.AccountNumber != "" {
				link.AttachAccount = ln.AccountNumber
			}
			plan.ToExisting[ln.ID] = link
			continue
		}

		// 3. Needs a new partner. A line with neither signal can't be linked.
		if name == "" && acct == "" {
			plan.Unmatchable = append(plan.Unmatchable, ln.ID)
			continue
		}
		displayName := name
		gkey := "name:" + strings.ToLower(name)
		if name == "" {
			displayName = ln.AccountNumber // name the partner after its account
			gkey = "acct:" + acct
		}
		g := plan.NewGroups[gkey]
		if g == nil {
			g = &partnerLinkNewGroup{Name: displayName, Accounts: map[string]bool{}}
			plan.NewGroups[gkey] = g
		}
		if ln.AccountNumber != "" {
			g.Accounts[ln.AccountNumber] = true
		}
		g.LineIDs = append(g.LineIDs, ln.ID)
	}
	return plan
}

// printPartnerLinkSummary shows the headline the user confirms against.
func printPartnerLinkSummary(plan partnerLinkPlan) {
	fmt.Printf("\n  %sPartner linking%s\n", Fmt.Bold, Fmt.Reset)
	fmt.Printf("    %s%s without a partner%s\n", Fmt.Dim, Pluralize(plan.NoPartner, "line", ""), Fmt.Reset)
	if len(plan.ToExisting) > 0 {
		fmt.Printf("    → linking %s to %s\n",
			Pluralize(len(plan.ToExisting), "line", ""), Pluralize(plan.existingPartnerCount(), "existing partner", ""))
	}
	if len(plan.NewGroups) > 0 {
		fmt.Printf("    → creating %s for %s\n",
			Pluralize(len(plan.NewGroups), "new partner", ""), Pluralize(plan.newLineCount(), "line", ""))
	}
	if len(plan.Unmatchable) > 0 {
		fmt.Printf("    %s→ %s with no name or account — skipped%s\n",
			Fmt.Dim, Pluralize(len(plan.Unmatchable), "line", ""), Fmt.Reset)
	}
}

// writePartnerOnLines sets partner_id on the given statement lines, chunked.
func writePartnerOnLines(creds *OdooCredentials, uid int, lineIDs []int, partnerID int) error {
	const chunk = 200
	for start := 0; start < len(lineIDs); start += chunk {
		end := start + chunk
		if end > len(lineIDs) {
			end = len(lineIDs)
		}
		if _, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
			"account.bank.statement.line", "write",
			[]interface{}{intsToInterfaces(lineIDs[start:end]), map[string]interface{}{"partner_id": partnerID}}, nil); err != nil {
			return err
		}
	}
	return nil
}

// applyPartnerLinks creates the new partners (+ their bank accounts) and writes
// partner_id on every planned line, batched per partner.
func applyPartnerLinks(creds *OdooCredentials, uid int, plan partnerLinkPlan, status *statusLine) (linked, created int) {
	// New partners first.
	keys := make([]string, 0, len(plan.NewGroups))
	for k := range plan.NewGroups {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for i, k := range keys {
		g := plan.NewGroups[k]
		if status != nil {
			status.Update("Creating partner %d/%d: %s", i+1, len(keys), g.Name)
		}
		pid, err := createOdooPartner(creds, uid, g.Name)
		if err != nil || pid == 0 {
			Warnf("  %s⚠ Could not create partner %q: %v%s", Fmt.Yellow, g.Name, err, Fmt.Reset)
			continue
		}
		created++
		for acct := range g.Accounts {
			if _, err := createOdooPartnerBank(creds, uid, pid, acct); err != nil {
				Warnf("  %s⚠ Could not attach account %s to %q: %v%s", Fmt.Yellow, acct, g.Name, err, Fmt.Reset)
			}
		}
		if err := writePartnerOnLines(creds, uid, g.LineIDs, pid); err != nil {
			Warnf("  %s⚠ Could not link %s to new partner %q: %v%s", Fmt.Yellow, Pluralize(len(g.LineIDs), "line", ""), g.Name, err, Fmt.Reset)
			continue
		}
		linked += len(g.LineIDs)
	}

	// Existing partners, grouped so each gets a single batched write. Collect
	// any accounts to attach (deduped) per partner.
	byPartner := map[int][]int{}
	attachByPartner := map[int]map[string]bool{}
	for lineID, link := range plan.ToExisting {
		byPartner[link.PartnerID] = append(byPartner[link.PartnerID], lineID)
		if link.AttachAccount != "" {
			if attachByPartner[link.PartnerID] == nil {
				attachByPartner[link.PartnerID] = map[string]bool{}
			}
			attachByPartner[link.PartnerID][link.AttachAccount] = true
		}
	}
	pids := make([]int, 0, len(byPartner))
	for pid := range byPartner {
		pids = append(pids, pid)
	}
	sort.Ints(pids)
	for _, pid := range pids {
		if status != nil {
			status.Update("Linking %s to existing partner #%d", Pluralize(len(byPartner[pid]), "line", ""), pid)
		}
		for acct := range attachByPartner[pid] {
			if _, err := createOdooPartnerBank(creds, uid, pid, acct); err != nil {
				Warnf("  %s⚠ Could not attach account %s to partner #%d: %v%s", Fmt.Yellow, acct, pid, err, Fmt.Reset)
			}
		}
		if err := writePartnerOnLines(creds, uid, byPartner[pid], pid); err != nil {
			Warnf("  %s⚠ Could not link %s to partner #%d: %v%s", Fmt.Yellow, Pluralize(len(byPartner[pid]), "line", ""), pid, err, Fmt.Reset)
			continue
		}
		linked += len(byPartner[pid])
	}
	return linked, created
}

// linkStatementLinePartners is the reconcile pre-pass that ensures every journal
// line is linked to a partner: a perfect bank-account match or a case-insensitive
// name match links to an existing partner, otherwise a new partner is created
// (carrying the bank account number). Previews a summary and, in live mode,
// confirms before writing. No-op when every line already has a partner.
func linkStatementLinePartners(creds *OdooCredentials, uid int, journalID int, assumeYes, dryRun bool) error {
	Progress("checking partner links")
	acc := linkedAccountForJournal(journalID)
	lines, err := fetchNoPartnerLines(creds, uid, journalID, acc)
	if err != nil {
		return fmt.Errorf("fetch lines without a partner: %v", err)
	}
	if len(lines) == 0 {
		return nil
	}
	bankResolver := func(acc string) int {
		_, pid, err := findOdooPartnerBankByAccountNumber(creds, uid, acc)
		if err != nil {
			return 0
		}
		return pid
	}
	nameResolver := func(name string) int {
		pid, _, err := findOdooPartnerByExactName(creds, uid, name)
		if err != nil {
			return 0
		}
		return pid
	}
	plan := planPartnerLinks(lines, bankResolver, nameResolver)
	if !plan.hasWork() {
		// Lines without a partner exist but none carry a name/account to match on.
		printPartnerLinkSummary(plan)
		return nil
	}
	printPartnerLinkSummary(plan)

	if dryRun {
		fmt.Printf("    %s(dry-run) no partners created or linked%s\n", Fmt.Dim, Fmt.Reset)
		return nil
	}

	proceed := assumeYes
	if !assumeYes && isInteractiveTTY() {
		fmt.Printf("\n  %sCreate %s and link %s?%s [Y/n] ",
			Fmt.Bold, Pluralize(len(plan.NewGroups), "new partner", ""),
			Pluralize(len(plan.ToExisting)+plan.newLineCount(), "line", ""), Fmt.Reset)
		reader := bufio.NewReader(os.Stdin)
		resp, _ := reader.ReadString('\n')
		resp = strings.TrimSpace(strings.ToLower(resp))
		proceed = resp != "n" && resp != "no"
	}
	if !proceed {
		fmt.Printf("  %sPartner linking skipped.%s\n", Fmt.Dim, Fmt.Reset)
		return nil
	}

	status := newStatusLine()
	linked, created := applyPartnerLinks(creds, uid, plan, status)
	status.Clear()
	fmt.Printf("  %s✓ Linked %s, created %s%s\n",
		Fmt.Green, Pluralize(linked, "line", ""), Pluralize(created, "partner", ""), Fmt.Reset)
	return nil
}
