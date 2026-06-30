package cmd

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	odoosource "github.com/CommonsHub/chb/providers/odoo"
)

// PartnerMerge is one recorded "these contacts are the same" decision. It's
// stored locally (decoupled from Odoo) — like the Nostr outbox — and applied to
// Odoo only on an explicit push. Until AppliedAt is set, it's a pending change.
type PartnerMerge struct {
	SurvivorID   int      `json:"survivorId"`
	SurvivorName string   `json:"survivorName,omitempty"`
	MergedIDs    []int    `json:"mergedIds"`
	MergedNames  []string `json:"mergedNames,omitempty"`
	CreatedAt    string   `json:"createdAt"`
	AppliedAt    string   `json:"appliedAt,omitempty"`
}

type partnerMergeFile struct {
	Merges []PartnerMerge `json:"merges"`
}

// partnerMergesPath is the local pending-merges store. Lives under the data
// dir's odoo pending tree (timeless), alongside the other "to push" state.
func partnerMergesPath() string {
	return odoosource.Path(DataDir(), "latest", "", "pending", "partner-merges.json")
}

func loadPartnerMerges() []PartnerMerge {
	data, err := os.ReadFile(partnerMergesPath())
	if err != nil {
		return nil
	}
	var f partnerMergeFile
	if json.Unmarshal(data, &f) != nil {
		return nil
	}
	return f.Merges
}

func savePartnerMerges(merges []PartnerMerge) error {
	p := partnerMergesPath()
	if err := os.MkdirAll(filepath.Dir(p), 0755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(partnerMergeFile{Merges: merges}, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(p, data, 0644)
}

func pendingPartnerMerges() []PartnerMerge {
	var out []PartnerMerge
	for _, m := range loadPartnerMerges() {
		if m.AppliedAt == "" {
			out = append(out, m)
		}
	}
	return out
}

// recordPartnerMerge appends a merge of victims into survivor. Idempotent-ish:
// it just records intent; aggregation and apply read it back.
func recordPartnerMerge(survivorID int, survivorName string, victimIDs []int, victimNames []string) error {
	merges := loadPartnerMerges()
	merges = append(merges, PartnerMerge{
		SurvivorID:   survivorID,
		SurvivorName: survivorName,
		MergedIDs:    victimIDs,
		MergedNames:  victimNames,
		CreatedAt:    time.Now().UTC().Format(time.RFC3339),
	})
	return savePartnerMerges(merges)
}

// partnerCanonical returns a victim-id → survivor-id map, resolved transitively
// across all recorded merges (applied or not), so any merged contact resolves
// to the single survivor it belongs to.
func partnerCanonical() map[int]int {
	direct := map[int]int{}
	for _, m := range loadPartnerMerges() {
		for _, v := range m.MergedIDs {
			if v != m.SurvivorID {
				direct[v] = m.SurvivorID
			}
		}
	}
	var resolve func(id int, seen map[int]bool) int
	resolve = func(id int, seen map[int]bool) int {
		if s, ok := direct[id]; ok && !seen[id] {
			seen[id] = true
			return resolve(s, seen)
		}
		return id
	}
	canon := map[int]int{}
	for v := range direct {
		canon[v] = resolve(v, map[int]bool{})
	}
	return canon
}

// ── Aggregation across merge groups ──────────────────────────────────────

// contactsContext holds everything needed to aggregate a contact's records,
// loaded once (it's heavy). Reused by `chb contacts` and the merge TUI.
type contactsContext struct {
	idx      *odooPartnerIndex
	invoices []moveRow
	bills    []moveRow
	txs      []TransactionEntry
	canon    map[int]int   // victim -> survivor
	members  map[int][]int // survivor -> victim ids (excludes the survivor itself)
}

func loadContactsContext(withTxs bool) *contactsContext {
	c := &contactsContext{idx: loadLatestOdooPartnerIndex(DataDir()), canon: partnerCanonical()}
	c.invoices, _ = loadMoveRows(moveKindInvoice, "", "")
	c.bills, _ = loadMoveRows(moveKindBill, "", "")
	if withTxs {
		c.txs = loadAllTransactions("")
	}
	c.members = map[int][]int{}
	for v, s := range c.canon {
		c.members[s] = append(c.members[s], v)
	}
	return c
}

func (c *contactsContext) survivorOf(id int) int {
	if s, ok := c.canon[id]; ok {
		return s
	}
	return id
}

func (c *contactsContext) groupIDs(id int) map[int]bool {
	s := c.survivorOf(id)
	g := map[int]bool{s: true}
	for _, m := range c.members[s] {
		g[m] = true
	}
	return g
}

// ContactView is a survivor partner plus everything aggregated across its merge
// group — including pending (not-yet-pushed) merges.
type ContactView struct {
	Partner       OdooPartner
	GroupIDs      []int
	PendingMerges []PartnerMerge
	Invoices      []moveRow
	Bills         []moveRow
	Txs           []TransactionEntry
	IBANs         []string
	Emails        []string
	Names         []string
}

func (c *contactsContext) view(id int) ContactView {
	surv := c.survivorOf(id)
	g := c.groupIDs(id)
	v := ContactView{}
	if c.idx != nil {
		v.Partner = c.idx.byID[surv]
	}
	emailSet, ibanSet, nameSet := map[string]bool{}, map[string]bool{}, map[string]bool{}
	for m := range g {
		v.GroupIDs = append(v.GroupIDs, m)
		if c.idx != nil {
			if p, ok := c.idx.byID[m]; ok {
				if p.Email != "" {
					emailSet[strings.ToLower(p.Email)] = true
				}
				if p.Name != "" {
					nameSet[p.Name] = true
				}
			}
		}
	}
	sort.Ints(v.GroupIDs)
	for _, r := range c.invoices {
		if g[r.PartnerID] {
			v.Invoices = append(v.Invoices, r)
			if r.IBAN != "" {
				ibanSet[r.IBAN] = true
			}
		}
	}
	for _, r := range c.bills {
		if g[r.PartnerID] {
			v.Bills = append(v.Bills, r)
			if r.IBAN != "" {
				ibanSet[r.IBAN] = true
			}
		}
	}
	v.IBANs = sortedKeys(ibanSet)
	v.Emails = sortedKeys(emailSet)
	v.Names = sortedKeys(nameSet)
	// Transactions don't carry a reliable partner id in the cache, so match
	// fuzzily on email / IBAN / full name (each distinctive enough).
	if len(c.txs) > 0 {
		for _, tx := range c.txs {
			if c.txMatchesContact(tx, v.Names, v.Emails, v.IBANs) {
				v.Txs = append(v.Txs, tx)
			}
		}
	}
	for _, m := range pendingPartnerMerges() {
		if c.survivorOf(m.SurvivorID) == surv {
			v.PendingMerges = append(v.PendingMerges, m)
		}
	}
	return v
}

func (c *contactsContext) txMatchesContact(tx TransactionEntry, names, emails, ibans []string) bool {
	hay := strings.ToLower(txSearchableText(tx) + " " + tx.Counterparty)
	hayNoSpace := strings.ReplaceAll(hay, " ", "")
	for _, e := range emails {
		if e != "" && strings.Contains(hay, e) {
			return true
		}
	}
	for _, ib := range ibans {
		ibn := strings.ToLower(strings.ReplaceAll(ib, " ", ""))
		if len(ibn) >= 8 && strings.Contains(hayNoSpace, ibn) {
			return true
		}
	}
	for _, n := range names {
		nn := strings.ToLower(strings.TrimSpace(n))
		if len(nn) >= 5 && strings.Contains(hay, nn) {
			return true
		}
	}
	return false
}

// contactSummary is the light per-survivor row for lists / the merge TUI:
// counts only, no transaction scan.
type contactSummary struct {
	Partner    OdooPartner
	GroupSize  int
	NumInvoice int
	NumBill    int
	IBANs      []string
}

func (c *contactsContext) summary(survivorID int) contactSummary {
	g := c.groupIDs(survivorID)
	s := contactSummary{GroupSize: len(g)}
	if c.idx != nil {
		s.Partner = c.idx.byID[survivorID]
	}
	ibanSet := map[string]bool{}
	for _, r := range c.invoices {
		if g[r.PartnerID] {
			s.NumInvoice++
			if r.IBAN != "" {
				ibanSet[r.IBAN] = true
			}
		}
	}
	for _, r := range c.bills {
		if g[r.PartnerID] {
			s.NumBill++
			if r.IBAN != "" {
				ibanSet[r.IBAN] = true
			}
		}
	}
	s.IBANs = sortedKeys(ibanSet)
	return s
}

// survivorPartners returns the canonical (non-victim) partners, optionally only
// active ones, sorted by name — the row set the merge TUI / contacts list show.
func (c *contactsContext) survivorPartners(activeOnly bool) []OdooPartner {
	if c.idx == nil {
		return nil
	}
	var out []OdooPartner
	for id, p := range c.idx.byID {
		if _, isVictim := c.canon[id]; isVictim {
			continue // merged away — represented by its survivor
		}
		if activeOnly && !p.Active {
			continue
		}
		out = append(out, p)
	}
	sort.Slice(out, func(i, j int) bool {
		return strings.ToLower(out[i].Name) < strings.ToLower(out[j].Name)
	})
	return out
}

// ensureTxs lazily loads transactions (only needed when rendering a single
// contact's full detail, not for lists).
func (c *contactsContext) ensureTxs() {
	if c.txs == nil {
		c.txs = loadAllTransactions("")
	}
}

// allSummaries computes counts + IBANs for every survivor partner in a single
// pass over the moves (cheap — no transaction scan).
func (c *contactsContext) allSummaries() map[int]contactSummary {
	nInv, nBill := map[int]int{}, map[int]int{}
	ibans := map[int]map[string]bool{}
	add := func(sid int, iban string, isBill bool) {
		if isBill {
			nBill[sid]++
		} else {
			nInv[sid]++
		}
		if iban != "" {
			if ibans[sid] == nil {
				ibans[sid] = map[string]bool{}
			}
			ibans[sid][iban] = true
		}
	}
	for _, r := range c.invoices {
		add(c.survivorOf(r.PartnerID), r.IBAN, false)
	}
	for _, r := range c.bills {
		add(c.survivorOf(r.PartnerID), r.IBAN, true)
	}
	out := map[int]contactSummary{}
	for _, p := range c.survivorPartners(false) {
		out[p.ID] = contactSummary{
			Partner: p, GroupSize: len(c.groupIDs(p.ID)),
			NumInvoice: nInv[p.ID], NumBill: nBill[p.ID], IBANs: sortedKeys(ibans[p.ID]),
		}
	}
	return out
}

// ── Pending preview & apply ──────────────────────────────────────────────

func mergeVictimLabel(m PartnerMerge) string {
	if len(m.MergedNames) > 0 {
		return strings.Join(m.MergedNames, ", ")
	}
	parts := make([]string, len(m.MergedIDs))
	for i, id := range m.MergedIDs {
		parts[i] = fmt.Sprintf("#%d", id)
	}
	return strings.Join(parts, ", ")
}

// printPendingMergesSummary lists the locally-recorded merges not yet pushed to
// Odoo. Surfaced by `chb odoo pull`, the merge TUI (non-TTY), and apply.
func printPendingMergesSummary() {
	pend := pendingPartnerMerges()
	if len(pend) == 0 {
		return
	}
	f := Fmt
	fmt.Printf("\n  %s%d pending contact merge(s)%s %s(apply with `chb odoo contacts apply`)%s\n",
		f.Bold, len(pend), f.Reset, f.Dim, f.Reset)
	for _, m := range pend {
		fmt.Printf("    • %s ← %s\n", firstNonEmptyStr(m.SurvivorName, fmt.Sprintf("#%d", m.SurvivorID)), mergeVictimLabel(m))
	}
}

// OdooContactsApply pushes locally-recorded contact merges to Odoo. Dry-run by
// default; --yes (or a y/N confirm) commits. Each merge is applied via Odoo's
// partner-merge wizard, then marked applied in the local store.
func OdooContactsApply(args []string) error {
	if HasFlag(args, "--help", "-h", "help") {
		printContactsMergeHelp()
		return nil
	}
	dryRun := HasFlag(args, "--dry-run")
	assumeYes := HasFlag(args, "--yes", "-y")
	pend := pendingPartnerMerges()

	f := Fmt
	fmt.Printf("\n  %sContact merges to apply%s — %d pending\n", f.Bold, f.Reset, len(pend))
	if len(pend) == 0 {
		fmt.Printf("  %snothing pending.%s\n\n", f.Dim, f.Reset)
		return nil
	}
	for _, m := range pend {
		fmt.Printf("    • %s%s%s (#%d) ← %s\n", f.Bold,
			firstNonEmptyStr(m.SurvivorName, fmt.Sprintf("#%d", m.SurvivorID)), f.Reset, m.SurvivorID, mergeVictimLabel(m))
	}
	if dryRun {
		fmt.Printf("\n  %s(dry-run — re-run with --yes to apply on Odoo.)%s\n\n", f.Dim, f.Reset)
		return nil
	}
	if !assumeYes {
		if !isInteractiveTTY() {
			fmt.Printf("\n  %sRefusing to write without --yes on a non-interactive shell.%s\n\n", f.Yellow, f.Reset)
			return nil
		}
		fmt.Printf("\n  %sApply %d merge(s) on Odoo? This is irreversible.%s [y/N] ", f.Bold, len(pend), f.Reset)
		resp, _ := bufio.NewReader(os.Stdin).ReadString('\n')
		if r := strings.TrimSpace(strings.ToLower(resp)); r != "y" && r != "yes" {
			fmt.Println("  Aborted.")
			return nil
		}
	}

	creds, err := ResolveOdooCredentials()
	if err != nil {
		return err
	}
	uid, err := odooAuth(creds.URL, creds.DB, creds.Login, creds.Password)
	if err != nil || uid == 0 {
		return fmt.Errorf("Odoo authentication failed: %v", err)
	}
	printOdooWriteBannerOnce(creds.URL, creds.DB)

	all := loadPartnerMerges()
	applied := 0
	for i := range all {
		if all[i].AppliedAt != "" {
			continue
		}
		if err := applyOneMergeToOdoo(creds, uid, all[i]); err != nil {
			fmt.Printf("  %s✗ %s: %v%s\n", f.Red, firstNonEmptyStr(all[i].SurvivorName, fmt.Sprintf("#%d", all[i].SurvivorID)), err, f.Reset)
			continue
		}
		all[i].AppliedAt = time.Now().UTC().Format(time.RFC3339)
		applied++
		fmt.Printf("  %s✓ merged into %s%s\n", f.Green, firstNonEmptyStr(all[i].SurvivorName, fmt.Sprintf("#%d", all[i].SurvivorID)), f.Reset)
	}
	if err := savePartnerMerges(all); err != nil {
		fmt.Printf("  %s⚠ could not update local merge store: %v%s\n", f.Yellow, err, f.Reset)
	}
	fmt.Printf("\n  %sApplied %d merge(s).%s\n\n", f.Green, applied, f.Reset)
	return nil
}

// applyOneMergeToOdoo runs Odoo's partner-merge wizard: create it with the
// destination + the full id set, then action_merge. (Odoo's _merge is private /
// not RPC-callable, so the wizard is the supported path.)
func applyOneMergeToOdoo(creds *OdooCredentials, uid int, m PartnerMerge) error {
	ids := append([]int{m.SurvivorID}, m.MergedIDs...)
	res, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
		"base.partner.merge.automatic.wizard", "create",
		[]interface{}{map[string]interface{}{
			"dst_partner_id": m.SurvivorID,
			"partner_ids":    []interface{}{[]interface{}{6, 0, ids}},
			"state":          "selection",
		}}, nil)
	if err != nil {
		return fmt.Errorf("create merge wizard: %w", err)
	}
	var wid int
	_ = json.Unmarshal(res, &wid)
	if wid == 0 {
		return fmt.Errorf("merge wizard create returned no id")
	}
	if _, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
		"base.partner.merge.automatic.wizard", "action_merge",
		[]interface{}{[]interface{}{wid}}, nil); err != nil {
		return fmt.Errorf("action_merge: %w", err)
	}
	return nil
}

func moveRowsTotal(rows []moveRow) float64 {
	t := 0.0
	for _, r := range rows {
		t += math.Abs(r.Move.TotalAmount)
	}
	return t
}
