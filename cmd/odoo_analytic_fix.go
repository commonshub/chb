package cmd

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
)

// OdooFix is the `chb odoo fix` entry point. Today it reviews analytic
// accounts: it removes empty duplicate accounts and binds each local
// category/collective slug to its surviving account by id (in
// odoo-analytic-accounts.json), so the analytic-plans sync never recreates a
// twin even when the Odoo display name differs from the slug's pretty form.
//
// Distinct from `chb odoo journals <id> fix`, which repairs the statement
// lines of one journal. This top-level form takes no id.
func OdooFix(args []string) error {
	if HasFlag(args, "--help", "-h", "help") {
		printOdooFixHelp()
		return nil
	}
	creds, err := ResolveOdooCredentials()
	if err != nil {
		return err
	}
	uid, err := odooAuth(creds.URL, creds.DB, creds.Login, creds.Password)
	if err != nil || uid == 0 {
		return wrapOdooAuthError(err)
	}
	return odooFixAnalyticAccounts(creds, uid, HasFlag(args, "--dry-run"), HasFlag(args, "--yes", "-y"))
}

// analyticFixAccountRow is one account inside an actionable group, decorated
// with its usage count and the action we propose for it.
type analyticFixAccountRow struct {
	Account analyticExistingAccount
	Entries int    // count of move lines referencing it; -1 = unknown (count failed)
	Action  string // "keep" | "delete" | "archive" | "keep (has entries)"
}

// analyticFixGroup is one (plan, normalized-name) cluster we propose to fix:
// a surviving account, zero or more removals, the proposed canonical name, and
// the slug bindings to write.
type analyticFixGroup struct {
	PlanLabel string
	NormName  string
	NewName   string                  // proposed canonical (singular) name for the survivor
	Rows      []analyticFixAccountRow // survivor first
	Survivor  analyticExistingAccount
	Bindings  []analyticBinding // every slug in the cluster → survivor (bound by id)
	Warn      string            // non-empty when something needs the operator's eye
}

// removals lists the accounts this group proposes to delete/archive.
func (g analyticFixGroup) removals() []analyticExistingAccount {
	var out []analyticExistingAccount
	for _, r := range g.Rows {
		if r.Action == "delete" || r.Action == "archive" {
			out = append(out, r.Account)
		}
	}
	return out
}

// manualMerge reports a cluster we can't safely auto-merge because more than
// one account carries entries.
func (g analyticFixGroup) manualMerge() bool {
	for _, r := range g.Rows {
		if r.Action == "keep (has entries)" {
			return true
		}
	}
	return false
}

// isDuplicate reports whether the cluster holds more than one account (vs a
// lone account that merely needs a binding so it isn't recreated).
func (g analyticFixGroup) isDuplicate() bool { return len(g.Rows) >= 2 }

type analyticBinding struct {
	Kind string
	Slug string
}

func odooFixAnalyticAccounts(creds *OdooCredentials, uid int, dryRun, assumeYes bool) error {
	fmt.Printf("\n%s🔧 Odoo analytic-account fix%s  %s%s (db: %s)%s\n",
		Fmt.Bold, Fmt.Reset, Fmt.Dim, creds.URL, creds.DB, Fmt.Reset)

	plans, err := ensureOdooAnalyticPlans(creds, uid)
	if err != nil {
		return fmt.Errorf("plans: %v", err)
	}
	planLabel := map[int]string{
		plans.Collective: "collective",
		plans.Costs:      "costs",
		plans.Income:     "income",
	}

	_, accounts, err := fetchOdooAnalyticAccountsByPlan(creds, uid, []int{plans.Collective, plans.Costs, plans.Income})
	if err != nil {
		return fmt.Errorf("accounts: %v", err)
	}

	catSpecs, err := categoryAccountSpecs(plans)
	if err != nil {
		return fmt.Errorf("category specs: %v", err)
	}
	collSpecs, err := collectiveAccountSpecs(plans)
	if err != nil {
		return fmt.Errorf("collective specs: %v", err)
	}
	allSpecs := append(append([]analyticAccountSpec{}, catSpecs...), collSpecs...)

	// Index accounts and specs by (planID, normalized name).
	accountsByGroup := map[string][]analyticExistingAccount{}
	for _, a := range accounts {
		accountsByGroup[analyticGroupKey(a.PlanID, a.Name)] = append(accountsByGroup[analyticGroupKey(a.PlanID, a.Name)], a)
	}
	specsByGroup := map[string][]analyticAccountSpec{}
	for _, s := range allSpecs {
		specsByGroup[analyticGroupKey(s.PlanID, s.Name)] = append(specsByGroup[analyticGroupKey(s.PlanID, s.Name)], s)
	}

	// entryCount memoises the per-account reference count so each account is
	// queried at most once even if it appears in survivor ranking twice.
	countCache := map[int]int{}
	entryCount := func(id int) int {
		if n, ok := countCache[id]; ok {
			return n
		}
		n, err := odooAnalyticAccountRefCount(creds, uid, id)
		if err != nil {
			n = -1 // unknown — never treat as deletable
		}
		countCache[id] = n
		return n
	}

	groupKeys := make([]string, 0, len(accountsByGroup))
	for k := range accountsByGroup {
		groupKeys = append(groupKeys, k)
	}
	sort.Strings(groupKeys)

	var groups []analyticFixGroup
	for _, key := range groupKeys {
		grpAccounts := accountsByGroup[key]
		specs := specsByGroup[key]
		if len(specs) == 0 && len(grpAccounts) < 2 {
			continue // a lone finance-created account with no local slug — leave it
		}

		dup := len(grpAccounts) >= 2
		// A lone account only needs attention when name-matching would fail for
		// at least one slug (so sync would otherwise recreate a twin).
		bindNeeded := false
		for _, s := range specs {
			if !nameMatchesAccount(s, grpAccounts) {
				bindNeeded = true
				break
			}
		}
		if !dup && !bindNeeded {
			continue // single account already reused by name-match; nothing to do
		}

		grp := buildAnalyticFixGroup(grpAccounts, specs, planLabel, dup, entryCount)
		if len(grp.Rows) == 0 {
			continue
		}
		groups = append(groups, grp)
	}

	if len(groups) == 0 {
		fmt.Printf("\n  %s✓ No duplicate or unbound analytic accounts — nothing to fix.%s\n\n", Fmt.Green, Fmt.Reset)
		return nil
	}

	// Summary first: one scannable table of every cluster we'd touch.
	renderAnalyticFixSummary(groups)

	if dryRun {
		fmt.Printf("\n  %sDry run — nothing written. Re-run without --dry-run to apply.%s\n\n", Fmt.Dim, Fmt.Reset)
		return nil
	}

	interactive := isInteractiveTTY() && !assumeYes
	if !assumeYes && !interactive {
		// Unattended and not explicitly approved: this deletes/renames accounts,
		// so never run it silently from cron.
		Warnf("%s⚠ %s would be removed and accounts renamed/bound. Re-run with --yes or interactively to apply.%s",
			Fmt.Yellow, Pluralize(countAnalyticRemovals(groups), "account", ""), Fmt.Reset)
		return nil
	}

	links := loadOdooAnalyticLinks()
	var deleted, archived, renamed, applied, skipped int
	for _, grp := range groups {
		if grp.manualMerge() {
			fmt.Printf("\n  %s⚠ %s — both accounts carry entries; merge them in Odoo first, then re-run. Skipping.%s\n",
				Fmt.Yellow, grp.NormName, Fmt.Reset)
			skipped++
			continue
		}

		newName := grp.NewName
		if interactive {
			header := analyticMergeHeader(grp)
			fmt.Printf("\n  %s%s%s\n", Fmt.Bold, header, Fmt.Reset)
			edited, skip, abort := promptEditableLine("  new analytical account name: ", grp.NewName)
			if abort {
				fmt.Printf("  %saborted — %d applied so far.%s\n", Fmt.Dim, applied, Fmt.Reset)
				break
			}
			if skip {
				fmt.Printf("  %sskipped%s\n", Fmt.Dim, Fmt.Reset)
				skipped++
				continue
			}
			if strings.TrimSpace(edited) != "" {
				newName = strings.TrimSpace(edited)
			}
		}

		d, a, r := applyAnalyticFixGroup(creds, uid, grp, newName, links)
		deleted += d
		archived += a
		if r {
			renamed++
		}
		applied++
		fmt.Printf("  %s✓ #%d → %q%s\n", Fmt.Green, grp.Survivor.ID, newName, Fmt.Reset)
	}
	if err := saveOdooAnalyticLinks(links); err != nil {
		return fmt.Errorf("save bindings: %v", err)
	}

	fmt.Printf("\n  %s✓ Applied %s — %d deleted, %d archived, %d renamed, %d bound%s",
		Fmt.Green, Pluralize(applied, "cluster", ""), deleted, archived, renamed, len(links), Fmt.Reset)
	if skipped > 0 {
		fmt.Printf(" %s(%d skipped)%s", Fmt.Dim, skipped, Fmt.Reset)
	}
	fmt.Printf("%s\n", Fmt.Reset)
	fmt.Printf("  %s↪ Run 'chb odoo pull' to refresh the analytic cache so categorize uses the surviving ids.%s\n\n", Fmt.Dim, Fmt.Reset)
	return nil
}

// applyAnalyticFixGroup removes the empty duplicate(s), renames the survivor to
// newName when it differs, and binds every slug in the cluster to the survivor
// by id. Mutates links in place.
func applyAnalyticFixGroup(creds *OdooCredentials, uid int, grp analyticFixGroup, newName string, links OdooAnalyticLinks) (deleted, archived int, renamed bool) {
	for _, acc := range grp.removals() {
		wasDeleted, err := deleteOrArchiveOdooAnalyticAccount(creds, uid, acc.ID)
		if err != nil {
			Warnf("%s⚠ could not remove #%d %q: %v%s", Fmt.Yellow, acc.ID, acc.Name, err, Fmt.Reset)
			continue
		}
		if wasDeleted {
			deleted++
		} else {
			archived++
		}
	}
	if newName != "" && !strings.EqualFold(newName, grp.Survivor.Name) {
		if err := renameOdooAnalyticAccount(creds, uid, grp.Survivor.ID, newName); err != nil {
			Warnf("%s⚠ could not rename #%d to %q: %v%s", Fmt.Yellow, grp.Survivor.ID, newName, err, Fmt.Reset)
		} else {
			renamed = true
		}
	}
	for _, b := range grp.Bindings {
		links[analyticLinkKey(b.Kind, b.Slug)] = grp.Survivor.ID
	}
	return deleted, archived, renamed
}

// analyticMergeHeader renders the "Merging X and Y (removing #a, keeping #b)"
// line shown before each cluster's rename prompt.
func analyticMergeHeader(grp analyticFixGroup) string {
	var names []string
	for _, r := range grp.Rows {
		names = append(names, r.Account.Name)
	}
	removed := grp.removals()
	if len(removed) == 0 {
		// Lone account that only needs a binding (no twin to remove).
		return fmt.Sprintf("Binding %q (#%d)", grp.Survivor.Name, grp.Survivor.ID)
	}
	var removedIDs []string
	for _, a := range removed {
		removedIDs = append(removedIDs, fmt.Sprintf("#%d", a.ID))
	}
	return fmt.Sprintf("Merging %s (removing %s, keeping #%d)",
		joinHumanList(names), strings.Join(removedIDs, ", "), grp.Survivor.ID)
}

// joinHumanList renders ["a","b","c"] as "a, b and c".
func joinHumanList(items []string) string {
	switch len(items) {
	case 0:
		return ""
	case 1:
		return items[0]
	case 2:
		return items[0] + " and " + items[1]
	default:
		return strings.Join(items[:len(items)-1], ", ") + " and " + items[len(items)-1]
	}
}

// buildAnalyticFixGroup ranks the accounts in one cluster, classifies each
// for removal, and computes the slug bindings to write onto the survivor.
func buildAnalyticFixGroup(grpAccounts []analyticExistingAccount, specs []analyticAccountSpec, planLabel map[int]string, dup bool, entryCount func(int) int) analyticFixGroup {
	// Resolve usage counts (only needed when ranking duplicates; a lone
	// bind-only account isn't deleted, so skip the RPC).
	counts := map[int]int{}
	if dup {
		for _, a := range grpAccounts {
			counts[a.ID] = entryCount(a.ID)
		}
	} else {
		for _, a := range grpAccounts {
			counts[a.ID] = -1
		}
	}

	matchesSlug := func(a analyticExistingAccount) bool {
		for _, s := range specs {
			if strings.EqualFold(s.Name, a.Name) {
				return true
			}
		}
		return false
	}

	ranked := append([]analyticExistingAccount{}, grpAccounts...)
	sort.SliceStable(ranked, func(i, j int) bool {
		ci, cj := counts[ranked[i].ID], counts[ranked[j].ID]
		if ci != cj {
			return ci > cj // most entries first (unknown -1 sinks below 0)
		}
		mi, mj := matchesSlug(ranked[i]), matchesSlug(ranked[j])
		if mi != mj {
			return mi // an account already named like a slug wins ties
		}
		return ranked[i].ID < ranked[j].ID
	})

	survivor := ranked[0]
	grp := analyticFixGroup{
		PlanLabel: planLabel[survivor.PlanID],
		NormName:  normalizeAnalyticName(survivor.Name),
		NewName:   proposeCanonicalAnalyticName(grpAccounts, specs),
		Survivor:  survivor,
	}

	for i, a := range ranked {
		row := analyticFixAccountRow{Account: a, Entries: counts[a.ID]}
		switch {
		case i == 0:
			row.Action = "keep"
		case counts[a.ID] == 0:
			row.Action = "delete" // verified empty
		default:
			row.Action = "keep (has entries)" // >0 or unknown — never auto-remove
			grp.Warn = "two accounts carry entries — merge them in Odoo before re-running"
		}
		grp.Rows = append(grp.Rows, row)
	}

	// Bind every slug in the cluster to the survivor by id. Binding is robust
	// to the rename above: even a slug whose pretty-name no longer matches the
	// chosen canonical name reuses the survivor instead of recreating a twin.
	for _, s := range specs {
		grp.Bindings = append(grp.Bindings, analyticBinding{Kind: s.Kind, Slug: s.Slug})
	}
	return grp
}

// proposeCanonicalAnalyticName picks the name to propose for the surviving
// account: among the cluster's display-name variants (account names + slug
// pretty-names) it drops any plural that has a singular twin, then prefers a
// human-readable spaced form, then the shortest. So {"Grant","Grants"} → "Grant"
// and {"Open Letter","Openletter"} → "Open Letter".
func proposeCanonicalAnalyticName(accounts []analyticExistingAccount, specs []analyticAccountSpec) string {
	seen := map[string]bool{}
	var cands []string
	add := func(n string) {
		n = strings.TrimSpace(n)
		if n == "" {
			return
		}
		if k := strings.ToLower(n); !seen[k] {
			seen[k] = true
			cands = append(cands, n)
		}
	}
	for _, a := range accounts {
		add(a.Name)
	}
	for _, s := range specs {
		add(s.Name)
	}
	if len(cands) == 0 {
		return ""
	}

	lower := map[string]bool{}
	for _, c := range cands {
		lower[strings.ToLower(c)] = true
	}
	var pref []string
	for _, c := range cands {
		lc := strings.ToLower(c)
		if strings.HasSuffix(lc, "s") && lower[strings.TrimSuffix(lc, "s")] {
			continue // a singular twin exists in the cluster; prefer it
		}
		pref = append(pref, c)
	}
	if len(pref) == 0 {
		pref = cands
	}
	best := pref[0]
	for _, c := range pref[1:] {
		if betterCanonicalName(c, best) {
			best = c
		}
	}
	return best
}

// betterCanonicalName reports whether a is a better canonical display name than
// b: prefer a spaced/human form, then the shorter string.
func betterCanonicalName(a, b string) bool {
	as, bs := strings.Contains(a, " "), strings.Contains(b, " ")
	if as != bs {
		return as
	}
	return len(a) < len(b)
}

// renderAnalyticFixSummary prints one row per cluster: the accounts (with
// entry counts), which one survives, which are removed, and the proposed new
// name. This is the at-a-glance overview shown before any per-cluster prompts.
func renderAnalyticFixSummary(groups []analyticFixGroup) {
	dupes, manual := 0, 0
	for _, g := range groups {
		if g.isDuplicate() {
			dupes++
		}
		if g.manualMerge() {
			manual++
		}
	}
	fmt.Printf("\n  %sFound %s%s",
		Fmt.Bold, Pluralize(dupes, "duplicate cluster", ""), Fmt.Reset)
	if bindOnly := len(groups) - dupes; bindOnly > 0 {
		fmt.Printf(" %s(+%d to bind)%s", Fmt.Dim, bindOnly, Fmt.Reset)
	}
	if manual > 0 {
		fmt.Printf(" %s· %d need a manual merge%s", Fmt.Yellow, manual, Fmt.Reset)
	}
	fmt.Printf("\n\n")

	headers := []string{"Plan", "Accounts (entries)", "Keep", "Remove", "→ New name"}
	var rows [][]string
	for _, grp := range groups {
		var accCells []string
		for _, r := range grp.Rows {
			accCells = append(accCells, fmt.Sprintf("%s (%s)", r.Account.Name, analyticEntriesLabel(r.Entries)))
		}
		removed := grp.removals()
		removeCell := "—"
		if grp.manualMerge() {
			removeCell = "⚠ manual merge"
		} else if len(removed) > 0 {
			var ids []string
			for _, a := range removed {
				ids = append(ids, fmt.Sprintf("#%d", a.ID))
			}
			removeCell = strings.Join(ids, ", ")
		}
		rows = append(rows, []string{
			grp.PlanLabel,
			strings.Join(accCells, ", "),
			fmt.Sprintf("#%d", grp.Survivor.ID),
			removeCell,
			grp.NewName,
		})
	}
	renderTicketsTable(headers, rows, nil, nil)
}

func countAnalyticRemovals(groups []analyticFixGroup) int {
	n := 0
	for _, grp := range groups {
		if grp.manualMerge() {
			continue
		}
		n += len(grp.removals())
	}
	return n
}

func analyticEntriesLabel(n int) string {
	if n < 0 {
		return "—"
	}
	return fmt.Sprintf("%d", n)
}

// renameOdooAnalyticAccount writes a new display name onto the account.
func renameOdooAnalyticAccount(creds *OdooCredentials, uid, accountID int, name string) error {
	_, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.analytic.account", "write",
		[]interface{}{[]interface{}{accountID}, map[string]interface{}{"name": name}}, nil)
	return err
}

// linePromptModel is a one-line editable text prompt: Enter applies the
// current value, Esc skips this item, Ctrl+C aborts the whole run.
type linePromptModel struct {
	label   string
	input   textinput.Model
	skipped bool
	aborted bool
}

func (m linePromptModel) Init() tea.Cmd { return textinput.Blink }

func (m linePromptModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	if k, ok := msg.(tea.KeyMsg); ok {
		switch k.String() {
		case "enter":
			return m, tea.Quit
		case "esc":
			m.skipped = true
			return m, tea.Quit
		case "ctrl+c":
			m.aborted = true
			return m, tea.Quit
		}
	}
	var cmd tea.Cmd
	m.input, cmd = m.input.Update(msg)
	return m, cmd
}

func (m linePromptModel) View() string {
	return m.label + m.input.View() + Fmt.Dim + "   (enter=apply · esc=skip · ctrl+c=abort)" + Fmt.Reset
}

// promptEditableLine shows an editable single-line prompt seeded with def.
// Returns the final value plus whether the user skipped (esc) or aborted
// (ctrl+c). Falls back to a plain line read if the TUI can't start.
func promptEditableLine(label, def string) (value string, skipped, aborted bool) {
	ti := textinput.New()
	ti.Prompt = ""
	ti.SetValue(def)
	ti.CursorEnd()
	ti.Focus()

	out, err := tea.NewProgram(linePromptModel{label: label, input: ti}).Run()
	if err != nil {
		fmt.Print(label)
		resp, _ := bufio.NewReader(os.Stdin).ReadString('\n')
		resp = strings.TrimSpace(resp)
		if resp == "" {
			resp = def
		}
		return resp, false, false
	}
	m := out.(linePromptModel)
	return strings.TrimSpace(m.input.Value()), m.skipped, m.aborted
}

// analyticGroupKey clusters accounts/specs that are "the same" up to
// case/spacing/plural differences, scoped to one analytic plan.
func analyticGroupKey(planID int, name string) string {
	return fmt.Sprintf("%d:%s", planID, normalizeAnalyticName(name))
}

// nameMatchesAccount reports whether the spec's slug-derived name already
// matches (case-insensitively) one of the accounts — i.e. sync would reuse it
// by name and no binding is needed.
func nameMatchesAccount(spec analyticAccountSpec, accounts []analyticExistingAccount) bool {
	for _, a := range accounts {
		if strings.EqualFold(spec.Name, a.Name) {
			return true
		}
	}
	return false
}

// odooAnalyticAccountRefCount counts the move lines whose analytic_distribution
// references the account. This — not account.analytic.line — is how chb records
// analytic usage (invoices_push.go / odoo_categorize.go write the JSON field),
// so it's the correct "is this account used / safe to delete" signal and it
// includes draft moves. Returns an error if the Odoo build doesn't support
// searching the JSON field, so callers can stay conservative.
func odooAnalyticAccountRefCount(creds *OdooCredentials, uid, accountID int) (int, error) {
	result, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.move.line", "search_count",
		[]interface{}{[]interface{}{
			[]interface{}{"analytic_distribution", "in", []interface{}{accountID}},
		}}, nil)
	if err != nil {
		return 0, err
	}
	var count int
	if err := json.Unmarshal(result, &count); err != nil {
		return 0, fmt.Errorf("parse search_count: %v", err)
	}
	return count, nil
}

// deleteOrArchiveOdooAnalyticAccount tries a hard unlink first; if Odoo blocks
// it (anything still references the account), it falls back to archiving
// (active=false). Returns true when the account was hard-deleted.
func deleteOrArchiveOdooAnalyticAccount(creds *OdooCredentials, uid, accountID int) (bool, error) {
	_, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.analytic.account", "unlink",
		[]interface{}{[]interface{}{accountID}}, nil)
	if err == nil {
		return true, nil
	}
	_, archiveErr := odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.analytic.account", "write",
		[]interface{}{[]interface{}{accountID}, map[string]interface{}{"active": false}}, nil)
	if archiveErr != nil {
		return false, fmt.Errorf("unlink failed (%v) and archive failed (%v)", err, archiveErr)
	}
	return false, nil
}

func printOdooFixHelp() {
	f := Fmt
	fmt.Printf(`
%schb odoo fix%s — Remove duplicate analytic accounts and stop sync recreating them

%sUSAGE%s
  %schb odoo fix%s [--dry-run] [-y|--yes]

%sDESCRIPTION%s
  Prints a summary table of every cluster of analytic accounts that are the
  same up to case/spacing/plural differences (e.g. "Grant" / "Grants", or
  "Open Letter" vs the chb-created "Openletter"), then walks them one by one.
  For each cluster it:
    - keeps the account with the most entries,
    - deletes the empty duplicate(s) — hard unlink, falling back to archive
      when Odoo still references them,
    - proposes a canonical (singular) name for the survivor, which you can
      accept (enter), edit, or skip (esc),
    - binds the local category/collective slug(s) to the surviving account by
      id (in settings/odoo-analytic-accounts.json) so the next sync reuses it
      and never recreates a twin.

  "Entries" are counted from move lines whose analytic_distribution references
  the account, so a used account is never proposed for deletion. A cluster
  where two accounts both carry entries is never auto-merged — you'll be told
  to merge it in Odoo first.

  Not to be confused with %schb odoo journals <id> fix%s, which repairs the
  statement lines of one journal.

%sOPTIONS%s
  %s--dry-run%s     Preview the changes; write nothing (local or Odoo)
  %s-y%s, %s--yes%s    Apply every proposed change without prompting
  %s--help%s, %s-h%s   Show this help

%sEXAMPLES%s
  %schb odoo fix --dry-run%s     Preview duplicates and proposed bindings
  %schb odoo fix%s               Review each cluster interactively
  %schb odoo fix --yes%s         Apply all (use after a dry-run)
`,
		f.Bold, f.Reset,
		f.Bold, f.Reset,
		f.Cyan, f.Reset,
		f.Bold, f.Reset,
		f.Cyan, f.Reset,
		f.Bold, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset, f.Yellow, f.Reset,
		f.Yellow, f.Reset, f.Yellow, f.Reset,
		f.Bold, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
	)
}
