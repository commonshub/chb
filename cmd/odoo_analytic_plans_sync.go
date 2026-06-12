package cmd

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	odoosource "github.com/CommonsHub/chb/providers/odoo"
)

// OdooAnalyticPlansFile is the local cache produced by the
// "analytic plans" stage of `chb odoo sync`. It records the plan and
// account ids the categorize command needs to set the right
// analytic_distribution on each line — without these, categorize would
// need to hit Odoo for every line.
type OdooAnalyticPlansFile struct {
	SchemaVersion int                     `json:"schemaVersion"`
	FetchedAt     string                  `json:"fetchedAt"`
	Plans         OdooAnalyticPlanIDs     `json:"plans"`
	Categories    []OdooAnalyticAccountID `json:"categories"`
	Collectives   []OdooAnalyticAccountID `json:"collectives"`
}

type OdooAnalyticPlanIDs struct {
	Collective int `json:"collective"` // plan id 3 by convention
	Costs      int `json:"costs"`      // plan id 8 by convention
	Income     int `json:"income"`     // created if missing
}

type OdooAnalyticAccountID struct {
	Slug      string `json:"slug"`      // category or collective slug
	Name      string `json:"name"`      // display name
	PlanID    int    `json:"planId"`    // which plan the account lives on
	AccountID int    `json:"accountId"` // account.analytic.account id
}

const odooAnalyticPlansSchemaVersion = 1

// syncOdooAnalyticInfrastructure ensures every plan + analytic.account
// referenced by the categorize step exists in Odoo. Idempotent: re-runs
// only create what's missing. Returns the resulting cache for callers
// that want to act on it immediately (categorize).
//
// Creating accounts is gated: missing accounts are previewed (with a
// near-duplicate hint when an existing account has almost the same name)
// and only created after explicit approval — assumeYes (--yes), or an
// interactive y/N prompt. Unattended runs (cron `chb sync`) skip creation
// and surface a warning instead, so a slug/name mismatch in rules.json can
// never silently mint twins like "Block 26" / "Block26" in Odoo.
func syncOdooAnalyticInfrastructure(creds *OdooCredentials, uid int, assumeYes, dryRun bool) (*OdooAnalyticPlansFile, error) {
	plans, err := ensureOdooAnalyticPlans(creds, uid)
	if err != nil {
		return nil, fmt.Errorf("plans: %v", err)
	}

	// Existing accounts indexed by (plan_id, lowercased name) so we can
	// reuse instead of creating duplicates.
	existing, existingAccounts, err := fetchOdooAnalyticAccountsByPlan(creds, uid, []int{plans.Collective, plans.Costs, plans.Income})
	if err != nil {
		return nil, fmt.Errorf("accounts: %v", err)
	}

	// Categories: each odoo_rule with a non-empty category becomes an
	// analytic account on the costs or income plan, depending on the
	// rule's direction.
	wantCategories, err := categoryAccountSpecs(plans)
	if err != nil {
		return nil, fmt.Errorf("category specs: %v", err)
	}

	// Collectives: every unique collective slug referenced in rules.json
	// becomes an analytic account on plan 3.
	wantCollectives, err := collectiveAccountSpecs(plans)
	if err != nil {
		return nil, fmt.Errorf("collective specs: %v", err)
	}

	var missing []analyticAccountSpec
	for _, spec := range append(append([]analyticAccountSpec{}, wantCategories...), wantCollectives...) {
		if existing[analyticAccountKey(spec.PlanID, spec.Name)] == 0 {
			missing = append(missing, spec)
		}
	}
	createMissing := true
	if len(missing) > 0 {
		createMissing = approveAnalyticAccountCreation(missing, existingAccounts, plans, assumeYes, dryRun)
	}

	catAccounts, err := ensureOdooAnalyticAccounts(creds, uid, wantCategories, existing, createMissing)
	if err != nil {
		return nil, fmt.Errorf("category accounts: %v", err)
	}
	collAccounts, err := ensureOdooAnalyticAccounts(creds, uid, wantCollectives, existing, createMissing)
	if err != nil {
		return nil, fmt.Errorf("collective accounts: %v", err)
	}

	file := &OdooAnalyticPlansFile{
		SchemaVersion: odooAnalyticPlansSchemaVersion,
		FetchedAt:     time.Now().UTC().Format(time.RFC3339),
		Plans:         plans,
		Categories:    catAccounts,
		Collectives:   collAccounts,
	}
	if err := saveOdooAnalyticPlansFile(file); err != nil {
		return nil, fmt.Errorf("save cache: %v", err)
	}
	return file, nil
}

// OdooAnalyticPlansSync is the `chb odoo sync` step entry. Mirrors the
// shape of OdooPartnersSync so it slots into OdooSyncAll cleanly.
func OdooAnalyticPlansSync(args []string) (int, error) {
	if HasFlag(args, "--help", "-h", "help") {
		printOdooSyncHelp()
		return 0, nil
	}
	creds, err := ResolveOdooCredentials()
	if err != nil {
		return 0, err
	}
	uid, err := odooAuth(creds.URL, creds.DB, creds.Login, creds.Password)
	if err != nil {
		return 0, err
	}
	if uid == 0 {
		return 0, fmt.Errorf("Odoo authentication failed")
	}
	odooLog("\n%s📊 Syncing Odoo analytic plans%s\n", Fmt.Bold, Fmt.Reset)
	file, err := syncOdooAnalyticInfrastructure(creds, uid,
		HasFlag(args, "--yes", "-y"), HasFlag(args, "--dry-run"))
	if err != nil {
		return 0, err
	}
	total := len(file.Categories) + len(file.Collectives)
	odooSyncLine("analytic plans", odooItemSyncStatus(total, "analytic account", ""))
	return total, nil
}

// ensureOdooAnalyticPlans returns the plan ids for collective/costs/income,
// creating the income plan if missing. Collective and costs use the
// well-known ids 3 and 8 by convention; we look them up by id and surface
// an error if they don't exist (the operator needs to create them in
// Odoo Studio first since they're part of the chart-of-accounts setup).
func ensureOdooAnalyticPlans(creds *OdooCredentials, uid int) (OdooAnalyticPlanIDs, error) {
	const (
		collectivePlanID = 3
		costsPlanID      = 8
	)

	rows, err := odooSearchReadAllMaps(creds, uid, "account.analytic.plan",
		[]interface{}{},
		[]string{"id", "name"},
		"id asc")
	if err != nil {
		return OdooAnalyticPlanIDs{}, err
	}

	have := map[int]string{}
	incomeID := 0
	for _, row := range rows {
		id := odooInt(row["id"])
		name := odooString(row["name"])
		have[id] = name
		if isIncomePlanName(name) && incomeID == 0 {
			incomeID = id
		}
	}

	plans := OdooAnalyticPlanIDs{}
	if _, ok := have[collectivePlanID]; ok {
		plans.Collective = collectivePlanID
	} else {
		return plans, fmt.Errorf("analytic plan #%d (collective) not found — create it in Odoo first", collectivePlanID)
	}
	if _, ok := have[costsPlanID]; ok {
		plans.Costs = costsPlanID
	} else {
		return plans, fmt.Errorf("analytic plan #%d (costs) not found — create it in Odoo first", costsPlanID)
	}
	if incomeID > 0 {
		plans.Income = incomeID
	} else {
		id, err := createOdooAnalyticPlan(creds, uid, "Income")
		if err != nil {
			return plans, fmt.Errorf("create income plan: %v", err)
		}
		plans.Income = id
	}
	return plans, nil
}

func isIncomePlanName(name string) bool {
	n := strings.ToLower(strings.TrimSpace(name))
	return n == "income" || n == "revenue" || n == "incomes"
}

func createOdooAnalyticPlan(creds *OdooCredentials, uid int, name string) (int, error) {
	data, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.analytic.plan", "create",
		[]interface{}{[]interface{}{map[string]interface{}{"name": name}}}, nil)
	if err != nil {
		return 0, err
	}
	ids := parseOdooCreatedIDs(data)
	if len(ids) == 0 {
		return 0, fmt.Errorf("no id returned")
	}
	return ids[0], nil
}

// analyticAccountSpec describes one analytic account we want to exist.
// It is consumed by ensureOdooAnalyticAccounts which idempotently creates
// missing rows.
type analyticAccountSpec struct {
	Slug   string
	Name   string
	PlanID int
}

// categoryAccountSpecs walks the OdooMapping chain. Each mapping with a
// non-empty category produces one spec on the income plan (direction:in)
// or the costs plan (direction:out). internal_transfer is excluded —
// it has no analytic account by design (account 580001 is enough).
func categoryAccountSpecs(plans OdooAnalyticPlanIDs) ([]analyticAccountSpec, error) {
	mappings, err := LoadOdooMappings()
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	out := make([]analyticAccountSpec, 0, len(mappings))
	for _, r := range mappings {
		cat := strings.TrimSpace(r.Match.Category)
		if cat == "" {
			continue
		}
		if cat == "internal_transfer" {
			continue
		}
		key := strings.ToLower(cat)
		if seen[key] {
			continue
		}
		seen[key] = true
		planID := plans.Costs
		switch strings.ToLower(strings.TrimSpace(r.Match.Direction)) {
		case "in":
			planID = plans.Income
		case "out", "":
			planID = plans.Costs
		}
		out = append(out, analyticAccountSpec{
			Slug:   key,
			Name:   prettyCategoryName(cat),
			PlanID: planID,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Slug < out[j].Slug })
	return out, nil
}

// collectiveAccountSpecs gathers every distinct collective slug from
// rules.json so each gets one analytic account on the collective plan.
func collectiveAccountSpecs(plans OdooAnalyticPlanIDs) ([]analyticAccountSpec, error) {
	rules, err := LoadRules()
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	out := make([]analyticAccountSpec, 0, len(rules))
	for _, r := range rules {
		coll := strings.TrimSpace(r.Assign.Collective)
		if coll == "" {
			continue
		}
		key := strings.ToLower(coll)
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, analyticAccountSpec{
			Slug:   key,
			Name:   prettyCollectiveName(coll),
			PlanID: plans.Collective,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Slug < out[j].Slug })
	return out, nil
}

func prettyCategoryName(slug string) string {
	parts := strings.Split(slug, "_")
	for i, p := range parts {
		if p == "" {
			continue
		}
		parts[i] = strings.ToUpper(p[:1]) + p[1:]
	}
	return strings.Join(parts, " ")
}

func prettyCollectiveName(slug string) string {
	return prettyCategoryName(slug)
}

// analyticExistingAccount is one already-existing analytic account on a
// managed plan, kept with its original casing so the creation preview can
// point at near-duplicate names.
type analyticExistingAccount struct {
	ID     int
	PlanID int
	Name   string
}

// fetchOdooAnalyticAccountsByPlan returns a map keyed by
// (planID, lowercased name) → accountID for accounts on the given plans,
// plus the flat account list for near-duplicate detection.
// Used by ensureOdooAnalyticAccounts to avoid duplicate creates.
func fetchOdooAnalyticAccountsByPlan(creds *OdooCredentials, uid int, planIDs []int) (map[string]int, []analyticExistingAccount, error) {
	out := map[string]int{}
	var accounts []analyticExistingAccount
	planArg := make([]interface{}, 0, len(planIDs))
	for _, p := range planIDs {
		if p > 0 {
			planArg = append(planArg, p)
		}
	}
	if len(planArg) == 0 {
		return out, accounts, nil
	}
	rows, err := odooSearchReadAllMaps(creds, uid, "account.analytic.account",
		[]interface{}{
			[]interface{}{"plan_id", "in", planArg},
			[]interface{}{"active", "=", true},
		},
		[]string{"id", "name", "plan_id"},
		"id asc",
	)
	if err != nil {
		return out, accounts, err
	}
	for _, row := range rows {
		planID := odooFieldID(row["plan_id"])
		name := strings.TrimSpace(odooString(row["name"]))
		if planID > 0 && name != "" {
			out[analyticAccountKey(planID, name)] = odooInt(row["id"])
			accounts = append(accounts, analyticExistingAccount{
				ID:     odooInt(row["id"]),
				PlanID: planID,
				Name:   name,
			})
		}
	}
	return out, accounts, nil
}

func analyticAccountKey(planID int, name string) string {
	return fmt.Sprintf("%d:%s", planID, strings.ToLower(name))
}

// ensureOdooAnalyticAccounts creates any missing accounts and returns
// the resulting cache entries. existing is mutated in-place so a single
// fetch can be reused across category + collective passes. When
// createMissing is false (creation declined or unattended run), specs
// without an existing account are skipped — they simply don't appear in
// the cache, so categorize leaves those lines untouched until the
// operator creates or renames the account.
func ensureOdooAnalyticAccounts(creds *OdooCredentials, uid int, specs []analyticAccountSpec, existing map[string]int, createMissing bool) ([]OdooAnalyticAccountID, error) {
	out := make([]OdooAnalyticAccountID, 0, len(specs))
	for _, spec := range specs {
		key := analyticAccountKey(spec.PlanID, spec.Name)
		if id, ok := existing[key]; ok && id > 0 {
			out = append(out, OdooAnalyticAccountID{
				Slug:      spec.Slug,
				Name:      spec.Name,
				PlanID:    spec.PlanID,
				AccountID: id,
			})
			continue
		}
		if !createMissing {
			continue
		}
		id, err := createOdooAnalyticAccount(creds, uid, spec.Name, spec.PlanID)
		if err != nil {
			return out, fmt.Errorf("create %s (plan %d): %v", spec.Name, spec.PlanID, err)
		}
		existing[key] = id
		out = append(out, OdooAnalyticAccountID{
			Slug:      spec.Slug,
			Name:      spec.Name,
			PlanID:    spec.PlanID,
			AccountID: id,
		})
	}
	return out, nil
}

// approveAnalyticAccountCreation previews the analytic accounts that would
// be created and decides whether creation may proceed. Each candidate is
// checked against the existing accounts on the same plan for a
// near-duplicate name (case/spacing/plural differences, e.g. "Block 26" vs
// "Block26") — those are usually a rules.json slug that drifted from the
// Odoo name, better fixed by renaming one side than by creating a twin.
//
// Resolution order: --dry-run never creates; --yes always creates; an
// interactive terminal gets a y/N prompt; unattended runs (cron) skip
// creation and emit a warning with the command to run after review.
func approveAnalyticAccountCreation(missing []analyticAccountSpec, existing []analyticExistingAccount, plans OdooAnalyticPlanIDs, assumeYes, dryRun bool) bool {
	planLabel := map[int]string{
		plans.Collective: "collective",
		plans.Costs:      "costs",
		plans.Income:     "income",
	}
	var b strings.Builder
	fmt.Fprintf(&b, "%d analytic account%s referenced by local rules but missing in Odoo:\n",
		len(missing), plural(len(missing)))
	for _, spec := range missing {
		fmt.Fprintf(&b, "    + %q on the %s plan (from slug %q)\n",
			spec.Name, planLabel[spec.PlanID], spec.Slug)
		if sim, ok := similarAnalyticAccount(spec, existing); ok {
			fmt.Fprintf(&b, "      ⚠ near-duplicate of existing %q (#%d) — rename it in Odoo (or fix the local slug) instead of creating a twin\n",
				sim.Name, sim.ID)
		}
	}

	switch {
	case dryRun:
		fmt.Fprintf(os.Stderr, "\n  %s%s  (dry-run: not creating)%s\n", Fmt.Yellow, b.String(), Fmt.Reset)
		return false
	case assumeYes:
		fmt.Fprintf(os.Stderr, "\n  %s%s  Creating them (--yes).%s\n", Fmt.Dim, b.String(), Fmt.Reset)
		return true
	case isInteractiveTTY():
		fmt.Fprintf(os.Stderr, "\n  %s%s%s", Fmt.Yellow, b.String(), Fmt.Reset)
		fmt.Fprintf(os.Stderr, "  Create %s in Odoo? [y/N] ", Pluralize(len(missing), "analytic account", ""))
		resp, _ := bufio.NewReader(os.Stdin).ReadString('\n')
		resp = strings.ToLower(strings.TrimSpace(resp))
		return resp == "y" || resp == "yes"
	default:
		Warnf("%s⚠ %s  Not creating them in an unattended run — review the list (fix slug/name mismatches), then run: chb odoo sync --yes%s",
			Fmt.Yellow, b.String(), Fmt.Reset)
		return false
	}
}

// similarAnalyticAccount returns an existing account on the same plan whose
// name differs from the spec only by case, spacing/punctuation, or a
// trailing plural "s" — i.e. an almost-certain duplicate-in-the-making.
func similarAnalyticAccount(spec analyticAccountSpec, existing []analyticExistingAccount) (analyticExistingAccount, bool) {
	want := normalizeAnalyticName(spec.Name)
	for _, acc := range existing {
		if acc.PlanID == spec.PlanID && normalizeAnalyticName(acc.Name) == want {
			return acc, true
		}
	}
	return analyticExistingAccount{}, false
}

// normalizeAnalyticName reduces a display name to a comparison key:
// lowercase, alphanumerics only, trailing plural "s" dropped.
func normalizeAnalyticName(s string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(s) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
		}
	}
	return strings.TrimSuffix(b.String(), "s")
}

func createOdooAnalyticAccount(creds *OdooCredentials, uid int, name string, planID int) (int, error) {
	data, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.analytic.account", "create",
		[]interface{}{[]interface{}{map[string]interface{}{
			"name":    name,
			"plan_id": planID,
		}}}, nil)
	if err != nil {
		return 0, err
	}
	ids := parseOdooCreatedIDs(data)
	if len(ids) == 0 {
		return 0, fmt.Errorf("no id returned")
	}
	return ids[0], nil
}

func odooAnalyticPlansCachePath() string {
	return odoosource.Path(DataDir(), "latest", "", odoosource.AnalyticPlansFile)
}

func saveOdooAnalyticPlansFile(file *OdooAnalyticPlansFile) error {
	return odoosource.WriteJSON(DataDir(), "latest", "", file, odoosource.AnalyticPlansFile)
}

// loadOdooAnalyticPlansFile reads the cache written by the analytic plans
// sync. Callers (categorize) use the AccountID lookups to set
// analytic_distribution on move lines. Returns nil when the cache is
// missing — the caller is expected to suggest `chb odoo sync`.
func loadOdooAnalyticPlansFile() *OdooAnalyticPlansFile {
	data, err := os.ReadFile(odooAnalyticPlansCachePath())
	if err != nil {
		return nil
	}
	var file OdooAnalyticPlansFile
	if err := json.Unmarshal(data, &file); err != nil {
		return nil
	}
	return &file
}

// CategoryAccountIDFor looks up the analytic account id for a category
// slug. Returns 0 if not found.
func (f *OdooAnalyticPlansFile) CategoryAccountIDFor(slug string) int {
	if f == nil {
		return 0
	}
	slug = strings.ToLower(strings.TrimSpace(slug))
	for _, a := range f.Categories {
		if a.Slug == slug {
			return a.AccountID
		}
	}
	return 0
}

// CollectiveAccountIDFor looks up the analytic account id for a
// collective slug. Returns 0 if not found.
func (f *OdooAnalyticPlansFile) CollectiveAccountIDFor(slug string) int {
	if f == nil {
		return 0
	}
	slug = strings.ToLower(strings.TrimSpace(slug))
	for _, a := range f.Collectives {
		if a.Slug == slug {
			return a.AccountID
		}
	}
	return 0
}
