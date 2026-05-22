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

	nostrsource "github.com/CommonsHub/chb/providers/nostr"
)

// MovePushCommandInvoices is the `chb invoices push` entry point.
func MovePushCommandInvoices(args []string) error {
	return MovesPushCommand(moveKindInvoice, args)
}

// MovePushCommandBills is the `chb bills push` entry point.
func MovePushCommandBills(args []string) error {
	return MovesPushCommand(moveKindBill, args)
}

// MovesPushCommand walks invoices/bills in scope, computes the desired
// analytic_distribution from (Collective, Category) — whether those
// came from a rules.json default, an Odoo pull, or a manual `[e]`
// edit — and writes the diff to Odoo's account.move.line records.
// Same time it writes a Nostr annotation event for the move's URI so
// other chb instances pulling from the same relays pick up the
// classification too.
//
// On every push:
//  1. Local desired state = ApplyMoveRules(JSON) + the move's own fields.
//  2. Compare against Odoo's per-line analytic_distribution (from the
//     cached invoices/bills file — refreshed by `chb pull`).
//  3. Compare against the Nostr annotation cache for the move URI.
//  4. Write the missing pieces (Odoo + Nostr) so the three converge.
//
// Default dry-run; --yes / -y applies.
func MovesPushCommand(kind moveKind, args []string) error {
	if HasFlag(args, "--help", "-h", "help") {
		printMovesPushHelp(kind)
		return nil
	}
	if err := RequireOdooWriteCapability(); err != nil {
		return err
	}
	dryRun := HasFlag(args, "--dry-run")
	assumeYes := HasFlag(args, "--yes", "-y")
	verbose := HasFlag(args, "--verbose", "-v")
	if dryRun {
		assumeYes = false
	}
	posYear, posMonth, _ := ParseYearMonthArg(args)

	rows, err := loadMoveRows(kind, posYear, posMonth)
	if err != nil {
		return err
	}
	scope := counterpartiesScopeLabel(posYear, posMonth)
	if scope == "" {
		scope = "all time"
	}
	fmt.Printf("\n  %sPush %s annotations — %s%s\n",
		Fmt.Bold, kind.labelPl, scope, Fmt.Reset)

	plans := loadOdooAnalyticPlansFile()
	if plans == nil {
		return fmt.Errorf("analytic plans cache missing — run `chb odoo pull` first")
	}

	creds, err := ResolveOdooCredentials()
	if err != nil {
		return err
	}
	odooHost := OdooHost(creds.URL)

	type plan struct {
		Row          moveRow
		DesiredDist  map[int]float64
		LinesToWrite []moveLineDiff
		NostrNeeded  bool
		LocalDirty   bool // rule fired but JSON still has empty fields
	}
	plans2 := make([]plan, 0, len(rows))
	var needWrite int
	for _, r := range rows {
		desired := computeDesiredMoveAnalytic(r.Move, plans)
		if len(desired) == 0 {
			continue
		}
		diffs := computeMoveLineAnalyticDiffs(r.Move, desired)
		nostrPresent := moveNostrAnnotationMatches(r, kind, odooHost, creds.DB, plans)
		jsonDirty := moveJSONStillBlank(r.Year, r.Month, kind, r.Move)
		if len(diffs) == 0 && nostrPresent && !jsonDirty {
			continue
		}
		needWrite++
		plans2 = append(plans2, plan{
			Row:          r,
			DesiredDist:  desired,
			LinesToWrite: diffs,
			NostrNeeded:  !nostrPresent,
			LocalDirty:   jsonDirty,
		})
	}
	if needWrite == 0 {
		fmt.Printf("  %sNothing to push. Local / Odoo / Nostr already converged.%s\n\n", Fmt.Dim, Fmt.Reset)
		return nil
	}

	// Sort newest first so the operator sees recent work at the top.
	sort.SliceStable(plans2, func(i, j int) bool { return plans2[i].Row.Move.Date > plans2[j].Row.Move.Date })

	fmt.Printf("  %s%d %s%s have annotation diffs to push%s\n\n",
		Fmt.Bold, needWrite, kind.label, plural(needWrite), Fmt.Reset)

	for _, p := range plans2 {
		printMovePushPreview(p.Row, p.LinesToWrite, p.NostrNeeded, p.LocalDirty, verbose)
	}

	if dryRun {
		fmt.Printf("\n  %s(dry-run — re-run with --yes to apply.)%s\n\n", Fmt.Dim, Fmt.Reset)
		return nil
	}

	if !assumeYes && isInteractiveTTY() {
		fmt.Printf("\n  %sPush %d annotation%s to Odoo + Nostr?%s [Y/n] ",
			Fmt.Bold, needWrite, plural(needWrite), Fmt.Reset)
		reader := bufio.NewReader(os.Stdin)
		resp, _ := reader.ReadString('\n')
		resp = strings.TrimSpace(strings.ToLower(resp))
		if resp == "n" || resp == "no" {
			fmt.Println("  Aborted.")
			return nil
		}
	} else if !assumeYes {
		return fmt.Errorf("refusing to write on a non-interactive shell without --yes")
	}

	printOdooWriteBannerOnce(creds.URL, creds.DB)
	uid, err := odooAuth(creds.URL, creds.DB, creds.Login, creds.Password)
	if err != nil || uid == 0 {
		return fmt.Errorf("Odoo authentication failed: %v", err)
	}

	var odooApplied, nostrApplied, jsonApplied, failed int
	for _, p := range plans2 {
		row := p.Row
		if len(p.LinesToWrite) > 0 {
			if err := applyMoveAnalyticDistribution(creds, uid, row.Move.ID, p.LinesToWrite); err != nil {
				failed++
				LogErrorf("push analytic_distribution on %s #%d: %v", kind.label, row.Move.ID, err)
				fmt.Printf("  %s✗%s %s #%d: %v\n", Fmt.Red, Fmt.Reset, kind.label, row.Move.ID, err)
				continue
			}
			odooApplied++
		}
		if p.NostrNeeded {
			if err := writeMoveNostrAnnotation(row, kind, odooHost, creds.DB, row.Move.Category, row.Move.Collective); err != nil {
				LogErrorf("nostr annotation for %s #%d: %v", kind.label, row.Move.ID, err)
				fmt.Printf("  %s⚠%s nostr write for %s #%d: %v\n", Fmt.Yellow, Fmt.Reset, kind.label, row.Move.ID, err)
			} else {
				nostrApplied++
			}
		}
		if p.LocalDirty {
			if err := persistMoveAnnotationsToJSON(row, kind); err != nil {
				LogErrorf("local JSON persist for %s #%d: %v", kind.label, row.Move.ID, err)
				fmt.Printf("  %s⚠%s local write for %s #%d: %v\n", Fmt.Yellow, Fmt.Reset, kind.label, row.Move.ID, err)
			} else {
				jsonApplied++
			}
		}
	}

	fmt.Printf("\n  %sOdoo:%s  %d updated", Fmt.Bold, Fmt.Reset, odooApplied)
	fmt.Printf("    %sNostr:%s %d annotation%s written", Fmt.Bold, Fmt.Reset, nostrApplied, plural(nostrApplied))
	fmt.Printf("    %sLocal:%s %d JSON file%s patched", Fmt.Bold, Fmt.Reset, jsonApplied, plural(jsonApplied))
	if failed > 0 {
		fmt.Printf("    %sFailed:%s %d", Fmt.Red, Fmt.Reset, failed)
	}
	fmt.Println()
	if nostrApplied > 0 {
		fmt.Printf("  %s(Annotations queued for next `chb nostr push`.)%s\n", Fmt.Dim, Fmt.Reset)
	}
	fmt.Println()
	return nil
}

// moveLineDiff carries the per-line analytic_distribution change we
// intend to push: the line ID and the new distribution (already
// normalised, so the same map is what gets written verbatim).
type moveLineDiff struct {
	LineID         int
	CurrentDist    map[int]float64
	DesiredDist    map[int]float64
	LineProductRef string // for the preview line; empty when unknown
}

// computeDesiredMoveAnalytic returns the analytic_distribution chb
// wants on every line of the move, derived from its (Collective,
// Category) via the analytic-plans lookup. Returns an empty map when
// neither resolves — the caller treats that as "nothing to push".
func computeDesiredMoveAnalytic(m OdooOutgoingInvoicePublic, plans *OdooAnalyticPlansFile) map[int]float64 {
	out := map[int]float64{}
	if id := plans.CategoryAccountIDFor(m.Category); id > 0 {
		out[id] = 100
	}
	if id := plans.CollectiveAccountIDFor(m.Collective); id > 0 {
		out[id] = 100
	}
	return out
}

// computeMoveLineAnalyticDiffs walks the move's line items and emits
// a diff entry for every line whose current Odoo
// analytic_distribution doesn't already match `desired`. Lines whose
// current distribution is empty get filled in; lines that already
// match are skipped; lines with a different non-empty distribution
// (someone in finance set them manually) are SKIPPED and a warning is
// surfaced — chb won't blindly overwrite a deliberate override.
func computeMoveLineAnalyticDiffs(m OdooOutgoingInvoicePublic, desired map[int]float64) []moveLineDiff {
	if len(desired) == 0 {
		return nil
	}
	var diffs []moveLineDiff
	for _, li := range m.LineItems {
		// Section / note rows have no analytic_distribution.
		if isSectionOrNote(li.DisplayType) {
			continue
		}
		current := lineItemAnalyticMap(li)
		if analyticDistributionsEqual(current, desired) {
			continue
		}
		// Manual finance override → skip (different non-empty dist).
		if len(current) > 0 && !analyticIsSubsetOf(current, desired) {
			continue
		}
		ref := strings.TrimSpace(firstNonEmpty(li.Title, li.ProductName))
		diffs = append(diffs, moveLineDiff{
			LineID:         li.ID,
			CurrentDist:    current,
			DesiredDist:    desired,
			LineProductRef: ref,
		})
	}
	return diffs
}

// analyticIsSubsetOf returns true when every key in `a` exists in `b`
// with the same value. Used to detect "Odoo has one of the two
// dimensions chb wants" — that's a legitimate partial state, NOT a
// manual override, so we DO want to fill in the missing dimension.
func analyticIsSubsetOf(a, b map[int]float64) bool {
	for k, va := range a {
		vb, ok := b[k]
		if !ok || math.Abs(va-vb) > 0.001 {
			return false
		}
	}
	return true
}

// lineItemAnalyticMap converts the wire-format AnalyticDistribution
// slice back into the Odoo {accountID: percentage} map shape that
// analyticDistributionsEqual / write-side helpers consume.
func lineItemAnalyticMap(li OdooInvoiceLineItem) map[int]float64 {
	out := map[int]float64{}
	for _, split := range li.AnalyticDistribution {
		if split.AccountID > 0 {
			out[split.AccountID] = split.Percentage
		}
	}
	return out
}

// applyMoveAnalyticDistribution writes the diff to Odoo. Groups lines
// by their (sorted) desired-distribution key so we make one
// account.move.line.write call per distinct distribution, regardless
// of how many lines share it. Same draft → write → repost dance as
// the bank-line side; the move is left in its original state.
func applyMoveAnalyticDistribution(creds *OdooCredentials, uid int, moveID int, diffs []moveLineDiff) error {
	if len(diffs) == 0 {
		return nil
	}
	type group struct {
		dist map[int]float64
		ids  []int
	}
	groups := map[string]*group{}
	for _, d := range diffs {
		key := distributionKey(d.DesiredDist)
		g, ok := groups[key]
		if !ok {
			g = &group{dist: d.DesiredDist}
			groups[key] = g
		}
		g.ids = append(g.ids, d.LineID)
	}

	return withOdooMoveTemporarilyDraft(creds, uid, moveID, func() error {
		keys := make([]string, 0, len(groups))
		for k := range groups {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			g := groups[k]
			vals := map[string]interface{}{
				"analytic_distribution": distributionForWrite(g.dist),
			}
			if _, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
				"account.move.line", "write",
				[]interface{}{g.ids, vals}, nil); err != nil {
				return fmt.Errorf("write distribution %s on %d line(s): %v", k, len(g.ids), err)
			}
		}
		return nil
	})
}

// writeMoveNostrAnnotation queues a Nostr annotation event for the
// move's canonical URI. Mirrors writeMoveTxAnnotation but uses the
// move's own URI rather than the linked-tx URI. The annotation lives
// in the nostr annotation cache for the move's month; `chb nostr
// push` ships it to relays. Other chb instances pulling from the
// same relay see the (category, collective) on their next pull.
func writeMoveNostrAnnotation(row moveRow, kind moveKind, odooHost, odooDB, category, collective string) error {
	if category == "" && collective == "" {
		return nil
	}
	uri := OdooURI(odooHost, odooDB, "account.move", row.Move.ID)
	dataDir := DataDir()
	path := nostrsource.Path(dataDir, row.Year, row.Month, nostrsource.AnnotationsFile)

	cache := NostrAnnotationCache{Annotations: map[string]*TxAnnotation{}}
	if data, readErr := os.ReadFile(path); readErr == nil {
		_ = json.Unmarshal(data, &cache)
	}
	if cache.Annotations == nil {
		cache.Annotations = map[string]*TxAnnotation{}
	}
	prev := cache.Annotations[uri]
	if prev == nil {
		prev = &TxAnnotation{URI: uri}
	}
	if category != "" {
		prev.Category = category
	}
	if collective != "" {
		prev.Collective = collective
	}
	prev.CreatedAt = time.Now().Unix()
	cache.Annotations[uri] = prev
	cache.FetchedAt = time.Now().UTC().Format(time.RFC3339)
	_ = kind // reserved for future per-kind tagging
	return nostrsource.WriteJSON(dataDir, row.Year, row.Month, cache, nostrsource.AnnotationsFile)
}

// moveNostrAnnotationMatches reports whether the local Nostr
// annotation cache already carries the same (category, collective)
// pair as the move. Returns true when both fields agree — i.e. there
// is nothing to push to Nostr. Returns false when the cache is
// missing or holds a different value.
func moveNostrAnnotationMatches(row moveRow, kind moveKind, odooHost, odooDB string, _ *OdooAnalyticPlansFile) bool {
	if row.Move.Category == "" && row.Move.Collective == "" {
		return true
	}
	uri := OdooURI(odooHost, odooDB, "account.move", row.Move.ID)
	dataDir := DataDir()
	path := nostrsource.Path(dataDir, row.Year, row.Month, nostrsource.AnnotationsFile)
	data, err := os.ReadFile(path)
	if err != nil {
		return false
	}
	var cache NostrAnnotationCache
	if json.Unmarshal(data, &cache) != nil {
		return false
	}
	ann, ok := cache.Annotations[uri]
	if !ok || ann == nil {
		return false
	}
	if row.Move.Category != "" && !strings.EqualFold(ann.Category, row.Move.Category) {
		return false
	}
	if row.Move.Collective != "" && !strings.EqualFold(ann.Collective, row.Move.Collective) {
		return false
	}
	_ = kind
	return true
}

// moveJSONStillBlank reports whether the move's persisted JSON still
// has empty Category/Collective even though the in-memory row has
// values. Happens when a rules.json default fired in loadMoveRows but
// the underlying invoices/bills.json was never re-written. Push needs
// to know so it can persist — otherwise the next push would re-do the
// same work.
func moveJSONStillBlank(year, month string, kind moveKind, m OdooOutgoingInvoicePublic) bool {
	stored, err := loadMoves(DataDir(), year, month, kind)
	if err != nil {
		return false
	}
	for _, sm := range stored {
		if sm.ID != m.ID {
			continue
		}
		if m.Category != "" && sm.Category == "" {
			return true
		}
		if m.Collective != "" && sm.Collective == "" {
			return true
		}
		return false
	}
	return false
}

// persistMoveAnnotationsToJSON writes the rule-derived
// (Category, Collective) onto the underlying invoices/bills.json so
// the next push can short-circuit. Re-reads + saves the month file
// to avoid clobbering concurrent edits.
func persistMoveAnnotationsToJSON(row moveRow, kind moveKind) error {
	dataDir := DataDir()
	moves, err := loadMoves(dataDir, row.Year, row.Month, kind)
	if err != nil {
		return err
	}
	for i := range moves {
		if moves[i].ID != row.Move.ID {
			continue
		}
		if row.Move.Category != "" {
			moves[i].Category = row.Move.Category
		}
		if row.Move.Collective != "" {
			moves[i].Collective = row.Move.Collective
		}
		return saveMoves(dataDir, row.Year, row.Month, kind, moves)
	}
	return fmt.Errorf("move #%d not in %s/%s", row.Move.ID, row.Year, row.Month)
}

// isSectionOrNote reports whether a line is one of Odoo's
// purely-presentational rows (line_section, line_note) — those have
// no analytic_distribution and shouldn't appear in our diffs.
func isSectionOrNote(displayType string) bool {
	switch strings.ToLower(displayType) {
	case "line_section", "line_note":
		return true
	}
	return false
}

func printMovePushPreview(row moveRow, diffs []moveLineDiff, nostrNeeded, localDirty, verbose bool) {
	icon := "✓"
	color := Fmt.Green
	if len(diffs) == 0 && !nostrNeeded && !localDirty {
		return
	}
	fmt.Printf("  %s%s%s %s  %10s  %s\n",
		color, icon, Fmt.Reset,
		row.Move.Date,
		fmtAmountCurrency(row.Move.TotalAmount, row.Move.Currency),
		Truncate(firstNonEmptyStr(row.Move.Title, fmt.Sprintf("#%d", row.Move.ID)), 50))
	tag := fmt.Sprintf("collective=%s, category=%s",
		defaultString(row.Move.Collective, "—"), defaultString(row.Move.Category, "—"))
	fmt.Printf("      %s→%s desired:  %s\n", Fmt.Dim, Fmt.Reset, tag)

	if len(diffs) > 0 {
		fmt.Printf("      %sOdoo:%s    %d line%s to update\n",
			Fmt.Dim, Fmt.Reset, len(diffs), plural(len(diffs)))
		if verbose {
			for _, d := range diffs {
				fmt.Printf("          %sline #%d:%s  %s → %s\n",
					Fmt.Dim, d.LineID, Fmt.Reset,
					formatAnalyticDist(d.CurrentDist),
					formatAnalyticDist(d.DesiredDist))
			}
		}
	}
	if nostrNeeded {
		fmt.Printf("      %sNostr:%s   annotation will be queued\n", Fmt.Dim, Fmt.Reset)
	}
	if localDirty {
		fmt.Printf("      %sLocal:%s   JSON will be persisted\n", Fmt.Dim, Fmt.Reset)
	}
}

func formatAnalyticDist(d map[int]float64) string {
	if len(d) == 0 {
		return "(empty)"
	}
	keys := make([]int, 0, len(d))
	for k := range d {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%d=%.0f%%", k, d[k]))
	}
	return strings.Join(parts, ", ")
}

func printMovesPushHelp(kind moveKind) {
	f := Fmt
	noun := kind.labelPl
	fmt.Printf(`
%schb %s push [YYYY[/MM]]%s — Push category/collective annotations on
%s back to Odoo (analytic_distribution on every line) and to Nostr
(annotation event keyed by the move URI).

Each push computes the diff:
  - desired distribution = (Category, Collective) → analytic-account IDs
    via the same OdooAnalyticPlansFile lookup the bank-line side uses
  - current distribution = what Odoo reports per line (from the cache)
  - lines where current == desired are skipped
  - lines with a different non-empty distribution (someone in finance
    set them manually) are also skipped, with a warning

%sUSAGE%s
  %schb %s push%s                 Dry-run preview (all time)
  %schb %s push 2026 --yes%s      Apply for year 2026
  %schb %s push 2026/05 -v --yes%s  Verbose per-line diffs, then apply

%sFLAGS%s
  %s--yes%s, %s-y%s             Apply (skips the y/N prompt)
  %s--dry-run%s             Force dry-run even when combined with --yes
  %s-v%s, %s--verbose%s        Per-line diffs (old → new)
  %s--help, -h%s           Show this help
`,
		f.Bold, kind.labelPl, f.Reset, noun,
		f.Bold, f.Reset,
		f.Cyan, kind.labelPl, f.Reset,
		f.Cyan, kind.labelPl, f.Reset,
		f.Cyan, kind.labelPl, f.Reset,
		f.Bold, f.Reset,
		f.Yellow, f.Reset, f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset, f.Yellow, f.Reset,
		f.Yellow, f.Reset,
	)
}
