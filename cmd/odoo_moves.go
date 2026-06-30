package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	odoosource "github.com/CommonsHub/chb/providers/odoo"
)

// activeOdooDatabaseLabel names the database the local caches are currently
// scoped to (the per-DB path namespace), or a "legacy" marker when unset.
func activeOdooDatabaseLabel() string {
	if ns := odoosource.PathNamespace(); ns != "" {
		return ns
	}
	return "default (legacy, non-namespaced)"
}

// recordNewestFetchedAt reads a moves file's fetchedAt and keeps the max per
// namespace key.
func recordNewestFetchedAt(into map[string]time.Time, ns, path string) {
	data, err := os.ReadFile(path)
	if err != nil {
		return
	}
	var meta struct {
		FetchedAt string `json:"fetchedAt"`
	}
	if json.Unmarshal(data, &meta) != nil {
		return
	}
	t, err := time.Parse(time.RFC3339, meta.FetchedAt)
	if err != nil {
		return
	}
	if cur, ok := into[ns]; !ok || t.After(cur) {
		into[ns] = t
	}
}

// fresherMoveNamespace scans every per-database moves cache for the scoped
// months and returns the namespace whose cache was pulled more recently than the
// active one (with both timestamps), or "" when none is newer. A different
// namespace being fresher is the usual sign the operator pulled one database but
// is reconciling another.
func fresherMoveNamespace(kind moveKind, posYear, posMonth string) (ns string, other, active time.Time) {
	dataDir := DataDir()
	activeNS := odoosource.PathNamespace()
	newest := map[string]time.Time{}
	yearDirs, _ := os.ReadDir(dataDir)
	for _, yd := range yearDirs {
		if !yd.IsDir() || len(yd.Name()) != 4 || (posYear != "" && yd.Name() != posYear) {
			continue
		}
		monthDirs, _ := os.ReadDir(filepath.Join(dataDir, yd.Name()))
		for _, md := range monthDirs {
			if !md.IsDir() || len(md.Name()) != 2 || (posMonth != "" && md.Name() != posMonth) {
				continue
			}
			base := filepath.Join(dataDir, yd.Name(), md.Name(), "providers", odoosource.Source)
			entries, _ := os.ReadDir(base)
			for _, e := range entries {
				if !e.IsDir() {
					if e.Name() == kind.file { // legacy non-namespaced file
						recordNewestFetchedAt(newest, "", filepath.Join(base, e.Name()))
					}
					continue
				}
				switch e.Name() {
				case "private", "pending", "journals":
					continue
				}
				recordNewestFetchedAt(newest, e.Name(), filepath.Join(base, e.Name(), kind.file))
			}
		}
	}
	active = newest[activeNS]
	for nsName, t := range newest {
		if nsName == activeNS {
			continue
		}
		if t.After(active) && t.After(other) {
			ns, other = nsName, t
		}
	}
	return ns, other, active
}

// moveCacheFreshness reports when the moves cache for the in-scope months was
// last pulled. Returns the oldest FetchedAt across those month files (the
// staleness floor), whether any file was served from the legacy non-namespaced
// path (which predates per-database namespacing and is usually stale), and how
// many month files were read. Used to warn that reconcile works off a local
// snapshot, so items paid/reconciled in Odoo since the last pull still appear.
func moveCacheFreshness(kind moveKind, posYear, posMonth string) (oldest time.Time, fromLegacy bool, months int) {
	dataDir := DataDir()
	_ = walkMoveMonths(dataDir, kind, func(year, month string) error {
		if posYear != "" && year != posYear {
			return nil
		}
		if posMonth != "" && month != posMonth {
			return nil
		}
		path := moveReadPath(dataDir, year, month, kind, false)
		data, err := os.ReadFile(path)
		if err != nil {
			return nil
		}
		if path != filepath.Join(dataDir, year, month, kind.relPath()) {
			fromLegacy = true
		}
		var meta struct {
			FetchedAt string `json:"fetchedAt"`
		}
		if json.Unmarshal(data, &meta) != nil {
			return nil
		}
		if t, err := time.Parse(time.RFC3339, meta.FetchedAt); err == nil {
			months++
			if oldest.IsZero() || t.Before(oldest) {
				oldest = t
			}
		}
		return nil
	})
	return oldest, fromLegacy, months
}

// preserveMoveAnnotations carries chb-authored annotations (Collective, Event,
// and Category when Odoo sends back an empty one) from a previously-cached
// invoice/bill onto a freshly-fetched copy. Without this, any Odoo re-fetch
// would wipe the user's classifications on modified moves.
func preserveMoveAnnotations(fresh, prev OdooOutgoingInvoice) OdooOutgoingInvoice {
	if fresh.Collective == "" && prev.Collective != "" {
		fresh.Collective = prev.Collective
	}
	if fresh.Event == "" && prev.Event != "" {
		fresh.Event = prev.Event
	}
	if fresh.Category == "" && prev.Category != "" {
		fresh.Category = prev.Category
	}
	return fresh
}

// moveKind describes which account.move subset a command is operating on.
// Both invoices and bills share the OdooOutgoingInvoicePublic wire type but
// live under different filenames / wrapping structures.
type moveKind struct {
	label   string // human label used in prompts and logs ("invoice", "bill")
	labelPl string // plural ("invoices", "bills")
	file    string // provider base filename (odoosource.InvoicesFile / BillsFile)
	model   string // Odoo model technical name
	isBill  bool
}

// relPath / privateRelPath resolve the per-month provider paths at call time so
// the active Odoo database namespace (set after package init, in main) is always
// applied — they must NOT be precomputed into package-level vars.
func (k moveKind) relPath() string        { return odoosource.RelPath(k.file) }
func (k moveKind) privateRelPath() string { return odoosource.PrivateRelPath(k.file) }

func (k moveKind) legacyRelPath() string        { return odoosource.LegacyRelPath(k.file) }
func (k moveKind) legacyPrivateRelPath() string { return odoosource.LegacyPrivateRelPath(k.file) }

// moveReadPath resolves the per-month moves file to read. It prefers the active
// per-database namespaced path, but falls back to the legacy non-namespaced
// path for data synced before namespacing was introduced — otherwise existing
// invoices/bills would read as "none found" until a full re-sync. When neither
// exists it returns the namespaced path so not-exist handling is unchanged.
func moveReadPath(dataDir, year, month string, kind moveKind, private bool) string {
	rel, legacy := kind.relPath(), kind.legacyRelPath()
	if private {
		rel, legacy = kind.privateRelPath(), kind.legacyPrivateRelPath()
	}
	p := filepath.Join(dataDir, year, month, rel)
	if _, err := os.Stat(p); err == nil {
		return p
	}
	if rel != legacy {
		if lp := filepath.Join(dataDir, year, month, legacy); fileExists(lp) {
			return lp
		}
	}
	return p
}

var (
	moveKindInvoice = moveKind{
		label:   "invoice",
		labelPl: "invoices",
		file:    odoosource.InvoicesFile,
		model:   "account.move",
		isBill:  false,
	}
	moveKindBill = moveKind{
		label:   "bill",
		labelPl: "bills",
		file:    odoosource.BillsFile,
		model:   "account.move",
		isBill:  true,
	}
)

// loadMovePartners reads a month's *private* moves file and returns a map of
// move ID → PartnerDisplayName. Used to enrich the categorize TUI with
// counterpart info without leaking PII into the public files.
func loadMovePartners(dataDir, year, month string, kind moveKind) map[int]string {
	path := moveReadPath(dataDir, year, month, kind, true)
	data, err := os.ReadFile(path)
	if err != nil {
		return map[int]string{}
	}
	out := map[int]string{}
	if kind.isBill {
		var f OdooVendorBillsPrivateFile
		if err := json.Unmarshal(data, &f); err != nil {
			return out
		}
		for _, b := range f.Bills {
			if name := firstNonEmptyStr(b.PartnerDisplayName, b.Partner.Name); name != "" {
				out[b.ID] = name
			}
		}
		return out
	}
	var f OdooOutgoingInvoicesPrivateFile
	if err := json.Unmarshal(data, &f); err != nil {
		return out
	}
	for _, inv := range f.Invoices {
		if name := firstNonEmptyStr(inv.PartnerDisplayName, inv.Partner.Name); name != "" {
			out[inv.ID] = name
		}
	}
	return out
}

// loadMoveReferences reads the month's private cache and returns a map of move
// ID -> structured communication / payment reference (e.g.
// "+++000/0031/16831+++"). This lives in the private file only; it's surfaced
// in the local reconcile review (to help match a bank transfer) and never in
// the public export.
func loadMoveReferences(dataDir, year, month string, kind moveKind) map[int]string {
	path := moveReadPath(dataDir, year, month, kind, true)
	data, err := os.ReadFile(path)
	if err != nil {
		return map[int]string{}
	}
	out := map[int]string{}
	var recs []OdooOutgoingInvoicePrivate
	if kind.isBill {
		var f OdooVendorBillsPrivateFile
		if json.Unmarshal(data, &f) != nil {
			return out
		}
		recs = f.Bills
	} else {
		var f OdooOutgoingInvoicesPrivateFile
		if json.Unmarshal(data, &f) != nil {
			return out
		}
		recs = f.Invoices
	}
	for _, r := range recs {
		if ref := strings.TrimSpace(r.Reference); ref != "" {
			out[r.ID] = ref
		}
	}
	return out
}

// loadMoveIBANs reads the month's private cache and returns a map of move ID ->
// counterparty bank account number / IBAN (from PartnerBank). Lives in the
// private file only; surfaced for local search.
func loadMoveIBANs(dataDir, year, month string, kind moveKind) map[int]string {
	path := moveReadPath(dataDir, year, month, kind, true)
	data, err := os.ReadFile(path)
	if err != nil {
		return map[int]string{}
	}
	out := map[int]string{}
	var recs []OdooOutgoingInvoicePrivate
	if kind.isBill {
		var f OdooVendorBillsPrivateFile
		if json.Unmarshal(data, &f) != nil {
			return out
		}
		recs = f.Bills
	} else {
		var f OdooOutgoingInvoicesPrivateFile
		if json.Unmarshal(data, &f) != nil {
			return out
		}
		recs = f.Invoices
	}
	for _, r := range recs {
		if r.PartnerBank == nil {
			continue
		}
		if iban := firstNonEmptyStr(r.PartnerBank.SanitizedNumber, r.PartnerBank.AccountNumber); iban != "" {
			out[r.ID] = iban
		}
	}
	return out
}

// loadMovePartnerIDs reads the month's private cache and returns a map of move
// ID -> Odoo partner id. Lets the contact aggregation reliably group a partner's
// invoices/bills (the public cache only carries the partner display name).
func loadMovePartnerIDs(dataDir, year, month string, kind moveKind) map[int]int {
	path := moveReadPath(dataDir, year, month, kind, true)
	data, err := os.ReadFile(path)
	if err != nil {
		return map[int]int{}
	}
	out := map[int]int{}
	var recs []OdooOutgoingInvoicePrivate
	if kind.isBill {
		var f OdooVendorBillsPrivateFile
		if json.Unmarshal(data, &f) != nil {
			return out
		}
		recs = f.Bills
	} else {
		var f OdooOutgoingInvoicesPrivateFile
		if json.Unmarshal(data, &f) != nil {
			return out
		}
		recs = f.Invoices
	}
	for _, r := range recs {
		if r.Partner.ID != 0 {
			out[r.ID] = r.Partner.ID
		}
	}
	return out
}

// loadMoves reads a single month's public moves file (invoices.json or
// bills.json) and returns the unmarshalled records. Returns (nil, nil) if
// the file doesn't exist — callers should treat that as "empty month".
func loadMoves(dataDir, year, month string, kind moveKind) ([]OdooOutgoingInvoicePublic, error) {
	path := moveReadPath(dataDir, year, month, kind, false)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	if kind.isBill {
		var f OdooVendorBillsFile
		if err := json.Unmarshal(data, &f); err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		return f.Bills, nil
	}
	var f OdooOutgoingInvoicesFile
	if err := json.Unmarshal(data, &f); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return f.Invoices, nil
}

// saveMoves rewrites a month's moves file with an updated slice, keeping the
// top-level metadata fields intact. Used by the categorize command to persist
// annotations without touching the rest of the payload.
func saveMoves(dataDir, year, month string, kind moveKind, moves []OdooOutgoingInvoicePublic) error {
	path := moveReadPath(dataDir, year, month, kind, false)
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if kind.isBill {
		var f OdooVendorBillsFile
		if err := json.Unmarshal(data, &f); err != nil {
			return err
		}
		f.Bills = moves
		f.Count = len(moves)
		out, err := json.MarshalIndent(f, "", "  ")
		if err != nil {
			return err
		}
		return writeMonthFile(dataDir, year, month, kind.relPath(), out)
	}
	var f OdooOutgoingInvoicesFile
	if err := json.Unmarshal(data, &f); err != nil {
		return err
	}
	f.Invoices = moves
	f.Count = len(moves)
	out, err := json.MarshalIndent(f, "", "  ")
	if err != nil {
		return err
	}
	return writeMonthFile(dataDir, year, month, kind.relPath(), out)
}

// walkMoveMonths calls fn for every (year, month) pair under dataDir that
// contains a file for the given kind, in chronological order.
func walkMoveMonths(dataDir string, kind moveKind, fn func(year, month string) error) error {
	type ym struct{ year, month string }
	var months []ym
	yearEntries, err := os.ReadDir(dataDir)
	if err != nil {
		return err
	}
	for _, ye := range yearEntries {
		if !ye.IsDir() || len(ye.Name()) != 4 {
			continue
		}
		monthEntries, _ := os.ReadDir(filepath.Join(dataDir, ye.Name()))
		for _, me := range monthEntries {
			if !me.IsDir() || len(me.Name()) != 2 {
				continue
			}
			if _, err := os.Stat(moveReadPath(dataDir, ye.Name(), me.Name(), kind, false)); err != nil {
				continue
			}
			months = append(months, ym{ye.Name(), me.Name()})
		}
	}
	sort.Slice(months, func(i, j int) bool {
		if months[i].year != months[j].year {
			return months[i].year < months[j].year
		}
		return months[i].month < months[j].month
	})
	for _, m := range months {
		if err := fn(m.year, m.month); err != nil {
			return err
		}
	}
	return nil
}

// moveDisplayLabel renders a short one-line description of a move for TUI
// selection. Example: "INV/2024/0001 — €1,234.56 EUR — 2024-03-15".
func moveDisplayLabel(m OdooOutgoingInvoicePublic) string {
	parts := []string{}
	if m.Title != "" {
		parts = append(parts, m.Title)
	}
	parts = append(parts, fmt.Sprintf("%.2f %s", m.TotalAmount, strings.ToUpper(firstNonEmptyStr(m.Currency, "EUR"))))
	if m.Date != "" {
		parts = append(parts, m.Date)
	}
	return strings.Join(parts, " — ")
}

func firstNonEmptyStr(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}
