package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	odoosource "github.com/CommonsHub/chb/sources/odoo"
)

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
	label          string // human label used in prompts and logs ("invoice", "bill")
	labelPl        string // plural ("invoices", "bills")
	relPath        string // per-month public source path, e.g. sources/odoo/invoices.json
	privateRelPath string // per-month private path with PII
	model          string // Odoo model technical name
	isBill         bool
}

var (
	moveKindInvoice = moveKind{
		label:          "invoice",
		labelPl:        "invoices",
		relPath:        odoosource.RelPath(odoosource.InvoicesFile),
		privateRelPath: odoosource.PrivateRelPath(odoosource.InvoicesFile),
		model:          "account.move",
		isBill:         false,
	}
	moveKindBill = moveKind{
		label:          "bill",
		labelPl:        "bills",
		relPath:        odoosource.RelPath(odoosource.BillsFile),
		privateRelPath: odoosource.PrivateRelPath(odoosource.BillsFile),
		model:          "account.move",
		isBill:         true,
	}
)

// loadMovePartners reads a month's *private* moves file and returns a map of
// move ID → PartnerDisplayName. Used to enrich the categorize TUI with
// counterpart info without leaking PII into the public files.
func loadMovePartners(dataDir, year, month string, kind moveKind) map[int]string {
	path := filepath.Join(dataDir, year, month, kind.privateRelPath)
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

// loadMoves reads a single month's public moves file (invoices.json or
// bills.json) and returns the unmarshalled records. Returns (nil, nil) if
// the file doesn't exist — callers should treat that as "empty month".
func loadMoves(dataDir, year, month string, kind moveKind) ([]OdooOutgoingInvoicePublic, error) {
	path := filepath.Join(dataDir, year, month, kind.relPath)
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
	path := filepath.Join(dataDir, year, month, kind.relPath)
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
		return writeMonthFile(dataDir, year, month, kind.relPath, out)
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
	return writeMonthFile(dataDir, year, month, kind.relPath, out)
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
			if _, err := os.Stat(filepath.Join(dataDir, ye.Name(), me.Name(), kind.relPath)); err != nil {
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
