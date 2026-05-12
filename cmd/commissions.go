package cmd

import (
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)

// commissionRate is the fraction of each collective's monthly gross income
// (incoming grossAmount) that is transferred to commissionHostSlug as the
// fiscal host's commission. Re-derived on every generate.
const commissionRate = 0.10

// commissionHostSlug is the collective that receives commission transfers
// from every other collective. Commissions from this slug itself are skipped.
const commissionHostSlug = "commonshub"

// commissionCategorySlug labels both sides of the synthetic transfer so it
// shows up under one bucket in category breakdowns.
const commissionCategorySlug = "commission"

// Commission is one synthetic 10% transfer for a (collective, currency,
// month). Amount is always non-negative; direction is implicit (DEBIT on
// Collective, CREDIT on commissionHostSlug).
type Commission struct {
	Collective string `json:"collective"`
	Currency   string `json:"currency"`
	Amount     string `json:"amount"`
	Base       string `json:"base"` // gross income that drove the calc
	Rate       string `json:"rate"`
}

type CommissionsFile struct {
	Year      string       `json:"year"`
	Month     string       `json:"month"`
	UpdatedAt string       `json:"updatedAt"`
	Rate      string       `json:"rate"`
	Host      string       `json:"host"`
	Items     []Commission `json:"items"`
}

const commissionsFile = "commissions.json"

func commissionsPath(dataDir, year, month string) string {
	return filepath.Join(dataDir, year, month, "generated", commissionsFile)
}

// LoadCommissions reads the commissions for a given month. Returns nil when
// the file is missing or unreadable.
func LoadCommissions(dataDir, year, month string) []Commission {
	data, err := os.ReadFile(commissionsPath(dataDir, year, month))
	if err != nil {
		return nil
	}
	var f CommissionsFile
	if json.Unmarshal(data, &f) != nil {
		return nil
	}
	return f.Items
}

// rebuildCommissions re-derives commissions.json for every month that has a
// generated/transactions.json, and removes orphan files for months that no
// longer have any qualifying income. Always rebuilt from scratch so rule or
// category changes don't leave stale entries.
func rebuildCommissions(dataDir string) error {
	current, err := findExistingCommissionFiles(dataDir)
	if err != nil {
		return err
	}

	now := time.Now().UTC().Format(time.RFC3339)
	written := map[string]bool{}
	rateStr := strconv.FormatFloat(commissionRate, 'f', -1, 64)

	years, err := os.ReadDir(dataDir)
	if err != nil {
		return err
	}
	for _, yearEntry := range years {
		if !yearEntry.IsDir() || len(yearEntry.Name()) != 4 || !allDigits(yearEntry.Name()) {
			continue
		}
		monthEntries, err := os.ReadDir(filepath.Join(dataDir, yearEntry.Name()))
		if err != nil {
			continue
		}
		for _, monthEntry := range monthEntries {
			if !monthEntry.IsDir() || len(monthEntry.Name()) != 2 || !allDigits(monthEntry.Name()) {
				continue
			}
			year, month := yearEntry.Name(), monthEntry.Name()
			items := commissionsForMonth(dataDir, year, month)
			if len(items) == 0 {
				continue
			}
			f := CommissionsFile{
				Year:      year,
				Month:     month,
				UpdatedAt: now,
				Rate:      rateStr,
				Host:      commissionHostSlug,
				Items:     items,
			}
			data, err := json.MarshalIndent(f, "", "  ")
			if err != nil {
				return err
			}
			path := commissionsPath(dataDir, year, month)
			if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
				return err
			}
			if err := os.WriteFile(path, data, 0644); err != nil {
				return err
			}
			written[path] = true
		}
	}
	for path := range current {
		if !written[path] {
			_ = os.Remove(path)
		}
	}
	return nil
}

// commissionsForMonth scans this month's transactions.json and returns one
// commission entry per (collective, currency) with non-zero gross income.
// commissionHostSlug never pays a commission to itself.
func commissionsForMonth(dataDir, year, month string) []Commission {
	data, err := os.ReadFile(filepath.Join(dataDir, year, month, "generated", "transactions.json"))
	if err != nil {
		return nil
	}
	var f TransactionsFile
	if json.Unmarshal(data, &f) != nil {
		return nil
	}

	type key struct{ collective, currency string }
	totals := map[key]float64{}
	for _, tx := range f.Transactions {
		if !tx.IsIncoming() {
			continue
		}
		if tx.Collective == "" || tx.Collective == commissionHostSlug {
			continue
		}
		gross := math.Abs(firstNonZeroFloat(tx.GrossAmount, tx.Amount, tx.NormalizedAmount, tx.NetAmount))
		if gross == 0 {
			continue
		}
		totals[key{tx.Collective, tx.Currency}] += gross
	}

	rateStr := strconv.FormatFloat(commissionRate, 'f', -1, 64)
	out := make([]Commission, 0, len(totals))
	for k, base := range totals {
		amt := roundReportAmount(base * commissionRate)
		if amt == 0 {
			continue
		}
		out = append(out, Commission{
			Collective: k.collective,
			Currency:   k.currency,
			Amount:     strconv.FormatFloat(amt, 'f', 2, 64),
			Base:       strconv.FormatFloat(roundReportAmount(base), 'f', 2, 64),
			Rate:       rateStr,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Collective != out[j].Collective {
			return out[i].Collective < out[j].Collective
		}
		return out[i].Currency < out[j].Currency
	})
	return out
}

func findExistingCommissionFiles(dataDir string) (map[string]bool, error) {
	out := map[string]bool{}
	years, err := os.ReadDir(dataDir)
	if err != nil {
		return nil, err
	}
	for _, yearEntry := range years {
		if !yearEntry.IsDir() || len(yearEntry.Name()) != 4 || !allDigits(yearEntry.Name()) {
			continue
		}
		monthEntries, err := os.ReadDir(filepath.Join(dataDir, yearEntry.Name()))
		if err != nil {
			continue
		}
		for _, monthEntry := range monthEntries {
			if !monthEntry.IsDir() || len(monthEntry.Name()) != 2 || !allDigits(monthEntry.Name()) {
				continue
			}
			path := commissionsPath(dataDir, yearEntry.Name(), monthEntry.Name())
			if _, err := os.Stat(path); err == nil {
				out[path] = true
			}
		}
	}
	return out, nil
}
