package cmd

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	nostrsource "github.com/CommonsHub/chb/sources/nostr"
)

func sumSpreadAmounts(spread []SpreadEntry) string {
	var total float64
	for _, e := range spread {
		v, err := strconv.ParseFloat(e.Amount, 64)
		if err != nil {
			continue
		}
		total += v
	}
	return strconv.FormatFloat(total, 'f', 2, 64)
}

// InboundSpread is one allocation that lands in a target month, carrying enough
// denormalized data to render a row without re-opening the source month's
// transactions.json. URI + NaturalYM is the back-reference the consumer uses
// to jump to the originating transaction.
type InboundSpread struct {
	URI          string `json:"uri"`
	NaturalYM    string `json:"naturalYM"`
	TxID         string `json:"txID"`
	Amount       string `json:"amount"`            // allocation for this target month
	Total        string `json:"total,omitempty"`   // sum of all spread allocations on the source tx
	Currency     string `json:"currency,omitempty"`
	Type         string `json:"type,omitempty"`
	Counterparty string `json:"counterparty,omitempty"`
	Category     string `json:"category,omitempty"`
	Collective   string `json:"collective,omitempty"`
	Description  string `json:"description,omitempty"`
}

type InboundSpreadsFile struct {
	Year      string          `json:"year"`
	Month     string          `json:"month"`
	UpdatedAt string          `json:"updatedAt"`
	Inbound   []InboundSpread `json:"inbound"`
}

const inboundSpreadsFile = "inbound_spreads.json"

// inboundSpreadsPath returns data/<year>/<month>/generated/inbound_spreads.json.
func inboundSpreadsPath(dataDir, year, month string) string {
	return filepath.Join(dataDir, year, month, "generated", inboundSpreadsFile)
}

// LoadInboundSpreads reads the inbound spreads for a given target month.
// Returns nil if the file is missing or empty.
func LoadInboundSpreads(dataDir, year, month string) []InboundSpread {
	data, err := os.ReadFile(inboundSpreadsPath(dataDir, year, month))
	if err != nil {
		return nil
	}
	var f InboundSpreadsFile
	if json.Unmarshal(data, &f) != nil {
		return nil
	}
	return f.Inbound
}

// scanAnnotationCachesForSpreads walks every existing
// data/<Y>/<M>/sources/nostr/transaction-annotations.json file and, for each
// annotation with a non-empty Spread, emits one InboundSpread per spread entry,
// keyed by target month. Display fields (currency, counterparty, category, …)
// are filled in by joining with the natural-month's transactions.json — loaded
// lazily, once per natural month.
func scanAnnotationCachesForSpreads(dataDir string) (map[string][]InboundSpread, error) {
	years, err := os.ReadDir(dataDir)
	if err != nil {
		return nil, err
	}
	inbound := map[string][]InboundSpread{}
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
			naturalYM := year + "-" + month

			annPath := nostrsource.Path(dataDir, year, month, nostrsource.AnnotationsFile)
			data, err := os.ReadFile(annPath)
			if err != nil {
				continue
			}
			var cache NostrAnnotationCache
			if json.Unmarshal(data, &cache) != nil {
				continue
			}

			var txIndex map[string]TransactionEntry // lazy, keyed by URI
			loadIndex := func() map[string]TransactionEntry {
				if txIndex != nil {
					return txIndex
				}
				txIndex = map[string]TransactionEntry{}
				txData, err := os.ReadFile(filepath.Join(dataDir, year, month, "generated", "transactions.json"))
				if err != nil {
					return txIndex
				}
				var f TransactionsFile
				if json.Unmarshal(txData, &f) != nil {
					return txIndex
				}
				for _, tx := range f.Transactions {
					if uri := txURI(tx); uri != "" {
						txIndex[uri] = tx
					}
				}
				return txIndex
			}

			for uri, ann := range cache.Annotations {
				if ann == nil || len(ann.Spread) == 0 {
					continue
				}
				idx := loadIndex()
				tx, hasTx := idx[uri]
				totalStr := sumSpreadAmounts(ann.Spread)
				for _, e := range ann.Spread {
					if e.Month == "" {
						continue
					}
					row := InboundSpread{
						URI:        uri,
						NaturalYM:  naturalYM,
						Amount:     e.Amount,
						Total:      totalStr,
						Category:   ann.Category,
						Collective: ann.Collective,
					}
					if hasTx {
						row.TxID = tx.ID
						row.Currency = tx.Currency
						row.Type = tx.Type
						row.Counterparty = tx.Counterparty
						if ann.Description != "" {
							row.Description = ann.Description
						} else if d, ok := tx.Metadata["description"].(string); ok {
							row.Description = d
						}
						if row.Category == "" {
							row.Category = tx.Category
						}
						if row.Collective == "" {
							row.Collective = tx.Collective
						}
					}
					inbound[e.Month] = append(inbound[e.Month], row)
				}
			}
		}
	}
	for ym := range inbound {
		sortInboundSpreads(inbound[ym])
	}
	return inbound, nil
}

func sortInboundSpreads(rows []InboundSpread) {
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].NaturalYM != rows[j].NaturalYM {
			return rows[i].NaturalYM < rows[j].NaturalYM
		}
		return rows[i].URI < rows[j].URI
	})
}

// rebuildInboundSpreads writes inbound_spreads.json for every target month
// referenced by any current spread, and deletes orphan files (target months
// that previously had inbound but no longer do). Always rebuilds from scratch
// so spread reductions/relocations don't leave stale entries.
func rebuildInboundSpreads(dataDir string) error {
	current, err := findExistingInboundSpreadFiles(dataDir)
	if err != nil {
		return err
	}
	target, err := scanAnnotationCachesForSpreads(dataDir)
	if err != nil {
		return err
	}

	now := time.Now().UTC().Format(time.RFC3339)
	written := map[string]bool{}
	for ym, rows := range target {
		parts := strings.SplitN(ym, "-", 2)
		if len(parts) != 2 {
			continue
		}
		year, month := parts[0], parts[1]
		f := InboundSpreadsFile{
			Year:      year,
			Month:     month,
			UpdatedAt: now,
			Inbound:   rows,
		}
		data, err := json.MarshalIndent(f, "", "  ")
		if err != nil {
			return err
		}
		path := inboundSpreadsPath(dataDir, year, month)
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			return err
		}
		if err := os.WriteFile(path, data, 0644); err != nil {
			return err
		}
		written[path] = true
	}
	for path := range current {
		if !written[path] {
			_ = os.Remove(path)
		}
	}
	return nil
}

func findExistingInboundSpreadFiles(dataDir string) (map[string]bool, error) {
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
			path := inboundSpreadsPath(dataDir, yearEntry.Name(), monthEntry.Name())
			if _, err := os.Stat(path); err == nil {
				out[path] = true
			}
		}
	}
	return out, nil
}

