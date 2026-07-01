// Package wise parses Wise (formerly TransferWise) account-statement CSV
// exports. Wise has no API access here — the operator downloads a per-balance
// statement CSV and drops it under latest/providers/wise/. One Wise account
// holds several currency/purpose "balances" (jars); each exports its own file
// named statement_<balanceId>_<CUR>_<from>_<to>.csv, and each balance maps to
// its own Odoo bank journal.
package wise

import (
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

const Source = "wise"

// Transaction is one parsed row of a Wise statement CSV.
type Transaction struct {
	BalanceID   string  `json:"balanceId"`  // Wise balance/jar id, from the filename
	TransferID  string  `json:"transferId"` // Wise "TransferWise ID" (TRANSFER-…, BALANCE-…)
	Date        string  `json:"date"`       // ISO YYYY-MM-DD
	Timestamp   int64   `json:"timestamp"`
	Amount      float64 `json:"amount"` // signed (credit +, debit −)
	Currency    string  `json:"currency"`
	Balance     float64 `json:"balance"` // running balance after this row
	Description string  `json:"description"`
	Reference   string  `json:"reference,omitempty"`
	PayerName   string  `json:"payerName,omitempty"`
	PayeeName   string  `json:"payeeName,omitempty"`
	Merchant    string  `json:"merchant,omitempty"`
	TotalFees   float64 `json:"totalFees,omitempty"`
	Hash        string  `json:"hash"` // deterministic short hash of the row
	// ImportSuffix disambiguates the ImportID when one TransferID survives as
	// more than one leg (a transfer sent then returned — same id, opposite
	// amounts). Empty for the primary leg (keeps the historic ImportID);
	// set to the row hash on any additional leg. Not serialised.
	ImportSuffix string `json:"-"`
}

// RelPath returns the path inside DATA_DIR for this provider.
func RelPath(elems ...string) string {
	return filepath.Join(append([]string{"providers", Source}, elems...)...)
}

// LatestDir is the folder where the operator drops Wise statement CSVs.
func LatestDir(dataDir string) string {
	return filepath.Join(dataDir, "latest", "providers", Source)
}

// Counterparty returns the most useful name for the other side of the tx:
// the payee for debits, the payer for credits, falling back to the merchant.
func (t Transaction) Counterparty() string {
	if t.Amount < 0 {
		if t.PayeeName != "" {
			return t.PayeeName
		}
	} else if t.PayerName != "" {
		return t.PayerName
	}
	if t.Merchant != "" {
		return t.Merchant
	}
	if t.PayeeName != "" {
		return t.PayeeName
	}
	return t.PayerName
}

// ImportID is the stable unique_import_id used on the Odoo side. The Wise
// transfer id is globally unique, but the same id appears on both legs of an
// intra-Wise conversion (one per balance), so it's scoped by balance. When a
// single TransferID survives as more than one leg (sent then returned), the
// extra legs carry an ImportSuffix so they don't collide in Odoo.
func (t Transaction) ImportID() string {
	id := fmt.Sprintf("%s:%s:%s", Source, t.BalanceID, t.TransferID)
	if t.ImportSuffix != "" {
		id += ":" + t.ImportSuffix
	}
	return id
}

// balanceIDFromFilename extracts the Wise balance id from
// statement_<balanceId>_<CUR>_<from>_<to>.csv.
func balanceIDFromFilename(name string) string {
	base := strings.TrimSuffix(filepath.Base(name), filepath.Ext(name))
	parts := strings.Split(base, "_")
	if len(parts) >= 2 && strings.EqualFold(parts[0], "statement") {
		return parts[1]
	}
	return ""
}

// LoadAllTransactions reads every statement CSV under latest/providers/wise/
// and returns the parsed rows, deduplicated by (balance, transfer id, amount)
// keeping the latest sighting, sorted by (timestamp, hash). Overlapping
// quarterly exports (identical amount) therefore collapse to one row, while a
// transfer that was sent then returned (same id, opposite amounts) keeps both
// legs — the extra leg carries an ImportSuffix so its ImportID stays unique.
func LoadAllTransactions(dataDir string) ([]Transaction, error) {
	dir := LatestDir(dataDir)
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read %s: %v", dir, err)
	}
	byKey := map[string]Transaction{}
	// primaryAmt records, per balance|transferId, the amount of the leg the
	// legacy dedup would have kept (last write wins in file/row order). That
	// leg keeps the historic ImportID — it's the one already imported into
	// Odoo — while any additional leg gets a suffixed ImportID below.
	primaryAmt := map[string]string{}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || strings.HasPrefix(name, "._") || !strings.HasSuffix(strings.ToLower(name), ".csv") {
			continue
		}
		balanceID := balanceIDFromFilename(name)
		if balanceID == "" {
			continue
		}
		txs, err := LoadCSV(filepath.Join(dir, name), balanceID)
		if err != nil {
			return nil, fmt.Errorf("%s: %v", name, err)
		}
		for _, tx := range txs {
			// Key by amount too: overlapping quarterly exports repeat the
			// same transaction (identical amount) and must collapse, but a
			// transfer that was sent then returned appears under one
			// TransferID as two opposite-sign legs — both are real and must
			// survive.
			byKey[fmt.Sprintf("%s|%s|%.2f", tx.BalanceID, tx.TransferID, tx.Amount)] = tx
			primaryAmt[tx.BalanceID+"|"+tx.TransferID] = fmt.Sprintf("%.2f", tx.Amount)
		}
	}
	out := make([]Transaction, 0, len(byKey))
	for _, tx := range byKey {
		out = append(out, tx)
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Timestamp == out[j].Timestamp {
			return out[i].Hash < out[j].Hash
		}
		return out[i].Timestamp < out[j].Timestamp
	})
	// When a TransferID kept more than one leg, the leg the legacy dedup would
	// have kept (already imported into Odoo with the historic ImportID) stays
	// plain; every other leg gets a hash-suffixed ImportID so it doesn't
	// collide on Odoo's unique_import_id.
	for i := range out {
		k := out[i].BalanceID + "|" + out[i].TransferID
		if primaryAmt[k] != fmt.Sprintf("%.2f", out[i].Amount) {
			out[i].ImportSuffix = out[i].Hash
		}
	}
	return out, nil
}

// LoadForBalance returns the rows for one Wise balance id, sorted.
func LoadForBalance(dataDir, balanceID string) ([]Transaction, error) {
	all, err := LoadAllTransactions(dataDir)
	if err != nil {
		return nil, err
	}
	var out []Transaction
	for _, tx := range all {
		if tx.BalanceID == balanceID {
			out = append(out, tx)
		}
	}
	return out, nil
}

// LoadCSV parses one Wise statement export. Wise uses standard RFC-4180 CSV
// (comma separator, double-quoted fields), DD-MM-YYYY dates and dot decimals.
// Columns are matched by header name so column-order changes don't break it.
func LoadCSV(path, balanceID string) ([]Transaction, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	text := strings.TrimPrefix(string(raw), "\xef\xbb\xbf")
	r := csv.NewReader(strings.NewReader(text))
	r.FieldsPerRecord = -1

	header, err := r.Read()
	if err != nil {
		if err == io.EOF {
			return nil, nil
		}
		return nil, fmt.Errorf("read header: %v", err)
	}
	col := map[string]int{}
	for i, h := range header {
		col[strings.ToLower(strings.TrimSpace(h))] = i
	}
	get := func(row []string, name string) string {
		if i, ok := col[name]; ok && i < len(row) {
			return strings.TrimSpace(row[i])
		}
		return ""
	}

	var out []Transaction
	for {
		row, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		tid := get(row, "transferwise id")
		if tid == "" {
			continue
		}
		date, ok := parseWiseDate(get(row, "date"))
		if !ok {
			continue
		}
		amount, ok := parseWiseNumber(get(row, "amount"))
		if !ok {
			continue
		}
		balance, _ := parseWiseNumber(get(row, "running balance"))
		fees, _ := parseWiseNumber(get(row, "total fees"))
		tx := Transaction{
			BalanceID:   balanceID,
			TransferID:  tid,
			Date:        date.Format("2006-01-02"),
			Timestamp:   date.Unix(),
			Amount:      amount,
			Currency:    get(row, "currency"),
			Balance:     balance,
			Description: get(row, "description"),
			Reference:   get(row, "payment reference"),
			PayerName:   get(row, "payer name"),
			PayeeName:   get(row, "payee name"),
			Merchant:    get(row, "merchant"),
			TotalFees:   fees,
		}
		tx.Hash = rowHash(tx)
		out = append(out, tx)
	}
	return out, nil
}

func brusselsTZ() *time.Location {
	if loc, err := time.LoadLocation("Europe/Brussels"); err == nil {
		return loc
	}
	return time.UTC
}

// parseWiseDate parses Wise's "DD-MM-YYYY" at Brussels noon.
func parseWiseDate(s string) (time.Time, bool) {
	if s == "" {
		return time.Time{}, false
	}
	t, err := time.ParseInLocation("02-01-2006", s, brusselsTZ())
	if err != nil {
		return time.Time{}, false
	}
	return time.Date(t.Year(), t.Month(), t.Day(), 12, 0, 0, 0, brusselsTZ()), true
}

// parseWiseNumber parses Wise's dot-decimal amounts ("-5000.00"). Empty → (0,false).
func parseWiseNumber(s string) (float64, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, false
	}
	v, err := strconv.ParseFloat(strings.ReplaceAll(s, ",", ""), 64)
	if err != nil {
		return 0, false
	}
	return v, true
}

func rowHash(tx Transaction) string {
	h := sha256.New()
	h.Write([]byte(strings.Join([]string{
		tx.BalanceID, tx.TransferID, tx.Date,
		fmt.Sprintf("%.2f", tx.Amount), fmt.Sprintf("%.2f", tx.Balance),
	}, "|")))
	return hex.EncodeToString(h.Sum(nil)[:8])
}
