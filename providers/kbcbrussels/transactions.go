package kbcbrussels

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

// Column indexes in the CSV header. KBC keeps the same column order
// across exports; if that ever changes we'd see parse errors loudly.
const (
	colAccountNumber = iota
	colHeading
	colName
	colCurrency
	colStatementNumber
	colDate
	colDescription
	colValueDate
	colAmount
	colBalance
	colCredit
	colDebit
	colCounterpartyIBAN
	colCounterpartyBIC
	colCounterpartyName
	colCounterpartyAddress
	colStandardReference
	colFreeReference
)

// brusselsTZ returns the canonical timezone for booking dates. We resolve
// it lazily and fall back to UTC if the system tzdata is missing so the
// parser stays self-contained (the cmd/format.go helper uses the same
// label).
func brusselsTZ() *time.Location {
	if loc, err := time.LoadLocation("Europe/Brussels"); err == nil {
		return loc
	}
	return time.UTC
}

// LoadAllTransactions reads every CSV in latest/providers/kbcbrussels and
// returns the parsed rows, deduplicated by (account, hash), sorted by
// (timestamp, hash). Each row's running balance is preserved as-is from
// the export — we don't recompute it.
func LoadAllTransactions(dataDir string) ([]Transaction, error) {
	dir := LatestDir(dataDir)
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read %s: %v", dir, err)
	}
	seen := map[string]bool{}
	var out []Transaction
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(strings.ToLower(e.Name()), ".csv") {
			continue
		}
		path := filepath.Join(dir, e.Name())
		txs, err := LoadCSV(path)
		if err != nil {
			return nil, fmt.Errorf("%s: %v", e.Name(), err)
		}
		for _, tx := range txs {
			key := tx.AccountIBAN + ":" + tx.Hash
			if seen[key] {
				continue
			}
			seen[key] = true
			out = append(out, tx)
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Timestamp == out[j].Timestamp {
			return out[i].Hash < out[j].Hash
		}
		return out[i].Timestamp < out[j].Timestamp
	})
	return out, nil
}

// LoadCSV parses one KBC export file. The file uses ';' as separator,
// '\r' (CR-only) line endings, DD/MM/YYYY dates and comma decimals.
func LoadCSV(path string) ([]Transaction, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	// KBC exports use CR-only line endings. Normalize to \n so encoding/csv
	// reads it correctly. Also strip a UTF-8 BOM if the file carries one.
	text := string(raw)
	text = strings.TrimPrefix(text, "\xef\xbb\xbf") // strip UTF-8 BOM if present
	text = strings.ReplaceAll(text, "\r\n", "\n")
	text = strings.ReplaceAll(text, "\r", "\n")

	r := csv.NewReader(strings.NewReader(text))
	r.Comma = ';'
	r.FieldsPerRecord = -1 // tolerate trailing-blank-column variations

	header, err := r.Read()
	if err != nil {
		return nil, fmt.Errorf("read header: %v", err)
	}
	if len(header) < colFreeReference+1 {
		return nil, fmt.Errorf("unexpected column count %d, want at least %d", len(header), colFreeReference+1)
	}

	var out []Transaction
	lineNo := 1
	for {
		row, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("line %d: %v", lineNo+1, err)
		}
		lineNo++
		if len(row) < colFreeReference+1 {
			continue
		}
		tx, ok := parseRow(row)
		if !ok {
			continue
		}
		out = append(out, tx)
	}
	return out, nil
}

func parseRow(row []string) (Transaction, bool) {
	iban := NormalizeIBAN(row[colAccountNumber])
	if iban == "" {
		return Transaction{}, false
	}
	rawDate := strings.TrimSpace(row[colDate])
	date, ok := parseKBCDate(rawDate)
	if !ok {
		return Transaction{}, false
	}
	amount, ok := parseKBCNumber(row[colAmount])
	if !ok {
		return Transaction{}, false
	}
	balance, _ := parseKBCNumber(row[colBalance])
	valueDate, _ := parseKBCDate(strings.TrimSpace(row[colValueDate]))

	tx := Transaction{
		AccountIBAN:         iban,
		AccountName:         strings.TrimSpace(row[colName]),
		Currency:            strings.TrimSpace(row[colCurrency]),
		StatementNumber:     strings.TrimSpace(row[colStatementNumber]),
		Date:                date.Format("2006-01-02"),
		ValueDate:           valueDate.Format("2006-01-02"),
		Timestamp:           date.Unix(),
		Description:         strings.TrimSpace(row[colDescription]),
		Amount:              amount,
		Balance:             balance,
		CounterpartyIBAN:    NormalizeIBAN(row[colCounterpartyIBAN]),
		CounterpartyBIC:     strings.TrimSpace(row[colCounterpartyBIC]),
		CounterpartyName:    strings.TrimSpace(row[colCounterpartyName]),
		CounterpartyAddress: strings.TrimSpace(row[colCounterpartyAddress]),
		StandardReference:   strings.TrimSpace(row[colStandardReference]),
		FreeReference:       strings.TrimSpace(row[colFreeReference]),
	}
	tx.Hash = rowHash(tx)
	return tx, true
}

// parseKBCDate parses KBC's "DD/MM/YYYY" date format at noon Brussels —
// the time-of-day doesn't matter for booking, but a non-zero offset
// avoids accidental cross-day classification under TZ math.
func parseKBCDate(s string) (time.Time, bool) {
	if s == "" {
		return time.Time{}, false
	}
	t, err := time.ParseInLocation("02/01/2006", s, brusselsTZ())
	if err != nil {
		return time.Time{}, false
	}
	return time.Date(t.Year(), t.Month(), t.Day(), 12, 0, 0, 0, brusselsTZ()), true
}

// parseKBCNumber parses KBC's European number format ("-2,99" → -2.99).
// Empty input returns (0, false) so callers can distinguish "no value"
// from a true zero.
func parseKBCNumber(s string) (float64, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, false
	}
	// KBC uses "," as decimal separator and never groups thousands.
	s = strings.ReplaceAll(s, ",", ".")
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, false
	}
	return v, true
}

// rowHash hashes the stable fields of a row into a 16-hex-char string,
// stable across re-exports of the same data. Description is included
// because amount+date alone can collide for repeated direct debits.
func rowHash(tx Transaction) string {
	h := sha256.New()
	parts := []string{
		strings.ToLower(tx.AccountIBAN),
		tx.StatementNumber,
		tx.Date,
		fmt.Sprintf("%.2f", tx.Amount),
		fmt.Sprintf("%.2f", tx.Balance),
		strings.ToLower(tx.CounterpartyIBAN),
		tx.Description,
	}
	h.Write([]byte(strings.Join(parts, "|")))
	return hex.EncodeToString(h.Sum(nil)[:8])
}

// LoadTransactionsForMonth returns CSV rows for a given IBAN whose
// booking date falls in YYYY-MM. Year + month are zero-padded strings.
// An empty IBAN matches all accounts.
func LoadTransactionsForMonth(dataDir, iban, year, month string) ([]Transaction, error) {
	all, err := LoadAllTransactions(dataDir)
	if err != nil {
		return nil, err
	}
	prefix := year + "-" + month + "-"
	wantIBAN := NormalizeIBAN(iban)
	var out []Transaction
	for _, tx := range all {
		if !strings.HasPrefix(tx.Date, prefix) {
			continue
		}
		if wantIBAN != "" && !strings.EqualFold(tx.AccountIBAN, wantIBAN) {
			continue
		}
		out = append(out, tx)
	}
	return out, nil
}

// PreferredDescription picks the most readable label for a KBC tx using
// the same priority that KBC Brussels' own statements use:
//
//  1. Free-format reference  (operator-supplied label)
//  2. Standard-format reference  (structured creditor reference)
//  3. substring after "REFERENCE" in the raw Description (case-insensitive)
//  4. the raw Description, trimmed
//
// The full Description is always preserved separately (callers that need
// to render the raw bank text use .Description directly).
func PreferredDescription(tx Transaction) string {
	if s := strings.TrimSpace(tx.FreeReference); s != "" {
		return s
	}
	if s := strings.TrimSpace(tx.StandardReference); s != "" {
		return s
	}
	if s := extractAfterReference(tx.Description); s != "" {
		return s
	}
	return strings.TrimSpace(tx.Description)
}

// extractAfterReference returns the substring that follows the first
// case-insensitive occurrence of "REFERENCE" in the description, with
// any leading whitespace or ":" trimmed (KBC writes "REFERENCE : foo"
// or "REFERENCE       : foo"). Returns "" if no such marker is found.
func extractAfterReference(desc string) string {
	upper := strings.ToUpper(desc)
	idx := strings.Index(upper, "REFERENCE")
	if idx < 0 {
		return ""
	}
	after := desc[idx+len("REFERENCE"):]
	for {
		trimmed := strings.TrimLeft(after, " \t:")
		if trimmed == after {
			break
		}
		after = trimmed
	}
	return strings.TrimSpace(after)
}

// LoadTransactionsForIBAN returns every row matching the supplied IBAN
// across all exports, sorted by (timestamp, hash). Used by the Odoo
// check command to compare the CSV's authoritative tx set against the
// linked journal.
func LoadTransactionsForIBAN(dataDir, iban string) ([]Transaction, error) {
	all, err := LoadAllTransactions(dataDir)
	if err != nil {
		return nil, err
	}
	wantIBAN := NormalizeIBAN(iban)
	if wantIBAN == "" {
		return all, nil
	}
	var out []Transaction
	for _, tx := range all {
		if strings.EqualFold(tx.AccountIBAN, wantIBAN) {
			out = append(out, tx)
		}
	}
	return out, nil
}
