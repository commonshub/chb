// Package annualaccounts reads the annual accounts of the association
// (Belgian abbreviated schema for associations, as produced by Odoo or
// filed with the National Bank): PDF text lines → figures by NBB code.
package annualaccounts

import (
	"math"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/ledongthuc/pdf"
)

// Source is the provider directory name.
const Source = "annual-accounts"

// RelPath returns providers/annual-accounts/<elems…>.
func RelPath(elems ...string) string {
	return filepath.Join(append([]string{"providers", Source}, elems...)...)
}

// Document kinds. Only the abbreviated statements are ever published.
const (
	KindBalanceSheet  = "balance-sheet"
	KindProfitAndLoss = "profit-and-loss"
	KindTrialBalance  = "trial-balance"
	KindInternal      = "internal"
	KindOther         = "other"
)

// PublicKind reports whether a document kind may be published: the
// filed abbreviated statements hold class-level aggregates only.
func PublicKind(kind string) bool {
	return kind == KindBalanceSheet || kind == KindProfitAndLoss
}

// DetectKind classifies a document from its file name and first lines.
func DetectKind(name string, lines []string) string {
	n := strings.ToLower(filepath.Base(name))
	head := strings.ToLower(strings.Join(firstN(lines, 15), " "))
	switch {
	case strings.Contains(n, "interne") || strings.Contains(n, "internal"):
		return KindInternal
	case strings.Contains(n, "trial") || strings.Contains(n, "balance_generale") || strings.Contains(head, "trial balance"):
		return KindTrialBalance
	case strings.Contains(n, "balance_sheet") || strings.Contains(head, "balance sheet (abbr") || strings.Contains(head, "bilan abrégé"):
		return KindBalanceSheet
	case strings.Contains(n, "profit_and_loss") || strings.Contains(head, "profit and loss (abbr") || strings.Contains(head, "compte de résultats abrégé"):
		return KindProfitAndLoss
	}
	return KindOther
}

func firstN(s []string, n int) []string {
	if len(s) < n {
		return s
	}
	return s[:n]
}

// ExtractLines returns the text of a PDF line by line, rebuilt from the
// glyph positions (top to bottom, left to right; " | " marks a column gap).
func ExtractLines(path string) ([]string, error) {
	f, r, err := pdf.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var out []string
	for i := 1; i <= r.NumPage(); i++ {
		type line struct {
			y     float64
			parts []pdf.Text
		}
		var lines []*line
		for _, t := range r.Page(i).Content().Text {
			var cur *line
			for _, l := range lines {
				if math.Abs(l.y-t.Y) < 2 {
					cur = l
					break
				}
			}
			if cur == nil {
				cur = &line{y: t.Y}
				lines = append(lines, cur)
			}
			cur.parts = append(cur.parts, t)
		}
		sort.Slice(lines, func(a, b int) bool { return lines[a].y > lines[b].y })
		for _, l := range lines {
			sort.Slice(l.parts, func(a, b int) bool { return l.parts[a].X < l.parts[b].X })
			var sb strings.Builder
			lastX := -1.0
			for _, p := range l.parts {
				if lastX >= 0 && p.X-lastX > p.FontSize*1.5 {
					sb.WriteString(" | ")
				}
				sb.WriteString(p.S)
				lastX = p.X + p.W
			}
			out = append(out, strings.TrimSpace(sb.String()))
		}
	}
	return out, nil
}

// Figure is one line of a statement.
type Figure struct {
	Code   string // NBB code as printed ("20/58", "9904", "(14)", "14P"); "14.otherAppropriations" for Odoo's uncoded sub-lines
	Label  string
	Amount float64 // euros
}

var (
	codeLine   = regexp.MustCompile(`^(\(?[0-9]{1,4}[A-Z]?(?:/[0-9]{1,3}[A-Z]?)?\)?|[0-9]{2}P)\s+-\s+(.+?)(?:\s+\|\s+(-?[0-9][0-9,]*\.[0-9]{2}))?$`)
	amountOnly = regexp.MustCompile(`^-?[0-9][0-9,]*\.[0-9]{2}$`)
	asOfDate   = regexp.MustCompile(`As of\s*\|?\s*([0-9]{2})/([0-9]{2})/([0-9]{4})`)
)

// subLines are the uncoded detail lines Odoo prints under code 14.
var subLines = map[string]string{
	"profit (loss) of the year":            "14.profitOfTheYear",
	"other appropriations of the year":     "14.otherAppropriations",
	"profits (losses) from previous years": "14.previousYears",
}

// ParseFigures reads the coded lines of a statement. A label that wraps
// with its amount on the line above or below is joined.
func ParseFigures(lines []string) []Figure {
	var out []Figure
	for i, l := range lines {
		if m := codeLine.FindStringSubmatch(l); m != nil {
			amount := m[3]
			if amount == "" {
				// "9903 - … (+)/(-)" with its amount alone on a neighbour line.
				for _, j := range []int{i - 1, i + 1} {
					if j >= 0 && j < len(lines) && amountOnly.MatchString(lines[j]) {
						amount = lines[j]
						break
					}
				}
			}
			if amount == "" {
				continue // a heading
			}
			out = append(out, Figure{Code: m[1], Label: strings.TrimSpace(m[2]), Amount: parseAmount(amount)})
			continue
		}
		label, amount, ok := strings.Cut(l, " | ")
		if !ok || !amountOnly.MatchString(strings.TrimSpace(amount)) {
			continue
		}
		if key, ok := subLines[strings.ToLower(strings.TrimSpace(label))]; ok {
			out = append(out, Figure{Code: key, Label: strings.TrimSpace(label), Amount: parseAmount(amount)})
		}
	}
	return out
}

// PeriodEnd finds "As of dd/mm/yyyy" (balance sheet) as YYYY-MM-DD.
func PeriodEnd(lines []string) string {
	for _, l := range lines {
		if m := asOfDate.FindStringSubmatch(l); m != nil {
			return m[3] + "-" + m[2] + "-" + m[1]
		}
	}
	return ""
}

func parseAmount(s string) float64 {
	v, _ := strconv.ParseFloat(strings.ReplaceAll(strings.TrimSpace(s), ",", ""), 64)
	return v
}

// ReadFiguresCSV reads an optional figures override file: one
// "code;amount" (or "code,amount") per line, '#' comments allowed. Use it
// for an NBB structured export or when a PDF cannot be parsed.
func ReadFiguresCSV(path string) ([]Figure, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var out []Figure
	for _, l := range strings.Split(string(data), "\n") {
		l = strings.TrimSpace(l)
		if l == "" || strings.HasPrefix(l, "#") {
			continue
		}
		sep := ";"
		if !strings.Contains(l, ";") {
			sep = ","
		}
		parts := strings.Split(l, sep)
		if len(parts) < 2 || strings.EqualFold(parts[0], "code") {
			continue
		}
		amount := strings.TrimSpace(parts[len(parts)-1])
		if sep == ";" {
			amount = strings.ReplaceAll(amount, ",", ".")
		}
		v, err := strconv.ParseFloat(amount, 64)
		if err != nil {
			continue
		}
		out = append(out, Figure{Code: strings.TrimSpace(parts[0]), Amount: v})
	}
	return out, nil
}
