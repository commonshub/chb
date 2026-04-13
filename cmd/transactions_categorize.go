package cmd

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/huh"
	"github.com/charmbracelet/lipgloss"
	stickertable "github.com/76creates/stickers/table"
)

// ── Bucket: a group of uncategorized transactions sharing a description ──

type txBucket struct {
	Description string
	Count       int
	TotalAmount float64
	FirstDate   time.Time
	LastDate    time.Time
	Currency    string
	TxIDs       []string // IDs of all transactions in this bucket
}

func findUncategorizedBuckets(txs []TransactionEntry) []txBucket {
	tz := BrusselsTZ()
	type bucketData struct {
		count  int
		total  float64
		first  time.Time
		last   time.Time
		curr   string
		ids    []string
	}

	groups := map[string]*bucketData{}

	for _, tx := range txs {
		if tx.Type == "INTERNAL" || tx.Type == "TRANSFER" {
			continue
		}
		// Skip if any assignment is set (category, collective, or event)
		if tx.Category != "" || tx.Collective != "" || tx.Event != "" {
			continue
		}

		desc := ""
		if d, ok := tx.Metadata["description"]; ok {
			if s, ok := d.(string); ok && s != "" {
				desc = s
			}
		}
		if desc == "" {
			desc = tx.Counterparty
		}
		if desc == "" || strings.HasPrefix(desc, "0x0000") {
			continue // skip raw zero addresses
		}

		t := time.Unix(tx.Timestamp, 0).In(tz)
		amt := math.Abs(txAmount(tx))

		b, ok := groups[desc]
		if !ok {
			b = &bucketData{first: t, last: t, curr: tx.Currency}
			groups[desc] = b
		}
		b.count++
		b.total += amt
		b.ids = append(b.ids, tx.ID)
		if t.Before(b.first) {
			b.first = t
		}
		if t.After(b.last) {
			b.last = t
		}
	}

	var buckets []txBucket
	for desc, b := range groups {
		buckets = append(buckets, txBucket{
			Description: desc,
			Count:       b.count,
			TotalAmount: b.total,
			FirstDate:   b.first,
			LastDate:    b.last,
			Currency:    b.curr,
			TxIDs:       b.ids,
		})
	}

	sort.Slice(buckets, func(i, j int) bool {
		return buckets[i].Count > buckets[j].Count
	})

	return buckets
}

// ── Bubbletea model ──

type catAction int

const (
	catQuit catAction = iota
	catAssign
)

type categorizeModel struct {
	table    *stickertable.Table
	buckets  []txBucket
	currency string
	quitting bool
	action   catAction
	width    int
	height   int
}

func newCategorizeModel(buckets []txBucket, w, h int) categorizeModel {
	headers := []string{"Description", "Count", "Total", "First", "Last"}
	t := stickertable.NewTable(w, h, headers)
	t.SetRatio([]int{10, 2, 3, 3, 3})
	t.SetMinWidth([]int{10, 5, 8, 6, 6})

	t.SetStyles(map[stickertable.StyleKey]lipgloss.Style{
		stickertable.StyleKeyHeader: lipgloss.NewStyle().
			Bold(true).Foreground(lipgloss.Color("252")).Background(lipgloss.Color("238")),
		stickertable.StyleKeyRows:           lipgloss.NewStyle().Foreground(lipgloss.Color("252")),
		stickertable.StyleKeyRowsSubsequent: lipgloss.NewStyle().Foreground(lipgloss.Color("252")),
		stickertable.StyleKeyRowsCursor: lipgloss.NewStyle().
			Foreground(lipgloss.Color("255")).Background(lipgloss.Color("236")).Bold(true),
		stickertable.StyleKeyCellCursor: lipgloss.NewStyle().
			Foreground(lipgloss.Color("255")).Background(lipgloss.Color("236")).Bold(true),
		stickertable.StyleKeyFooter: lipgloss.NewStyle().Foreground(lipgloss.Color("240")).Height(1),
	})

	for _, b := range buckets {
		desc := b.Description
		if len(desc) > 50 {
			desc = desc[:47] + "..."
		}

		var amtStr string
		if isEURCurrency(b.Currency) {
			amtStr = fmtEUR(b.TotalAmount)
		} else {
			amtStr = fmt.Sprintf("%.2f %s", b.TotalAmount, b.Currency)
		}

		t.MustAddRows([][]any{{
			fmt.Sprintf(" %s", desc),
			fmt.Sprintf(" %d", b.Count),
			fmt.Sprintf(" %s", amtStr),
			fmt.Sprintf(" %s", b.FirstDate.Format("02/01")),
			fmt.Sprintf(" %s", b.LastDate.Format("02/01")),
		}})
	}

	return categorizeModel{
		table:   t,
		buckets: buckets,
		width:   w,
		height:  h,
	}
}

func (m categorizeModel) Init() tea.Cmd { return nil }

func (m categorizeModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		m.table.SetWidth(msg.Width)
		m.table.SetHeight(msg.Height - 5)
	case tea.KeyMsg:
		switch msg.String() {
		case "q", "ctrl+c", "esc":
			m.quitting = true
			m.action = catQuit
			return m, tea.Quit
		case "down", "j":
			m.table.CursorDown()
		case "up", "k":
			m.table.CursorUp()
		case "pgdown":
			for i := 0; i < 10; i++ {
				m.table.CursorDown()
			}
		case "pgup":
			for i := 0; i < 10; i++ {
				m.table.CursorUp()
			}
		case "enter":
			m.action = catAssign
			return m, tea.Quit
		}
	}
	return m, nil
}

func (m categorizeModel) View() string {
	if m.quitting {
		return ""
	}

	var b strings.Builder

	uncatCount := 0
	for _, bk := range m.buckets {
		uncatCount += bk.Count
	}
	title := "📋 Uncategorized Transactions"
	if m.currency != "" {
		title += " (" + m.currency + ")"
	}
	title += fmt.Sprintf(" — %d groups, %d transactions", len(m.buckets), uncatCount)
	b.WriteString(lipgloss.NewStyle().Bold(true).Render(title))
	b.WriteString("\n")

	// Strip footer
	rendered := m.table.Render()
	lines := strings.Split(rendered, "\n")
	if len(lines) > 1 {
		lines = lines[:len(lines)-1]
	}
	b.WriteString(strings.Join(lines, "\n"))
	b.WriteString("\n")

	b.WriteString(lipgloss.NewStyle().Faint(true).Render(
		"  [Enter] Assign category  [↑↓] Navigate  [q] Quit"))
	b.WriteString("\n")

	return b.String()
}

func (m categorizeModel) selectedIdx() int {
	_, y := m.table.GetCursorLocation()
	return y
}

// ── Assign form (inline, after quitting bubbletea) ──

func assignBucket(bucket *txBucket, allTxs []TransactionEntry) bool {
	fmt.Println()
	fmt.Printf("  %s%s%s\n", Fmt.Bold, bucket.Description, Fmt.Reset)
	fmt.Printf("  %s%d transactions, %s, %s — %s%s\n",
		Fmt.Dim, bucket.Count,
		fmtEUR(bucket.TotalAmount),
		bucket.FirstDate.Format("02/01/2006"),
		bucket.LastDate.Format("02/01/2006"),
		Fmt.Reset)
	fmt.Printf("  %s(Esc to skip a question, Enter to confirm)%s\n\n", Fmt.Dim, Fmt.Reset)

	settings, _ := LoadSettings()
	defaultCollective := "commonshub"
	if settings != nil && settings.Accounting != nil && settings.Accounting.DefaultCollective != "" {
		defaultCollective = settings.Accounting.DefaultCollective
	}

	// 1. Collective (first — Esc skips)
	collective := pickCollective(defaultCollective)

	// 2. Category (Esc skips)
	var category string
	var catOptions []huh.Option[string]
	catOptions = append(catOptions, huh.NewOption("(skip)", ""))
	for _, c := range LoadCategories() {
		catOptions = append(catOptions, huh.NewOption(fmt.Sprintf("%s (%s)", c.Label, c.Direction), c.Slug))
	}
	catOptions = append(catOptions, huh.NewOption("+ New category...", "__new__"))

	catErr := huh.NewForm(huh.NewGroup(
		huh.NewSelect[string]().Title("Category").Options(catOptions...).Value(&category),
	)).Run()
	if catErr != nil {
		// Esc/abort — skip category
		category = ""
	}

	if category == "__new__" {
		var newSlug, newLabel, newDir string
		runField(huh.NewInput().Title("Category slug").Value(&newSlug))
		runField(huh.NewInput().Title("Category label").Value(&newLabel))
		runField(huh.NewSelect[string]().Title("Direction").
			Options(huh.NewOption("Income", "income"), huh.NewOption("Expense", "expense")).
			Value(&newDir))
		if newSlug != "" && newLabel != "" {
			AddCategory(CategoryDef{Slug: newSlug, Label: newLabel, Direction: newDir})
			category = newSlug
		} else {
			category = ""
		}
	}

	// 3. Event (Esc skips) — show events sorted by proximity to bucket dates
	event := pickEvent(bucket.FirstDate.Unix())

	// Nothing to assign?
	if category == "" && collective == "" && event == "" {
		fmt.Printf("  %sSkipped%s\n\n", Fmt.Dim, Fmt.Reset)
		return false
	}

	// Show what will be applied
	var assignParts []string
	if collective != "" {
		assignParts = append(assignParts, fmt.Sprintf("collective=%s", collective))
	}
	if category != "" {
		assignParts = append(assignParts, fmt.Sprintf("category=%s", category))
	}
	if event != "" {
		assignParts = append(assignParts, fmt.Sprintf("event=%s", truncForDisplay(event, 20)))
	}
	fmt.Printf("  Assigning %s to %d transactions\n",
		lipgloss.NewStyle().Foreground(lipgloss.Color("2")).Render(strings.Join(assignParts, ", ")),
		bucket.Count)

	// Also create a rule?
	var createRule bool
	ruleErr := huh.NewForm(huh.NewGroup(
		huh.NewConfirm().
			Title(fmt.Sprintf("Also create a rule for \"%s\"?", truncForDisplay(bucket.Description, 40))).
			Value(&createRule),
	)).Run()
	if ruleErr != nil {
		createRule = false
	}

	// Apply
	idSet := map[string]bool{}
	for _, id := range bucket.TxIDs {
		idSet[id] = true
	}

	updated := applyCategoriesToTxs(idSet, category, collective, event)
	fmt.Printf("  %s✓ %d transactions updated%s\n", Fmt.Green, updated, Fmt.Reset)

	if createRule {
		rule := Rule{
			Match: RuleMatch{
				Description: fmt.Sprintf("*%s*", escapeGlob(bucket.Description)),
			},
			Assign: RuleAssign{
				Category:   category,
				Collective: collective,
				Event:      event,
			},
		}
		rules, _ := LoadRules()
		rules = append(rules, rule)
		SaveRules(rules)
		fmt.Printf("  %s✓ Rule created%s\n", Fmt.Green, Fmt.Reset)
	}
	fmt.Println()

	return true
}

func truncForDisplay(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max-3] + "..."
}

func escapeGlob(s string) string {
	// For glob patterns, we just use the string as-is inside *...*
	// Remove any existing * to avoid double wildcards
	return strings.ReplaceAll(s, "*", "")
}

func applyCategoriesToTxs(idSet map[string]bool, category, collective, event string) int {
	dataDir := DataDir()
	updated := 0

	yearDirs, _ := os.ReadDir(dataDir)
	for _, yd := range yearDirs {
		if !yd.IsDir() || len(yd.Name()) != 4 {
			continue
		}
		monthDirs, _ := os.ReadDir(filepath.Join(dataDir, yd.Name()))
		for _, md := range monthDirs {
			if !md.IsDir() || len(md.Name()) != 2 {
				continue
			}
			txPath := filepath.Join(dataDir, yd.Name(), md.Name(), "generated", "transactions.json")
			data, err := os.ReadFile(txPath)
			if err != nil {
				continue
			}
			var txFile TransactionsFile
			if json.Unmarshal(data, &txFile) != nil {
				continue
			}

			changed := false
			for i := range txFile.Transactions {
				if idSet[txFile.Transactions[i].ID] {
					if category != "" {
						txFile.Transactions[i].Category = category
					}
					if collective != "" {
						txFile.Transactions[i].Collective = collective
					}
					if event != "" {
						txFile.Transactions[i].Event = event
					}
					changed = true
					updated++
				}
			}

			if changed {
				out, _ := json.MarshalIndent(txFile, "", "  ")
				writeMonthFile(dataDir, yd.Name(), md.Name(),
					filepath.Join("generated", "transactions.json"), out)
			}
		}
	}

	return updated
}

// ── Command ──

func parseCurrencyArg(args []string) string {
	for _, arg := range args {
		if strings.HasPrefix(arg, "-") {
			continue
		}
		upper := strings.ToUpper(arg)
		switch upper {
		case "EUR", "EURE", "EURB", "CHT":
			return upper
		}
	}
	return ""
}

func TransactionsCategorize(args []string) {
	if HasFlag(args, "--help", "-h", "help") {
		printCategorizHelp()
		return
	}

	currency := parseCurrencyArg(args)

	label := "Loading transactions..."
	if currency != "" {
		label = fmt.Sprintf("Loading %s transactions...", currency)
	}
	fmt.Printf("  %s\n", label)
	txs := loadAllTransactions(currency)

	for {
		buckets := findUncategorizedBuckets(txs)
		if len(buckets) == 0 {
			fmt.Printf("\n  %s✓ All transactions are categorized!%s\n\n", Fmt.Green, Fmt.Reset)
			return
		}

		m := newCategorizeModel(buckets, 0, 0)
		m.currency = currency
		p := tea.NewProgram(m, tea.WithAltScreen())
		result, err := p.Run()
		if err != nil {
			fmt.Printf("%sError: %v%s\n", Fmt.Red, err, Fmt.Reset)
			return
		}
		fm := result.(categorizeModel)

		switch fm.action {
		case catQuit:
			return
		case catAssign:
			idx := fm.selectedIdx()
			if idx >= 0 && idx < len(buckets) {
				if assignBucket(&buckets[idx], txs) {
					// Reload transactions to reflect changes
					txs = loadAllTransactions(currency)
				}
			}
		}
	}
}

func printCategorizHelp() {
	f := Fmt
	fmt.Printf(`
%schb transactions categorize%s — Bulk-categorize uncategorized transactions

%sUSAGE%s
  %schb transactions categorize%s

%sDESCRIPTION%s
  Analyzes all uncategorized transactions and groups them by description.
  For each group, you can assign a category and collective, and optionally
  create a rule for future transactions.

%sINTERACTIVE KEYS%s
  %s↑↓%s        Navigate groups
  %sEnter%s     Assign category to selected group
  %sq/Esc%s     Quit
`,
		f.Bold, f.Reset,
		f.Bold, f.Reset,
		f.Cyan, f.Reset,
		f.Bold, f.Reset,
		f.Bold, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
	)
}
