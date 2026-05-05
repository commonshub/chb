package cmd

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"

	stickertable "github.com/76creates/stickers/table"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/huh"
	"github.com/charmbracelet/lipgloss"
)

// ── Styles ──

var (
	ruleHeaderStyle = lipgloss.NewStyle().Bold(true)
	ruleDimStyle    = lipgloss.NewStyle().Faint(true)
	ruleSelStyle    = lipgloss.NewStyle().Background(lipgloss.Color("236")).Foreground(lipgloss.Color("255"))
	ruleGreenStyle  = lipgloss.NewStyle().Foreground(lipgloss.Color("2"))
	ruleCyanStyle   = lipgloss.NewStyle().Foreground(lipgloss.Color("6"))
	ruleYellowStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("3"))
)

// ══════════════════════════════════════════════════════════════════════════════
// Rules List (unchanged)
// ══════════════════════════════════════════════════════════════════════════════

type ruleAction int

const (
	actionQuit ruleAction = iota
	actionAdd
	actionEdit
	actionDelete
)

type rulesModel struct {
	rules    []Rule
	cursor   int
	quitting bool
	action   ruleAction
}

func (m rulesModel) Init() tea.Cmd { return nil }

func (m rulesModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "q", "esc", "ctrl+c":
			m.quitting = true
			m.action = actionQuit
			return m, tea.Quit
		case "up", "k":
			if m.cursor > 0 {
				m.cursor--
			}
		case "down", "j":
			if m.cursor < len(m.rules)-1 {
				m.cursor++
			}
		case "a":
			m.action = actionAdd
			return m, tea.Quit
		case "e", "enter":
			if len(m.rules) > 0 {
				m.action = actionEdit
				return m, tea.Quit
			}
		case "d":
			if len(m.rules) > 0 {
				m.action = actionDelete
				return m, tea.Quit
			}
		}
	}
	return m, nil
}

func (m rulesModel) View() string {
	if m.quitting {
		return ""
	}

	var b strings.Builder
	b.WriteString(ruleHeaderStyle.Render(fmt.Sprintf("📋 Categorization Rules (%d)", len(m.rules))))
	b.WriteString("\n\n")

	if len(m.rules) == 0 {
		b.WriteString(ruleDimStyle.Render("  No rules configured yet. Press [a] to add one."))
		b.WriteString("\n")
	} else {
		header := fmt.Sprintf("  %-3s %-42s →  %-14s %s", "#", "Match", "Category", "Collective")
		b.WriteString(ruleDimStyle.Render(header))
		b.WriteString("\n")
		b.WriteString(ruleDimStyle.Render("  " + strings.Repeat("─", 78)))
		b.WriteString("\n")

		for i, r := range m.rules {
			num := fmt.Sprintf("%2d", i+1)
			summary := r.RuleSummary()
			if len(summary) > 40 {
				summary = summary[:37] + "..."
			}
			line := fmt.Sprintf("  %s  %-40s  →  %-14s %s", num, summary, r.Assign.Category, r.Assign.Collective)
			if i == m.cursor {
				b.WriteString(ruleSelStyle.Render(line))
			} else {
				b.WriteString(line)
			}
			b.WriteString("\n")
		}
	}

	b.WriteString("\n")
	b.WriteString(ruleDimStyle.Render("  [a] Add  [e/Enter] Edit  [d] Delete  [↑↓] Navigate  [q] Quit"))
	b.WriteString("\n")
	return b.String()
}

// ══════════════════════════════════════════════════════════════════════════════
// Rule Editor with Live Preview Table
// ══════════════════════════════════════════════════════════════════════════════

type editorMode int

const (
	editorMenu    editorMode = iota // showing condition menu
	editorField                     // editing a field via huh form
	editorConfirm                   // confirm apply
)

type ruleEditorModel struct {
	rule       Rule
	allTxs     []TransactionEntry
	matchedTxs []TransactionEntry
	table      *stickertable.Table
	mode       editorMode
	editForm   *huh.Form
	editValue  string // bound to huh input/select
	editing    bool   // true = existing rule, false = new
	width      int
	height     int
	quitting   bool
	saved      bool
	// Menu
	menuOptions []huh.Option[string]
	menuChoice  string
	menuForm    *huh.Form
}

func newRuleEditor(existing *Rule, allTxs []TransactionEntry) ruleEditorModel {
	m := ruleEditorModel{
		allTxs:  allTxs,
		editing: existing != nil,
	}
	if existing != nil {
		m.rule = *existing
	}
	m.recomputeMatches()
	m.showMenu()
	return m
}

func (m *ruleEditorModel) recomputeMatches() {
	m.matchedTxs = nil
	for _, tx := range m.allTxs {
		if m.rule.MatchesTransaction(tx) {
			m.matchedTxs = append(m.matchedTxs, tx)
		}
	}
	m.table = m.buildMatchTable()
}

func (m *ruleEditorModel) buildMatchTable() *stickertable.Table {
	tz := BrusselsTZ()
	hasAssign := m.rule.Assign.Category != ""

	tableHeight := 10
	if m.height > 30 {
		tableHeight = m.height - 24
	} else if m.height > 20 {
		tableHeight = m.height - 18
	}
	if tableHeight < 4 {
		tableHeight = 4
	}

	headers := []string{"Date", "Source", "Collective", "Category", "Counterparty", "Description", "Amount"}
	t := stickertable.NewTable(m.width, tableHeight, headers)
	t.SetRatio([]int{2, 3, 3, 3, 5, 8, 4})
	t.SetMinWidth([]int{6, 6, 1, 1, 6, 8, 8})

	// Same styling as the transaction browser
	t.SetStyles(map[stickertable.StyleKey]lipgloss.Style{
		stickertable.StyleKeyHeader: lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("252")).
			Background(lipgloss.Color("238")),
		stickertable.StyleKeyRows: lipgloss.NewStyle().
			Foreground(lipgloss.Color("252")),
		stickertable.StyleKeyRowsSubsequent: lipgloss.NewStyle().
			Foreground(lipgloss.Color("252")),
		stickertable.StyleKeyRowsCursor: lipgloss.NewStyle().
			Foreground(lipgloss.Color("255")).
			Background(lipgloss.Color("236")).
			Bold(true),
		stickertable.StyleKeyCellCursor: lipgloss.NewStyle().
			Foreground(lipgloss.Color("255")).
			Background(lipgloss.Color("236")).
			Bold(true),
		stickertable.StyleKeyFooter: lipgloss.NewStyle().
			Foreground(lipgloss.Color("240")).
			Height(1),
	})

	for _, tx := range m.matchedTxs {
		tm := time.Unix(tx.Timestamp, 0).In(tz)
		amt := txAmount(tx)
		absAmt := math.Abs(amt)

		var amtStr string
		if isEURCurrency(tx.Currency) {
			if tx.Type == "CREDIT" {
				amtStr = styleGreen.Render(fmt.Sprintf("+€%.2f", absAmt))
			} else {
				amtStr = styleRed.Render(fmt.Sprintf("-€%.2f", absAmt))
			}
		} else {
			if tx.Type == "CREDIT" || tx.Type == "TRANSFER" {
				amtStr = styleGreen.Render(fmt.Sprintf("+%.2f %s", absAmt, tx.Currency))
			} else {
				amtStr = styleRed.Render(fmt.Sprintf("-%.2f %s", absAmt, tx.Currency))
			}
		}

		cat := fmt.Sprintf(" %s", tx.Category)
		col := fmt.Sprintf(" %s", tx.Collective)
		if hasAssign {
			cat = " " + ruleGreenStyle.Render(m.rule.Assign.Category)
			if m.rule.Assign.Collective != "" {
				col = " " + ruleGreenStyle.Render(m.rule.Assign.Collective)
			}
		}

		t.MustAddRows([][]any{{
			fmt.Sprintf(" %s", tm.Format("02/01")),
			fmt.Sprintf(" %s", txSource(tx)),
			col,
			cat,
			fmt.Sprintf(" %s", shortAddr(txDisplayCounterparty(tx))),
			fmt.Sprintf(" %s", txDisplayDescription(tx)),
			fmt.Sprintf(" %s", amtStr),
		}})
	}

	return t
}

func (m *ruleEditorModel) showMenu() {
	m.mode = editorMenu
	m.menuChoice = ""

	var opts []huh.Option[string]

	// WHEN conditions
	fields := []struct{ key, label, cur string }{
		{"sender", "sender", m.rule.Match.Sender},
		{"recipient", "recipient", m.rule.Match.Recipient},
		{"description", "description", m.rule.Match.Description},
		{"account", "account", m.rule.Match.Account},
		{"provider", "provider", m.rule.Match.Provider},
		{"currency", "currency", m.rule.Match.Currency},
		{"direction", "direction", m.rule.Match.Direction},
		{"application", "application", m.rule.Match.Application},
	}
	for _, f := range fields {
		label := "WHEN " + f.key
		if f.cur != "" {
			label = fmt.Sprintf("WHEN %s = %s  ✎", f.key, f.cur)
		}
		opts = append(opts, huh.NewOption(label, f.key))
	}

	// THEN assignments
	catLabel := "THEN category"
	if m.rule.Assign.Category != "" {
		catLabel = fmt.Sprintf("THEN category = %s  ✎", m.rule.Assign.Category)
	}
	opts = append(opts, huh.NewOption(catLabel, "category"))

	colLabel := "THEN collective"
	if m.rule.Assign.Collective != "" {
		colLabel = fmt.Sprintf("THEN collective = %s  ✎", m.rule.Assign.Collective)
	}
	opts = append(opts, huh.NewOption(colLabel, "collective"))

	// Actions
	if m.rule.Assign.Category != "" && m.hasConditions() {
		opts = append(opts, huh.NewOption(
			fmt.Sprintf("✓ Apply to %d transactions & save", len(m.matchedTxs)), "apply"))
	}
	opts = append(opts, huh.NewOption("✗ Cancel", "cancel"))

	m.menuForm = huh.NewForm(huh.NewGroup(
		huh.NewSelect[string]().
			Title("").
			Options(opts...).
			Value(&m.menuChoice),
	))
}

func (m *ruleEditorModel) hasConditions() bool {
	r := m.rule.Match
	return r.Sender != "" || r.Recipient != "" || r.Description != "" ||
		r.Account != "" || r.Provider != "" || r.Currency != "" ||
		r.Direction != "" || r.Application != ""
}

func (m *ruleEditorModel) startFieldEdit(field string) {
	m.mode = editorField
	m.editValue = ""

	// Pre-fill current value
	switch field {
	case "sender":
		m.editValue = m.rule.Match.Sender
	case "recipient":
		m.editValue = m.rule.Match.Recipient
	case "description":
		m.editValue = m.rule.Match.Description
	case "account":
		m.editValue = m.rule.Match.Account
	case "currency":
		m.editValue = m.rule.Match.Currency
	}

	var formField huh.Field
	switch field {
	case "provider":
		m.editValue = m.rule.Match.Provider
		formField = huh.NewSelect[string]().Title("Provider").Options(
			huh.NewOption("(any)", ""), huh.NewOption("stripe", "stripe"),
			huh.NewOption("etherscan", "etherscan"), huh.NewOption("monerium", "monerium"),
		).Value(&m.editValue)
	case "direction":
		m.editValue = m.rule.Match.Direction
		formField = huh.NewSelect[string]().Title("Direction").Options(
			huh.NewOption("(any)", ""), huh.NewOption("in (incoming)", "in"), huh.NewOption("out (outgoing)", "out"),
		).Value(&m.editValue)
	case "application":
		m.editValue = m.rule.Match.Application
		formField = huh.NewSelect[string]().Title("Application").Options(
			huh.NewOption("(any)", ""), huh.NewOption("Luma", "Luma"),
			huh.NewOption("Open Collective", "Open Collective"),
		).Value(&m.editValue)
	case "category":
		m.editValue = m.rule.Assign.Category
		var catOpts []huh.Option[string]
		catOpts = append(catOpts, huh.NewOption("(none)", ""))
		for _, c := range LoadCategories() {
			catOpts = append(catOpts, huh.NewOption(fmt.Sprintf("%s (%s)", c.Label, c.Direction), c.Slug))
		}
		formField = huh.NewSelect[string]().Title("Category").Options(catOpts...).Value(&m.editValue)
	case "collective":
		m.editValue = m.rule.Assign.Collective
		var colOpts []huh.Option[string]
		colOpts = append(colOpts, huh.NewOption("(none)", ""))
		for _, slug := range CollectiveSlugs() {
			colOpts = append(colOpts, huh.NewOption(slug, slug))
		}
		formField = huh.NewSelect[string]().Title("Collective").Options(colOpts...).Value(&m.editValue)
	default:
		// Text input for glob patterns
		titles := map[string]string{
			"sender":      "Sender pattern (* for wildcards)",
			"recipient":   "Recipient pattern (* for wildcards)",
			"description": "Description pattern (* for wildcards)",
			"account":     "Account slug",
			"currency":    "Currency (EUR, EURe, EURb, CHT)",
		}
		title := titles[field]
		if title == "" {
			title = field
		}
		formField = huh.NewInput().Title(title).Value(&m.editValue)
	}

	m.editForm = huh.NewForm(huh.NewGroup(formField))
}

func (m *ruleEditorModel) applyFieldValue(field string) {
	switch field {
	case "sender":
		m.rule.Match.Sender = m.editValue
	case "recipient":
		m.rule.Match.Recipient = m.editValue
	case "description":
		m.rule.Match.Description = m.editValue
	case "account":
		m.rule.Match.Account = m.editValue
	case "provider":
		m.rule.Match.Provider = m.editValue
	case "currency":
		m.rule.Match.Currency = m.editValue
	case "direction":
		m.rule.Match.Direction = m.editValue
	case "application":
		m.rule.Match.Application = m.editValue
	case "category":
		m.rule.Assign.Category = m.editValue
	case "collective":
		m.rule.Assign.Collective = m.editValue
	}
	m.recomputeMatches()
}

func (m ruleEditorModel) Init() tea.Cmd {
	if m.menuForm != nil {
		return m.menuForm.Init()
	}
	return nil
}

func (m ruleEditorModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		m.table.SetWidth(msg.Width)
		tableHeight := msg.Height - 18
		if tableHeight < 5 {
			tableHeight = 5
		}
		m.table.SetHeight(tableHeight)
	case tea.KeyMsg:
		if msg.String() == "ctrl+c" {
			m.quitting = true
			return m, tea.Quit
		}
	}

	switch m.mode {
	case editorMenu:
		return m.updateMenu(msg)
	case editorField:
		return m.updateField(msg)
	}

	return m, nil
}

func (m ruleEditorModel) updateMenu(msg tea.Msg) (tea.Model, tea.Cmd) {
	if keyMsg, ok := msg.(tea.KeyMsg); ok {
		switch keyMsg.String() {
		case "esc":
			m.quitting = true
			return m, tea.Quit
		case "pgdown":
			for i := 0; i < 10; i++ {
				m.table.CursorDown()
			}
			return m, nil
		case "pgup":
			for i := 0; i < 10; i++ {
				m.table.CursorUp()
			}
			return m, nil
		}
	}

	form, cmd := m.menuForm.Update(msg)
	if f, ok := form.(*huh.Form); ok {
		m.menuForm = f
	}

	if m.menuForm.State == huh.StateCompleted {
		switch m.menuChoice {
		case "cancel":
			m.quitting = true
			return m, tea.Quit
		case "apply":
			m.applyRule()
			m.saved = true
			m.quitting = true
			return m, tea.Quit
		default:
			// Start editing a field
			m.startFieldEdit(m.menuChoice)
			return m, m.editForm.Init()
		}
	}
	if m.menuForm.State == huh.StateAborted {
		m.quitting = true
		return m, tea.Quit
	}

	return m, cmd
}

func (m ruleEditorModel) updateField(msg tea.Msg) (tea.Model, tea.Cmd) {
	if keyMsg, ok := msg.(tea.KeyMsg); ok {
		if keyMsg.String() == "esc" {
			m.showMenu()
			return m, m.menuForm.Init()
		}
	}

	form, cmd := m.editForm.Update(msg)
	if f, ok := form.(*huh.Form); ok {
		m.editForm = f
	}

	if m.editForm.State == huh.StateCompleted {
		// Which field were we editing? Derive from menu choice
		m.applyFieldValue(m.menuChoice)
		m.showMenu()
		return m, m.menuForm.Init()
	}
	if m.editForm.State == huh.StateAborted {
		m.showMenu()
		return m, m.menuForm.Init()
	}

	return m, cmd
}

func (m *ruleEditorModel) applyRule() {
	// 1. Save rule
	rules, _ := LoadRules()
	if m.editing {
		// Find and update existing rule (by matching the original)
		// For simplicity, just append — the caller handles replacement
		rules = append(rules, m.rule)
	} else {
		rules = append(rules, m.rule)
	}
	SaveRules(rules)

	// 2. Update all matching transactions
	type monthKey struct{ year, month string }
	byMonth := map[monthKey][]int{} // month → indices into matchedTxs

	tz := BrusselsTZ()
	for i, tx := range m.matchedTxs {
		t := time.Unix(tx.Timestamp, 0).In(tz)
		mk := monthKey{fmt.Sprintf("%d", t.Year()), fmt.Sprintf("%02d", t.Month())}
		byMonth[mk] = append(byMonth[mk], i)
	}

	dataDir := DataDir()
	updated := 0
	for mk, indices := range byMonth {
		txPath := filepath.Join(dataDir, mk.year, mk.month, "generated", "transactions.json")
		data, err := os.ReadFile(txPath)
		if err != nil {
			continue
		}
		var txFile TransactionsFile
		if json.Unmarshal(data, &txFile) != nil {
			continue
		}

		// Build set of IDs to update
		updateIDs := map[string]bool{}
		for _, idx := range indices {
			updateIDs[m.matchedTxs[idx].ID] = true
		}

		changed := false
		for i := range txFile.Transactions {
			if updateIDs[txFile.Transactions[i].ID] {
				if m.rule.Assign.Category != "" {
					txFile.Transactions[i].Category = m.rule.Assign.Category
				}
				if m.rule.Assign.Collective != "" {
					txFile.Transactions[i].Collective = m.rule.Assign.Collective
				}
				changed = true
				updated++
			}
		}

		if changed {
			out, _ := json.MarshalIndent(txFile, "", "  ")
			writeMonthFile(dataDir, mk.year, mk.month, filepath.Join("generated", "transactions.json"), out)
		}
	}

	fmt.Printf("\n  %s✓ Rule saved + %d transactions updated%s\n\n", Fmt.Green, updated, Fmt.Reset)
}

func (m ruleEditorModel) View() string {
	if m.quitting {
		return ""
	}

	var b strings.Builder

	// ── Side-by-side WHEN / THEN panel ──
	b.WriteString(m.renderRulePanel())
	b.WriteString("\n")

	// ── Form (menu or field edit) ──
	switch m.mode {
	case editorMenu:
		if m.menuForm != nil {
			b.WriteString(m.menuForm.View())
		}
	case editorField:
		if m.editForm != nil {
			b.WriteString(m.editForm.View())
		}
	}

	// ── Match count + table ──
	matchLabel := fmt.Sprintf("  Matching: %d transactions", len(m.matchedTxs))
	if !m.hasConditions() {
		matchLabel = fmt.Sprintf("  All: %d transactions (add WHEN conditions to filter)", len(m.matchedTxs))
	}

	actions := ""
	if m.rule.Assign.Category != "" && m.hasConditions() {
		actions = fmt.Sprintf("  [Enter] Apply to %d txs & save", len(m.matchedTxs))
	}

	b.WriteString(fmt.Sprintf("\n%s%s\n", ruleHeaderStyle.Render(matchLabel), ruleDimStyle.Render(actions)))

	// Strip stickers built-in footer (last line)
	rendered := m.table.Render()
	tableLines := strings.Split(rendered, "\n")
	if len(tableLines) > 1 {
		tableLines = tableLines[:len(tableLines)-1]
	}
	b.WriteString(strings.Join(tableLines, "\n"))
	b.WriteString("\n")

	return b.String()
}

func (m ruleEditorModel) renderRulePanel() string {
	halfWidth := 38
	if m.width > 20 {
		halfWidth = (m.width - 6) / 2
	}

	condStyle := ruleCyanStyle
	valStyle := ruleYellowStyle
	assignStyle := ruleGreenStyle

	// WHEN column
	var whenLines []string
	whenLines = append(whenLines, ruleHeaderStyle.Render("WHEN"))
	whenLines = append(whenLines, "")

	addCond := func(field, value string) {
		if value != "" {
			whenLines = append(whenLines, fmt.Sprintf("  %s  %s",
				condStyle.Render(fmt.Sprintf("%-13s", field)),
				valStyle.Render(value)))
		}
	}

	addCond("sender", m.rule.Match.Sender)
	addCond("recipient", m.rule.Match.Recipient)
	addCond("description", m.rule.Match.Description)
	addCond("account", m.rule.Match.Account)
	addCond("provider", m.rule.Match.Provider)
	addCond("currency", m.rule.Match.Currency)
	addCond("direction", m.rule.Match.Direction)
	addCond("application", m.rule.Match.Application)

	if len(whenLines) == 2 {
		whenLines = append(whenLines, ruleDimStyle.Render("  (no conditions)"))
	}

	// THEN column
	var thenLines []string
	thenLines = append(thenLines, ruleHeaderStyle.Render("THEN"))
	thenLines = append(thenLines, "")

	if m.rule.Assign.Category != "" {
		thenLines = append(thenLines, fmt.Sprintf("  %s  %s",
			condStyle.Render("category     "),
			assignStyle.Render(m.rule.Assign.Category)))
	}
	if m.rule.Assign.Collective != "" {
		thenLines = append(thenLines, fmt.Sprintf("  %s  %s",
			condStyle.Render("collective   "),
			assignStyle.Render(m.rule.Assign.Collective)))
	}
	if len(thenLines) == 2 {
		thenLines = append(thenLines, ruleDimStyle.Render("  (not set)"))
	}

	// Pad to same height
	for len(whenLines) < len(thenLines) {
		whenLines = append(whenLines, "")
	}
	for len(thenLines) < len(whenLines) {
		thenLines = append(thenLines, "")
	}

	boxStyle := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color("240")).
		Padding(0, 1).
		Width(halfWidth)

	whenBox := boxStyle.Render(strings.Join(whenLines, "\n"))
	thenBox := boxStyle.Render(strings.Join(thenLines, "\n"))

	return lipgloss.JoinHorizontal(lipgloss.Top, "  ", whenBox, " ", thenBox)
}

// ══════════════════════════════════════════════════════════════════════════════
// RulesCommand — entry point
// ══════════════════════════════════════════════════════════════════════════════

func RulesCommand(args []string) {
	if HasFlag(args, "--help", "-h", "help") {
		printRulesHelp()
		return
	}

	rules, err := LoadRules()
	if err != nil {
		fmt.Printf("%sError loading rules: %v%s\n", Fmt.Red, err, Fmt.Reset)
		return
	}

	// Pre-load all transactions for the editor
	var allTxs []TransactionEntry

	cursor := 0

	for {
		m := rulesModel{rules: rules, cursor: cursor}
		p := tea.NewProgram(m, tea.WithAltScreen())
		finalModel, err := p.Run()
		if err != nil {
			fmt.Printf("%sError: %v%s\n", Fmt.Red, err, Fmt.Reset)
			return
		}
		fm := finalModel.(rulesModel)
		cursor = fm.cursor

		switch fm.action {
		case actionQuit:
			return

		case actionAdd:
			if allTxs == nil {
				fmt.Printf("  Loading transactions...\n")
				allTxs = loadAllTransactions("")
			}
			em := newRuleEditor(nil, allTxs)
			ep := tea.NewProgram(em, tea.WithAltScreen())
			result, _ := ep.Run()
			efm := result.(ruleEditorModel)
			if efm.saved {
				rules, _ = LoadRules()
				allTxs = loadAllTransactions("") // reload after updates
				cursor = len(rules) - 1
			}

		case actionEdit:
			if cursor >= 0 && cursor < len(rules) {
				if allTxs == nil {
					fmt.Printf("  Loading transactions...\n")
					allTxs = loadAllTransactions("")
				}
				// Remove the old rule first (we'll re-add via editor)
				oldRule := rules[cursor]
				rules = append(rules[:cursor], rules[cursor+1:]...)
				SaveRules(rules)

				em := newRuleEditor(&oldRule, allTxs)
				ep := tea.NewProgram(em, tea.WithAltScreen())
				result, _ := ep.Run()
				efm := result.(ruleEditorModel)
				if !efm.saved {
					// Cancelled — restore old rule
					rules = append(rules[:cursor], append([]Rule{oldRule}, rules[cursor:]...)...)
					SaveRules(rules)
				} else {
					rules, _ = LoadRules()
					allTxs = loadAllTransactions("")
				}
			}

		case actionDelete:
			if cursor >= 0 && cursor < len(rules) {
				r := rules[cursor]
				fmt.Printf("\n  Delete rule: %s → %s\n", r.RuleSummary(), r.Assign.Category)
				var confirm bool
				runField(huh.NewConfirm().Title("Delete this rule?").Value(&confirm))
				if confirm {
					rules = append(rules[:cursor], rules[cursor+1:]...)
					SaveRules(rules)
					if cursor >= len(rules) && cursor > 0 {
						cursor--
					}
				}
			}
		}
	}
}

// pickCategory is still used by createRuleFromBrowser in transactions_browser.go
func pickCategory() string {
	cats := LoadCategories()
	var catOptions []huh.Option[string]
	catOptions = append(catOptions, huh.NewOption("(cancel)", ""))
	for _, cat := range cats {
		label := fmt.Sprintf("%s (%s)", cat.Label, cat.Direction)
		catOptions = append(catOptions, huh.NewOption(label, cat.Slug))
	}
	catOptions = append(catOptions, huh.NewOption("+ New category...", "__new__"))

	var category string
	runField(huh.NewSelect[string]().Title("Category").Options(catOptions...).Value(&category))

	if category == "__new__" {
		var newSlug, newLabel, newDir string
		runField(huh.NewInput().Title("Category slug").Value(&newSlug))
		runField(huh.NewInput().Title("Category label").Value(&newLabel))
		runField(huh.NewSelect[string]().Title("Direction").
			Options(huh.NewOption("Income", "income"), huh.NewOption("Expense", "expense")).
			Value(&newDir))
		if newSlug != "" && newLabel != "" {
			AddCategory(CategoryDef{Slug: newSlug, Label: newLabel, Direction: newDir})
			fmt.Printf("  %s✓ Created category: %s%s\n", Fmt.Green, newLabel, Fmt.Reset)
			return newSlug
		}
		return ""
	}
	return category
}

// printRulePreview is still used by createRuleFromBrowser in transactions_browser.go
func printRulePreview(r *Rule) {
	condStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("6"))
	valStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("3"))
	assignStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("2"))

	boxStyle := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color("240")).
		Padding(0, 2).MarginLeft(2)

	var lines []string
	lines = append(lines, ruleHeaderStyle.Render("Rule Preview"))
	lines = append(lines, "")

	hasMatch := false
	addCond := func(field, value string) {
		if value != "" {
			lines = append(lines, fmt.Sprintf("  %s %s",
				condStyle.Render(fmt.Sprintf("%-13s", field)), valStyle.Render(value)))
			hasMatch = true
		}
	}

	lines = append(lines, ruleDimStyle.Render("WHEN"))
	addCond("sender", r.Match.Sender)
	addCond("recipient", r.Match.Recipient)
	addCond("description", r.Match.Description)
	addCond("account", r.Match.Account)
	addCond("provider", r.Match.Provider)
	addCond("currency", r.Match.Currency)
	addCond("direction", r.Match.Direction)
	addCond("application", r.Match.Application)
	if !hasMatch {
		lines = append(lines, ruleDimStyle.Render("  (no conditions set)"))
	}
	lines = append(lines, "")
	lines = append(lines, ruleDimStyle.Render("THEN"))
	lines = append(lines, fmt.Sprintf("  %s %s", condStyle.Render("category     "), assignStyle.Render(r.Assign.Category)))
	if r.Assign.Collective != "" {
		lines = append(lines, fmt.Sprintf("  %s %s", condStyle.Render("collective   "), assignStyle.Render(r.Assign.Collective)))
	}
	fmt.Println(boxStyle.Render(strings.Join(lines, "\n")))
	fmt.Println()
}

func printRulesHelp() {
	f := Fmt
	fmt.Printf(`
%schb rules%s — Manage transaction categorization rules

%sUSAGE%s
  %schb rules%s              Interactive rule editor
  %schb rules --help%s       Show this help

%sDESCRIPTION%s
  Rules define how transactions are automatically categorized.
  Each rule has match conditions and category assignments.
  Rules are stored in %sAPP_DATA_DIR/settings/rules.json%s.

%sMATCH FIELDS%s
  %ssender%s        Glob on counterparty for incoming tx
  %srecipient%s     Glob on counterparty for outgoing tx
  %sdescription%s   Glob on transaction description/memo
  %saccount%s       Account slug (fridge, coffee, savings)
  %sprovider%s      Provider (stripe, etherscan, monerium)
  %scurrency%s      Currency code (EUR, EURe, CHT)
  %sdirection%s     "in" or "out"
  %sapplication%s   Stripe Connect app (Luma, Open Collective)

%sINTERACTIVE KEYS%s
  %s↑↓%s       Navigate rules
  %sa%s        Add new rule (live preview)
  %se/Enter%s  Edit selected rule
  %sd%s        Delete selected rule
  %sq/Esc%s    Quit
`,
		f.Bold, f.Reset,
		f.Bold, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Bold, f.Reset,
		f.Cyan, f.Reset,
		f.Bold, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Bold, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
	)
}
