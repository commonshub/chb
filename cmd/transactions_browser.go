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
	"github.com/charmbracelet/lipgloss"
	stickertable "github.com/76creates/stickers/table"
	overlay "github.com/rmhubbert/bubbletea-overlay"
)

// ── Styles ──

var (
	styleGreen = lipgloss.NewStyle().Foreground(lipgloss.Color("2"))
	styleRed   = lipgloss.NewStyle().Foreground(lipgloss.Color("1"))
)

// ── Helpers ──

func txAmount(tx TransactionEntry) float64 {
	if tx.NormalizedAmount != 0 {
		return tx.NormalizedAmount
	}
	return tx.Amount
}

func txDisplayCounterparty(tx TransactionEntry) string {
	cp := tx.Counterparty
	if tx.Provider == "stripe" {
		if desc, ok := tx.Metadata["description"]; ok {
			if s, ok := desc.(string); ok && cp != s && cp != "" {
				return cp
			}
		}
		return "Stripe"
	}
	return shortAddr(cp)
}

func txDisplayDescription(tx TransactionEntry) string {
	if desc, ok := tx.Metadata["description"]; ok {
		if s, ok := desc.(string); ok && s != "" {
			return s
		}
	}
	if tx.Provider == "stripe" {
		return tx.Counterparty
	}
	return ""
}

func txSource(tx TransactionEntry) string {
	if tx.Provider == "stripe" {
		return "Stripe"
	}
	if tx.Provider == "etherscan" {
		return tx.AccountSlug
	}
	if tx.Provider == "monerium" {
		return "Monerium"
	}
	return tx.Provider
}

func shortAddr(s string) string {
	if strings.HasPrefix(s, "0x") && len(s) > 14 {
		return s[:6] + "..." + s[len(s)-4:]
	}
	return s
}

// ── Data loading ──

// TxFilter narrows the set of transactions returned by loadFilteredTransactions.
// Zero-valued fields are treated as "no filter".
type TxFilter struct {
	AccountSlug string    // matches AccountSlug or Slug-like account fields
	Currency    string    // "EUR" matches the EUR family; other codes are exact
	Since       time.Time // inclusive lower bound
	Until       time.Time // inclusive upper bound (end-of-day handled by caller)
}

func loadAllTransactions(currencyFilter string) []TransactionEntry {
	return loadFilteredTransactions(TxFilter{Currency: currencyFilter})
}

func loadFilteredTransactions(f TxFilter) []TransactionEntry {
	dataDir := DataDir()
	var all []TransactionEntry

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
			txFile := LoadTransactionsWithPII(dataDir, yd.Name(), md.Name())
			if txFile == nil {
				continue
			}
			for _, tx := range txFile.Transactions {
				if tx.Type == "INTERNAL" {
					continue
				}
				if !f.matches(tx) {
					continue
				}
				all = append(all, tx)
			}
		}
	}

	sort.Slice(all, func(i, j int) bool {
		return all[i].Timestamp > all[j].Timestamp
	})
	return all
}

func (f TxFilter) matches(tx TransactionEntry) bool {
	if f.Currency != "" {
		if strings.EqualFold(f.Currency, "EUR") {
			if !isEURCurrency(tx.Currency) {
				return false
			}
		} else if !strings.EqualFold(tx.Currency, f.Currency) {
			return false
		}
	}
	if f.AccountSlug != "" && !strings.EqualFold(tx.AccountSlug, f.AccountSlug) {
		return false
	}
	if !f.Since.IsZero() && tx.Timestamp < f.Since.Unix() {
		return false
	}
	if !f.Until.IsZero() && tx.Timestamp > f.Until.Unix() {
		return false
	}
	return true
}

// ── Build table rows ──

func buildStickerRows(txs []TransactionEntry) [][]string {
	tz := BrusselsTZ()
	rows := make([][]string, len(txs))
	for i, tx := range txs {
		t := time.Unix(tx.Timestamp, 0).In(tz)
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

		rows[i] = []string{
			fmt.Sprintf(" %s", t.Format("02/01")),
			fmt.Sprintf(" %s", txSource(tx)),
			fmt.Sprintf(" %s", tx.Collective),
			fmt.Sprintf(" %s", tx.Category),
			fmt.Sprintf(" %s", txDisplayCounterparty(tx)),
			fmt.Sprintf(" %s", txDisplayDescription(tx)),
			fmt.Sprintf(" %s", amtStr),
		}
	}
	return rows
}

var columnHeaders = []string{"Date", "Source", "Collective", "Category", "Counterparty", "Description", "Amount"}

func newStickerTable(txs []TransactionEntry, w, h int) *stickertable.Table {
	t := stickertable.NewTable(w, h, columnHeaders)
	t.SetRatio([]int{2, 3, 3, 3, 5, 8, 4})
	t.SetMinWidth([]int{6, 6, 1, 1, 6, 8, 8})
	t.SetStylePassing(true)

	t.SetStyles(map[stickertable.StyleKey]lipgloss.Style{
		stickertable.StyleKeyHeader: lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("252")).
			Background(lipgloss.Color("238")),
		stickertable.StyleKeyRows: lipgloss.NewStyle().
			Foreground(lipgloss.Color("252")),
		stickertable.StyleKeyRowsSubsequent: lipgloss.NewStyle().
			Foreground(lipgloss.Color("252")),
		// Selected row
		stickertable.StyleKeyRowsCursor: lipgloss.NewStyle().
			Foreground(lipgloss.Color("255")).
			Background(lipgloss.Color("236")).
			Bold(true),
		// Same as row cursor (column selection is in the header bar, not in cells)
		stickertable.StyleKeyCellCursor: lipgloss.NewStyle().
			Foreground(lipgloss.Color("255")).
			Background(lipgloss.Color("236")).
			Bold(true),
		stickertable.StyleKeyFooter: lipgloss.NewStyle().
			Foreground(lipgloss.Color("240")).
			Height(1),
	})

	rows := buildStickerRows(txs)
	for _, row := range rows {
		anyRow := make([]any, len(row))
		for j, v := range row {
			anyRow[j] = v
		}
		t.MustAddRows([][]any{anyRow})
	}

	return t
}

// ── Browser modes and actions ──

type browserMode int

const (
	modeTable browserMode = iota
	modeDetail
	modeEditCollective
	modeEditCategory
	modeEditDate
)

type browserAction int

const (
	browserNone browserAction = iota
	browserQuit
	browserCreateRule
	browserSaved
)

// ── Detail panel (implements overlay.Viewable) ──

type detailPanel struct {
	content string
}

func (d *detailPanel) View() string { return d.content }

// ── Table view wrapper (implements overlay.Viewable) ──

type tableView struct {
	content string
}

func (t *tableView) View() string { return t.content }

// ── Bubbletea model ──

type txBrowserModel struct {
	table     *stickertable.Table
	txs       []TransactionEntry
	currency  string
	quitting  bool
	action    browserAction
	mode      browserMode
	detailTx  *TransactionEntry
	detailIdx int
	// Inline edit fields
	editInput   string   // current text input for inline edit
	editOptions []string // available options for autocomplete
	editCursor  int      // cursor position in filtered options
	// Column selection + filter
	selectedCol int    // which column is highlighted in the header
	filterStr   string // active filter text (empty = not filtering)
	filtering   bool   // true = in filter input mode (typing filters)
	// Layout
	width  int
	height int
}

func (m txBrowserModel) Init() tea.Cmd { return nil }

func (m txBrowserModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		m.table.SetWidth(msg.Width)
		m.table.SetHeight(msg.Height - 4) // title + footer
	}

	switch m.mode {
	case modeTable:
		return m.updateTable(msg)
	case modeDetail:
		return m.updateDetail(msg)
	case modeEditCollective, modeEditCategory, modeEditDate:
		return m.updateInlineEdit(msg)
	}

	return m, nil
}

func (m txBrowserModel) updateTable(msg tea.Msg) (tea.Model, tea.Cmd) {
	if keyMsg, ok := msg.(tea.KeyMsg); ok {
		key := keyMsg.String()

		// Filter input mode: typing goes to filter
		if m.filtering {
			switch key {
			case "esc":
				m.filtering = false
				m.filterStr = ""
				m.table.UnsetFilter()
			case "backspace":
				if len(m.filterStr) > 1 {
					m.filterStr = m.filterStr[:len(m.filterStr)-1]
					m.table.SetFilter(m.selectedCol, m.filterStr)
				} else if len(m.filterStr) == 1 {
					m.filterStr = ""
					m.table.UnsetFilter()
				} else {
					m.filtering = false
				}
			case "enter":
				m.filtering = false // keep filter active, exit typing mode
			case "down", "j":
				m.table.CursorDown()
			case "up", "k":
				m.table.CursorUp()
			case "left":
				if m.selectedCol > 0 {
					m.selectedCol--
					if m.filterStr != "" {
						m.table.SetFilter(m.selectedCol, m.filterStr)
					}
				}
			case "right":
				if m.selectedCol < len(columnHeaders)-1 {
					m.selectedCol++
					if m.filterStr != "" {
						m.table.SetFilter(m.selectedCol, m.filterStr)
					}
				}
			default:
				if len(key) == 1 && key >= " " && key <= "~" {
					m.filterStr += key
					m.table.SetFilter(m.selectedCol, m.filterStr)
				}
			}
			return m, nil
		}

		// Normal mode
		switch key {
		case "q", "ctrl+c":
			m.quitting = true
			m.action = browserQuit
			return m, tea.Quit
		case "esc":
			m.filtering = false
			if m.filterStr != "" {
				m.filterStr = ""
				m.table.UnsetFilter()
			}
		case "/":
			m.filtering = true
			m.filterStr = ""
		case "down", "j":
			m.table.CursorDown()
		case "up", "k":
			m.table.CursorUp()
		case "left":
			if m.selectedCol > 0 {
				m.selectedCol--
			}
		case "right":
			if m.selectedCol < len(columnHeaders)-1 {
				m.selectedCol++
			}
		case "pgdown":
			pageSize := m.height - 6
			if pageSize < 5 {
				pageSize = 5
			}
			for i := 0; i < pageSize; i++ {
				m.table.CursorDown()
			}
		case "pgup":
			pageSize := m.height - 6
			if pageSize < 5 {
				pageSize = 5
			}
			for i := 0; i < pageSize; i++ {
				m.table.CursorUp()
			}
		case "home":
			_, y := m.table.GetCursorLocation()
			for i := 0; i < y; i++ {
				m.table.CursorUp()
			}
		case "end":
			for i := 0; i < len(m.txs); i++ {
				m.table.CursorDown()
			}
		case "enter":
			_, y := m.table.GetCursorLocation()
			filtered := m.getFilteredTxs()
			if y >= 0 && y < len(filtered) {
				m.detailTx = &filtered[y]
				m.detailIdx = y
				m.mode = modeDetail
			}
		case "C":
			_, y := m.table.GetCursorLocation()
			filtered := m.getFilteredTxs()
			if y >= 0 && y < len(filtered) {
				m.detailTx = &filtered[y]
				m.detailIdx = y
				m.startEditCollective()
			}
		case "c":
			_, y := m.table.GetCursorLocation()
			filtered := m.getFilteredTxs()
			if y >= 0 && y < len(filtered) {
				m.detailTx = &filtered[y]
				m.detailIdx = y
				m.startEditCategory()
			}
		case "d":
			_, y := m.table.GetCursorLocation()
			filtered := m.getFilteredTxs()
			if y >= 0 && y < len(filtered) {
				m.detailTx = &filtered[y]
				m.detailIdx = y
				m.startEditDate()
			}
		case "s":
			colIdx, phase := m.table.GetOrder()
			if colIdx == m.selectedCol {
				if phase == stickertable.SortingOrderAscending {
					m.table.OrderByDesc(m.selectedCol)
				} else {
					m.table.OrderByAsc(m.selectedCol)
				}
			} else {
				m.table.OrderByAsc(m.selectedCol)
			}
		case "r":
			m.action = browserCreateRule
			return m, tea.Quit
		}
	}

	return m, nil
}

func (m txBrowserModel) updateDetail(msg tea.Msg) (tea.Model, tea.Cmd) {
	if keyMsg, ok := msg.(tea.KeyMsg); ok {
		switch keyMsg.String() {
		case "esc", "q", "enter":
			m.mode = modeTable
			m.detailTx = nil
			// Re-sync filter state with stickers table
			if m.filterStr != "" {
				m.table.SetFilter(m.selectedCol, m.filterStr)
			}
		case "C":
			m.startEditCollective()
		case "c":
			m.startEditCategory()
		case "d":
			m.startEditDate()
		}
	}
	return m, nil
}

// ── Inline edit helpers ──

func (m txBrowserModel) filteredEditOptions() []string {
	if m.editInput == "" {
		return m.editOptions
	}
	filter := strings.ToLower(m.editInput)
	var result []string
	for _, o := range m.editOptions {
		if strings.Contains(strings.ToLower(o), filter) {
			result = append(result, o)
		}
	}
	return result
}

func (m *txBrowserModel) startEditCollective() {
	m.editOptions = CollectiveSlugs()
	sort.Strings(m.editOptions)
	m.editInput = m.detailTx.Collective
	m.editCursor = 0
	m.mode = modeEditCollective
}

func (m *txBrowserModel) startEditCategory() {
	cats := LoadCategories()
	m.editOptions = make([]string, 0, len(cats))
	for _, c := range cats {
		m.editOptions = append(m.editOptions, c.Slug)
	}
	sort.Strings(m.editOptions)
	m.editInput = m.detailTx.Category
	m.editCursor = 0
	m.mode = modeEditCategory
}

func (m *txBrowserModel) startEditDate() {
	tz := BrusselsTZ()
	t := time.Unix(m.detailTx.Timestamp, 0).In(tz)
	m.editInput = fmt.Sprintf("%d-%02d", t.Year(), t.Month())
	m.editOptions = nil
	m.editCursor = 0
	m.mode = modeEditDate
}

func (m *txBrowserModel) commitInlineEdit() {
	if m.detailTx == nil {
		return
	}
	switch m.mode {
	case modeEditCollective:
		m.detailTx.Collective = m.editInput
		for i := range m.txs {
			if m.txs[i].ID == m.detailTx.ID {
				m.txs[i].Collective = m.editInput
				break
			}
		}
	case modeEditCategory:
		m.detailTx.Category = m.editInput
		for i := range m.txs {
			if m.txs[i].ID == m.detailTx.ID {
				m.txs[i].Category = m.editInput
				break
			}
		}
	case modeEditDate:
		// editInput is stored as spread metadata; save handled by saveTransactionUpdate
	}
	saveTransactionUpdate(m.detailTx)
	// Rebuild table
	m.table.ClearRows()
	rows := buildStickerRows(m.txs)
	for _, row := range rows {
		anyRow := make([]any, len(row))
		for j, v := range row {
			anyRow[j] = v
		}
		m.table.MustAddRows([][]any{anyRow})
	}
}

func (m txBrowserModel) updateInlineEdit(msg tea.Msg) (tea.Model, tea.Cmd) {
	if keyMsg, ok := msg.(tea.KeyMsg); ok {
		key := keyMsg.String()
		switch key {
		case "esc":
			m.mode = modeDetail
			return m, nil
		case "enter":
			// If there are filtered options and one is selected, use it
			if m.mode != modeEditDate {
				filtered := m.filteredEditOptions()
				if len(filtered) > 0 && m.editCursor < len(filtered) {
					m.editInput = filtered[m.editCursor]
				}
			}
			m.commitInlineEdit()
			m.mode = modeDetail
			return m, nil
		case "tab":
			// Autocomplete: fill input with selected option
			if m.mode != modeEditDate {
				filtered := m.filteredEditOptions()
				if len(filtered) > 0 && m.editCursor < len(filtered) {
					m.editInput = filtered[m.editCursor]
				}
			}
			return m, nil
		case "up":
			if m.editCursor > 0 {
				m.editCursor--
			}
		case "down":
			filtered := m.filteredEditOptions()
			if m.editCursor < len(filtered)-1 {
				m.editCursor++
			}
		case "backspace":
			if len(m.editInput) > 0 {
				m.editInput = m.editInput[:len(m.editInput)-1]
				m.editCursor = 0
			}
		default:
			if len(key) == 1 && key >= " " && key <= "~" {
				m.editInput += key
				m.editCursor = 0
			}
		}
	}
	return m, nil
}

func (m txBrowserModel) View() string {
	if m.quitting {
		return ""
	}

	switch m.mode {
	case modeDetail:
		// Overlay the detail panel on top of the table
		bgView := &tableView{content: m.renderTable()}
		fg := &detailPanel{content: m.renderDetailBox()}
		ov := overlay.New(fg, bgView, overlay.Center, overlay.Center, 0, 0)
		return ov.View()
	case modeEditCollective, modeEditCategory, modeEditDate:
		bgView := &tableView{content: m.renderTable()}
		fg := &detailPanel{content: m.renderInlineEditBox()}
		ov := overlay.New(fg, bgView, overlay.Center, overlay.Center, 0, 0)
		return ov.View()
	default:
		return m.renderTable()
	}
}

func (m txBrowserModel) renderTable() string {
	var b strings.Builder

	title := "💰 Transactions"
	if m.currency != "" {
		title += " (" + m.currency + ")"
	}
	colName := columnHeaders[m.selectedCol]
	b.WriteString(lipgloss.NewStyle().Bold(true).Render(title))

	// Show selected column + filter info
	if m.filtering {
		filterVal := m.filterStr
		if filterVal == "" {
			filterVal = "…"
		}
		b.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color("3")).Render(
			fmt.Sprintf("  🔍 %s: %s", colName, filterVal)))
	} else if m.filterStr != "" {
		b.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color("3")).Render(
			fmt.Sprintf("  🔍 %s: %s", colName, m.filterStr)))
	} else {
		b.WriteString(lipgloss.NewStyle().Faint(true).Render(
			fmt.Sprintf("  [%s]", colName)))
	}
	b.WriteString("\n")

	// Render table and strip the built-in footer (last line)
	rendered := m.table.Render()
	lines := strings.Split(rendered, "\n")
	if len(lines) > 1 {
		lines = lines[:len(lines)-1]
	}
	b.WriteString(strings.Join(lines, "\n"))
	b.WriteString("\n")

	// Custom footer — compute from filtered set
	filteredTxs := m.getFilteredTxs()
	_, cursorY := m.table.GetCursorLocation()
	pageSize := m.height - 8
	if pageSize < 5 {
		pageSize = 20
	}
	currentPage := 1
	totalPages := 1
	if pageSize > 0 && len(filteredTxs) > 0 {
		currentPage = (cursorY / pageSize) + 1
		totalPages = (len(filteredTxs) + pageSize - 1) / pageSize
	}

	var totalIn, totalOut float64
	for _, tx := range filteredTxs {
		amt := math.Abs(txAmount(tx))
		if isEURCurrency(tx.Currency) {
			if tx.Type == "CREDIT" {
				totalIn += amt
			} else if tx.Type == "DEBIT" {
				totalOut += amt
			}
		}
	}

	countStr := fmt.Sprintf("%d transactions", len(filteredTxs))
	if len(filteredTxs) != len(m.txs) {
		countStr = fmt.Sprintf("%d of %d transactions", len(filteredTxs), len(m.txs))
	}
	footerInfo := fmt.Sprintf("  %s — Page %d/%d — In: %s  Out: %s  Net: %s",
		countStr, currentPage, totalPages,
		styleGreen.Render(fmtEUR(totalIn)),
		styleRed.Render(fmtEUR(totalOut)),
		fmtEURSigned(totalIn-totalOut))
	b.WriteString(lipgloss.NewStyle().Faint(true).Render(footerInfo))
	b.WriteString("\n")

	var keys string
	if m.filtering {
		keys = "  Type to filter  [←→] Column  [Esc] Clear  [Enter] Done  [↑↓] Navigate"
	} else {
		keys = "  [←→] Column  [/] Filter  [s] Sort  [c] Category  [C] Collective  [d] Date  [r] Rule  [Enter] Details  [q] Quit"
	}
	b.WriteString(lipgloss.NewStyle().Faint(true).Render(keys))
	b.WriteString("\n")

	return b.String()
}

func (m txBrowserModel) renderDetailBox() string {
	tx := m.detailTx
	tz := BrusselsTZ()
	t := time.Unix(tx.Timestamp, 0).In(tz)

	bg := lipgloss.Color("235")
	labelStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("6")).Background(bg).Width(16)

	valueStyle := lipgloss.NewStyle().Background(bg)

	var lines []string
	add := func(label, value string) {
		if value != "" {
			lines = append(lines, fmt.Sprintf("%s %s", labelStyle.Render(label), valueStyle.Render(value)))
		}
	}

	add("Date", t.Format("02/01/2006 15:04"))
	add("Type", tx.Type)
	add("Provider", tx.Provider)
	if tx.Chain != nil {
		add("Chain", *tx.Chain)
	}
	add("Account", tx.AccountName)
	if tx.Account != "" && tx.Account != tx.AccountName {
		add("Address", shortAddr(tx.Account))
	}
	add("Currency", tx.Currency)

	amt := txAmount(*tx)
	if tx.Type == "CREDIT" {
		add("Amount", styleGreen.Render(fmt.Sprintf("+%.2f", math.Abs(amt))))
	} else {
		add("Amount", styleRed.Render(fmt.Sprintf("-%.2f", math.Abs(amt))))
	}
	if tx.Fee > 0 {
		add("Fee", fmt.Sprintf("%.2f", tx.Fee))
	}
	add("Counterparty", txDisplayCounterparty(*tx))
	add("Description", txDisplayDescription(*tx))
	add("Category", tx.Category)
	add("Collective", tx.Collective)
	if tx.Event != "" {
		add("Event", tx.Event)
	}
	if app, ok := tx.Metadata["application"]; ok {
		if s, ok := app.(string); ok && s != "" {
			add("Application", s)
		}
	}
	if email, ok := tx.Metadata["email"]; ok {
		if s, ok := email.(string); ok && s != "" {
			add("Email", s)
		}
	}
	if pm, ok := tx.Metadata["paymentMethod"]; ok {
		if s, ok := pm.(string); ok && s != "" {
			add("Payment", s)
		}
	}
	for k, v := range tx.Metadata {
		if strings.HasPrefix(k, "stripe_") {
			if s, ok := v.(string); ok && s != "" {
				add(strings.TrimPrefix(k, "stripe_"), s)
			}
		}
	}
	for k, v := range tx.Metadata {
		if strings.HasPrefix(k, "custom_") {
			if s, ok := v.(string); ok && s != "" {
				add(strings.TrimPrefix(k, "custom_"), s)
			}
		}
	}
	// Show Nostr/custom tags (metadata keys that aren't standard enrichment fields)
	standardKeys := map[string]bool{
		"category": true, "description": true, "application": true,
		"email": true, "paymentMethod": true, "paymentLink": true,
		"memo": true, "state": true, "accountSlug": true,
	}
	var tagLines []string
	for k, v := range tx.Metadata {
		if standardKeys[k] || strings.HasPrefix(k, "stripe_") || strings.HasPrefix(k, "custom_") {
			continue
		}
		if s, ok := v.(string); ok && s != "" && len(k) > 0 {
			tagLines = append(tagLines, fmt.Sprintf("%s %s",
				labelStyle.Render(k),
				lipgloss.NewStyle().Foreground(lipgloss.Color("5")).Background(bg).Render(s)))
		}
	}
	if len(tagLines) > 0 {
		sort.Strings(tagLines)
		lines = append(lines, "")
		lines = append(lines, lipgloss.NewStyle().Faint(true).Background(bg).Render("Tags"))
		lines = append(lines, tagLines...)
	}

	add("TX Hash", shortAddr(tx.TxHash))

	lines = append(lines, "")
	lines = append(lines, lipgloss.NewStyle().Faint(true).Background(bg).Render("[Enter/Esc] Back  [c] Category  [C] Collective  [d] Date"))

	boxWidth := 56
	if m.width > 20 {
		boxWidth = m.width / 2
		if boxWidth > 70 {
			boxWidth = 70
		}
	}

	return lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color("62")).
		Background(lipgloss.Color("235")).
		Padding(1, 2).
		Width(boxWidth).
		Render(lipgloss.NewStyle().Bold(true).Background(bg).Render("Transaction Detail") + "\n\n" + strings.Join(lines, "\n"))
}

func (m txBrowserModel) renderInlineEditBox() string {
	bg := lipgloss.Color("235")
	labelStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("6")).Background(bg)
	selectedStyle := lipgloss.NewStyle().Background(lipgloss.Color("62")).Foreground(lipgloss.Color("255"))
	optionStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("252")).Background(bg)

	var title string
	switch m.mode {
	case modeEditCollective:
		title = "Assign Collective"
	case modeEditCategory:
		title = "Assign Category"
	case modeEditDate:
		title = "Set Accounting Date (YYYY-MM)"
	}

	var lines []string
	lines = append(lines, lipgloss.NewStyle().Bold(true).Background(bg).Render(title))
	lines = append(lines, "")

	// Input field
	inputDisplay := m.editInput
	if inputDisplay == "" {
		inputDisplay = "…"
	}
	lines = append(lines, labelStyle.Render("> ")+lipgloss.NewStyle().Foreground(lipgloss.Color("255")).Background(bg).Render(inputDisplay+"▎"))

	// Autocomplete options (not for date)
	if m.mode != modeEditDate {
		filtered := m.filteredEditOptions()
		lines = append(lines, "")
		maxShow := 10
		if len(filtered) < maxShow {
			maxShow = len(filtered)
		}
		// Scroll window around cursor
		start := 0
		if m.editCursor >= maxShow {
			start = m.editCursor - maxShow + 1
		}
		end := start + maxShow
		if end > len(filtered) {
			end = len(filtered)
			start = end - maxShow
			if start < 0 {
				start = 0
			}
		}
		for i := start; i < end; i++ {
			if i == m.editCursor {
				lines = append(lines, selectedStyle.Render(" > "+filtered[i]+" "))
			} else {
				lines = append(lines, optionStyle.Render("   "+filtered[i]))
			}
		}
		if len(filtered) == 0 {
			lines = append(lines, lipgloss.NewStyle().Faint(true).Background(bg).Render("   (new: "+m.editInput+")"))
		}
	}

	lines = append(lines, "")
	lines = append(lines, lipgloss.NewStyle().Faint(true).Background(bg).Render("[Enter] Save  [Tab] Complete  [Esc] Cancel"))

	boxWidth := 40
	if m.width > 20 {
		boxWidth = m.width / 3
		if boxWidth < 40 {
			boxWidth = 40
		}
		if boxWidth > 60 {
			boxWidth = 60
		}
	}

	return lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color("62")).
		Background(bg).
		Padding(1, 2).
		Width(boxWidth).
		Render(strings.Join(lines, "\n"))
}

// getFilteredTxs returns the transactions matching the current filter.
// Mirrors the stickers filter logic (case-insensitive substring on column).
func (m txBrowserModel) getFilteredTxs() []TransactionEntry {
	if m.filterStr == "" {
		return m.txs
	}
	tz := BrusselsTZ()
	filter := strings.ToLower(m.filterStr)
	var result []TransactionEntry
	for _, tx := range m.txs {
		t := time.Unix(tx.Timestamp, 0).In(tz)
		// Build the cell value for the selected column (same order as buildStickerRows)
		var cellValue string
		switch m.selectedCol {
		case 0: // Date
			cellValue = t.Format("02/01")
		case 1: // Source
			cellValue = txSource(tx)
		case 2: // Collective
			cellValue = tx.Collective
		case 3: // Category
			cellValue = tx.Category
		case 4: // Counterparty
			cellValue = txDisplayCounterparty(tx)
		case 5: // Description
			cellValue = txDisplayDescription(tx)
		case 6: // Amount
			amt := txAmount(tx)
			if tx.Type == "CREDIT" {
				cellValue = fmt.Sprintf("+%.2f", math.Abs(amt))
			} else {
				cellValue = fmt.Sprintf("-%.2f", math.Abs(amt))
			}
		}
		if strings.Contains(strings.ToLower(cellValue), filter) {
			result = append(result, tx)
		}
	}
	return result
}

// ── Save ──

func saveTransactionUpdate(tx *TransactionEntry) bool {
	dataDir := DataDir()
	t := time.Unix(tx.Timestamp, 0).In(BrusselsTZ())
	year := fmt.Sprintf("%d", t.Year())
	month := fmt.Sprintf("%02d", t.Month())

	txPath := filepath.Join(dataDir, year, month, "generated", "transactions.json")
	data, err := os.ReadFile(txPath)
	if err != nil {
		return false
	}
	var txFile TransactionsFile
	if json.Unmarshal(data, &txFile) != nil {
		return false
	}
	for i := range txFile.Transactions {
		if txFile.Transactions[i].ID == tx.ID {
			txFile.Transactions[i].Category = tx.Category
			txFile.Transactions[i].Collective = tx.Collective
			out, _ := json.MarshalIndent(txFile, "", "  ")
			writeMonthFile(dataDir, year, month, filepath.Join("generated", "transactions.json"), out)
			return true
		}
	}
	return false
}

// ── Create rule ──

func createRuleFromBrowser(allTxs []TransactionEntry) {
	em := newRuleEditor(nil, allTxs)
	ep := tea.NewProgram(em, tea.WithAltScreen())
	ep.Run()
}

// ── Command ──

func TransactionsBrowser(args []string) {
	if HasFlag(args, "--help", "-h", "help") {
		printTransactionsBrowserHelp()
		return
	}

	filter, n, skip, err := parseTxListFlags(args)
	if err != nil {
		if JSONMode(args) {
			EmitJSONError(err)
			os.Exit(1)
		}
		fmt.Printf("%sError: %v%s\n", Fmt.Red, err, Fmt.Reset)
		os.Exit(1)
	}

	if JSONMode(args) {
		emitTransactionsJSON(filter, n, skip)
		return
	}

	fmt.Printf("  Loading transactions...\n")
	txs := applyOffsetLimit(loadFilteredTransactions(filter), skip, n)

	for {
		t := newStickerTable(txs, 0, 0)

		m := txBrowserModel{
			table:    t,
			txs:      txs,
			currency: filter.Currency,
		}

		p := tea.NewProgram(m, tea.WithAltScreen())
		result, err := p.Run()
		if err != nil {
			fmt.Printf("%sError: %v%s\n", Fmt.Red, err, Fmt.Reset)
			return
		}
		fm := result.(txBrowserModel)
		txs = fm.txs

		switch fm.action {
		case browserQuit:
			return
		case browserCreateRule:
			createRuleFromBrowser(txs)
			txs = applyOffsetLimit(loadFilteredTransactions(filter), skip, n)
		}
	}
}

// parseTxListFlags reads the shared filter flags used by both the interactive
// browser and the JSON listing. A negative limit means "no limit". A bare
// positional currency code (EUR/EURE/EURB/CHT) is still accepted as a
// shorthand for --currency to keep prior muscle memory working.
func parseTxListFlags(args []string) (TxFilter, int, int, error) {
	f := TxFilter{
		AccountSlug: GetOption(args, "--account"),
		Currency:    strings.ToUpper(GetOption(args, "--currency")),
	}

	if f.Currency == "" {
		for _, a := range args {
			if strings.HasPrefix(a, "-") {
				continue
			}
			upper := strings.ToUpper(a)
			if upper == "EUR" || upper == "EURE" || upper == "EURB" || upper == "CHT" {
				f.Currency = upper
				break
			}
		}
	}

	if s := GetOption(args, "--since"); s != "" {
		t, ok := ParseSinceDate(s)
		if !ok {
			return f, 0, 0, fmt.Errorf("invalid --since value %q (expected YYYYMMDD)", s)
		}
		f.Since = t
	}
	if s := GetOption(args, "--until"); s != "" {
		t, ok := ParseSinceDate(s)
		if !ok {
			return f, 0, 0, fmt.Errorf("invalid --until value %q (expected YYYYMMDD)", s)
		}
		// --until is inclusive: extend to the very end of that day.
		f.Until = t.Add(24*time.Hour - time.Second)
	}

	limit := GetNumber(args, []string{"-n", "--limit"}, -1)
	skip := GetNumber(args, []string{"--skip"}, 0)
	if skip < 0 {
		skip = 0
	}
	return f, limit, skip, nil
}

func applyOffsetLimit(txs []TransactionEntry, skip, limit int) []TransactionEntry {
	if skip > 0 {
		if skip >= len(txs) {
			return nil
		}
		txs = txs[skip:]
	}
	if limit >= 0 && limit < len(txs) {
		txs = txs[:limit]
	}
	return txs
}

func emitTransactionsJSON(f TxFilter, limit, skip int) {
	txs := applyOffsetLimit(loadFilteredTransactions(f), skip, limit)
	out := struct {
		Count        int                `json:"count"`
		Transactions []TransactionEntry `json:"transactions"`
	}{
		Count:        len(txs),
		Transactions: txs,
	}
	if txs == nil {
		out.Transactions = []TransactionEntry{}
	}
	_ = EmitJSON(out)
}

func printTransactionsBrowserHelp() {
	f := Fmt
	fmt.Printf(`
%schb transactions%s [filters] — Browse transactions interactively

%sUSAGE%s
  %schb transactions%s                                  Browse all transactions
  %schb transactions --account savings%s                Browse one account
  %schb transactions --currency EUR%s                   Browse EUR-family transactions
  %schb transactions --since 20260101 --until 20260131%s   Date range (inclusive)
  %schb transactions -n 50 --skip 100%s                 Paginate
  %schb transactions --json%s                           Print matching txs as JSON
  %schb transactions stats%s                            Show transaction statistics

%sFILTERS%s
  %s--account <slug>%s     Limit to one account (e.g. savings, stripe-asbl)
  %s--currency <CODE>%s    EUR (matches the EUR family), CHT, etc.
  %s--since YYYYMMDD%s     Inclusive lower bound on transaction date
  %s--until YYYYMMDD%s     Inclusive upper bound on transaction date
  %s-n N%s                 Limit to N transactions (most recent first)
  %s--skip N%s             Skip the first N matches before applying -n
  %s--json%s               Emit JSON instead of launching the interactive browser

%sINTERACTIVE KEYS%s
  %s↑↓/jk%s       Navigate rows
  %s←→%s          Select column (for filter/sort)
  %sPgUp/PgDn%s   Scroll page
  %s/%s           Filter on selected column
  %ss%s           Sort by selected column
  %sc%s           Assign category
  %sC%s           Assign collective
  %sd%s           Set accounting date
  %sr%s           Create categorization rule
  %sEnter%s       Show transaction details
  %sEsc%s         Clear filter / Back
  %sq%s           Quit
`,
		f.Bold, f.Reset, // heading
		f.Bold, f.Reset, // USAGE
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Bold, f.Reset, // FILTERS
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Bold, f.Reset, // INTERACTIVE KEYS
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
	)
}
