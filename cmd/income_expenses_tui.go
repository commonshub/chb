package cmd

import (
	"fmt"
	"strings"

	tea "github.com/charmbracelet/bubbletea"
)

// runIncomeExpenseTUI launches the interactive `chb income -i` /
// `chb expenses -i` view. Two modes:
//
//   - LIST: every category that produced ≥1 transaction in the
//     selected range, sorted by total amount descending. Up/down to
//     navigate, Enter drills into the selected category.
//
//   - DRILL: every transaction in the selected category, sorted by
//     absolute gross amount descending. Esc/q goes back to the list.
//
// No writes; the TUI is read-only.
func runIncomeExpenseTUI(direction, rangeLabel, accountSlug string, cats []*incomeCategoryBucket, totalCount int, totalAmount float64) {
	if len(cats) == 0 {
		fmt.Printf("\n%sNo %s in this range.%s\n\n", Fmt.Dim, direction, Fmt.Reset)
		return
	}
	m := incomeTUIModel{
		direction:   direction,
		rangeLabel:  rangeLabel,
		accountSlug: accountSlug,
		cats:        cats,
		totalCount:  totalCount,
		totalAmount: totalAmount,
	}
	p := tea.NewProgram(m, tea.WithAltScreen())
	if _, err := p.Run(); err != nil {
		fmt.Printf("%sError: %v%s\n", Fmt.Red, err, Fmt.Reset)
	}
}

type incomeTUIMode int

const (
	incomeModeList incomeTUIMode = iota
	incomeModeDrill
)

type incomeTUIModel struct {
	direction   string  // "income" or "expenses"
	rangeLabel  string  // human range, e.g. "2025/Q1"
	accountSlug string  // empty for "all accounts"
	cats        []*incomeCategoryBucket
	totalCount  int
	totalAmount float64

	mode   incomeTUIMode
	cursor int // index into cats (list) or into the drilled-into cats[drillIdx].Txs (drill)
	offset int

	// drillIdx is the category index the user pressed Enter on while
	// in list mode. Stored so the back button can restore the list
	// cursor at the same row.
	drillIdx int

	width, height int
}

func (m incomeTUIModel) Init() tea.Cmd { return nil }

func (m incomeTUIModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		return m, nil
	case tea.KeyMsg:
		switch m.mode {
		case incomeModeDrill:
			return m.updateDrill(msg)
		}
		return m.updateList(msg)
	}
	return m, nil
}

func (m incomeTUIModel) updateList(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "q", "esc", "ctrl+c":
		return m, tea.Quit
	case "up", "k":
		if m.cursor > 0 {
			m.cursor--
		}
	case "down", "j":
		if m.cursor < len(m.cats)-1 {
			m.cursor++
		}
	case "pgup":
		m.cursor -= 10
		if m.cursor < 0 {
			m.cursor = 0
		}
	case "pgdown":
		m.cursor += 10
		if m.cursor >= len(m.cats) {
			m.cursor = len(m.cats) - 1
		}
	case "home", "g":
		m.cursor = 0
	case "end", "G":
		m.cursor = len(m.cats) - 1
	case "enter":
		if m.cursor < 0 || m.cursor >= len(m.cats) {
			return m, nil
		}
		if len(m.cats[m.cursor].Txs) == 0 {
			return m, nil
		}
		m.drillIdx = m.cursor
		m.mode = incomeModeDrill
		m.cursor = 0
		m.offset = 0
	}
	return m, nil
}

func (m incomeTUIModel) updateDrill(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	txs := m.cats[m.drillIdx].Txs
	switch msg.String() {
	case "q", "esc":
		m.mode = incomeModeList
		m.cursor = m.drillIdx
		m.offset = 0
	case "ctrl+c":
		return m, tea.Quit
	case "up", "k":
		if m.cursor > 0 {
			m.cursor--
		}
	case "down", "j":
		if m.cursor < len(txs)-1 {
			m.cursor++
		}
	case "pgup":
		m.cursor -= 10
		if m.cursor < 0 {
			m.cursor = 0
		}
	case "pgdown":
		m.cursor += 10
		if m.cursor >= len(txs) {
			m.cursor = len(txs) - 1
		}
	case "home", "g":
		m.cursor = 0
	case "end", "G":
		m.cursor = len(txs) - 1
	}
	return m, nil
}

func (m incomeTUIModel) View() string {
	if m.mode == incomeModeDrill {
		return m.viewDrill()
	}
	return m.viewList()
}

func (m incomeTUIModel) viewList() string {
	var b strings.Builder
	icon := "💰"
	if m.direction == "expenses" {
		icon = "💸"
	}
	scope := "all accounts"
	if m.accountSlug != "" {
		scope = "account " + m.accountSlug
	}
	b.WriteString(cpTUIHeaderStyle.Render(fmt.Sprintf("%s %s by category — %s  (%s)",
		icon, strings.Title(m.direction), m.rangeLabel, scope)))
	b.WriteString("\n")
	b.WriteString(cpTUIDimStyle.Render(fmt.Sprintf("%d categories — %d transactions — %s total",
		len(m.cats), m.totalCount, fmtEUR(m.totalAmount))))
	b.WriteString("\n\n")

	pageSize := m.height - 8
	if pageSize < 5 {
		pageSize = 20
	}
	if m.cursor < m.offset {
		m.offset = m.cursor
	}
	if m.cursor >= m.offset+pageSize {
		m.offset = m.cursor - pageSize + 1
	}
	end := m.offset + pageSize
	if end > len(m.cats) {
		end = len(m.cats)
	}

	headers := []string{"Category", "Txs", "Amount", "Share"}
	rightAlign := map[int]bool{1: true, 2: true, 3: true}
	caps := []int{36, 6, 14, 8}

	plain := make([][]string, 0, end-m.offset)
	for i := m.offset; i < end; i++ {
		c := m.cats[i]
		share := 0.0
		if m.totalAmount > 0 {
			share = c.Amount / m.totalAmount * 100
		}
		plain = append(plain, []string{
			Truncate(c.Category, 36),
			fmt.Sprintf("%d", c.Count),
			fmtEUR(c.Amount),
			fmt.Sprintf("%.1f%%", share),
		})
	}

	widths := make([]int, len(headers))
	for i, h := range headers {
		widths[i] = displayWidth(h)
	}
	for _, r := range plain {
		for i, c := range r {
			if w := displayWidth(c); w > widths[i] {
				widths[i] = w
			}
		}
	}
	for i := range widths {
		if widths[i] > caps[i] {
			widths[i] = caps[i]
		}
	}

	renderRow := func(cells []string, isHeader, isCursor bool) string {
		parts := make([]string, len(cells))
		for i, c := range cells {
			if rightAlign[i] {
				parts[i] = padLeft(c, widths[i])
			} else {
				parts[i] = padRight(c, widths[i])
			}
		}
		line := "  " + strings.Join(parts, "  ")
		switch {
		case isHeader:
			return cpTUIDimStyle.Render(line)
		case isCursor:
			return cpTUICursorStyle.Render(line)
		}
		return line
	}

	b.WriteString(renderRow(headers, true, false))
	b.WriteString("\n")
	for i, r := range plain {
		abs := m.offset + i
		b.WriteString(renderRow(r, false, abs == m.cursor))
		b.WriteString("\n")
	}
	if m.offset > 0 || end < len(m.cats) {
		b.WriteString(cpTUIDimStyle.Render(fmt.Sprintf("\n  showing %d–%d of %d", m.offset+1, end, len(m.cats))))
		b.WriteString("\n")
	}

	b.WriteString("\n  ")
	b.WriteString(cpTUIDimStyle.Render("[↑/↓] navigate   [enter] drill into category   [q] quit"))
	b.WriteString("\n")
	return b.String()
}

func (m incomeTUIModel) viewDrill() string {
	var b strings.Builder
	cat := m.cats[m.drillIdx]
	icon := "💰"
	if m.direction == "expenses" {
		icon = "💸"
	}
	b.WriteString(cpTUIHeaderStyle.Render(fmt.Sprintf("%s %s — %s  (%s)",
		icon, strings.Title(m.direction), cat.Category, m.rangeLabel)))
	b.WriteString("\n")
	b.WriteString(cpTUIDimStyle.Render(fmt.Sprintf("%d transactions — %s total — sorted by amount desc",
		cat.Count, fmtEUR(cat.Amount))))
	b.WriteString("\n\n")

	pageSize := m.height - 8
	if pageSize < 5 {
		pageSize = 20
	}
	if m.cursor < m.offset {
		m.offset = m.cursor
	}
	if m.cursor >= m.offset+pageSize {
		m.offset = m.cursor - pageSize + 1
	}
	end := m.offset + pageSize
	if end > len(cat.Txs) {
		end = len(cat.Txs)
	}

	headers := []string{"Date", "Counterparty", "Account", "Amount"}
	rightAlign := map[int]bool{3: true}
	caps := []int{10, 40, 14, 14}

	plain := make([][]string, 0, end-m.offset)
	for i := m.offset; i < end; i++ {
		t := cat.Txs[i]
		plain = append(plain, []string{
			t.Date,
			Truncate(firstNonEmptyStr(t.Counterparty, t.Description, "—"), 40),
			Truncate(t.AccountSlug, 14),
			fmtAmountCurrency(t.Amount, t.Currency),
		})
	}

	widths := make([]int, len(headers))
	for i, h := range headers {
		widths[i] = displayWidth(h)
	}
	for _, r := range plain {
		for i, c := range r {
			if w := displayWidth(c); w > widths[i] {
				widths[i] = w
			}
		}
	}
	for i := range widths {
		if widths[i] > caps[i] {
			widths[i] = caps[i]
		}
	}

	renderRow := func(cells []string, isHeader, isCursor bool) string {
		parts := make([]string, len(cells))
		for i, c := range cells {
			if rightAlign[i] {
				parts[i] = padLeft(c, widths[i])
			} else {
				parts[i] = padRight(c, widths[i])
			}
		}
		line := "  " + strings.Join(parts, "  ")
		switch {
		case isHeader:
			return cpTUIDimStyle.Render(line)
		case isCursor:
			return cpTUICursorStyle.Render(line)
		}
		return line
	}

	b.WriteString(renderRow(headers, true, false))
	b.WriteString("\n")
	for i, r := range plain {
		abs := m.offset + i
		b.WriteString(renderRow(r, false, abs == m.cursor))
		b.WriteString("\n")
	}
	if m.offset > 0 || end < len(cat.Txs) {
		b.WriteString(cpTUIDimStyle.Render(fmt.Sprintf("\n  showing %d–%d of %d", m.offset+1, end, len(cat.Txs))))
		b.WriteString("\n")
	}

	// Detail panel for the cursor row — URI is useful to copy out for
	// cross-referencing (nostr lookup, `chb rules add`, etc.).
	if m.cursor < len(cat.Txs) {
		t := cat.Txs[m.cursor]
		b.WriteString("\n  ")
		b.WriteString(cpTUIHeaderStyle.Render("▸ " + firstNonEmptyStr(t.Counterparty, t.Description, t.URI)))
		b.WriteString("\n")
		if t.Description != "" && t.Description != t.Counterparty {
			b.WriteString("  ")
			b.WriteString(cpTUIDimStyle.Render(padRight("Description", 14)))
			b.WriteString(t.Description)
			b.WriteString("\n")
		}
		if t.URI != "" {
			b.WriteString("  ")
			b.WriteString(cpTUIDimStyle.Render(padRight("URI", 14)))
			b.WriteString(t.URI)
			b.WriteString("\n")
		}
	}

	b.WriteString("\n  ")
	b.WriteString(cpTUIDimStyle.Render("[↑/↓] navigate   [esc/q] back to categories"))
	b.WriteString("\n")
	return b.String()
}
