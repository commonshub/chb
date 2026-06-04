package cmd

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
)

// runSearchTUI opens the spotlight: a search box that live-filters the unified
// index (transactions + invoices + bills) as you type. Read-only.
func runSearchTUI(index []SearchItem, initial string) {
	in := textinput.New()
	in.Placeholder = "reference · counterparty · IBAN · memo · amount (100, +100, -50.12)"
	in.Prompt = "  🔎 "
	in.CharLimit = 120
	in.Width = 60
	in.SetValue(initial)
	in.CursorEnd()
	in.Focus()

	m := searchTUIModel{all: index, input: in}
	m = m.refilter()
	if _, err := tea.NewProgram(m, tea.WithAltScreen()).Run(); err != nil {
		fmt.Printf("%sError: %v%s\n", Fmt.Red, err, Fmt.Reset)
	}
}

type searchTUIModel struct {
	all        []SearchItem
	input      textinput.Model
	results    []SearchItem
	cursor     int
	showDetail bool
	width      int
	height     int
}

func (m searchTUIModel) Init() tea.Cmd { return textinput.Blink }

func (m searchTUIModel) refilter() searchTUIModel {
	q := strings.TrimSpace(m.input.Value())
	if q == "" {
		m.results = m.all
	} else {
		terms := parseSearchTerms(q)
		out := make([]SearchItem, 0, 64)
		for _, it := range m.all {
			if itemMatchesSearch(it, terms) {
				out = append(out, it)
			}
		}
		sortSearchResults(out, terms)
		m.results = out
	}
	if m.cursor >= len(m.results) {
		m.cursor = 0
	}
	return m
}

func (m searchTUIModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width, m.height = msg.Width, msg.Height
		return m, nil
	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c":
			return m, tea.Quit
		case "esc":
			if m.showDetail {
				m.showDetail = false
				return m, nil
			}
			return m, tea.Quit
		case "up", "ctrl+p":
			if m.cursor > 0 {
				m.cursor--
			}
			return m, nil
		case "down", "ctrl+n":
			if m.cursor < len(m.results)-1 {
				m.cursor++
			}
			return m, nil
		case "enter":
			if len(m.results) > 0 {
				m.showDetail = !m.showDetail
			}
			return m, nil
		}
		// Any other key edits the query; re-filter live.
		var cmd tea.Cmd
		m.input, cmd = m.input.Update(msg)
		m = m.refilter()
		m.showDetail = false
		return m, cmd
	}
	return m, nil
}

func (m searchTUIModel) View() string {
	var b strings.Builder
	b.WriteString(cpTUIHeaderStyle.Render("🔎 chb search"))
	b.WriteString("  ")
	b.WriteString(cpTUIDimStyle.Render(fmt.Sprintf("%d / %d records", len(m.results), len(m.all))))
	b.WriteString("\n\n")
	b.WriteString(m.input.View())
	b.WriteString("\n\n")

	if m.showDetail && m.cursor < len(m.results) {
		b.WriteString(m.renderDetail(m.results[m.cursor]))
		b.WriteString("\n  ")
		b.WriteString(cpTUIDimStyle.Render("[esc] back   [↑↓] move   [q via esc] quit"))
		b.WriteString("\n")
		return b.String()
	}

	if len(m.results) == 0 {
		b.WriteString(cpTUIDimStyle.Render("  no matches"))
		b.WriteString("\n\n  ")
		b.WriteString(cpTUIDimStyle.Render("[esc] quit"))
		b.WriteString("\n")
		return b.String()
	}

	headers := []string{"", "Kind", "Date", "Amount", "Counterparty", "Reference", "Memo"}
	caps := []int{1, 4, 10, 13, 24, 20, 32}
	rightAlign := map[int]bool{3: true}

	pageSize := m.height - 8
	if pageSize < 5 {
		pageSize = 15
	}
	start := 0
	if m.cursor >= pageSize {
		start = m.cursor - pageSize + 1
	}
	end := start + pageSize
	if end > len(m.results) {
		end = len(m.results)
	}

	rows := [][]string{headers}
	for i := start; i < end; i++ {
		it := m.results[i]
		mark := " "
		if i == m.cursor {
			mark = "▸"
		}
		rows = append(rows, []string{
			mark,
			searchKindLabel(it.Kind),
			it.Date,
			searchAmountCell(it),
			Truncate(firstNonEmptyStr(it.Counterparty, it.Account), caps[4]),
			Truncate(firstNonEmptyStr(it.Reference, it.Communication), caps[5]),
			Truncate(it.Memo, caps[6]),
		})
	}

	widths := make([]int, len(headers))
	for _, r := range rows {
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
	for ri, r := range rows {
		parts := make([]string, len(r))
		for i, c := range r {
			if rightAlign[i] {
				parts[i] = padLeft(c, widths[i])
			} else {
				parts[i] = padRight(c, widths[i])
			}
		}
		line := "  " + strings.Join(parts, "  ")
		switch {
		case ri == 0:
			b.WriteString(cpTUIDimStyle.Render(line))
		case r[0] == "▸":
			b.WriteString(cpTUICursorStyle.Render(line))
		default:
			b.WriteString(line)
		}
		b.WriteString("\n")
	}
	if end < len(m.results) || start > 0 {
		b.WriteString(cpTUIDimStyle.Render(fmt.Sprintf("  %d–%d of %d", start+1, end, len(m.results))))
		b.WriteString("\n")
	}
	b.WriteString("\n  ")
	b.WriteString(cpTUIDimStyle.Render("type to filter   [↑↓] move   [enter] details   [esc] quit"))
	b.WriteString("\n")
	return b.String()
}

func (m searchTUIModel) renderDetail(it SearchItem) string {
	var b strings.Builder
	b.WriteString(cpTUIHeaderStyle.Render(fmt.Sprintf("▸ %s — %s",
		searchKindLabel(it.Kind), firstNonEmptyStr(it.Reference, it.ID))))
	b.WriteString("\n")
	kv := func(k, v string) {
		if strings.TrimSpace(v) == "" {
			return
		}
		b.WriteString("  ")
		b.WriteString(cpTUIDimStyle.Render(padRight(k, 14)))
		b.WriteString(v)
		b.WriteString("\n")
	}
	kv("Date", it.Date)
	kv("Amount", searchAmountCell(it))
	kv("Counterparty", it.Counterparty)
	kv("IBAN", it.IBAN)
	kv("Reference", it.Reference)
	if it.Communication != it.Reference {
		kv("Communication", it.Communication)
	}
	kv("Memo", it.Memo)
	kv("Account", it.Account)
	kv("ID", it.ID)
	return b.String()
}
