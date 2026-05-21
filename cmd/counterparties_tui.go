package cmd

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

// runCounterpartiesTUI launches an interactive vendors/customers list:
// arrow-key navigation, space to multi-select, and `e` to open an edit
// overlay that adds a categorization rule (collective + category) for
// every selected counterparty. Selected rows update in-place after the
// rule is saved so the user can see the effect immediately.
func runCounterpartiesTUI(kind, direction, scope string, rows []counterpartyAgg) {
	if len(rows) == 0 {
		fmt.Printf("\n%sNo %s to display.%s\n\n", Fmt.Dim, kind, Fmt.Reset)
		return
	}

	collInput := textinput.New()
	collInput.Placeholder = "collective (slug)"
	collInput.Prompt = ""
	collInput.CharLimit = 64
	collInput.Width = 32

	catInput := textinput.New()
	catInput.Placeholder = "category (slug)"
	catInput.Prompt = ""
	catInput.CharLimit = 64
	catInput.Width = 32

	// Sorted known slugs power the autocomplete hint and the
	// confirm-on-new prompt. Loaded once at TUI launch — the user can
	// add new ones via the confirm flow, but those persist via
	// AddCollective / AddCategory and re-loading on next launch.
	knownColls := CollectiveSlugs()
	sort.Strings(knownColls)
	knownCats := make([]string, 0, len(LoadCategories()))
	for _, c := range LoadCategories() {
		if c.Slug != "" {
			knownCats = append(knownCats, c.Slug)
		}
	}
	sort.Strings(knownCats)

	m := counterpartiesTUIModel{
		title:            counterpartiesTUITitle(kind, scope),
		kind:             kind,
		direction:        direction,
		rows:             rows,
		selected:         map[int]bool{},
		collInput:        collInput,
		catInput:         catInput,
		focusField:       0,
		knownCollectives: knownColls,
		knownCategories:  knownCats,
	}

	p := tea.NewProgram(m, tea.WithAltScreen())
	if _, err := p.Run(); err != nil {
		fmt.Printf("%sError: %v%s\n", Fmt.Red, err, Fmt.Reset)
	}
}

func counterpartiesTUITitle(kind, scope string) string {
	icon := "💸"
	if kind == "customers" {
		icon = "💰"
	}
	return fmt.Sprintf("%s %s — %s", icon, titleASCII(kind), scope)
}

type counterpartiesTUIMode int

const (
	cpModeList counterpartiesTUIMode = iota
	cpModeEdit
	cpModeDetail
	cpModeConfirmNew // confirming creation of a new collective/category slug
)

type counterpartiesTUIModel struct {
	title     string
	kind      string
	direction string // "in" or "out", used when adding rules for cus_/0x
	rows      []counterpartyAgg
	cursor    int
	offset    int // top row currently visible (for scrolling)
	selected  map[int]bool
	mode      counterpartiesTUIMode

	collInput  textinput.Model
	catInput   textinput.Model
	focusField int // 0 = collective, 1 = category

	// Known slugs loaded from settings — drive the autocomplete hint
	// underneath each input and the create-confirm prompt.
	knownCollectives []string
	knownCategories  []string
	// Pending creation flags set when the operator presses enter on a
	// value that doesn't match any known slug. View renders a y/N prompt
	// based on these; updateConfirmNew consumes them.
	createCollective string
	createCategory   string

	status      string
	statusError bool
	width       int
	height      int
}

func (m counterpartiesTUIModel) Init() tea.Cmd { return nil }

func (m counterpartiesTUIModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		return m, nil
	case tea.KeyMsg:
		switch m.mode {
		case cpModeEdit:
			return m.updateEdit(msg)
		case cpModeDetail:
			return m.updateDetail(msg)
		case cpModeConfirmNew:
			return m.updateConfirmNew(msg)
		}
		return m.updateList(msg)
	}
	return m, nil
}

func (m counterpartiesTUIModel) updateList(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "q", "esc", "ctrl+c":
		return m, tea.Quit
	case "up", "k":
		if m.cursor > 0 {
			m.cursor--
		}
	case "down", "j":
		if m.cursor < len(m.rows)-1 {
			m.cursor++
		}
	case "pgup":
		m.cursor -= 10
		if m.cursor < 0 {
			m.cursor = 0
		}
	case "pgdown":
		m.cursor += 10
		if m.cursor >= len(m.rows) {
			m.cursor = len(m.rows) - 1
		}
	case "home", "g":
		m.cursor = 0
	case "end", "G":
		m.cursor = len(m.rows) - 1
	case " ":
		m.selected[m.cursor] = !m.selected[m.cursor]
		if !m.selected[m.cursor] {
			delete(m.selected, m.cursor)
		}
	case "a":
		// Toggle select-all: if all are selected, clear; else select all.
		if len(m.selected) == len(m.rows) {
			m.selected = map[int]bool{}
		} else {
			m.selected = map[int]bool{}
			for i := range m.rows {
				m.selected[i] = true
			}
		}
	case "c":
		m.selected = map[int]bool{}
	case "enter":
		// Open the per-counterparty detail modal for the row under the cursor.
		if m.cursor >= 0 && m.cursor < len(m.rows) {
			m.mode = cpModeDetail
			m.status = ""
			m.statusError = false
		}
		return m, nil
	case "e":
		if len(m.targets()) == 0 {
			m.status = "Nothing selected — press space to mark rows (or move cursor), then e to edit"
			m.statusError = true
			return m, nil
		}
		m.mode = cpModeEdit
		m.focusField = 0
		m.collInput.SetValue(m.commonCollective())
		m.catInput.SetValue(m.commonCategory())
		m.collInput.Focus()
		m.catInput.Blur()
		m.status = ""
		m.statusError = false
		return m, textinput.Blink
	}
	return m, nil
}

func (m counterpartiesTUIModel) updateDetail(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "esc", "q", "enter":
		m.mode = cpModeList
	case "e":
		// Quick-edit shortcut from the detail view — opens the edit
		// overlay for just this counterparty.
		m.selected = map[int]bool{m.cursor: true}
		m.mode = cpModeEdit
		m.focusField = 0
		m.collInput.SetValue(m.commonCollective())
		m.catInput.SetValue(m.commonCategory())
		m.collInput.Focus()
		m.catInput.Blur()
		return m, textinput.Blink
	case "up", "k":
		// Move cursor up and keep the detail view in sync.
		if m.cursor > 0 {
			m.cursor--
		}
	case "down", "j":
		if m.cursor < len(m.rows)-1 {
			m.cursor++
		}
	case "ctrl+c":
		return m, tea.Quit
	}
	return m, nil
}

func (m counterpartiesTUIModel) updateEdit(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "esc":
		m.mode = cpModeList
		m.status = "Edit cancelled."
		m.statusError = false
		return m, nil
	case "tab":
		// Soft autocomplete: if the focused field's text is a prefix of
		// exactly one known slug, complete it. Otherwise rotate focus
		// like before. Saves keystrokes when there's no ambiguity.
		focusedInput := &m.collInput
		known := m.knownCollectives
		if m.focusField == 1 {
			focusedInput = &m.catInput
			known = m.knownCategories
		}
		prefix := strings.ToLower(strings.TrimSpace(focusedInput.Value()))
		if prefix != "" {
			var matches []string
			for _, s := range known {
				if strings.HasPrefix(strings.ToLower(s), prefix) {
					matches = append(matches, s)
				}
			}
			if len(matches) == 1 {
				focusedInput.SetValue(matches[0])
				focusedInput.SetCursor(len(matches[0]))
				return m, nil
			}
		}
		m.focusField = 1 - m.focusField
		if m.focusField == 0 {
			m.collInput.Focus()
			m.catInput.Blur()
		} else {
			m.collInput.Blur()
			m.catInput.Focus()
		}
		return m, textinput.Blink
	case "shift+tab":
		m.focusField = 1 - m.focusField
		if m.focusField == 0 {
			m.collInput.Focus()
			m.catInput.Blur()
		} else {
			m.collInput.Blur()
			m.catInput.Focus()
		}
		return m, textinput.Blink
	case "enter":
		coll := strings.TrimSpace(m.collInput.Value())
		cat := strings.TrimSpace(m.catInput.Value())
		if coll == "" && cat == "" {
			m.status = "Set at least one of collective or category before pressing enter"
			m.statusError = true
			return m, nil
		}
		// Safety net: any unknown slug triggers a y/N confirm so the
		// operator can't typo their way into a new collective/category
		// silently. Confirmed slugs are persisted via AddCollective /
		// AddCategory in updateConfirmNew before applyRules runs.
		m.createCollective = ""
		m.createCategory = ""
		if coll != "" && !containsSlug(m.knownCollectives, coll) {
			m.createCollective = coll
		}
		if cat != "" && !containsSlug(m.knownCategories, cat) {
			m.createCategory = cat
		}
		if m.createCollective != "" || m.createCategory != "" {
			m.mode = cpModeConfirmNew
			m.status = ""
			m.statusError = false
			return m, nil
		}
		added, merged, skipped, err := m.applyRules(coll, cat)
		if err != nil {
			m.status = fmt.Sprintf("Failed: %v", err)
			m.statusError = true
			return m, nil
		}
		m.mode = cpModeList
		m.selected = map[int]bool{}
		m.status = fmt.Sprintf("✓ %d added, %d merged, %d skipped", added, merged, skipped)
		m.statusError = false
		return m, nil
	}
	var cmd tea.Cmd
	if m.focusField == 0 {
		m.collInput, cmd = m.collInput.Update(msg)
	} else {
		m.catInput, cmd = m.catInput.Update(msg)
	}
	return m, cmd
}

// updateConfirmNew handles the y/N prompt shown when the operator typed
// a collective or category slug that doesn't match any known one. On
// 'y' we persist the new slug(s) via AddCollective / AddCategory, then
// proceed with applyRules; on anything else we bounce back to edit so
// the operator can correct the typo.
func (m counterpartiesTUIModel) updateConfirmNew(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "y", "Y":
		if m.createCollective != "" {
			AddCollective(m.createCollective)
			m.knownCollectives = append(m.knownCollectives, m.createCollective)
			sort.Strings(m.knownCollectives)
		}
		if m.createCategory != "" {
			// Direction baked from the list kind: vendors → expense,
			// customers → income. Avoids the operator having to pick.
			catDir := "expense"
			if m.kind == "customers" {
				catDir = "income"
			}
			AddCategory(CategoryDef{Slug: m.createCategory, Label: m.createCategory, Direction: catDir})
			m.knownCategories = append(m.knownCategories, m.createCategory)
			sort.Strings(m.knownCategories)
		}
		coll := strings.TrimSpace(m.collInput.Value())
		cat := strings.TrimSpace(m.catInput.Value())
		added, merged, skipped, err := m.applyRules(coll, cat)
		if err != nil {
			m.status = fmt.Sprintf("Failed: %v", err)
			m.statusError = true
			m.mode = cpModeEdit
			return m, nil
		}
		m.mode = cpModeList
		m.selected = map[int]bool{}
		m.createCollective = ""
		m.createCategory = ""
		m.status = fmt.Sprintf("✓ %d added, %d merged, %d skipped", added, merged, skipped)
		m.statusError = false
		return m, nil
	case "esc", "n", "N":
		m.createCollective = ""
		m.createCategory = ""
		m.mode = cpModeEdit
		m.status = "Cancelled — fix the typo or tab/enter again to confirm."
		m.statusError = false
		return m, textinput.Blink
	}
	return m, nil
}

// targets returns the row indices the next [e] action will affect:
// every selected row, or the row under the cursor when no selection.
func (m counterpartiesTUIModel) targets() []int {
	if len(m.selected) == 0 {
		return []int{m.cursor}
	}
	out := make([]int, 0, len(m.selected))
	for i := range m.selected {
		out = append(out, i)
	}
	return out
}

// commonCollective returns the collective shared by every targeted row,
// or empty when they disagree. Used to pre-fill the edit form so the
// user doesn't have to retype a value they already had.
func (m counterpartiesTUIModel) commonCollective() string {
	return commonField(m.targets(), m.rows, func(a counterpartyAgg) string { return a.Collective })
}

func (m counterpartiesTUIModel) commonCategory() string {
	return commonField(m.targets(), m.rows, func(a counterpartyAgg) string { return a.Category })
}

// containsSlug reports whether `s` is a case-insensitive exact match of
// any slug in the known list.
func containsSlug(known []string, s string) bool {
	target := strings.ToLower(strings.TrimSpace(s))
	if target == "" {
		return false
	}
	for _, k := range known {
		if strings.EqualFold(k, target) {
			return true
		}
	}
	return false
}

// matchHint returns a "matches: a, b, c" suffix for the autocomplete
// row under each input. Shows nothing once an exact match exists (the
// value is unambiguous — no need for the operator to scan).
func matchHint(value string, known []string) string {
	prefix := strings.ToLower(strings.TrimSpace(value))
	if prefix == "" {
		return ""
	}
	for _, k := range known {
		if strings.EqualFold(k, prefix) {
			return "✓ known"
		}
	}
	var matches []string
	for _, k := range known {
		if strings.HasPrefix(strings.ToLower(k), prefix) {
			matches = append(matches, k)
		}
	}
	if len(matches) == 0 {
		return "(new — will prompt to create)"
	}
	limit := 5
	if len(matches) > limit {
		matches = append(matches[:limit], fmt.Sprintf("+%d more", len(matches)-limit))
	}
	return "matches: " + strings.Join(matches, ", ")
}

func commonField(idxs []int, rows []counterpartyAgg, pick func(counterpartyAgg) string) string {
	if len(idxs) == 0 {
		return ""
	}
	first := pick(rows[idxs[0]])
	for _, i := range idxs[1:] {
		if pick(rows[i]) != first {
			return ""
		}
	}
	return first
}

// applyRules builds a categorization rule for every target row and
// merges it into rules.json. Returns (added, merged, skipped, err)
// where `skipped` counts targets without a usable identifier.
func (m counterpartiesTUIModel) applyRules(collective, category string) (int, int, int, error) {
	rules, err := LoadRules()
	if err != nil {
		return 0, 0, 0, err
	}
	var added, merged, skipped int
	for _, idx := range m.targets() {
		row := m.rows[idx]
		identifier := displayCounterpartyIdentifier(row.URI)
		if identifier == "" {
			skipped++
			continue
		}
		rule, err := buildCounterpartyRule(identifier, m.direction, category, collective)
		if err != nil {
			skipped++
			continue
		}
		if existing := findRuleByMatch(rules, rule.Match); existing >= 0 {
			rules[existing].Assign = mergeRuleAssign(rules[existing].Assign, rule.Assign)
			merged++
		} else {
			rules = append(rules, rule)
			added++
		}
		// Reflect the change in the in-memory rows so the table updates
		// immediately without a reload.
		if collective != "" {
			m.rows[idx].Collective = collective
		}
		if category != "" {
			m.rows[idx].Category = category
		}
	}
	if err := SaveRules(rules); err != nil {
		return added, merged, skipped, err
	}
	return added, merged, skipped, nil
}

// ── Rendering ────────────────────────────────────────────────────────

var (
	cpTUIHeaderStyle = lipgloss.NewStyle().Bold(true)
	cpTUIDimStyle    = lipgloss.NewStyle().Faint(true)
	cpTUICursorStyle = lipgloss.NewStyle().Background(lipgloss.Color("236")).Foreground(lipgloss.Color("255"))
	cpTUIMarkStyle   = lipgloss.NewStyle().Foreground(lipgloss.Color("2")).Bold(true)
	cpTUIErrStyle    = lipgloss.NewStyle().Foreground(lipgloss.Color("1"))
	cpTUIOKStyle     = lipgloss.NewStyle().Foreground(lipgloss.Color("2"))
)

func (m counterpartiesTUIModel) View() string {
	var b strings.Builder
	b.WriteString(cpTUIHeaderStyle.Render(m.title))
	b.WriteString("\n")
	b.WriteString(cpTUIDimStyle.Render(fmt.Sprintf("%d total — %d selected", len(m.rows), len(m.selected))))
	b.WriteString("\n\n")

	// Scroll window based on terminal height with sensible defaults.
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
	if end > len(m.rows) {
		end = len(m.rows)
	}

	// Build all visible rows as PLAIN text (no ANSI codes) so column
	// widths can be computed correctly. Styling is applied per-cell
	// AFTER padding, never inside the cell content.
	const (
		nameCap = 30
		idCap   = 24
		slugCap = 14
	)
	headers := []string{"Sel", "Name", "Identifier", "# txs", "Volume", "Collective", "Category"}
	rightAlign := map[int]bool{3: true, 4: true}

	plainRows := make([][]string, 0, end-m.offset)
	for i := m.offset; i < end; i++ {
		row := m.rows[i]
		mark := "[ ]"
		if m.selected[i] {
			mark = "[×]"
		}
		name := row.Name
		ident := row.Identifier
		if name == "" {
			// No display name — promote identifier into the Name column.
			name = ident
			ident = ""
		}
		plainRows = append(plainRows, []string{
			mark,
			Truncate(name, nameCap),
			Truncate(ident, idCap),
			fmt.Sprintf("%d", row.TxCount),
			fmtEUR(row.Volume),
			Truncate(row.Collective, slugCap),
			Truncate(row.Category, slugCap),
		})
	}

	// Column widths: max of header + visible cells, capped.
	caps := []int{3, nameCap, idCap, 6, 14, slugCap, slugCap}
	widths := make([]int, len(headers))
	for i, h := range headers {
		widths[i] = displayWidth(h)
	}
	for _, r := range plainRows {
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

	renderRow := func(cells []string, isHeader bool, isCursor bool, isSelected bool) string {
		parts := make([]string, len(cells))
		for i, c := range cells {
			if rightAlign[i] {
				parts[i] = padLeft(c, widths[i])
			} else {
				parts[i] = padRight(c, widths[i])
			}
		}
		// Per-cell styling — applied after padding so widths stay correct.
		if !isHeader {
			if isSelected && !isCursor {
				parts[0] = cpTUIMarkStyle.Render(parts[0])
			}
			if !isCursor {
				parts[2] = cpTUIDimStyle.Render(parts[2]) // identifier subdued
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

	b.WriteString(renderRow(headers, true, false, false))
	b.WriteString("\n")

	for i, r := range plainRows {
		absIdx := m.offset + i
		b.WriteString(renderRow(r, false, absIdx == m.cursor, m.selected[absIdx]))
		b.WriteString("\n")
	}

	// Scroll indicator
	if m.offset > 0 || end < len(m.rows) {
		b.WriteString(cpTUIDimStyle.Render(fmt.Sprintf("\n  showing %d–%d of %d", m.offset+1, end, len(m.rows))))
		b.WriteString("\n")
	}

	// Status line
	if m.status != "" {
		b.WriteString("\n  ")
		if m.statusError {
			b.WriteString(cpTUIErrStyle.Render(m.status))
		} else {
			b.WriteString(cpTUIOKStyle.Render(m.status))
		}
		b.WriteString("\n")
	}

	switch m.mode {
	case cpModeEdit:
		b.WriteString("\n")
		b.WriteString(cpTUIHeaderStyle.Render(fmt.Sprintf("✎ Create rule for %d counterparty(ies)", len(m.targets()))))
		b.WriteString("\n  ")
		b.WriteString("Collective: ")
		b.WriteString(m.collInput.View())
		if hint := matchHint(m.collInput.Value(), m.knownCollectives); hint != "" {
			b.WriteString("  ")
			b.WriteString(cpTUIDimStyle.Render(hint))
		}
		b.WriteString("\n  ")
		b.WriteString("Category:   ")
		b.WriteString(m.catInput.View())
		if hint := matchHint(m.catInput.Value(), m.knownCategories); hint != "" {
			b.WriteString("  ")
			b.WriteString(cpTUIDimStyle.Render(hint))
		}
		b.WriteString("\n\n  ")
		b.WriteString(cpTUIDimStyle.Render("[tab] complete / switch field   [enter] apply   [esc] cancel"))
		b.WriteString("\n")
	case cpModeConfirmNew:
		b.WriteString("\n")
		b.WriteString(cpTUIHeaderStyle.Render("⚠ Create new entries?"))
		b.WriteString("\n")
		if m.createCollective != "" {
			b.WriteString("  ")
			b.WriteString(fmt.Sprintf("Collective %q is not in collectives.json\n", m.createCollective))
		}
		if m.createCategory != "" {
			b.WriteString("  ")
			b.WriteString(fmt.Sprintf("Category %q is not in categories.json\n", m.createCategory))
		}
		b.WriteString("\n  ")
		b.WriteString(cpTUIDimStyle.Render("Create + apply rule? [y] yes, persist + apply   [n/esc] back to edit"))
		b.WriteString("\n")
	case cpModeDetail:
		b.WriteString("\n")
		b.WriteString(m.renderDetail())
		b.WriteString("\n  ")
		b.WriteString(cpTUIDimStyle.Render("[↑/↓] navigate   [e] add rule   [esc/enter] close   [q] back"))
		b.WriteString("\n")
	default:
		b.WriteString("\n  ")
		b.WriteString(cpTUIDimStyle.Render("[↑/↓] move   [space] select   [enter] details   [e] edit   [a] all   [c] clear   [q] quit"))
		b.WriteString("\n")
	}

	return b.String()
}

// renderDetail builds the per-counterparty modal: header with name,
// canonical URI, totals, current rule assignment; plus a compact table
// of the latest transactions stashed during aggregation.
func (m counterpartiesTUIModel) renderDetail() string {
	if m.cursor < 0 || m.cursor >= len(m.rows) {
		return ""
	}
	row := m.rows[m.cursor]

	var b strings.Builder
	heading := row.Name
	if heading == "" {
		heading = row.Identifier
	}
	b.WriteString(cpTUIHeaderStyle.Render("▸ " + heading))
	b.WriteString("\n")

	writeKV := func(k, v string) {
		if v == "" {
			return
		}
		b.WriteString("  ")
		b.WriteString(cpTUIDimStyle.Render(padRight(k, 12)))
		b.WriteString(v)
		b.WriteString("\n")
	}
	writeKV("URI", row.URI)
	if row.Name != "" {
		writeKV("Identifier", row.Identifier)
	}
	writeKV("Tx count", fmt.Sprintf("%d", row.TxCount))
	writeKV("Volume", fmtEUR(row.Volume))
	if row.Collective != "" || row.Category != "" {
		writeKV("Rule", fmt.Sprintf("collective=%s, category=%s",
			defaultString(row.Collective, "—"), defaultString(row.Category, "—")))
	} else {
		writeKV("Rule", cpTUIDimStyle.Render("(none — press [e] to add)"))
	}

	if len(row.RecentTxs) == 0 {
		b.WriteString("\n  ")
		b.WriteString(cpTUIDimStyle.Render("(no recent transactions)"))
		return b.String()
	}

	b.WriteString("\n  ")
	cap := len(row.RecentTxs)
	b.WriteString(cpTUIHeaderStyle.Render(fmt.Sprintf("Latest %s", Pluralize(cap, "tx", ""))))
	if row.TxCount > cap {
		b.WriteString(cpTUIDimStyle.Render(fmt.Sprintf("  (of %d total)", row.TxCount)))
	}
	b.WriteString("\n")

	// Build a properly-aligned tx table the same way as the list: plain
	// text → widths → padded render. Cells:
	//   Date | Type | Amount | Currency | Description
	headers := []string{"Date", "Type", "Amount", "Cur", "Description"}
	rightAlign := map[int]bool{2: true}
	const descCap = 50

	plain := make([][]string, 0, len(row.RecentTxs))
	for _, tx := range row.RecentTxs {
		t := time.Unix(tx.Timestamp, 0).In(BrusselsTZ())
		amt := counterpartyTxAmount(tx)
		if tx.IsOutgoing() && amt > 0 {
			amt = -amt
		}
		desc := txDisplayDescription(tx)
		if desc == "" {
			desc = txDisplayCounterparty(tx)
		}
		plain = append(plain, []string{
			t.Format("2006-01-02"),
			tx.Type,
			fmtEURSigned(amt),
			tx.Currency,
			Truncate(desc, descCap),
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
	caps := []int{10, 8, 14, 6, descCap}
	for i := range widths {
		if widths[i] > caps[i] {
			widths[i] = caps[i]
		}
	}

	renderTxRow := func(cells []string, dim bool) string {
		parts := make([]string, len(cells))
		for i, c := range cells {
			if rightAlign[i] {
				parts[i] = padLeft(c, widths[i])
			} else {
				parts[i] = padRight(c, widths[i])
			}
		}
		line := "  " + strings.Join(parts, "  ")
		if dim {
			return cpTUIDimStyle.Render(line)
		}
		return line
	}

	b.WriteString(renderTxRow(headers, true))
	b.WriteString("\n")
	for _, r := range plain {
		b.WriteString(renderTxRow(r, false))
		b.WriteString("\n")
	}
	return b.String()
}

func defaultString(s, fallback string) string {
	if s == "" {
		return fallback
	}
	return s
}
