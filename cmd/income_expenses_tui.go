package cmd

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/bubbles/textinput"
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

	m := incomeTUIModel{
		direction:   direction,
		rangeLabel:  rangeLabel,
		accountSlug: accountSlug,
		cats:        cats,
		totalCount:  totalCount,
		totalAmount: totalAmount,
		collInput:   collInput,
		catInput:    catInput,
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
	incomeModeEdit
	incomeModeReconcile
)

type incomeTUIModel struct {
	direction   string // "income" or "expenses"
	rangeLabel  string // human range, e.g. "2025/Q1"
	accountSlug string // empty for "all accounts"
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

	// Edit overlay state — reused across reopens. Pre-filled with the
	// cursor tx's current (collective, category) when [e] is pressed.
	collInput  textinput.Model
	catInput   textinput.Model
	focusField int // 0 = collective, 1 = category

	// Reconcile picker state — populated when [r] is pressed in drill
	// mode. Holds the resolved Odoo bank statement line + journal +
	// suggester output (unreconciled first; broadens to already-paid
	// invoices/bills when no open match exists). The cursor here is
	// a local index into reconcileCands; the outer m.cursor stays
	// pointed at the underlying tx so we can return to it on Esc.
	reconcileLine            OdooCacheLine
	reconcileJournal         int
	reconcileCands           []Suggestion
	reconcileCursor          int
	reconcileConfirmReattach bool // set when [enter] is pressed on an AlreadyAttached candidate; [y] confirms

	status      string
	statusError bool

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
		case incomeModeEdit:
			return m.updateEdit(msg)
		case incomeModeReconcile:
			return m.updateReconcile(msg)
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
		m.status = ""
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
	case "e":
		if m.cursor < 0 || m.cursor >= len(txs) {
			return m, nil
		}
		t := txs[m.cursor]
		if t.URI == "" {
			m.status = "Cannot edit: tx has no URI."
			m.statusError = true
			return m, nil
		}
		m.mode = incomeModeEdit
		m.focusField = 0
		m.collInput.SetValue(t.Collective)
		m.catInput.SetValue(t.Category)
		m.collInput.Focus()
		m.catInput.Blur()
		m.status = ""
		m.statusError = false
		return m, textinput.Blink
	case "r":
		if m.cursor < 0 || m.cursor >= len(txs) {
			return m, nil
		}
		t := txs[m.cursor]
		if t.ImportID == "" {
			m.status = "Cannot reconcile: tx has no Odoo import id (account not configured?)."
			m.statusError = true
			return m, nil
		}
		line, jid, ok := findOdooLineForTx(t.ImportID)
		if !ok {
			m.status = "Tx not found in any local journal cache — run `chb pull` first."
			m.statusError = true
			return m, nil
		}
		if line.IsReconciled {
			m.status = fmt.Sprintf("Tx is already reconciled on Odoo (journal #%d, line #%d).", jid, line.ID)
			m.statusError = false
			return m, nil
		}
		cands := SuggestForTx(t)
		if len(cands) == 0 {
			noun := "invoice"
			if t.SignedAmount < 0 {
				noun = "bill"
			}
			m.status = fmt.Sprintf("No %s candidates with amount %s — checked open AND paid.",
				noun, fmtAmountCurrency(t.Amount, t.Currency))
			m.statusError = false
			return m, nil
		}
		m.mode = incomeModeReconcile
		m.reconcileLine = line
		m.reconcileJournal = jid
		m.reconcileCands = cands
		m.reconcileCursor = FirstUnattachedIndex(cands)
		m.reconcileConfirmReattach = false
		m.status = ""
		m.statusError = false
		return m, nil
	}
	return m, nil
}

func (m incomeTUIModel) updateReconcile(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "esc", "q":
		m.mode = incomeModeDrill
		m.reconcileCands = nil
		m.reconcileCursor = 0
		m.reconcileConfirmReattach = false
		return m, nil
	case "ctrl+c":
		return m, tea.Quit
	case "up", "k":
		m.reconcileConfirmReattach = false
		if m.reconcileCursor > 0 {
			m.reconcileCursor--
		}
		return m, nil
	case "down", "j":
		m.reconcileConfirmReattach = false
		if m.reconcileCursor < len(m.reconcileCands)-1 {
			m.reconcileCursor++
		}
		return m, nil
	case "enter":
		if m.reconcileCursor < 0 || m.reconcileCursor >= len(m.reconcileCands) {
			return m, nil
		}
		sugg := m.reconcileCands[m.reconcileCursor]
		// Picking a paid candidate triggers unreconcile + reattach
		// inside reconcileStatementLineWithMove. Confirm explicitly
		// so a reflex Enter doesn't override a previous decision.
		if sugg.AlreadyAttached && !m.reconcileConfirmReattach {
			m.reconcileConfirmReattach = true
			m.status = fmt.Sprintf("↻ %s %s is already %s. Press [y] to UNRECONCILE its existing match and reattach this tx, or [esc] to back out.",
				sugg.Move.Kind, sugg.Move.label(),
				defaultString(sugg.PaymentState, "settled"))
			m.statusError = false
			return m, nil
		}
		if err := attachTxToInvoiceCandidate(m.reconcileLine, m.reconcileJournal, sugg.Move); err != nil {
			m.status = fmt.Sprintf("Attach failed: %v", err)
			m.statusError = true
			m.reconcileConfirmReattach = false
			return m, nil
		}
		verb := "Reconciled with"
		if sugg.AlreadyAttached {
			verb = "Re-reconciled (unattached + reattached) with"
		}
		m.status = fmt.Sprintf("✓ %s %s %s (%s)",
			verb, sugg.Move.Kind, sugg.Move.label(), sugg.Move.Date)
		m.statusError = false
		m.mode = incomeModeDrill
		m.reconcileCands = nil
		m.reconcileCursor = 0
		m.reconcileConfirmReattach = false
		return m, nil
	case "y", "Y":
		if !m.reconcileConfirmReattach ||
			m.reconcileCursor < 0 || m.reconcileCursor >= len(m.reconcileCands) {
			return m, nil
		}
		sugg := m.reconcileCands[m.reconcileCursor]
		if err := attachTxToInvoiceCandidate(m.reconcileLine, m.reconcileJournal, sugg.Move); err != nil {
			m.status = fmt.Sprintf("Reattach failed: %v", err)
			m.statusError = true
			m.reconcileConfirmReattach = false
			return m, nil
		}
		m.status = fmt.Sprintf("✓ Re-reconciled (unattached + reattached) with %s %s (%s)",
			sugg.Move.Kind, sugg.Move.label(), sugg.Move.Date)
		m.statusError = false
		m.mode = incomeModeDrill
		m.reconcileCands = nil
		m.reconcileCursor = 0
		m.reconcileConfirmReattach = false
		return m, nil
	}
	return m, nil
}

func (m incomeTUIModel) updateEdit(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "esc":
		m.mode = incomeModeDrill
		m.status = "Edit cancelled."
		m.statusError = false
		return m, nil
	case "ctrl+c":
		return m, tea.Quit
	case "tab", "shift+tab":
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
		txs := m.cats[m.drillIdx].Txs
		t := &txs[m.cursor]
		if err := WriteNostrAnnotation(t.URI, t.Date, cat, coll); err != nil {
			m.status = fmt.Sprintf("Failed: %v", err)
			m.statusError = true
			return m, nil
		}
		if coll != "" {
			t.Collective = coll
		}
		if cat != "" {
			t.Category = cat
		}
		m.mode = incomeModeDrill
		m.status = fmt.Sprintf("✓ Annotation written (collective=%s, category=%s). Run `chb generate` to re-bucket.",
			defaultString(t.Collective, "—"), defaultString(t.Category, "—"))
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

func (m incomeTUIModel) View() string {
	switch m.mode {
	case incomeModeReconcile:
		return m.viewReconcile()
	case incomeModeDrill, incomeModeEdit:
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

	headers := []string{"Date", "Counterparty", "Description", "Account", "Amount"}
	rightAlign := map[int]bool{4: true}
	caps := []int{10, 28, 32, 12, 14}

	plain := make([][]string, 0, end-m.offset)
	for i := m.offset; i < end; i++ {
		t := cat.Txs[i]
		plain = append(plain, []string{
			t.Date,
			Truncate(t.Counterparty, 28),
			Truncate(t.Description, 32),
			Truncate(t.AccountSlug, 12),
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
		header := firstNonEmptyStr(t.Counterparty, t.Description, t.URI)
		b.WriteString(cpTUIHeaderStyle.Render("▸ " + header))
		b.WriteString("\n")
		writeKV := func(k, v string) {
			b.WriteString("  ")
			b.WriteString(cpTUIDimStyle.Render(padRight(k, 14)))
			b.WriteString(v)
			b.WriteString("\n")
		}
		if t.Counterparty != "" {
			writeKV("Counterparty", t.Counterparty)
		}
		if t.Description != "" {
			writeKV("Description", t.Description)
		}
		writeKV("Collective", defaultString(t.Collective, "—"))
		writeKV("Category", defaultString(t.Category, "—"))
		if t.URI != "" {
			writeKV("URI", t.URI)
		}
	}

	if m.status != "" {
		b.WriteString("\n  ")
		if m.statusError {
			b.WriteString(cpTUIErrStyle.Render(m.status))
		} else {
			b.WriteString(cpTUIOKStyle.Render(m.status))
		}
		b.WriteString("\n")
	}

	if m.mode == incomeModeEdit {
		b.WriteString("\n")
		b.WriteString(cpTUIHeaderStyle.Render("✎ Edit collective / category for this tx"))
		b.WriteString("\n  ")
		b.WriteString("Collective: ")
		b.WriteString(m.collInput.View())
		b.WriteString("\n  ")
		b.WriteString("Category:   ")
		b.WriteString(m.catInput.View())
		b.WriteString("\n\n  ")
		b.WriteString(cpTUIDimStyle.Render("[tab] switch field   [enter] apply (writes Nostr annotation)   [esc] cancel"))
		b.WriteString("\n")
	} else {
		b.WriteString("\n  ")
		b.WriteString(cpTUIDimStyle.Render("[↑/↓] navigate   [e] edit collective/category   [r] reconcile with invoice/bill   [esc/q] back"))
		b.WriteString("\n")
	}
	return b.String()
}

func (m incomeTUIModel) viewReconcile() string {
	var b strings.Builder
	txs := m.cats[m.drillIdx].Txs
	t := txs[m.cursor]
	b.WriteString(cpTUIHeaderStyle.Render(fmt.Sprintf("⇄ Reconcile tx with invoice/bill — %s — %s",
		t.Date, fmtAmountCurrency(t.Amount, t.Currency))))
	b.WriteString("\n")
	partnerHits, attachedHits := 0, 0
	for _, c := range m.reconcileCands {
		if c.PartnerMatch {
			partnerHits++
		}
		if c.AlreadyAttached {
			attachedHits++
		}
	}
	subtitle := fmt.Sprintf("  %d candidate(s) — %d partner-match, then by date proximity",
		len(m.reconcileCands), partnerHits)
	if attachedHits > 0 {
		subtitle += fmt.Sprintf("  ·  %d already paid (pick to unreconcile+reattach)", attachedHits)
	}
	b.WriteString(cpTUIDimStyle.Render(subtitle))
	b.WriteString("\n\n")

	headers := []string{"Sel", "Status", "Partner", "Date", "Δ", "Number", "Residual", "First line"}
	rightAlign := map[int]bool{6: true}
	caps := []int{3, 6, 8, 10, 6, 22, 14, 36}

	plain := make([][]string, 0, len(m.reconcileCands))
	for i, c := range m.reconcileCands {
		mark := " "
		if i == m.reconcileCursor {
			mark = "▸"
		}
		status := ""
		if c.AlreadyAttached {
			status = defaultString(c.PaymentState, "paid")
		}
		match := ""
		if c.PartnerMatch {
			match = "match"
		}
		delta := dateDeltaLabel(c.Move.Date, t.Date)
		// Display amount = residual when open, signed total when settled
		// (residual goes to 0 once paid).
		amt := c.Move.Residual
		if amt == 0 {
			amt = c.Move.SignedTotal
		}
		plain = append(plain, []string{
			mark,
			status,
			match,
			c.Move.Date,
			delta,
			Truncate(c.Move.label(), 22),
			fmtAmountCurrency(amt, "EUR"),
			Truncate(c.Move.FirstLineItem, 36),
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
		b.WriteString(renderRow(r, false, i == m.reconcileCursor))
		b.WriteString("\n")
	}

	// Detail panel for the cursor candidate.
	if m.reconcileCursor >= 0 && m.reconcileCursor < len(m.reconcileCands) {
		c := m.reconcileCands[m.reconcileCursor].Move
		b.WriteString("\n  ")
		b.WriteString(cpTUIHeaderStyle.Render("▸ " + firstNonEmptyStr(c.PartnerName, c.Number)))
		b.WriteString("\n")
		writeKV := func(k, v string) {
			if v == "" {
				return
			}
			b.WriteString("  ")
			b.WriteString(cpTUIDimStyle.Render(padRight(k, 14)))
			b.WriteString(v)
			b.WriteString("\n")
		}
		writeKV("Kind", c.Kind)
		writeKV("Number", c.Number)
		writeKV("Date", c.Date)
		writeKV("Residual", fmtAmountCurrency(c.Residual, "EUR"))
		writeKV("Partner", c.PartnerName)
		writeKV("First line", c.FirstLineItem)
	}

	b.WriteString("\n  ")
	b.WriteString(cpTUIDimStyle.Render("[↑/↓] pick   [enter] attach (writes to Odoo)   [esc] back"))
	b.WriteString("\n")
	return b.String()
}
