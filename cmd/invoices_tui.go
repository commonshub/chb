package cmd

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
)

// runMovesTUI is the interactive viewer for `chb invoices -i` / `chb
// bills -i`. Navigation, drill-in detail with line items + linked txs,
// and an edit overlay that stamps category/collective directly on the
// move record (and propagates the same to each reconciled tx via a
// local Nostr annotation entry — no rule indirection).
func runMovesTUI(kind moveKind, scope string, rows []moveRow) {
	if len(rows) == 0 {
		fmt.Printf("\n%sNo %s to display.%s\n\n", Fmt.Dim, kind.labelPl, Fmt.Reset)
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

	m := movesTUIModel{
		title:     moveListTitle(kind, scope),
		kind:      kind,
		rows:      rows,
		selected:  map[int]bool{},
		collInput: collInput,
		catInput:  catInput,
	}

	p := tea.NewProgram(m, tea.WithAltScreen())
	if _, err := p.Run(); err != nil {
		fmt.Printf("%sError: %v%s\n", Fmt.Red, err, Fmt.Reset)
	}
}

type movesTUIMode int

const (
	movesModeList movesTUIMode = iota
	movesModeEdit
	movesModeDetail
	movesModeAttach
)

type movesTUIModel struct {
	title    string
	kind     moveKind
	rows     []moveRow
	cursor   int
	offset   int
	selected map[int]bool
	mode     movesTUIMode

	collInput  textinput.Model
	catInput   textinput.Model
	focusField int

	// Attach-payment picker state. Loaded lazily on `r` from the detail
	// mode; reset when the detail view closes. Holds the two-tier
	// suggester output: unreconciled-first, then AlreadyAttached
	// (paid / reconciled) candidates the operator can pick to
	// trigger unreconcile-and-reattach.
	attachCands           []Suggestion
	attachCursor          int
	attachConfirmReattach bool // set when the operator presses [enter] on an AlreadyAttached candidate; [y] then applies

	status      string
	statusError bool
	width       int
	height      int
}

func (m movesTUIModel) Init() tea.Cmd { return nil }

func (m movesTUIModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		return m, nil
	case tea.KeyMsg:
		switch m.mode {
		case movesModeEdit:
			return m.updateEdit(msg)
		case movesModeDetail:
			return m.updateDetail(msg)
		case movesModeAttach:
			return m.updateAttach(msg)
		}
		return m.updateList(msg)
	}
	return m, nil
}

func (m movesTUIModel) updateList(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
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
		if m.cursor >= 0 && m.cursor < len(m.rows) {
			m.mode = movesModeDetail
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
		m.mode = movesModeEdit
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

func (m movesTUIModel) updateDetail(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "esc", "q":
		m.mode = movesModeList
		m.attachCands = nil
		m.attachCursor = 0
	case "enter":
		m.mode = movesModeList
		m.attachCands = nil
		m.attachCursor = 0
	case "e":
		m.selected = map[int]bool{m.cursor: true}
		m.mode = movesModeEdit
		m.focusField = 0
		m.collInput.SetValue(m.commonCollective())
		m.catInput.SetValue(m.commonCategory())
		m.collInput.Focus()
		m.catInput.Blur()
		return m, textinput.Blink
	case "r":
		// Open the attach-payment picker. Always recomputes candidates
		// so a fresh `chb pull` between sessions is reflected.
		if m.cursor < 0 || m.cursor >= len(m.rows) {
			return m, nil
		}
		row := m.rows[m.cursor]
		m.attachCands = SuggestForMove(row, m.kind)
		m.attachCursor = FirstUnattachedIndex(m.attachCands)
		if len(m.attachCands) == 0 {
			m.status = fmt.Sprintf("No matching bank lines found for %s (amount %s) — checked unreconciled AND reconciled.",
				kindLabelN(m.kind, 1),
				fmtAmountCurrency(row.Move.TotalAmount, row.Move.Currency))
			m.statusError = false
			return m, nil
		}
		m.mode = movesModeAttach
		m.status = ""
		m.statusError = false
		return m, nil
	case "up", "k":
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

func (m movesTUIModel) updateAttach(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "esc", "q":
		m.mode = movesModeDetail
		m.attachCands = nil
		m.attachCursor = 0
		m.attachConfirmReattach = false
		return m, nil
	case "ctrl+c":
		return m, tea.Quit
	case "up", "k":
		// Cursor movement cancels a pending reattach confirm.
		m.attachConfirmReattach = false
		if m.attachCursor > 0 {
			m.attachCursor--
		}
		return m, nil
	case "down", "j":
		m.attachConfirmReattach = false
		if m.attachCursor < len(m.attachCands)-1 {
			m.attachCursor++
		}
		return m, nil
	case "enter":
		if m.attachCursor < 0 || m.attachCursor >= len(m.attachCands) {
			return m, nil
		}
		row := &m.rows[m.cursor]
		cand := m.attachCands[m.attachCursor]
		// For AlreadyAttached bank lines, picking one triggers
		// unreconcile + reattach inside reconcileStatementLineWithMove.
		// Pause for an explicit "yes" via `y` so the operator can't
		// override a previous reconciliation by reflex.
		if cand.AlreadyAttached && !m.attachConfirmReattach {
			m.attachConfirmReattach = true
			m.status = fmt.Sprintf("↻ Line #%d is already matched to another invoice/bill. Press [y] to DETACH that match and re-attach to %s #%d, or [esc] to back out.",
				cand.Line.ID, m.kind.label, row.Move.ID)
			m.statusError = false
			return m, nil
		}
		if err := applyAttachPayment(row, cand); err != nil {
			m.status = fmt.Sprintf("Attach failed: %v", err)
			m.statusError = true
			m.attachConfirmReattach = false
			return m, nil
		}
		verb := "Attached"
		if cand.AlreadyAttached {
			verb = "Reattached"
		}
		m.status = fmt.Sprintf("✓ %s line #%d (%s, %s) to %s #%d",
			verb, cand.Line.ID, cand.JournalName, cand.Line.Date, m.kind.label, row.Move.ID)
		m.statusError = false
		m.mode = movesModeDetail
		m.attachCands = nil
		m.attachCursor = 0
		m.attachConfirmReattach = false
		return m, nil
	case "y", "Y":
		// Only meaningful as the confirm response to a reattach
		// prompt. Treat any other context as a no-op.
		if m.attachConfirmReattach && m.attachCursor >= 0 && m.attachCursor < len(m.attachCands) {
			row := &m.rows[m.cursor]
			cand := m.attachCands[m.attachCursor]
			if err := applyAttachPayment(row, cand); err != nil {
				m.status = fmt.Sprintf("Reattach failed: %v", err)
				m.statusError = true
				m.attachConfirmReattach = false
				return m, nil
			}
			m.status = fmt.Sprintf("✓ Reattached line #%d (%s, %s) to %s #%d",
				cand.Line.ID, cand.JournalName, cand.Line.Date, m.kind.label, row.Move.ID)
			m.statusError = false
			m.mode = movesModeDetail
			m.attachCands = nil
			m.attachCursor = 0
			m.attachConfirmReattach = false
		}
		return m, nil
	}
	return m, nil
}

// applyAttachPayment resolves Odoo creds, authenticates, and calls
// attachMoveToBankLine via the existing invoice-side write path.
// Works for both fresh attaches and reattaches — when the bank line
// is already reconciled, the chain inside reconcileStatementLineWithMove
// detects the case and unreconciles before relinking.
func applyAttachPayment(row *moveRow, sugg Suggestion) error {
	creds, err := ResolveOdooCredentials()
	if err != nil {
		return err
	}
	uid, err := odooAuth(creds.URL, creds.DB, creds.Login, creds.Password)
	if err != nil || uid == 0 {
		return fmt.Errorf("Odoo authentication failed: %v", err)
	}
	return attachMoveToBankLine(creds, uid, row, sugg)
}

func (m movesTUIModel) updateEdit(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "esc":
		m.mode = movesModeList
		m.status = "Edit cancelled."
		m.statusError = false
		return m, nil
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
		moves, txs, err := m.applyEdit(coll, cat)
		if err != nil {
			m.status = fmt.Sprintf("Failed: %v", err)
			m.statusError = true
			return m, nil
		}
		m.mode = movesModeList
		m.selected = map[int]bool{}
		m.status = fmt.Sprintf("✓ %d %s updated, %d linked tx annotation(s) written", moves, kindLabelN(m.kind, moves), txs)
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

func kindLabelN(kind moveKind, n int) string {
	if n == 1 {
		return kind.label
	}
	return kind.labelPl
}

func (m movesTUIModel) targets() []int {
	if len(m.selected) == 0 {
		return []int{m.cursor}
	}
	out := make([]int, 0, len(m.selected))
	for i := range m.selected {
		out = append(out, i)
	}
	return out
}

func (m movesTUIModel) commonCollective() string {
	idxs := m.targets()
	if len(idxs) == 0 {
		return ""
	}
	first := m.rows[idxs[0]].Move.Collective
	for _, i := range idxs[1:] {
		if m.rows[i].Move.Collective != first {
			return ""
		}
	}
	return first
}

func (m movesTUIModel) commonCategory() string {
	idxs := m.targets()
	if len(idxs) == 0 {
		return ""
	}
	first := m.rows[idxs[0]].Move.Category
	for _, i := range idxs[1:] {
		if m.rows[i].Move.Category != first {
			return ""
		}
	}
	return first
}

// applyEdit walks every target row, saves the new category/collective
// on the move record, and propagates the same to each linked tx via a
// local Nostr annotation entry. Updates the in-memory rows so the table
// reflects the change immediately.
func (m movesTUIModel) applyEdit(collective, category string) (movesUpdated, txsAnnotated int, err error) {
	for _, idx := range m.targets() {
		row := &m.rows[idx]
		mu, tu, e := saveMoveRowAnnotation(row, m.kind, category, collective)
		if e != nil {
			return movesUpdated, txsAnnotated, e
		}
		movesUpdated += mu
		txsAnnotated += tu
	}
	return movesUpdated, txsAnnotated, nil
}

// ── Rendering ────────────────────────────────────────────────────────

func (m movesTUIModel) View() string {
	var b strings.Builder
	b.WriteString(cpTUIHeaderStyle.Render(m.title))
	b.WriteString("\n")
	b.WriteString(cpTUIDimStyle.Render(fmt.Sprintf("%d total — %d selected", len(m.rows), len(m.selected))))
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
	if end > len(m.rows) {
		end = len(m.rows)
	}

	const (
		partnerCap = 24
		refCap     = 20
		descCap    = 28
		slugCap    = 12
	)
	headers := []string{"Sel", "Date", partnerColumnLabel(m.kind), "Reference", "Description", "Gross", "VAT", "Net", "Paid", "Collective", "Category"}
	rightAlign := map[int]bool{5: true, 6: true, 7: true, 8: true}

	plain := make([][]string, 0, end-m.offset)
	for i := m.offset; i < end; i++ {
		r := m.rows[i]
		mark := "[ ]"
		if m.selected[i] {
			mark = "[×]"
		}
		cur := r.Move.Currency
		plain = append(plain, []string{
			mark,
			r.Move.Date,
			Truncate(r.Partner, partnerCap),
			Truncate(moveReference(r.Move), refCap),
			Truncate(moveFirstLineItem(r.Move), descCap),
			fmtAmountCurrency(r.Move.TotalAmount, cur),
			fmtAmountCurrency(r.Move.VATAmount, cur),
			fmtAmountCurrency(r.Move.UntaxedAmount, cur),
			movePaidCell(r.Move),
			Truncate(r.Move.Collective, slugCap),
			Truncate(r.Move.Category, slugCap),
		})
	}

	caps := []int{3, 10, partnerCap, refCap, descCap, 12, 12, 12, 4, slugCap, slugCap}
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

	renderRow := func(cells []string, isHeader, isCursor, isSelected bool) string {
		parts := make([]string, len(cells))
		for i, c := range cells {
			if rightAlign[i] {
				parts[i] = padLeft(c, widths[i])
			} else {
				parts[i] = padRight(c, widths[i])
			}
		}
		if !isHeader && isSelected && !isCursor {
			parts[0] = cpTUIMarkStyle.Render(parts[0])
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
	for i, r := range plain {
		abs := m.offset + i
		b.WriteString(renderRow(r, false, abs == m.cursor, m.selected[abs]))
		b.WriteString("\n")
	}
	if m.offset > 0 || end < len(m.rows) {
		b.WriteString(cpTUIDimStyle.Render(fmt.Sprintf("\n  showing %d–%d of %d", m.offset+1, end, len(m.rows))))
		b.WriteString("\n")
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

	switch m.mode {
	case movesModeEdit:
		b.WriteString("\n")
		b.WriteString(cpTUIHeaderStyle.Render(fmt.Sprintf("✎ Set collective/category on %d %s",
			len(m.targets()), kindLabelN(m.kind, len(m.targets())))))
		b.WriteString("\n  ")
		b.WriteString("Collective: ")
		b.WriteString(m.collInput.View())
		b.WriteString("\n  ")
		b.WriteString("Category:   ")
		b.WriteString(m.catInput.View())
		b.WriteString("\n\n  ")
		b.WriteString(cpTUIDimStyle.Render("[tab] switch field   [enter] apply (writes JSON + tx annotations)   [esc] cancel"))
		b.WriteString("\n")
	case movesModeDetail:
		b.WriteString("\n")
		b.WriteString(m.renderDetail())
		b.WriteString("\n  ")
		hint := "[↑/↓] navigate   [e] edit   [r] attach payment   [esc/enter] close   [q] back"
		if m.cursor >= 0 && m.cursor < len(m.rows) {
			rec := m.rows[m.cursor].Move.ReconciledTransaction
			if rec != nil && rec.ID != "" {
				hint = "[↑/↓] navigate   [e] edit   [esc/enter] close   [q] back"
			}
		}
		b.WriteString(cpTUIDimStyle.Render(hint))
		b.WriteString("\n")
	case movesModeAttach:
		b.WriteString("\n")
		b.WriteString(m.renderAttach())
		b.WriteString("\n  ")
		b.WriteString(cpTUIDimStyle.Render("[↑/↓] pick   [enter] attach (writes to Odoo)   [esc] back"))
		b.WriteString("\n")
	default:
		b.WriteString("\n  ")
		b.WriteString(cpTUIDimStyle.Render("[↑/↓] move   [space] select   [enter] details   [e] edit   [a] all   [c] clear   [q] quit"))
		b.WriteString("\n")
	}
	return b.String()
}

// renderAttach prints the candidate-payment picker for the currently
// focussed row. Always reached from detail mode, so the moveRow at
// m.cursor is the target.
func (m movesTUIModel) renderAttach() string {
	if m.cursor < 0 || m.cursor >= len(m.rows) {
		return ""
	}
	row := m.rows[m.cursor]
	mv := row.Move

	var b strings.Builder
	b.WriteString(cpTUIHeaderStyle.Render(fmt.Sprintf("⇄ Attach payment to %s #%d (%s — %s)",
		m.kind.label, mv.ID, mv.Date,
		fmtAmountCurrency(mv.TotalAmount, mv.Currency))))
	b.WriteString("\n")
	partnerHits, attachedHits := 0, 0
	for _, c := range m.attachCands {
		if c.PartnerMatch {
			partnerHits++
		}
		if c.AlreadyAttached {
			attachedHits++
		}
	}
	subtitle := fmt.Sprintf("  %d candidate(s) — %d partner-match, then by date proximity",
		len(m.attachCands), partnerHits)
	if attachedHits > 0 {
		subtitle += fmt.Sprintf("  ·  %d matched to another invoice/bill (pick to detach+re-attach)", attachedHits)
	}
	b.WriteString(cpTUIDimStyle.Render(subtitle))
	b.WriteString("\n\n")

	headers := []string{"Sel", "Status", "Partner", "Date", "Δ", "Amount", "Journal", "Description", "Ref / Counterparty"}
	rightAlign := map[int]bool{5: true}
	caps := []int{3, 6, 8, 10, 6, 14, 14, 36, 30}

	plain := make([][]string, 0, len(m.attachCands))
	for i, c := range m.attachCands {
		mark := " "
		if i == m.attachCursor {
			mark = "▸"
		}
		desc := strings.TrimSpace(c.Line.PaymentRef)
		if desc == "" {
			desc = strings.ReplaceAll(strings.TrimSpace(c.Line.Narration), "\n", " ")
		}
		delta := dateDeltaLabel(c.Line.Date, mv.Date)
		journal := c.JournalName
		if journal == "" {
			journal = fmt.Sprintf("#%d", c.JournalID)
		}
		stableID := c.Line.UniqueImportID
		if stableID == "" {
			stableID = fmt.Sprintf("line #%d", c.Line.ID)
		}
		status := ""
		if c.AlreadyAttached {
			status = "paid"
		}
		partnerBadge := ""
		if c.PartnerMatch {
			partnerBadge = "match"
		}
		plain = append(plain, []string{
			mark,
			status,
			partnerBadge,
			c.Line.Date,
			delta,
			fmtAmountCurrency(c.Line.Amount, "EUR"),
			Truncate(journal, 14),
			Truncate(desc, 36),
			Truncate(stableID, 30),
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
		b.WriteString(renderRow(r, false, i == m.attachCursor))
		b.WriteString("\n")
	}
	return b.String()
}

// dateDeltaLabel returns a short "+Nd" / "-Nd" / "0d" string for the
// candidate date relative to the invoice date. "?" when either side
// is unparseable. The sign convention is candidate − invoice: positive
// means the payment landed after the invoice was issued, negative
// means it preceded the invoice.
func dateDeltaLabel(candidateDate, moveDate string) string {
	ta, err1 := parseOdooDate(candidateDate)
	tb, err2 := parseOdooDate(moveDate)
	if err1 != nil || err2 != nil {
		return "?"
	}
	days := int(ta.Sub(tb).Hours() / 24)
	switch {
	case days > 0:
		return fmt.Sprintf("+%dd", days)
	case days < 0:
		return fmt.Sprintf("%dd", days)
	}
	return "0d"
}

func (m movesTUIModel) renderDetail() string {
	if m.cursor < 0 || m.cursor >= len(m.rows) {
		return ""
	}
	row := m.rows[m.cursor]
	mv := row.Move

	var b strings.Builder
	b.WriteString(cpTUIHeaderStyle.Render("▸ " + firstNonEmptyStr(mv.Title, fmt.Sprintf("#%d", mv.ID))))
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
	writeKV(partnerColumnLabel(m.kind), row.Partner)
	writeKV("Date", mv.Date)
	if mv.Journal.Name != "" {
		writeKV("Journal", fmt.Sprintf("#%d %s", mv.Journal.ID, mv.Journal.Name))
	}
	writeKV("State", strings.TrimSpace(mv.State+" / "+mv.PaymentState))
	cur := strings.ToUpper(firstNonEmptyStr(mv.Currency, "EUR"))
	writeKV("Net", fmt.Sprintf("%s %s", fmtEUR(mv.UntaxedAmount), cur))
	writeKV("VAT", fmt.Sprintf("%s %s", fmtEUR(mv.VATAmount), cur))
	writeKV("Gross", fmt.Sprintf("%s %s", fmtEUR(mv.TotalAmount), cur))
	rule := fmt.Sprintf("collective=%s, category=%s",
		defaultString(mv.Collective, "—"), defaultString(mv.Category, "—"))
	if mv.Collective == "" && mv.Category == "" {
		rule = cpTUIDimStyle.Render("(none — press [e] to set)")
	}
	writeKV("Tag", rule)

	// Line items
	if len(mv.LineItems) > 0 {
		b.WriteString("\n  ")
		b.WriteString(cpTUIHeaderStyle.Render(fmt.Sprintf("Line items (%d)", len(mv.LineItems))))
		b.WriteString("\n")
		liHeaders := []string{"Qty", "Description", "Unit", "Subtotal", "Taxes", "Total"}
		liRight := map[int]bool{0: true, 2: true, 3: true, 5: true}
		const liDescCap = 44
		liPlain := make([][]string, 0, len(mv.LineItems))
		for _, li := range mv.LineItems {
			taxes := []string{}
			for _, t := range li.Taxes {
				taxes = append(taxes, t.Name)
			}
			liPlain = append(liPlain, []string{
				fmt.Sprintf("%g", li.Quantity),
				Truncate(firstNonEmptyStr(li.Title, li.ProductName), liDescCap),
				fmtEUR(li.UnitPrice),
				fmtEUR(li.SubtotalAmount),
				strings.Join(taxes, ", "),
				fmtEUR(li.TotalAmount),
			})
		}
		liCaps := []int{6, liDescCap, 12, 14, 12, 14}
		liWidths := make([]int, len(liHeaders))
		for i, h := range liHeaders {
			liWidths[i] = displayWidth(h)
		}
		for _, r := range liPlain {
			for i, c := range r {
				if w := displayWidth(c); w > liWidths[i] {
					liWidths[i] = w
				}
			}
		}
		for i := range liWidths {
			if liWidths[i] > liCaps[i] {
				liWidths[i] = liCaps[i]
			}
		}
		render := func(cells []string, dim bool) string {
			parts := make([]string, len(cells))
			for i, c := range cells {
				if liRight[i] {
					parts[i] = padLeft(c, liWidths[i])
				} else {
					parts[i] = padRight(c, liWidths[i])
				}
			}
			line := "  " + strings.Join(parts, "  ")
			if dim {
				return cpTUIDimStyle.Render(line)
			}
			return line
		}
		b.WriteString(render(liHeaders, true))
		b.WriteString("\n")
		for _, r := range liPlain {
			b.WriteString(render(r, false))
			b.WriteString("\n")
		}
	}

	// Linked txs
	rec := mv.ReconciledTransaction
	if rec != nil && rec.ID != "" {
		b.WriteString("\n  ")
		b.WriteString(cpTUIHeaderStyle.Render("Linked tx"))
		b.WriteString("\n")
		writeKV("URI", rec.ID)
		if rec.AccountName != "" || rec.AccountSlug != "" {
			writeKV("Account", firstNonEmptyStr(rec.AccountName, rec.AccountSlug))
		}
		writeKV("Provider", rec.Provider)
		writeKV("Date", rec.Date)
		writeKV("Amount", fmt.Sprintf("%s %s", fmtEURSigned(rec.Amount), strings.ToUpper(firstNonEmptyStr(rec.Currency, "EUR"))))
		writeKV("Reference", rec.Reference)
		writeKV("State", rec.State)
		writeKV("Counterparty", rec.Counterparty)
	} else {
		b.WriteString("\n  ")
		b.WriteString(cpTUIDimStyle.Render("(no reconciled tx — press [r] to pick from candidate bank lines)"))
		b.WriteString("\n")
	}
	return b.String()
}
