package cmd

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
)

// runReconcileReviewTUI drives the guided, one-invoice-at-a-time reconcile
// review for `chb invoices reconcile -i` / `chb bills reconcile -i`. For each
// open move it shows the best scored bank-line candidates (memo > amount >
// partner) and lets the operator Accept / Skip / Search. Writes to Odoo only on
// an explicit accept; everything else is read-only.
func runReconcileReviewTUI(kind moveKind, scope string, rows []moveRow) {
	if len(rows) == 0 {
		fmt.Printf("\n%sNo unreconciled %s to review.%s\n\n", Fmt.Dim, kind.labelPl, Fmt.Reset)
		return
	}
	search := textinput.New()
	search.Placeholder = "amount (1464, >1000) · counterpart · IBAN · memo"
	search.Prompt = "  search ❯ "
	search.CharLimit = 80
	search.Width = 48

	m := reconcileReviewModel{kind: kind, scope: scope, rows: rows, search: search}
	m = m.loadCands()

	p := tea.NewProgram(m, tea.WithAltScreen())
	res, err := p.Run()
	if err != nil {
		fmt.Printf("%sError: %v%s\n", Fmt.Red, err, Fmt.Reset)
		return
	}
	if fm, ok := res.(reconcileReviewModel); ok {
		fmt.Printf("\n  %sReconcile review done%s — %s%d accepted%s, %d skipped",
			Fmt.Bold, Fmt.Reset, Fmt.Green, fm.accepted, Fmt.Reset, fm.skipped)
		if fm.failed > 0 {
			fmt.Printf(", %s%d failed%s", Fmt.Red, fm.failed, Fmt.Reset)
		}
		fmt.Println()
		if fm.accepted > 0 {
			fmt.Printf("  %s↻ Run `chb invoices pull` to refresh the local cache from Odoo.%s\n", Fmt.Dim, Fmt.Reset)
		}
		fmt.Println()
	}
}

type reconcileReviewMode int

const (
	reviewCard reconcileReviewMode = iota
	reviewSearch
)

type reconcileReviewModel struct {
	kind  moveKind
	scope string
	rows  []moveRow
	idx   int

	cands      []Suggestion
	candCursor int

	mode         reconcileReviewMode
	search       textinput.Model
	pool         []Suggestion // all direction-matching bank lines (lazy)
	poolLoaded   bool
	results      []Suggestion
	resultCursor int

	confirmReattach bool
	pending         *Suggestion // candidate awaiting [y] confirmation

	// reconciledIdx maps a reconciled bank-line uniqueImportId -> the move it
	// is reconciled to ("CHB/2025/00163 — Partner"). Built lazily on the first
	// reattach confirm so the prompt can say what would be overridden.
	reconciledIdx map[string]string

	creds *OdooCredentials
	uid   int

	accepted, skipped, failed int
	status                    string
	statusErr                 bool
	width, height             int
	quitting                  bool
}

func (m reconcileReviewModel) Init() tea.Cmd { return nil }

func (m reconcileReviewModel) loadCands() reconcileReviewModel {
	if m.idx >= 0 && m.idx < len(m.rows) {
		m.cands = SuggestBankLinesForMove(m.rows[m.idx], m.kind)
	} else {
		m.cands = nil
	}
	m.candCursor = 0
	m.confirmReattach = false
	m.pending = nil
	return m
}

func (m reconcileReviewModel) advance() reconcileReviewModel {
	m.idx++
	if m.idx >= len(m.rows) {
		m.quitting = true
		return m
	}
	st, se := m.status, m.statusErr
	m = m.loadCands()
	m.status, m.statusErr = st, se // keep the "✓ attached…" line visible on the next card
	return m
}

func (m reconcileReviewModel) ensureAuth() (reconcileReviewModel, error) {
	if m.uid != 0 && m.creds != nil {
		return m, nil
	}
	creds, err := ResolveOdooCredentials()
	if err != nil {
		return m, err
	}
	uid, err := odooAuth(creds.URL, creds.DB, creds.Login, creds.Password)
	if err != nil || uid == 0 {
		return m, fmt.Errorf("Odoo authentication failed: %v", err)
	}
	m.creds, m.uid = creds, uid
	return m, nil
}

// requestAccept either applies the candidate or, for an already-reconciled bank
// line, arms the [y] confirmation so the operator can't override a previous
// reconciliation by reflex.
func (m reconcileReviewModel) requestAccept(cand Suggestion) reconcileReviewModel {
	if cand.AlreadyAttached && !m.confirmReattach {
		if m.reconciledIdx == nil {
			m.reconciledIdx = buildReconciledMoveIndex()
		}
		m.confirmReattach = true
		c := cand
		m.pending = &c
		target := firstNonEmptyStr(m.rows[m.idx].Move.Title, fmt.Sprintf("#%d", m.rows[m.idx].Move.ID))
		m.status = fmt.Sprintf("↻ line #%d (%s · %s · %s) is already reconciled%s — [y] unreconcile & attach to %s, [esc] cancel",
			cand.Line.ID, firstNonEmptyStr(cand.JournalName, "?"), cand.Date,
			fmtAmountCurrency(cand.Amount, "EUR"), m.describeAttached(cand), target)
		m.statusErr = false
		return m
	}
	return m.applyAccept(cand)
}

// describeAttached explains WHAT a candidate bank line is currently reconciled
// to, so the operator can judge whether overriding is worth it. Prefers the
// resolved move (from the local cache); falls back to the line's own memo and
// counterparty, which usually name what the payment was for.
func (m reconcileReviewModel) describeAttached(cand Suggestion) string {
	if lbl, ok := m.reconciledIdx[cand.Line.UniqueImportID]; ok && lbl != "" {
		return " to " + lbl
	}
	var parts []string
	if memo := strings.TrimSpace(firstNonEmptyStr(cand.Reference, cand.Line.Narration)); memo != "" {
		parts = append(parts, "memo: \""+Truncate(memo, 50)+"\"")
	}
	cp := strings.TrimSpace(cand.Partner)
	if cp == "" {
		cp = strings.TrimSpace(cand.IBAN)
	}
	if cp != "" {
		parts = append(parts, "counterparty: "+cp)
	}
	if len(parts) == 0 {
		return ""
	}
	return " (" + strings.Join(parts, ", ") + ")"
}

func (m reconcileReviewModel) applyAccept(cand Suggestion) reconcileReviewModel {
	var err error
	m, err = m.ensureAuth()
	if err != nil {
		m.status = err.Error()
		m.statusErr = true
		m.confirmReattach, m.pending = false, nil
		return m
	}
	row := &m.rows[m.idx]
	if e := attachMoveToBankLine(m.creds, m.uid, row, cand); e != nil {
		m.failed++
		m.status = fmt.Sprintf("attach failed: %v", e)
		m.statusErr = true
		m.confirmReattach, m.pending = false, nil
		return m
	}
	m.accepted++
	verb := "attached"
	if cand.AlreadyAttached {
		verb = "reattached"
	}
	m.status = fmt.Sprintf("✓ %s line #%d (%s, %s) → %s #%d",
		verb, cand.Line.ID, cand.JournalName, cand.Line.Date, m.kind.label, row.Move.ID)
	m.statusErr = false
	m.mode = reviewCard
	m.search.Blur()
	return m.advance()
}

func (m reconcileReviewModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width, m.height = msg.Width, msg.Height
		return m, nil
	case tea.KeyMsg:
		if m.mode == reviewSearch {
			return m.updateSearch(msg)
		}
		return m.updateCard(msg)
	}
	return m, nil
}

func (m reconcileReviewModel) updateCard(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "q", "esc", "ctrl+c":
		m.quitting = true
		return m, tea.Quit
	case "s", "n", "right", "l":
		m.skipped++
		m.status = ""
		m = m.advance()
		if m.quitting {
			return m, tea.Quit
		}
		return m, nil
	case "left", "h", "p":
		if m.idx > 0 {
			m.idx -= 2 // advance() will +1
			m = m.advance()
			m.status = ""
		}
		return m, nil
	case "up", "k":
		m.confirmReattach, m.pending = false, nil
		if m.candCursor > 0 {
			m.candCursor--
		}
		return m, nil
	case "down", "j":
		m.confirmReattach, m.pending = false, nil
		if m.candCursor < len(m.cands)-1 {
			m.candCursor++
		}
		return m, nil
	case "/":
		m = m.enterSearch()
		return m, textinput.Blink
	case "y", "Y":
		if m.confirmReattach && m.pending != nil {
			m = m.applyAccept(*m.pending)
			if m.quitting {
				return m, tea.Quit
			}
		}
		return m, nil
	case "a", "enter":
		if len(m.cands) == 0 {
			m.status = "no auto candidates — press [/] to search bank lines"
			m.statusErr = false
			return m, nil
		}
		if m.candCursor < 0 || m.candCursor >= len(m.cands) {
			return m, nil
		}
		m = m.requestAccept(m.cands[m.candCursor])
		if m.quitting {
			return m, tea.Quit
		}
		return m, nil
	}
	return m, nil
}

func (m reconcileReviewModel) enterSearch() reconcileReviewModel {
	if !m.poolLoaded {
		m.pool = buildReconcileSearchPool(m.kind)
		m.poolLoaded = true
	}
	m.mode = reviewSearch
	m.confirmReattach, m.pending = false, nil
	m.search.SetValue("")
	m.search.Focus()
	m.results = m.pool
	m.resultCursor = 0
	m.status = ""
	return m
}

func (m reconcileReviewModel) updateSearch(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "esc", "ctrl+c":
		m.mode = reviewCard
		m.search.Blur()
		m.confirmReattach, m.pending = false, nil
		return m, nil
	case "up":
		m.confirmReattach, m.pending = false, nil
		if m.resultCursor > 0 {
			m.resultCursor--
		}
		return m, nil
	case "down":
		m.confirmReattach, m.pending = false, nil
		if m.resultCursor < len(m.results)-1 {
			m.resultCursor++
		}
		return m, nil
	case "y", "Y":
		if m.confirmReattach && m.pending != nil {
			m = m.applyAccept(*m.pending)
			if m.quitting {
				return m, tea.Quit
			}
			return m, nil
		}
		// otherwise treat as a normal character for the search box
	case "enter":
		if m.resultCursor >= 0 && m.resultCursor < len(m.results) {
			m = m.requestAccept(m.results[m.resultCursor])
			if m.quitting {
				return m, tea.Quit
			}
		}
		return m, nil
	}
	var cmd tea.Cmd
	m.search, cmd = m.search.Update(msg)
	m.results = filterSuggestions(m.pool, m.search.Value())
	if m.resultCursor >= len(m.results) {
		m.resultCursor = 0
	}
	return m, cmd
}

func filterSuggestions(pool []Suggestion, query string) []Suggestion {
	if strings.TrimSpace(query) == "" {
		return pool
	}
	out := pool[:0:0]
	for _, s := range pool {
		if suggestionMatchesQuery(s, query) {
			out = append(out, s)
		}
	}
	return out
}

// ── Rendering ──────────────────────────────────────────────────────────

func (m reconcileReviewModel) View() string {
	if m.quitting {
		return ""
	}
	var b strings.Builder
	b.WriteString(cpTUIHeaderStyle.Render(fmt.Sprintf("⇄ Reconcile %s — %s", m.kind.labelPl, m.scope)))
	b.WriteString("  ")
	b.WriteString(cpTUIDimStyle.Render(fmt.Sprintf("%s %d/%d   ✓%d accepted · ⤼%d skipped", m.kind.label, m.idx+1, len(m.rows), m.accepted, m.skipped)))
	b.WriteString("\n\n")

	row := m.rows[m.idx]
	mv := row.Move
	cur := mv.Currency
	b.WriteString("  ")
	b.WriteString(cpTUIHeaderStyle.Render(firstNonEmptyStr(mv.Title, fmt.Sprintf("#%d", mv.ID))))
	b.WriteString("  ")
	b.WriteString(row.Partner)
	b.WriteString("\n  ")
	flags := strings.TrimSpace(mv.State + " / " + mv.PaymentState)
	if isMoveCreditNote(mv) {
		flags += "  [credit note]"
	}
	b.WriteString(cpTUIDimStyle.Render(fmt.Sprintf("%s  %s  %s", fmtAmountCurrency(mv.TotalAmount, cur), mv.Date, flags)))
	// Structured communication (+++…+++) / payment reference. Odoo often puts
	// it in the move name, so it's already the title above — only show this
	// line when it carries something the title doesn't already display.
	if ref := strings.TrimSpace(row.Reference); ref != "" && !strings.EqualFold(ref, strings.TrimSpace(mv.Title)) {
		b.WriteString("\n  ")
		b.WriteString(cpTUIDimStyle.Render("Communication: "))
		b.WriteString(ref)
	}
	if d := moveFirstLineItem(mv); d != "" {
		b.WriteString("\n  ")
		b.WriteString(cpTUIDimStyle.Render(Truncate(d, 80)))
	}
	b.WriteString("\n\n")

	if m.mode == reviewSearch {
		b.WriteString(m.renderSearch())
	} else {
		b.WriteString(m.renderCandidates())
	}

	if m.status != "" {
		b.WriteString("\n  ")
		if m.statusErr {
			b.WriteString(cpTUIErrStyle.Render(m.status))
		} else {
			b.WriteString(cpTUIOKStyle.Render(m.status))
		}
		b.WriteString("\n")
	}

	b.WriteString("\n  ")
	if m.mode == reviewSearch {
		b.WriteString(cpTUIDimStyle.Render("[↑↓] pick   [enter] attach (writes to Odoo)   [esc] back to candidates"))
	} else {
		b.WriteString(cpTUIDimStyle.Render("[a/enter] accept   [s] skip   [/] search   [↑↓] pick   [p] previous   [q] quit"))
	}
	b.WriteString("\n")
	return b.String()
}

func suggestionConfidenceBadge(s Suggestion) string {
	switch matchConfidence(s) {
	case "high":
		return "★★★"
	case "medium":
		return "★★"
	case "low":
		return "★"
	}
	return ""
}

func (m reconcileReviewModel) renderCandidates() string {
	var b strings.Builder
	if len(m.cands) == 0 {
		b.WriteString("  ")
		b.WriteString(cpTUIDimStyle.Render("No auto-matched bank lines. Press [/] to search by amount, counterpart, IBAN or memo."))
		b.WriteString("\n")
		return b.String()
	}
	b.WriteString("  ")
	b.WriteString(cpTUIDimStyle.Render(fmt.Sprintf("Candidates (%d) — best first:", len(m.cands))))
	b.WriteString("\n")
	const cap = 12
	start := 0
	if m.candCursor >= cap {
		start = m.candCursor - cap + 1
	}
	end := start + cap
	if end > len(m.cands) {
		end = len(m.cands)
	}
	rows := suggestionTableRows(m.cands[start:end], m.candCursor-start, true)
	b.WriteString(renderSuggestionTable(rows))
	if end < len(m.cands) {
		b.WriteString(cpTUIDimStyle.Render(fmt.Sprintf("  … %d more (↓)", len(m.cands)-end)))
		b.WriteString("\n")
	}
	return b.String()
}

func (m reconcileReviewModel) renderSearch() string {
	var b strings.Builder
	b.WriteString(m.search.View())
	b.WriteString("\n  ")
	b.WriteString(cpTUIDimStyle.Render(fmt.Sprintf("%d / %d bank lines match", len(m.results), len(m.pool))))
	b.WriteString("\n")
	const cap = 14
	shown := m.results
	start := 0
	if m.resultCursor >= cap {
		start = m.resultCursor - cap + 1
	}
	end := start + cap
	if end > len(shown) {
		end = len(shown)
	}
	rows := suggestionTableRows(shown[start:end], m.resultCursor-start, false)
	b.WriteString(renderSuggestionTable(rows))
	return b.String()
}

// suggestionTableRows builds the display matrix (header + body) for a candidate
// list. withConf includes the confidence/why columns (the auto-candidate view);
// the search view drops them since there's no per-move score.
func suggestionTableRows(cands []Suggestion, cursor int, withConf bool) [][]string {
	var headers []string
	if withConf {
		headers = []string{"", "Conf", "Date", "Δ", "Amount", "Journal", "Counterparty / IBAN", "Memo", "Why"}
	} else {
		headers = []string{"", "Date", "Amount", "St", "Journal", "Counterparty / IBAN", "Memo"}
	}
	out := [][]string{headers}
	for i, c := range cands {
		mark := " "
		if i == cursor {
			mark = "▸"
		}
		cp := c.Partner
		if c.IBAN != "" {
			if cp != "" {
				cp += "  "
			}
			cp += c.IBAN
		}
		memo := strings.TrimSpace(c.Reference)
		if memo == "" {
			memo = strings.ReplaceAll(strings.TrimSpace(c.Line.Narration), "\n", " ")
		}
		st := "open"
		if c.AlreadyAttached {
			st = "paid"
		}
		if withConf {
			out = append(out, []string{
				mark, suggestionConfidenceBadge(c), c.Date,
				dateDeltaLabelDays(c.DaysDelta, c.AlreadyAttached),
				fmtAmountCurrency(c.Amount, "EUR"),
				Truncate(firstNonEmptyStr(c.JournalName, "?"), 12),
				Truncate(firstNonEmptyStr(cp, "—"), 30),
				Truncate(memo, 34),
				Truncate(c.MatchReason, 28),
			})
		} else {
			out = append(out, []string{
				mark, c.Date,
				fmtAmountCurrency(c.Amount, "EUR"),
				st,
				Truncate(firstNonEmptyStr(c.JournalName, "?"), 12),
				Truncate(firstNonEmptyStr(cp, "—"), 32),
				Truncate(memo, 40),
			})
		}
	}
	return out
}

func dateDeltaLabelDays(days int, _ bool) string {
	if days == 0 {
		return "0d"
	}
	return fmt.Sprintf("%dd", days)
}

// renderSuggestionTable renders the matrix (row 0 = header) with column widths
// derived from content. Cursor row is the one whose first cell is "▸".
func renderSuggestionTable(rows [][]string) string {
	if len(rows) == 0 {
		return ""
	}
	n := len(rows[0])
	widths := make([]int, n)
	for _, r := range rows {
		for i := 0; i < n && i < len(r); i++ {
			if w := displayWidth(r[i]); w > widths[i] {
				widths[i] = w
			}
		}
	}
	rightAlign := map[int]bool{}
	for i, h := range rows[0] {
		if h == "Amount" {
			rightAlign[i] = true
		}
	}
	var b strings.Builder
	for ri, r := range rows {
		parts := make([]string, n)
		for i := 0; i < n; i++ {
			cell := ""
			if i < len(r) {
				cell = r[i]
			}
			if rightAlign[i] {
				parts[i] = padLeft(cell, widths[i])
			} else {
				parts[i] = padRight(cell, widths[i])
			}
		}
		line := "  " + strings.Join(parts, "  ")
		switch {
		case ri == 0:
			b.WriteString(cpTUIDimStyle.Render(line))
		case len(r) > 0 && r[0] == "▸":
			b.WriteString(cpTUICursorStyle.Render(line))
		default:
			b.WriteString(line)
		}
		b.WriteString("\n")
	}
	return b.String()
}
