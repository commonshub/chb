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
	if creds, err := ResolveOdooCredentials(); err == nil {
		m.odooBaseURL = creds.URL
	}
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
	// remaining is the balance still to reconcile on the current move. It starts
	// at the move's residual/total and shrinks as partial payments are attached;
	// while it's > 0 the same move is re-shown so more bank lines can be linked.
	remaining float64

	mode         reconcileReviewMode
	search       textinput.Model
	pool         []Suggestion // all direction-matching bank lines (lazy)
	poolLoaded   bool
	results      []Suggestion
	resultCursor int

	confirmReattach bool
	pending         *Suggestion // candidate awaiting [y] confirmation
	// The invoice/bill the pending candidate is currently matched to, resolved
	// when the confirm is armed (local cache first, then a live Odoo lookup) so
	// the panel can link straight to it.
	pendingMatchMoveID int
	pendingMatchName   string

	// reconciledIdx maps a reconciled bank-line uniqueImportId -> the move it
	// is reconciled to ("CHB/2025/00163 — Partner"); reconciledMoveID maps it to
	// that move's Odoo id (for a clickable inspect URL). Built lazily on the
	// first reattach confirm so the prompt can say what would be overridden.
	reconciledIdx    map[string]string
	reconciledMoveID map[string]int

	// odooBaseURL is resolved once at startup so the reattach panel can render
	// clickable Odoo links without an auth round-trip.
	odooBaseURL string

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
		m.remaining = moveReconcileAmount(m.rows[m.idx].Move)
		m.cands = suggestBankLinesForAmount(m.rows[m.idx], m.kind, m.remaining)
	} else {
		m.cands = nil
		m.remaining = 0
	}
	m.candCursor = 0
	m.confirmReattach = false
	m.pending = nil
	m.pendingMatchMoveID = 0
	m.pendingMatchName = ""
	return m
}

// reloadForRemaining re-ranks candidates for the SAME move against its shrunken
// remaining balance after a partial payment, without advancing.
func (m reconcileReviewModel) reloadForRemaining() reconcileReviewModel {
	if m.idx >= 0 && m.idx < len(m.rows) {
		m.cands = suggestBankLinesForAmount(m.rows[m.idx], m.kind, m.remaining)
	}
	m.candCursor = 0
	m.confirmReattach = false
	m.pending = nil
	m.pendingMatchMoveID = 0
	m.pendingMatchName = ""
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
			m.reconciledIdx, m.reconciledMoveID = buildReconciledMoveIndex()
		}
		m.confirmReattach = true
		c := cand
		m.pending = &c
		// Resolve the invoice/bill this line is currently matched to so the panel
		// can link straight to it. Local cache first (offline); fall back to a
		// live lookup via the bank line's reconciled counterpart.
		m.pendingMatchMoveID = m.reconciledMoveID[cand.Line.UniqueImportID]
		m.pendingMatchName = m.reconciledIdx[cand.Line.UniqueImportID]
		if m.pendingMatchMoveID == 0 && cand.Line.CounterpartID > 0 {
			if mm, err := m.ensureAuth(); err == nil {
				m = mm
				if st, err := reconciledLineMatches(m.creds, m.uid, cand.Line.CounterpartID); err == nil && len(st.matches) > 0 {
					m.pendingMatchMoveID = st.matches[0].moveID
					m.pendingMatchName = firstNonEmptyStr(st.matchNames(), m.pendingMatchName)
				}
			}
		}
		// The full detail (what it's matched to, full memo, inspect URL, the
		// [y]/[esc] prompt) renders as a multi-line panel in View — a single
		// status line overflowed the terminal and hid the memo/URL.
		m.status = ""
		m.statusErr = false
		return m
	}
	return m.applyAccept(cand)
}

// renderReattachConfirm is the multi-line panel shown when the operator picks an
// already-reconciled bank line. It lays the detail out over several lines —
// what the line is currently matched to, its full (untruncated) memo, the
// counterparty, and a clickable Odoo URL to inspect the existing match — so
// nothing is lost to terminal truncation, then prompts for [y]/[esc].
func (m reconcileReviewModel) renderReattachConfirm() string {
	cand := *m.pending
	target := firstNonEmptyStr(m.rows[m.idx].Move.Title, fmt.Sprintf("#%d", m.rows[m.idx].Move.ID))

	var b strings.Builder
	b.WriteString("  ")
	b.WriteString(cpTUIErrStyle.Render(fmt.Sprintf("↻ bank line #%d is already matched to another invoice/bill", cand.Line.ID)))
	b.WriteString("\n     ")
	b.WriteString(cpTUIDimStyle.Render(fmt.Sprintf("%s · %s · %s",
		firstNonEmptyStr(cand.JournalName, "?"), cand.Date, fmtAmountCurrency(cand.Amount, "EUR"))))

	// What it's currently matched to — name + a direct link to that invoice/bill.
	if m.pendingMatchName != "" {
		b.WriteString("\n     ")
		b.WriteString(cpTUIDimStyle.Render("currently matched to: "))
		b.WriteString(m.pendingMatchName)
	}
	if m.odooBaseURL != "" && m.pendingMatchMoveID > 0 {
		b.WriteString("\n     ")
		b.WriteString(cpTUIDimStyle.Render("matched invoice/bill: "))
		b.WriteString(OdooWebURL(m.odooBaseURL, "account.move", m.pendingMatchMoveID))
	}
	if memo := strings.TrimSpace(firstNonEmptyStr(cand.Reference, cand.Line.Narration)); memo != "" {
		b.WriteString("\n     ")
		b.WriteString(cpTUIDimStyle.Render("memo: "))
		b.WriteString(memo) // full — no truncation
	}
	if cp := strings.TrimSpace(firstNonEmptyStr(cand.Partner, cand.IBAN)); cp != "" {
		b.WriteString("\n     ")
		b.WriteString(cpTUIDimStyle.Render("counterparty: "))
		b.WriteString(cp)
	}
	// Only when the matched invoice/bill couldn't be resolved, fall back to the
	// bank entry so the operator still has something to open.
	if m.odooBaseURL != "" && m.pendingMatchMoveID == 0 && cand.Line.MoveID > 0 {
		b.WriteString("\n     ")
		b.WriteString(cpTUIDimStyle.Render("bank entry: "))
		b.WriteString(OdooWebURL(m.odooBaseURL, "account.move", cand.Line.MoveID))
	}

	b.WriteString("\n  ")
	b.WriteString(cpTUIOKStyle.Render(fmt.Sprintf("[y] unreconcile & re-attach to %s     [esc] cancel", target)))
	return b.String()
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
	m.mode = reviewCard
	m.search.Blur()

	// Reduce the outstanding balance by the amount we just reconciled. If a
	// balance remains, the payment didn't cover the move — re-show the SAME move
	// with the new remaining so the operator can link more bank lines until it's
	// fully paid. Otherwise advance to the next move.
	m.remaining = roundCents(m.remaining - cand.Amount)
	if m.remaining > 0.01 {
		m.status = fmt.Sprintf("✓ %s line #%d (%s) — %s still to reconcile on %s #%d, pick more",
			verb, cand.Line.ID, fmtAmountCurrency(cand.Amount, "EUR"),
			fmtAmountCurrency(m.remaining, "EUR"), m.kind.label, row.Move.ID)
		m.statusErr = false
		return m.reloadForRemaining()
	}
	m.status = fmt.Sprintf("✓ %s line #%d (%s, %s) → %s #%d",
		verb, cand.Line.ID, cand.JournalName, cand.Line.Date, m.kind.label, row.Move.ID)
	m.statusErr = false
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
	amountLabel := fmtAmountCurrency(mv.TotalAmount, cur)
	// When only part of the total is still owed (a partial payment is already
	// reconciled, or we just attached one), show the remaining balance — that's
	// what the next bank line should cover.
	if m.remaining > 0.01 && m.remaining < roundCents(mv.TotalAmount)-0.01 {
		amountLabel = fmt.Sprintf("%s remaining of %s", fmtAmountCurrency(m.remaining, cur), fmtAmountCurrency(mv.TotalAmount, cur))
	}
	b.WriteString(cpTUIDimStyle.Render(fmt.Sprintf("%s  %s  %s", amountLabel, mv.Date, flags)))
	// Direct link to the invoice/bill being processed.
	if m.odooBaseURL != "" && mv.ID > 0 {
		b.WriteString("\n  ")
		b.WriteString(cpTUIDimStyle.Render(OdooWebURL(m.odooBaseURL, "account.move", mv.ID)))
	}
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

	if m.confirmReattach && m.pending != nil {
		b.WriteString("\n")
		b.WriteString(m.renderReattachConfirm())
		b.WriteString("\n")
	} else if m.status != "" {
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
