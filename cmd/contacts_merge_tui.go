package cmd

import (
	"fmt"
	"sort"
	"strings"

	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
)

// OdooContactsMerge opens the contact-merge TUI: filter the contact list, select
// duplicates with [space], and press [m] to merge them. The merge is recorded
// LOCALLY (decoupled from Odoo, like the Nostr outbox); `chb odoo contacts apply`
// pushes pending merges to Odoo after a confirmation.
func OdooContactsMerge(args []string) error {
	if HasFlag(args, "--help", "-h", "help") {
		printContactsMergeHelp()
		return nil
	}
	ctx := loadContactsContext(false)
	rows := summariesSorted(ctx.allSummaries())
	if len(rows) == 0 {
		fmt.Printf("\n%sNo contacts found. Run `chb odoo pull` first.%s\n\n", Fmt.Dim, Fmt.Reset)
		return nil
	}
	if !isInteractiveTTY() {
		// Non-interactive: just surface the current pending merges.
		printPendingMergesSummary()
		fmt.Printf("\n%sOpen an interactive terminal to merge: chb odoo contacts merge%s\n\n", Fmt.Dim, Fmt.Reset)
		return nil
	}

	in := textinput.New()
	in.Placeholder = "filter by name · email · IBAN"
	in.Prompt = "  filter ❯ "
	in.CharLimit = 80
	in.Width = 48
	in.Focus()

	m := contactMergeModel{rows: rows, input: in, selected: map[int]bool{}, pending: len(pendingPartnerMerges())}
	m = m.refilter()
	if _, err := tea.NewProgram(m, tea.WithAltScreen()).Run(); err != nil {
		fmt.Printf("%sError: %v%s\n", Fmt.Red, err, Fmt.Reset)
	}
	return nil
}

func summariesSorted(byID map[int]contactSummary) []contactSummary {
	out := make([]contactSummary, 0, len(byID))
	for _, s := range byID {
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool {
		return strings.ToLower(out[i].Partner.Name) < strings.ToLower(out[j].Partner.Name)
	})
	return out
}

type contactMergeModel struct {
	rows          []contactSummary
	filtered      []int // indices into rows
	input         textinput.Model
	cursor        int          // index into filtered
	selected      map[int]bool // keys = index into rows
	confirm       bool         // [m] pressed once; press again to apply
	pending       int          // pending merge count
	status        string
	statusErr     bool
	width, height int
}

func (m contactMergeModel) Init() tea.Cmd { return textinput.Blink }

func (m contactMergeModel) refilter() contactMergeModel {
	q := strings.ToLower(strings.TrimSpace(m.input.Value()))
	terms := strings.Fields(q)
	m.filtered = m.filtered[:0]
	for i, s := range m.rows {
		if contactSummaryMatches(s, terms) {
			m.filtered = append(m.filtered, i)
		}
	}
	if m.cursor >= len(m.filtered) {
		m.cursor = 0
	}
	return m
}

func (m contactMergeModel) selectedIdxs() []int {
	var out []int
	for i := range m.selected {
		out = append(out, i)
	}
	sort.Ints(out)
	return out
}

// pickSurvivor chooses the merge survivor among the selected rows: the contact
// with the most invoices+bills (tiebreak: lowest partner id).
func (m contactMergeModel) pickSurvivor(idxs []int) int {
	best := idxs[0]
	for _, i := range idxs[1:] {
		bi, ci := m.rows[best], m.rows[i]
		bn, cn := bi.NumInvoice+bi.NumBill, ci.NumInvoice+ci.NumBill
		if cn > bn || (cn == bn && ci.Partner.ID < bi.Partner.ID) {
			best = i
		}
	}
	return best
}

func (m contactMergeModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width, m.height = msg.Width, msg.Height
		return m, nil
	case tea.KeyMsg:
		switch msg.String() {
		case "esc", "ctrl+c":
			return m, tea.Quit
		case "up", "ctrl+p":
			m.confirm = false
			if m.cursor > 0 {
				m.cursor--
			}
			return m, nil
		case "down", "ctrl+n":
			m.confirm = false
			if m.cursor < len(m.filtered)-1 {
				m.cursor++
			}
			return m, nil
		case " ":
			if m.cursor < len(m.filtered) {
				idx := m.filtered[m.cursor]
				if m.selected[idx] {
					delete(m.selected, idx)
				} else {
					m.selected[idx] = true
				}
				m.confirm = false
			}
			return m, nil
		case "c":
			m.selected = map[int]bool{}
			m.confirm = false
			return m, nil
		case "m":
			idxs := m.selectedIdxs()
			if len(idxs) < 2 {
				m.status = "select at least 2 contacts (space) to merge"
				m.statusErr = true
				return m, nil
			}
			survIdx := m.pickSurvivor(idxs)
			surv := m.rows[survIdx]
			if !m.confirm {
				m.confirm = true
				var victims []string
				for _, i := range idxs {
					if i != survIdx {
						victims = append(victims, m.rows[i].Partner.Name)
					}
				}
				m.status = fmt.Sprintf("merge %d contact(s) [%s] into %q? press [m] again to record locally, [c] to cancel",
					len(idxs)-1, strings.Join(victims, ", "), surv.Partner.Name)
				m.statusErr = false
				return m, nil
			}
			// confirmed
			var vIDs []int
			var vNames []string
			for _, i := range idxs {
				if i != survIdx {
					vIDs = append(vIDs, m.rows[i].Partner.ID)
					vNames = append(vNames, m.rows[i].Partner.Name)
				}
			}
			if err := recordPartnerMerge(surv.Partner.ID, surv.Partner.Name, vIDs, vNames); err != nil {
				m.status = fmt.Sprintf("failed to record merge: %v", err)
				m.statusErr = true
				m.confirm = false
				return m, nil
			}
			m.status = fmt.Sprintf("✓ recorded: %d → %q (apply with `chb odoo contacts apply`)", len(vIDs), surv.Partner.Name)
			m.statusErr = false
			m.confirm = false
			m.selected = map[int]bool{}
			m.pending = len(pendingPartnerMerges())
			// Reload so merged victims drop out of the list and the survivor's
			// counts reflect the new group.
			ctx := loadContactsContext(false)
			m.rows = summariesSorted(ctx.allSummaries())
			m = m.refilter()
			return m, nil
		}
		var cmd tea.Cmd
		m.input, cmd = m.input.Update(msg)
		m.confirm = false
		m = m.refilter()
		return m, cmd
	}
	return m, nil
}

func (m contactMergeModel) View() string {
	var b strings.Builder
	b.WriteString(cpTUIHeaderStyle.Render("🔗 Merge contacts"))
	b.WriteString("  ")
	b.WriteString(cpTUIDimStyle.Render(fmt.Sprintf("%d/%d shown · %d selected · %d pending merge(s)",
		len(m.filtered), len(m.rows), len(m.selected), m.pending)))
	b.WriteString("\n\n")
	b.WriteString(m.input.View())
	b.WriteString("\n\n")

	headers := []string{"", "ID", "Name", "Email", "Inv", "Bill", "IBAN"}
	caps := []int{1, 7, 28, 28, 4, 4, 20}
	rightAlign := map[int]bool{4: true, 5: true}

	pageSize := m.height - 9
	if pageSize < 5 {
		pageSize = 15
	}
	start := 0
	if m.cursor >= pageSize {
		start = m.cursor - pageSize + 1
	}
	end := start + pageSize
	if end > len(m.filtered) {
		end = len(m.filtered)
	}

	rows := [][]string{headers}
	for vi := start; vi < end; vi++ {
		idx := m.filtered[vi]
		s := m.rows[idx]
		mark := " "
		if m.selected[idx] {
			mark = "✓"
		}
		if vi == m.cursor {
			if m.selected[idx] {
				mark = "▸✓"
			} else {
				mark = "▸"
			}
		}
		name := s.Partner.Name
		if s.GroupSize > 1 {
			name += fmt.Sprintf(" (+%d)", s.GroupSize-1)
		}
		rows = append(rows, []string{
			mark,
			fmt.Sprintf("%d", s.Partner.ID),
			Truncate(name, caps[2]),
			Truncate(s.Partner.Email, caps[3]),
			fmt.Sprintf("%d", s.NumInvoice),
			fmt.Sprintf("%d", s.NumBill),
			Truncate(firstIBAN(s.IBANs), caps[6]),
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
		case strings.HasPrefix(r[0], "▸"):
			b.WriteString(cpTUICursorStyle.Render(line))
		case r[0] == "✓":
			b.WriteString(cpTUIMarkStyle.Render(line))
		default:
			b.WriteString(line)
		}
		b.WriteString("\n")
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
	b.WriteString(cpTUIDimStyle.Render("type to filter   [space] select   [m] merge selected   [c] clear   [esc] quit"))
	b.WriteString("\n")
	return b.String()
}

func printContactsMergeHelp() {
	f := Fmt
	fmt.Printf(`
%schb odoo contacts merge%s — Merge duplicate contacts (recorded locally)

%sUSAGE%s
  %schb odoo contacts merge%s        Open the TUI: filter, [space] select, [m] merge
  %schb odoo contacts apply%s        Push pending merges to Odoo (after confirm)

The merge is saved to a local pending file (decoupled from Odoo, like the Nostr
outbox). Aggregated views (%schb contacts%s, %schb search%s) reflect it immediately;
%schb odoo contacts apply%s commits it to Odoo. The survivor is the selected
contact with the most invoices+bills.
`,
		f.Bold, f.Reset,
		f.Bold, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset, f.Cyan, f.Reset,
		f.Cyan, f.Reset,
	)
}
