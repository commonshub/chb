package cmd

import (
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/huh"
)

// InvoicesCategorize walks cached invoice months and interactively prompts
// the user to assign category/collective/event to any invoice that doesn't
// yet have a collective set. Writes straight back to the month's
// invoices.json — annotations are preserved across re-syncs by
// preserveMoveAnnotations.
func InvoicesCategorize(args []string) error {
	if HasFlag(args, "--help", "-h", "help") {
		printMovesCategorizeHelp("invoices")
		return nil
	}
	return categorizeMoves(moveKindInvoice, args)
}

// BillsCategorize is the vendor-bill equivalent of InvoicesCategorize.
func BillsCategorize(args []string) error {
	if HasFlag(args, "--help", "-h", "help") {
		printMovesCategorizeHelp("bills")
		return nil
	}
	return categorizeMoves(moveKindBill, args)
}

func categorizeMoves(kind moveKind, args []string) error {
	dataDir := DataDir()
	posYear, posMonth, posFound := ParseYearMonthArg(args)

	var startMonth, endMonth string
	if posFound {
		if posMonth != "" {
			startMonth = fmt.Sprintf("%s-%s", posYear, posMonth)
			endMonth = startMonth
		} else {
			startMonth = fmt.Sprintf("%s-01", posYear)
			endMonth = fmt.Sprintf("%s-12", posYear)
		}
	}

	categories := LoadCategories()
	if len(categories) == 0 {
		return fmt.Errorf("no categories configured — add some to APP_DATA_DIR/categories.json first")
	}
	catOptions := []huh.Option[string]{huh.NewOption("(skip)", "")}
	for _, c := range categories {
		catOptions = append(catOptions, huh.NewOption(c.Label+" ("+c.Slug+")", c.Slug))
	}

	totalUnannotated := 0
	totalAnnotated := 0
	var stopped bool

	err := walkMoveMonths(dataDir, kind, func(year, month string) error {
		if stopped {
			return nil
		}
		ym := year + "-" + month
		if startMonth != "" && (ym < startMonth || ym > endMonth) {
			return nil
		}

		moves, err := loadMoves(dataDir, year, month, kind)
		if err != nil {
			Warnf("  %s⚠ %s: %v%s", Fmt.Yellow, ym, err, Fmt.Reset)
			return nil
		}
		if len(moves) == 0 {
			return nil
		}

		// Load partner names from the private file so the TUI can show who
		// sent/received the invoice without ever writing PII to the public
		// output.
		partners := loadMovePartners(dataDir, year, month, kind)

		// Find moves without a collective (treat collective as the primary
		// "annotated" signal — a move may have a Category from Odoo analytics
		// but still need a collective).
		var unannotated []int
		for i, m := range moves {
			if m.Collective == "" {
				unannotated = append(unannotated, i)
			}
		}
		if len(unannotated) == 0 {
			return nil
		}
		totalUnannotated += len(unannotated)

		fmt.Printf("\n%s📝 %s — %d %s to annotate%s\n",
			Fmt.Bold, ym, len(unannotated), kind.labelPl, Fmt.Reset)

		changed := false
		for _, idx := range unannotated {
			m := moves[idx]
			label := moveDisplayLabel(m)
			if partner := partners[m.ID]; partner != "" {
				label = partner + " — " + label
			}
			fmt.Printf("\n  %s%s%s\n", Fmt.Bold, label, Fmt.Reset)
			if m.Category != "" {
				fmt.Printf("  %sOdoo category: %s%s\n", Fmt.Dim, m.Category, Fmt.Reset)
			}

			// Category prompt (default to existing Odoo category if set).
			category := m.Category
			var chosenCategory string
			catOpts := catOptions
			if category != "" {
				catOpts = append([]huh.Option[string]{
					huh.NewOption("(keep "+category+")", category),
				}, catOptions...)
			}
			form := huh.NewForm(huh.NewGroup(
				huh.NewSelect[string]().Title("Category").Options(catOpts...).Value(&chosenCategory),
			))
			if err := form.Run(); err != nil {
				stopped = true
				break
			}
			if chosenCategory != "" {
				category = chosenCategory
			}

			collective := pickCollective("")
			if stopped {
				break
			}

			event := ""
			// Use invoice date as the anchor for event proximity.
			if m.Date != "" {
				if t, ok := parseMoveDate(m.Date); ok {
					event = pickEvent(t)
				}
			}

			if category == "" && collective == "" && event == "" {
				continue
			}
			m.Category = category
			m.Collective = collective
			m.Event = event
			moves[idx] = m
			changed = true
			totalAnnotated++
			fmt.Printf("  %s✓ category=%s collective=%s event=%s%s\n",
				Fmt.Green, firstNonEmptyStr(m.Category, "-"),
				firstNonEmptyStr(m.Collective, "-"),
				firstNonEmptyStr(m.Event, "-"), Fmt.Reset)
		}

		if changed {
			if err := saveMoves(dataDir, year, month, kind, moves); err != nil {
				return fmt.Errorf("save %s: %w", ym, err)
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	fmt.Printf("\n%s✓ Annotated %d/%d %s%s\n\n",
		Fmt.Green, totalAnnotated, totalUnannotated, kind.labelPl, Fmt.Reset)
	return nil
}

// parseMoveDate converts an Odoo date string (YYYY-MM-DD) to a unix timestamp
// in the Brussels timezone. Returns (0, false) when unparseable. Used to
// anchor event-proximity matching during categorization.
func parseMoveDate(s string) (int64, bool) {
	if len(s) < 10 {
		return 0, false
	}
	t, err := time.ParseInLocation("2006-01-02", s[:10], BrusselsTZ())
	if err != nil {
		return 0, false
	}
	return t.Unix(), true
}

func printMovesCategorizeHelp(labelPl string) {
	f := Fmt
	singular := strings.TrimSuffix(labelPl, "s")
	fmt.Printf(`
%schb %s categorize%s — Assign category, collective, and event

%sUSAGE%s
  %schb %s categorize%s [year[/month]]

%sDESCRIPTION%s
  Walks cached months and prompts you to assign a category, collective,
  and (optional) event to every %s that doesn't yet have a collective.

  Annotations are written back into the month's %s.json file. They're
  preserved across subsequent %schb odoo %s sync%s runs.

  Follow up with %schb %s publish%s to broadcast annotations to Nostr.
`,
		f.Bold, labelPl, f.Reset,
		f.Bold, f.Reset,
		f.Cyan, labelPl, f.Reset,
		f.Bold, f.Reset,
		singular,
		labelPl,
		f.Cyan, labelPl, f.Reset,
		f.Cyan, labelPl, f.Reset,
	)
}
