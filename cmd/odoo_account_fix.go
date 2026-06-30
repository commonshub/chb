package cmd

import (
	"bufio"
	"fmt"
	"math"
	"os"
	"sort"
	"strings"
	"time"
)

// accountFixLine is one posted/draft move line sitting on a GL account, reduced
// to the fields the reconciler needs. Pulled from account.move.line.
type accountFixLine struct {
	MoveID      int
	JournalID   int
	JournalName string
	Date        string
	Balance     float64
	State       string // "posted" | "draft" | …
	Name        string
}

// accountFixMove aggregates the lines of one move that touch the GL account.
type accountFixMove struct {
	MoveID      int
	Date        string
	JournalID   int
	JournalName string
	Sum         float64
	Name        string
	Draft       bool
}

// accountFixPlan is the decomposition of a GL account's lines relative to its
// own bank journal.
type accountFixPlan struct {
	OwnPostedSum     float64 // posted lines from the account's own bank journal
	ForeignPostedSum float64 // posted lines from OTHER journals (inflate the GL wrongly)
	OwnDraftSum      float64 // unposted lines from the own journal (should be posted)
	ForeignDraftSum  float64

	PostableDrafts []accountFixMove // own-journal drafts — safe to post automatically
	ForeignMoves   []accountFixMove // foreign entries (posted or draft) — manual review
}

// GLPosted is the account's posted GL balance (what Odoo reports).
func (p accountFixPlan) GLPosted() float64 { return roundCents(p.OwnPostedSum + p.ForeignPostedSum) }

// ProjectedAfterPostingDrafts is the GL balance once the safe own-journal drafts
// are posted.
func (p accountFixPlan) ProjectedAfterPostingDrafts() float64 {
	return roundCents(p.GLPosted() + p.OwnDraftSum)
}

func (p accountFixPlan) hasWork() bool {
	return len(p.PostableDrafts) > 0 || len(p.ForeignMoves) > 0
}

// classifyAccountFixLines partitions a GL account's lines against its own bank
// journal: own-journal posted lines are legitimate; own-journal drafts should be
// posted (they're uncounted); any line whose move lives in a different journal is
// "foreign" — a manual booking that `chb odoo journals <id> fix` can never see.
// ownJournalID == 0 disables the foreign split (nothing is treated as foreign).
func classifyAccountFixLines(lines []accountFixLine, ownJournalID int) accountFixPlan {
	postable := map[int]*accountFixMove{}
	foreign := map[int]*accountFixMove{}
	var postableOrder, foreignOrder []int
	var p accountFixPlan

	get := func(m map[int]*accountFixMove, order *[]int, ln accountFixLine, draft bool) *accountFixMove {
		mv := m[ln.MoveID]
		if mv == nil {
			mv = &accountFixMove{MoveID: ln.MoveID, Date: ln.Date, JournalID: ln.JournalID, JournalName: ln.JournalName, Name: ln.Name, Draft: draft}
			m[ln.MoveID] = mv
			*order = append(*order, ln.MoveID)
		}
		return mv
	}

	for _, ln := range lines {
		posted := ln.State == "posted"
		isForeign := ownJournalID > 0 && ln.JournalID != ownJournalID
		switch {
		case isForeign:
			mv := get(foreign, &foreignOrder, ln, !posted)
			mv.Sum += ln.Balance
			if !posted {
				mv.Draft = true
				p.ForeignDraftSum += ln.Balance
			} else {
				p.ForeignPostedSum += ln.Balance
			}
		case !posted:
			mv := get(postable, &postableOrder, ln, true)
			mv.Sum += ln.Balance
			p.OwnDraftSum += ln.Balance
		default:
			p.OwnPostedSum += ln.Balance
		}
	}

	p.OwnPostedSum = roundCents(p.OwnPostedSum)
	p.ForeignPostedSum = roundCents(p.ForeignPostedSum)
	p.OwnDraftSum = roundCents(p.OwnDraftSum)
	p.ForeignDraftSum = roundCents(p.ForeignDraftSum)
	for _, mid := range postableOrder {
		m := postable[mid]
		m.Sum = roundCents(m.Sum)
		p.PostableDrafts = append(p.PostableDrafts, *m)
	}
	for _, mid := range foreignOrder {
		m := foreign[mid]
		m.Sum = roundCents(m.Sum)
		p.ForeignMoves = append(p.ForeignMoves, *m)
	}
	sort.SliceStable(p.PostableDrafts, func(i, j int) bool { return p.PostableDrafts[i].Date < p.PostableDrafts[j].Date })
	sort.SliceStable(p.ForeignMoves, func(i, j int) bool { return p.ForeignMoves[i].Date < p.ForeignMoves[j].Date })
	return p
}

// OdooAccountFix reconciles a GL account's posted balance against the linked
// local account, decomposing any gap into two kinds of fixable defect:
//
//   - unposted (draft) entries from the account's own bank journal — e.g. an
//     opening balance left in draft; these are SAFE to post and are offered as
//     an automatic fix (Odoo's GL only counts posted lines, so a draft is
//     invisible);
//   - posted entries whose move lives in a DIFFERENT journal ("foreign") — manual
//     misc-operations bookings that `chb odoo journals <id> fix` can never see;
//     reported with direct Odoo URLs for manual review/reversal (we never
//     auto-delete accounting entries).
//
//	chb odoo accounts 550013 fix
//	chb odoo accounts 550013 fix --dry-run
func OdooAccountFix(creds *OdooCredentials, uid int, code string, args []string) error {
	if HasFlag(args, "--help", "-h", "help") {
		printOdooAccountFixHelp()
		return nil
	}
	dryRun := HasFlag(args, "--dry-run")
	assumeYes := HasFlag(args, "-y", "--yes")

	acc, ok, err := fetchOdooAccountByCode(creds, uid, code)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("no Odoo account with code %q", code)
	}

	// Resolve the account's own bank journal + the linked local CHB account.
	var chbAcc *AccountConfig
	for _, a := range LoadAccountConfigs() {
		if a.OdooGlAccountCode == code {
			ac := a
			chbAcc = &ac
			break
		}
	}
	ownJournalID := 0
	if chbAcc != nil {
		ownJournalID = chbAcc.OdooJournalID
	}
	if ownJournalID == 0 {
		if js, jerr := fetchJournalsUsingAccount(creds, uid, acc.ID); jerr == nil && len(js) == 1 {
			ownJournalID = odooInt(js[0]["id"])
		}
	}

	rows, err := odooSearchReadAllMaps(creds, uid, "account.move.line",
		[]interface{}{[]interface{}{"account_id", "=", acc.ID}},
		[]string{"id", "move_id", "journal_id", "date", "balance", "parent_state", "name"}, "date asc")
	if err != nil {
		return fmt.Errorf("fetch account lines: %v", err)
	}
	lines := make([]accountFixLine, 0, len(rows))
	for _, r := range rows {
		lines = append(lines, accountFixLine{
			MoveID:      odooFieldID(r["move_id"]),
			JournalID:   odooFieldID(r["journal_id"]),
			JournalName: odooFieldName(r["journal_id"]),
			Date:        odooString(r["date"]),
			Balance:     odooFloat(r["balance"]),
			State:       odooString(r["parent_state"]),
			Name:        odooString(r["name"]),
		})
	}
	plan := classifyAccountFixLines(lines, ownJournalID)

	hasLocal := false
	localBal := 0.0
	if chbAcc != nil {
		localBal, _, _, _ = accountBalanceAtCutoff(chbAcc, endOfDay(time.Now().In(BrusselsTZ())))
		hasLocal = true
	}

	// ---- report ----
	fmt.Printf("\n%s🧾 %s — %s%s  %s(account fix)%s\n", Fmt.Bold, acc.Code, acc.Name, Fmt.Reset, Fmt.Dim, Fmt.Reset)
	if hasLocal {
		fmt.Printf("  %sLocal balance:%s       %14s  %s(%s)%s\n", Fmt.Dim, Fmt.Reset, formatBalancePlain(localBal, "EUR"), Fmt.Dim, chbAcc.Slug, Fmt.Reset)
	}
	fmt.Printf("  %sGL balance (posted):%s %14s\n", Fmt.Dim, Fmt.Reset, formatBalancePlain(plan.GLPosted(), "EUR"))
	if ownJournalID > 0 {
		fmt.Printf("    %sown journal #%d:%s      %14s\n", Fmt.Dim, ownJournalID, Fmt.Reset, formatBalancePlain(plan.OwnPostedSum, "EUR"))
	}
	if plan.ForeignPostedSum != 0 || foreignPostedCount(plan) > 0 {
		fmt.Printf("    %sforeign journals:%s    %14s  %s(%s)%s\n", Fmt.Dim, Fmt.Reset, formatBalancePlain(plan.ForeignPostedSum, "EUR"), Fmt.Dim, Pluralize(foreignPostedCount(plan), "entry", "entries"), Fmt.Reset)
	}
	if plan.OwnDraftSum != 0 || len(plan.PostableDrafts) > 0 {
		fmt.Printf("    %sunposted (draft):%s    %14s  %s(%s, uncounted)%s\n", Fmt.Dim, Fmt.Reset, formatBalancePlain(plan.OwnDraftSum, "EUR"), Fmt.Dim, Pluralize(len(plan.PostableDrafts), "entry", "entries"), Fmt.Reset)
	}
	if hasLocal {
		fmt.Printf("  %sGap (GL − local):%s    %14s\n", Fmt.Dim, Fmt.Reset, fmtEURSigned(roundCents(plan.GLPosted()-localBal)))
	}

	if !plan.hasWork() {
		if hasLocal && math.Abs(roundCents(plan.GLPosted()-localBal)) > 0.005 {
			fmt.Printf("\n  %s⚠ GL and local disagree but no drafts/foreign entries explain it — the gap is\n    inside the own journal's posted lines. Run `chb odoo journals %d fix`.%s\n\n", Fmt.Yellow, ownJournalID, Fmt.Reset)
		} else {
			fmt.Printf("\n  %s✓ Nothing to fix.%s\n\n", Fmt.Green, Fmt.Reset)
		}
		return nil
	}

	if len(plan.PostableDrafts) > 0 {
		fmt.Printf("\n  %s1) Unposted entries on this account — post to count them:%s\n", Fmt.Bold, Fmt.Reset)
		for _, m := range plan.PostableDrafts {
			fmt.Printf("     move #%d  %s  %14s  %q\n       %s%s%s\n",
				m.MoveID, m.Date, fmtEURSigned(m.Sum), Truncate(m.Name, 40), Fmt.Cyan, OdooWebURL(creds.URL, "account.move", m.MoveID), Fmt.Reset)
		}
	}

	if len(plan.ForeignMoves) > 0 {
		fmt.Printf("\n  %s2) Entries from OTHER journals posted to this account — review/reverse in Odoo:%s\n", Fmt.Bold, Fmt.Reset)
		for _, m := range plan.ForeignMoves {
			tag := ""
			if m.Draft {
				tag = Fmt.Yellow + " (draft)" + Fmt.Reset
			}
			fmt.Printf("     move #%d  %s  %14s  [%s]%s\n       %s%s%s\n",
				m.MoveID, m.Date, fmtEURSigned(m.Sum), Truncate(m.JournalName, 32), tag, Fmt.Cyan, OdooWebURL(creds.URL, "account.move", m.MoveID), Fmt.Reset)
		}
		footer := fmt.Sprintf("net posted %s", fmtEURSigned(plan.ForeignPostedSum))
		if math.Abs(plan.ForeignDraftSum) > 0.005 {
			footer += fmt.Sprintf(", plus %s unposted (would re-break the account if posted)", fmtEURSigned(plan.ForeignDraftSum))
		}
		fmt.Printf("     %s%s — not owned by journal #%d, so `journals fix` can't see these.%s\n",
			Fmt.Dim, footer, ownJournalID, Fmt.Reset)
	}

	// Projected outcome.
	if len(plan.PostableDrafts) > 0 {
		fmt.Printf("\n  %sAfter posting the draft%s, GL → %s",
			Fmt.Dim, plural(len(plan.PostableDrafts)), formatBalancePlain(plan.ProjectedAfterPostingDrafts(), "EUR"))
		if hasLocal {
			remaining := roundCents(plan.ProjectedAfterPostingDrafts() - localBal)
			fmt.Printf("; remaining vs local %s", fmtEURSigned(remaining))
			if len(plan.ForeignMoves) > 0 && math.Abs(remaining-plan.ForeignPostedSum) < 0.005 {
				fmt.Printf(" = the %s above", Pluralize(foreignPostedCount(plan), "foreign entry", "foreign entries"))
			}
		}
		fmt.Printf("%s\n", Fmt.Reset)
	}

	// Only the foreign entries need attention → nothing to auto-apply.
	if len(plan.PostableDrafts) == 0 {
		fmt.Printf("\n  %sNo automatic fix — the foreign entries above need manual review (URLs).%s\n\n", Fmt.Dim, Fmt.Reset)
		return nil
	}

	if dryRun {
		fmt.Printf("\n  %s(dry-run) would post %s; no writes.%s\n\n", Fmt.Dim, Pluralize(len(plan.PostableDrafts), "draft entry", "draft entries"), Fmt.Reset)
		return nil
	}
	if !assumeYes {
		fmt.Printf("\n  %sPost %s now?%s [y/N] ", Fmt.Bold, Pluralize(len(plan.PostableDrafts), "draft entry", "draft entries"), Fmt.Reset)
		reader := bufio.NewReader(os.Stdin)
		resp, _ := reader.ReadString('\n')
		if r := strings.TrimSpace(resp); r != "y" && r != "Y" && r != "yes" {
			fmt.Printf("  %scancelled%s\n\n", Fmt.Dim, Fmt.Reset)
			return nil
		}
	}

	moveIDs := make([]interface{}, 0, len(plan.PostableDrafts))
	for _, m := range plan.PostableDrafts {
		moveIDs = append(moveIDs, m.MoveID)
	}
	if _, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.move", "action_post", []interface{}{moveIDs}, nil); err != nil {
		return fmt.Errorf("post draft moves: %v", err)
	}
	fmt.Printf("  %s✓ Posted %s%s\n", Fmt.Green, Pluralize(len(plan.PostableDrafts), "entry", "entries"), Fmt.Reset)
	if len(plan.ForeignMoves) > 0 {
		fmt.Printf("  %s↳ %s from other journals still need manual review (URLs above).%s\n",
			Fmt.Dim, Pluralize(len(plan.ForeignMoves), "entry", "entries"), Fmt.Reset)
	}
	fmt.Println()
	return nil
}

func foreignPostedCount(p accountFixPlan) int {
	n := 0
	for _, m := range p.ForeignMoves {
		if !m.Draft {
			n++
		}
	}
	return n
}

func printOdooAccountFixHelp() {
	f := Fmt
	fmt.Printf(`
%schb odoo accounts <code> fix%s — Reconcile a GL account to its local source.

Decomposes the gap between the account's posted GL balance and the linked local
account into fixable defects:

  %s•%s unposted (draft) entries from the account's own journal — e.g. an opening
    balance left in draft. SAFE to post; offered as an automatic fix.
  %s•%s posted entries from OTHER journals ("foreign") booked against this account —
    manual misc-operations entries that %schb odoo journals <id> fix%s can't see.
    Reported with direct Odoo URLs for manual review (never auto-deleted).

%sUSAGE%s
  %schb odoo accounts 550013 fix --dry-run%s   Preview only; no writes
  %schb odoo accounts 550013 fix%s             Post the safe drafts (after confirm)

%sOPTIONS%s
  %s--dry-run%s   Preview the plan; write nothing
  %s-y, --yes%s   Skip the confirmation prompt
`,
		f.Bold, f.Reset,
		f.Cyan, f.Reset, f.Cyan, f.Reset, f.Cyan, f.Reset,
		f.Bold, f.Reset,
		f.Cyan, f.Reset, f.Cyan, f.Reset,
		f.Bold, f.Reset,
		f.Yellow, f.Reset, f.Yellow, f.Reset,
	)
}
