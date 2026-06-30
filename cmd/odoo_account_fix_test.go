package cmd

import (
	"math"
	"testing"
)

// TestClassifyAccountFixLines models the real hacked-account (550013) shape:
// the own bank journal (47) holds the legitimate posted lines plus a DRAFT
// opening, and four "OPERATIONS DIVERSES" entries from a foreign journal inflate
// the GL. The classifier must (a) offer the draft opening as a postable fix,
// (b) flag only the foreign entries for review, (c) leave own posted lines alone.
func TestClassifyAccountFixLines(t *testing.T) {
	const ownJournal = 47
	const foreignJournal = 99

	lines := []accountFixLine{
		// own journal, posted: real 2025 drain, nets to -115,525.44
		{MoveID: 1, JournalID: ownJournal, JournalName: "hacked", Date: "2025-06-01", Balance: -115525.44, State: "posted", Name: "drain"},
		// own journal, DRAFT opening (uncounted)
		{MoveID: 2, JournalID: ownJournal, JournalName: "hacked", Date: "2025-01-01", Balance: 115525.44, State: "draft", Name: "Solde d'ouverture"},
		// foreign journal, posted: the +51,221.02 (two real + a self-cancelling pair)
		{MoveID: 3, JournalID: foreignJournal, JournalName: "OPERATIONS DIVERSES", Date: "2025-12-31", Balance: 762.26, State: "posted", Name: ""},
		{MoveID: 4, JournalID: foreignJournal, JournalName: "OPERATIONS DIVERSES", Date: "2025-12-31", Balance: -762.26, State: "posted", Name: ""},
		{MoveID: 5, JournalID: foreignJournal, JournalName: "OPERATIONS DIVERSES", Date: "2025-12-31", Balance: 35731.83, State: "posted", Name: ""},
		{MoveID: 6, JournalID: foreignJournal, JournalName: "OPERATIONS DIVERSES", Date: "2025-12-31", Balance: 15489.19, State: "posted", Name: ""},
	}

	p := classifyAccountFixLines(lines, ownJournal)

	eq := func(label string, got, want float64) {
		if math.Abs(got-want) > 0.005 {
			t.Errorf("%s = %.2f, want %.2f", label, got, want)
		}
	}
	eq("OwnPostedSum", p.OwnPostedSum, -115525.44)
	eq("ForeignPostedSum", p.ForeignPostedSum, 51221.02)
	eq("OwnDraftSum", p.OwnDraftSum, 115525.44)
	eq("GLPosted", p.GLPosted(), -64304.42)
	eq("ProjectedAfterPostingDrafts", p.ProjectedAfterPostingDrafts(), 51221.02)

	if len(p.PostableDrafts) != 1 || p.PostableDrafts[0].MoveID != 2 {
		t.Fatalf("PostableDrafts = %+v, want only move #2 (the draft opening)", p.PostableDrafts)
	}
	if len(p.ForeignMoves) != 4 {
		t.Fatalf("ForeignMoves count = %d, want 4", len(p.ForeignMoves))
	}
	for _, m := range p.ForeignMoves {
		if m.JournalID == ownJournal {
			t.Errorf("own-journal move #%d wrongly flagged foreign", m.MoveID)
		}
	}
	if !p.hasWork() {
		t.Error("hasWork() = false, want true")
	}
}

// TestClassifyAccountFixLines_NoOwnJournal: with ownJournalID == 0 the foreign
// split is disabled — nothing is treated as foreign, only drafts are surfaced.
func TestClassifyAccountFixLines_NoOwnJournal(t *testing.T) {
	lines := []accountFixLine{
		{MoveID: 1, JournalID: 5, Date: "2025-01-01", Balance: 100, State: "posted"},
		{MoveID: 2, JournalID: 9, Date: "2025-01-02", Balance: 50, State: "draft"},
	}
	p := classifyAccountFixLines(lines, 0)
	if len(p.ForeignMoves) != 0 {
		t.Errorf("ForeignMoves = %d, want 0 (foreign split disabled)", len(p.ForeignMoves))
	}
	if len(p.PostableDrafts) != 1 {
		t.Errorf("PostableDrafts = %d, want 1", len(p.PostableDrafts))
	}
	if p.GLPosted() != 100 {
		t.Errorf("GLPosted = %.2f, want 100", p.GLPosted())
	}
}

// TestClassifyAccountFixLines_Clean: a healthy account (all posted, own journal)
// has no work.
func TestClassifyAccountFixLines_Clean(t *testing.T) {
	lines := []accountFixLine{
		{MoveID: 1, JournalID: 44, Date: "2025-01-01", Balance: 1000, State: "posted"},
		{MoveID: 2, JournalID: 44, Date: "2025-02-01", Balance: -200, State: "posted"},
	}
	p := classifyAccountFixLines(lines, 44)
	if p.hasWork() {
		t.Errorf("hasWork() = true, want false for a clean account")
	}
	if p.GLPosted() != 800 {
		t.Errorf("GLPosted = %.2f, want 800", p.GLPosted())
	}
}
