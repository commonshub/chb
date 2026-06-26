package cmd

import "testing"

func TestComputeStatementBalanceRewrites(t *testing.T) {
	// S1 anchor start 0, lines +100 → end should be 100 (stored 100, ok).
	// S2 start stored 1 (wrong; should chain from 100), lines -30 → end 70.
	// S3 start stored 70 (ok), lines +5 → end 75 but stored 999 (stale).
	stmts := []statementBalanceInput{
		{ID: 1, Name: "S1", Start: 0, End: 100, LineSum: 100},
		{ID: 2, Name: "S2", Start: 1, End: 70, LineSum: -30},
		{ID: 3, Name: "S3", Start: 70, End: 999, LineSum: 5},
	}
	got := computeStatementBalanceRewrites(stmts)

	// S1 agrees → no rewrite. S2 start wrong. S3 end wrong.
	if len(got) != 2 {
		t.Fatalf("rewrites = %d, want 2: %+v", len(got), got)
	}
	if got[0].ID != 2 || !got[0].StartChanged || got[0].NewStart != 100 || got[0].EndChanged {
		t.Fatalf("S2 rewrite wrong: %+v", got[0])
	}
	if got[0].NewEnd != 70 {
		t.Fatalf("S2 new end = %.2f, want 70 (chained start 100 + (-30))", got[0].NewEnd)
	}
	if got[1].ID != 3 || got[1].StartChanged || !got[1].EndChanged || got[1].NewEnd != 75 {
		t.Fatalf("S3 rewrite wrong: %+v", got[1])
	}
}

// A fully-consistent chain yields no rewrites.
func TestComputeStatementBalanceRewritesNoop(t *testing.T) {
	stmts := []statementBalanceInput{
		{ID: 1, Name: "S1", Start: 9326.90, End: 9426.90, LineSum: 100},
		{ID: 2, Name: "S2", Start: 9426.90, End: 9326.90, LineSum: -100},
	}
	if got := computeStatementBalanceRewrites(stmts); len(got) != 0 {
		t.Fatalf("expected no rewrites, got %+v", got)
	}
}

// A stale end cascades: fixing S2's end re-chains S3's start. The first
// statement is the anchor and is left to the opening_in_line repair.
func TestComputeStatementBalanceRewritesCascades(t *testing.T) {
	stmts := []statementBalanceInput{
		{ID: 1, Name: "S1", Start: 0, End: 100, LineSum: 100},      // anchor, consistent → skipped
		{ID: 2, Name: "S2", Start: 100, End: 65000, LineSum: 200},  // stale end 65k; real 300
		{ID: 3, Name: "S3", Start: 65000, End: 65100, LineSum: -50}, // start re-chains from 300
	}
	got := computeStatementBalanceRewrites(stmts)
	if len(got) != 2 {
		t.Fatalf("want 2 rewrites, got %d: %+v", len(got), got)
	}
	if got[0].ID != 2 || got[0].NewEnd != 300 { // S2 end corrected (100+200)
		t.Fatalf("S2 new end = %.2f, want 300", got[0].NewEnd)
	}
	if got[1].ID != 3 || got[1].NewStart != 300 || got[1].NewEnd != 250 { // S3 re-chained from 300
		t.Fatalf("S3 re-chain wrong: start=%.2f end=%.2f, want 300/250", got[1].NewStart, got[1].NewEnd)
	}
}

// The first statement is never rewritten — it is the chain anchor / opening.
func TestComputeStatementBalanceRewritesSkipsAnchor(t *testing.T) {
	stmts := []statementBalanceInput{
		{ID: 1, Name: "S1", Start: 0.39, End: 20673.56, LineSum: 20673.17}, // wrong, but anchor → skipped
		{ID: 2, Name: "S2", Start: 20673.56, End: 20673.56, LineSum: 0},
	}
	got := computeStatementBalanceRewrites(stmts)
	for _, rw := range got {
		if rw.ID == 1 {
			t.Fatalf("anchor statement must not be rewritten: %+v", rw)
		}
	}
}

func TestPickStatementForDate(t *testing.T) {
	// Statements sorted by date asc; the last (open) has the latest date.
	stmts := []statementDate{
		{ID: 1, Date: "2025-04-01"},
		{ID: 2, Date: "2025-09-25"}, // same-day pair; first one wins
		{ID: 3, Date: "2025-09-25"},
		{ID: 9, Date: "2026-06-25"}, // open
	}
	cases := []struct {
		date string
		want int
	}{
		{"2025-01-15", 1}, // before first stmt → first (opening period)
		{"2025-04-01", 1}, // on the boundary → that statement
		{"2025-06-10", 2}, // between Apr and Sep → Sep (first same-day)
		{"2025-09-25", 2}, // same-day → earliest same-day statement
		{"2026-03-01", 9}, // between Sep and open → open
		{"2026-06-26", 9}, // after every statement date → last (open)
	}
	for _, c := range cases {
		if got := pickStatementForDate(stmts, c.date); got != c.want {
			t.Fatalf("pickStatementForDate(%s) = %d, want %d", c.date, got, c.want)
		}
	}
	if pickStatementForDate(nil, "2025-01-01") != 0 {
		t.Fatal("no statements → 0")
	}
}
