package cmd

import "testing"

func TestReviewProposedHelpers(t *testing.T) {
	items := []reviewSuspenseItem{
		{ProposedCode: "700150", StatementLine: 1},
		{ProposedCode: "730000", StatementLine: 2},
		{ProposedCode: "700150", StatementLine: 3}, // dup code
		{ProposedCode: "", ImportID: "x"},          // unresolved
		{ProposedCode: "", ImportID: ""},           // manual
	}
	got := distinctProposedCodes(items)
	if len(got) != 2 || got[0] != "700150" || got[1] != "730000" {
		t.Fatalf("distinctProposedCodes = %v, want [700150 730000]", got)
	}

	names := map[string]string{"700150": "Event tickets"}
	if l := codeLabel("700150", names); l != "700150 Event tickets" {
		t.Fatalf("codeLabel with name = %q", l)
	}
	if l := codeLabel("730000", names); l != "730000" {
		t.Fatalf("codeLabel without name = %q, want bare code", l)
	}
	if l := codeLabel("", names); l != "?" {
		t.Fatalf("codeLabel empty = %q, want ?", l)
	}
}
