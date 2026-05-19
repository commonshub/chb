package cmd

import "testing"

func TestParseDateSpecFormats(t *testing.T) {
	tests := []struct {
		in         string
		start      string
		end        string
		startMonth string
		endMonth   string
		precision  string
	}{
		{"20260519", "2026-05-19", "2026-05-20", "2026-05", "2026-05", "day"},
		{"2026-05-19", "2026-05-19", "2026-05-20", "2026-05", "2026-05", "day"},
		{"2026/05/19", "2026-05-19", "2026-05-20", "2026-05", "2026-05", "day"},
		{"202605", "2026-05-01", "2026-06-01", "2026-05", "2026-05", "month"},
		{"2026-05", "2026-05-01", "2026-06-01", "2026-05", "2026-05", "month"},
		{"2026/5", "2026-05-01", "2026-06-01", "2026-05", "2026-05", "month"},
		{"2026", "2026-01-01", "2027-01-01", "2026-01", "2026-12", "year"},
		{"2026/Q1", "2026-01-01", "2026-04-01", "2026-01", "2026-03", "quarter"},
		{"2026-Q4", "2026-10-01", "2027-01-01", "2026-10", "2026-12", "quarter"},
		{"2026S2", "2026-07-01", "2027-01-01", "2026-07", "2026-12", "semester"},
	}

	for _, tt := range tests {
		spec, ok := ParseDateSpec(tt.in)
		if !ok {
			t.Fatalf("ParseDateSpec(%q) failed", tt.in)
		}
		if got := spec.Start.Format("2006-01-02"); got != tt.start {
			t.Fatalf("ParseDateSpec(%q) start = %s, want %s", tt.in, got, tt.start)
		}
		if got := spec.End.Format("2006-01-02"); got != tt.end {
			t.Fatalf("ParseDateSpec(%q) end = %s, want %s", tt.in, got, tt.end)
		}
		if spec.StartMonth != tt.startMonth || spec.EndMonth != tt.endMonth || spec.Precision != tt.precision {
			t.Fatalf("ParseDateSpec(%q) = months %s..%s precision %s, want %s..%s %s",
				tt.in, spec.StartMonth, spec.EndMonth, spec.Precision, tt.startMonth, tt.endMonth, tt.precision)
		}
	}
}

func TestParseDateSpecInvalid(t *testing.T) {
	for _, in := range []string{"2026/Q5", "2026/S3", "2026/13", "20260231", "2026/00"} {
		if _, ok := ParseDateSpec(in); ok {
			t.Fatalf("ParseDateSpec(%q) unexpectedly succeeded", in)
		}
	}
}

func TestParseMonthRangeArgSkipsFlagValues(t *testing.T) {
	start, end, ok := ParseMonthRangeArg([]string{"--since", "2026/Q1", "2026/S2"})
	if !ok {
		t.Fatal("expected positional range")
	}
	if start != "2026-07" || end != "2026-12" {
		t.Fatalf("range = %s..%s, want 2026-07..2026-12", start, end)
	}
}

func TestParseDateRangeSpecExplicitDayRange(t *testing.T) {
	for _, in := range []string{"20260110-20260305", "2026-01-10-2026-03-05", "2026/01/10-2026/03/05"} {
		spec, ok := ParseDateRangeSpec(in)
		if !ok {
			t.Fatalf("ParseDateRangeSpec(%q) failed", in)
		}
		if got := spec.Start.Format("2006-01-02"); got != "2026-01-10" {
			t.Fatalf("ParseDateRangeSpec(%q) start = %s", in, got)
		}
		if got := spec.End.Format("2006-01-02"); got != "2026-03-06" {
			t.Fatalf("ParseDateRangeSpec(%q) end = %s", in, got)
		}
		if spec.StartMonth != "2026-01" || spec.EndMonth != "2026-03" {
			t.Fatalf("ParseDateRangeSpec(%q) months = %s..%s", in, spec.StartMonth, spec.EndMonth)
		}
	}
}

func TestParseYearMonthArgDoesNotCollapseRangeToFirstMonth(t *testing.T) {
	if _, _, ok := ParseYearMonthArg([]string{"2026/Q1"}); ok {
		t.Fatal("ParseYearMonthArg should not accept quarter ranges")
	}
}

func TestParseDateValueRejectsRangeOnlyFormats(t *testing.T) {
	for _, in := range []string{"2026/Q1", "2026/S2", "20260101-20260331"} {
		if _, ok := ParseDateValue(in); ok {
			t.Fatalf("ParseDateValue(%q) unexpectedly succeeded", in)
		}
	}
}

func TestResolveSinceMonthHistoryAlwaysStartsFullHistory(t *testing.T) {
	got, ok := ResolveSinceMonth([]string{"--history"}, "finance")
	if !ok {
		t.Fatal("expected history mode")
	}
	if got != "2024-01" {
		t.Fatalf("expected 2024-01, got %q", got)
	}
}

func TestResolveSinceMonthSinceStillWinsOverHistory(t *testing.T) {
	got, ok := ResolveSinceMonth([]string{"--history", "--since", "2025/06"}, "finance")
	if !ok {
		t.Fatal("expected since/history mode")
	}
	if got != "2025-06" {
		t.Fatalf("expected 2025-06, got %q", got)
	}
}

func TestResolveSinceMonthRejectsQuarter(t *testing.T) {
	if got, ok := ResolveSinceMonth([]string{"--since", "2026/Q1"}, "finance"); ok {
		t.Fatalf("expected --since quarter to be rejected, got %q", got)
	}
}
