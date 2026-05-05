package cmd

import (
	"reflect"
	"strconv"
	"testing"
)

func TestParseSpreadInput(t *testing.T) {
	cases := []struct {
		in    string
		want  []string
		isErr bool
	}{
		{in: "", want: nil},
		{in: "  ", want: nil},
		{
			in:   "2025",
			want: spreadMonths("2025-01", "2025-02", "2025-03", "2025-04", "2025-05", "2025-06", "2025-07", "2025-08", "2025-09", "2025-10", "2025-11", "2025-12"),
		},
		{
			in: "2024-2025",
			want: append(
				spreadMonths("2024-01", "2024-02", "2024-03", "2024-04", "2024-05", "2024-06", "2024-07", "2024-08", "2024-09", "2024-10", "2024-11", "2024-12"),
				spreadMonths("2025-01", "2025-02", "2025-03", "2025-04", "2025-05", "2025-06", "2025-07", "2025-08", "2025-09", "2025-10", "2025-11", "2025-12")...,
			),
		},
		{in: "202501-202503", want: spreadMonths("2025-01", "2025-02", "2025-03")},
		{in: "2025-01-2025-03", want: spreadMonths("2025-01", "2025-02", "2025-03")},
		{in: "2025-04", want: spreadMonths("2025-04")},
		{in: "202504", want: spreadMonths("2025-04")},
		{in: "202501,202503,202507", want: spreadMonths("2025-01", "2025-03", "2025-07")},
		{in: "2025-01,2025-03,2025-07", want: spreadMonths("2025-01", "2025-03", "2025-07")},
		{
			// dedupe + sort across mixed forms
			in:   "2025-12,2025,2025-01",
			want: spreadMonths("2025-01", "2025-02", "2025-03", "2025-04", "2025-05", "2025-06", "2025-07", "2025-08", "2025-09", "2025-10", "2025-11", "2025-12"),
		},
		{in: "abcd", isErr: true},
		{in: "2025-13", isErr: true},
		{in: "2026-2024", isErr: true},
		{in: "202503-202501", isErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			got, err := ParseSpreadInput(tc.in)
			if tc.isErr {
				if err == nil {
					t.Fatalf("expected error for %q, got %v", tc.in, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("input %q\n  got:  %v\n  want: %v", tc.in, got, tc.want)
			}
		})
	}
}

func TestBuildSpreadEntriesTotalAddsUp(t *testing.T) {
	cases := []struct {
		months []string
		total  float64
	}{
		{months: spreadMonths("2025-01"), total: 100},
		{months: spreadMonths("2025-01", "2025-02", "2025-03"), total: 100},  // doesn't divide evenly
		{months: spreadMonths("2025-01", "2025-02", "2025-03"), total: -100}, // negative
		{months: spreadMonths("2025-01", "2025-02"), total: 0.03},
	}
	for _, tc := range cases {
		t.Run("", func(t *testing.T) {
			entries := BuildSpreadEntries(tc.months, tc.total)
			if len(entries) != len(tc.months) {
				t.Fatalf("got %d entries, want %d", len(entries), len(tc.months))
			}
			var sum float64
			for _, e := range entries {
				v, err := strconv.ParseFloat(e.Amount, 64)
				if err != nil {
					t.Fatalf("amount %q: %v", e.Amount, err)
				}
				sum += v
			}
			if diff := sum - tc.total; diff > 0.001 || diff < -0.001 {
				t.Fatalf("sum %.4f != total %.4f (entries=%v)", sum, tc.total, entries)
			}
		})
	}
}

func spreadMonths(ms ...string) []string {
	out := make([]string, len(ms))
	copy(out, ms)
	return out
}
