package cmd

import (
	"reflect"
	"testing"
	"time"
)

func TestJournalLineSinceDomain(t *testing.T) {
	// No cutoff → just the journal filter.
	base := journalLineSinceDomain(44, time.Time{})
	wantBase := []interface{}{[]interface{}{"journal_id", "=", 44}}
	if !reflect.DeepEqual(base, wantBase) {
		t.Errorf("no since: got %v, want %v", base, wantBase)
	}

	// With cutoff → adds a date >= clause (Odoo date format).
	since := time.Date(2026, 6, 30, 0, 0, 0, 0, time.UTC)
	got := journalLineSinceDomain(44, since)
	want := []interface{}{
		[]interface{}{"journal_id", "=", 44},
		[]interface{}{"date", ">=", "2026-06-30"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("with since: got %v, want %v", got, want)
	}
}

func TestParseSinceDateCompactForm(t *testing.T) {
	tm, ok := ParseSinceDate("20260630")
	if !ok {
		t.Fatalf("ParseSinceDate(20260630) failed")
	}
	if tm.Format("2006-01-02") != "2026-06-30" {
		t.Errorf("ParseSinceDate(20260630) = %s, want 2026-06-30", tm.Format("2006-01-02"))
	}
}
