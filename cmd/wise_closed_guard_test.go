package cmd

import "testing"

// TestWiseClosingPlug pins the guard that makes `wise sync` report-only for a
// closed journal while leaving a fresh/open journal fully syncable.
func TestWiseClosingPlug(t *testing.T) {
	const maxDate = "2025-04-16" // last CSV transaction

	opening := OdooCacheLine{Date: "2025-01-01", PaymentRef: "Solde de départ 2025-01-01"}
	tx1 := OdooCacheLine{Date: "2025-02-10", PaymentRef: "Card payment", UniqueImportID: "wise:1:TRANSFER-1", Amount: -50}
	tx2 := OdooCacheLine{Date: "2025-04-16", PaymentRef: "Converted EUR", UniqueImportID: "wise:1:TRANSFER-2", Amount: -20}
	closing := OdooCacheLine{Date: "2025-12-31", PaymentRef: "Solde compte", Amount: -680.06}

	t.Run("closed journal is detected", func(t *testing.T) {
		lines := []OdooCacheLine{opening, tx1, tx2, closing}
		plug := wiseClosingPlug(lines, maxDate)
		if plug == nil {
			t.Fatal("expected a closing plug on a closed journal, got nil")
		}
		if plug.Date != "2025-12-31" {
			t.Fatalf("expected the year-end plug, got %s (%q)", plug.Date, plug.PaymentRef)
		}
	})

	t.Run("open journal has no plug (stays syncable)", func(t *testing.T) {
		lines := []OdooCacheLine{opening, tx1, tx2}
		if plug := wiseClosingPlug(lines, maxDate); plug != nil {
			t.Fatalf("open journal must have no closing plug, got %s (%q)", plug.Date, plug.PaymentRef)
		}
	})

	t.Run("opening at cutoff is not mistaken for a closing plug", func(t *testing.T) {
		// Only the opening balance entry, dated before the range → not a plug.
		lines := []OdooCacheLine{opening, tx1}
		if plug := wiseClosingPlug(lines, maxDate); plug != nil {
			t.Fatalf("opening balance must not count as closing plug, got %s", plug.Date)
		}
	})

	t.Run("a within-range balance entry does not count", func(t *testing.T) {
		midBalance := OdooCacheLine{Date: "2025-03-01", PaymentRef: "Solde intermédiaire"}
		lines := []OdooCacheLine{opening, tx1, midBalance, tx2}
		if plug := wiseClosingPlug(lines, maxDate); plug != nil {
			t.Fatalf("in-range balance entry must not count as closing plug, got %s", plug.Date)
		}
	})

	t.Run("latest post-range plug wins", func(t *testing.T) {
		earlyPlug := OdooCacheLine{Date: "2025-06-30", PaymentRef: "Solde compte interim"}
		lines := []OdooCacheLine{opening, tx1, earlyPlug, closing}
		plug := wiseClosingPlug(lines, maxDate)
		if plug == nil || plug.Date != "2025-12-31" {
			t.Fatalf("expected the latest plug 2025-12-31, got %v", plug)
		}
	})
}
