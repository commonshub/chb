package cmd

import (
	"strings"
	"testing"

	odoosource "github.com/CommonsHub/chb/providers/odoo"
)

func TestOdooDataNamespaceFromEnv(t *testing.T) {
	t.Run("explicit ODOO_DATABASE", func(t *testing.T) {
		t.Setenv("ODOO_DATABASE", "citizenspring-test")
		t.Setenv("ODOO_URL", "https://citizenspring-test.odoo.com")
		if got := odooDataNamespace(); got != "citizenspring-test" {
			t.Fatalf("namespace = %q, want citizenspring-test", got)
		}
	})

	t.Run("derived from ODOO_URL when DB empty", func(t *testing.T) {
		t.Setenv("ODOO_DATABASE", "")
		t.Setenv("ODOO_URL", "https://citizen-spring-vzw.odoo.com")
		if got := odooDataNamespace(); got != "citizen-spring-vzw" {
			t.Fatalf("namespace = %q, want citizen-spring-vzw", got)
		}
	})

	t.Run("empty when no odoo configured", func(t *testing.T) {
		t.Setenv("ODOO_DATABASE", "")
		t.Setenv("ODOO_URL", "")
		if got := odooDataNamespace(); got != "" {
			t.Fatalf("namespace = %q, want empty", got)
		}
	})
}

func TestInitOdooDataNamespaceWiresPaths(t *testing.T) {
	defer odoosource.SetPathNamespace("")
	t.Setenv("ODOO_DATABASE", "citizenspring-test")
	t.Setenv("ODOO_URL", "https://citizenspring-test.odoo.com")

	InitOdooDataNamespace()

	// The cmd-side path builders must all carry the namespace segment.
	if got := odooPendingMonthPath("/data", "2026", "05"); !strings.Contains(got, "/odoo/citizenspring-test/pending/") {
		t.Fatalf("pending path not namespaced: %q", got)
	}
	if got := odooJournalLinesCachePath(48); !strings.Contains(got, "/odoo/citizenspring-test/journals/") {
		t.Fatalf("journal cache path not namespaced: %q", got)
	}
	if got := partnerMergesPath(); !strings.Contains(got, "/odoo/citizenspring-test/pending/") {
		t.Fatalf("partner-merges path not namespaced: %q", got)
	}
}

func TestSyncCursorKeyNamespaced(t *testing.T) {
	defer odoosource.SetPathNamespace("")

	odoosource.SetPathNamespace("")
	if got := SyncCursorKeyForOdooJournal(48); got != "odoo.journal.48" {
		t.Fatalf("un-namespaced cursor key = %q", got)
	}

	odoosource.SetPathNamespace("citizenspring-test")
	if got := SyncCursorKeyForOdooJournal(48); got != "odoo.citizenspring-test.journal.48" {
		t.Fatalf("namespaced cursor key = %q", got)
	}
}
