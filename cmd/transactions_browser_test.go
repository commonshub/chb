package cmd

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	nostrsource "github.com/CommonsHub/chb/sources/nostr"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/nbd-wtf/go-nostr"
)

func TestParseTransactionAmountCell(t *testing.T) {
	tests := []struct {
		in   string
		want float64
	}{
		{"+€10.50", 10.5},
		{"-€2.00", -2},
		{"+12.34 CHT", 12.34},
		{" -1,234.56 EUR", -1234.56},
		{styleGreen.Render("+€7.25"), 7.25},
	}

	for _, tt := range tests {
		if got := parseTransactionAmountCell(tt.in); got != tt.want {
			t.Fatalf("parseTransactionAmountCell(%q) = %v, want %v", tt.in, got, tt.want)
		}
	}
}

func TestSortTransactionsForAmountUsesNumericValue(t *testing.T) {
	txs := []TransactionEntry{
		{ID: "ten", Type: "CREDIT", NormalizedAmount: 10, Currency: "EUR", Timestamp: 1},
		{ID: "two", Type: "CREDIT", NormalizedAmount: 2, Currency: "EUR", Timestamp: 2},
		{ID: "minus-hundred", Type: "DEBIT", NormalizedAmount: 100, Currency: "EUR", Timestamp: 3},
	}

	sortTransactionsForColumn(txs, amountColumnIndex, true)
	if got := transactionIDs(txs); !reflect.DeepEqual(got, []string{"minus-hundred", "two", "ten"}) {
		t.Fatalf("ascending amount sort = %#v", got)
	}

	sortTransactionsForColumn(txs, amountColumnIndex, false)
	if got := transactionIDs(txs); !reflect.DeepEqual(got, []string{"ten", "two", "minus-hundred"}) {
		t.Fatalf("descending amount sort = %#v", got)
	}
}

func TestTransactionTableDisplaysAssignmentsFromTags(t *testing.T) {
	tx := TransactionEntry{
		ID:       "tagged",
		Type:     "CREDIT",
		Currency: "EUR",
		Tags:     [][]string{{"category", "accounting"}, {"collective", "commonshub"}},
	}

	if got := txDisplayCategory(tx); got != "accounting" {
		t.Fatalf("txDisplayCategory() = %q, want accounting", got)
	}
	if got := txDisplayCollective(tx); got != "commonshub" {
		t.Fatalf("txDisplayCollective() = %q, want commonshub", got)
	}

	rows := buildStickerRows([]TransactionEntry{tx})
	if got := rows[0][2]; got != " commonshub" {
		t.Fatalf("collective cell = %q, want commonshub", got)
	}
	if got := rows[0][3]; got != " accounting" {
		t.Fatalf("category cell = %q, want accounting", got)
	}
}

func TestCategoryOptionsFilterByTransactionDirection(t *testing.T) {
	cats := []CategoryDef{
		{Slug: "tickets", Direction: "income"},
		{Slug: "catering", Direction: "expense"},
		{Slug: "catering", Direction: "income"},
		{Slug: "rent", Direction: "expense"},
	}

	expenseTx := TransactionEntry{Type: "DEBIT", NormalizedAmount: 12, Currency: "EUR"}
	if got := categoryOptionsForTransaction(expenseTx, cats); !reflect.DeepEqual(got, []string{"catering", "rent"}) {
		t.Fatalf("expense category options = %#v", got)
	}

	incomeTx := TransactionEntry{Type: "CREDIT", NormalizedAmount: 12, Currency: "EUR"}
	if got := categoryOptionsForTransaction(incomeTx, cats); !reflect.DeepEqual(got, []string{"catering", "tickets"}) {
		t.Fatalf("income category options = %#v", got)
	}
}

func TestTransactionTableColumnsHideAccountWhenFiltered(t *testing.T) {
	cols := transactionTableColumns(false, true)
	for _, col := range cols {
		if col.Kind == txColumnSource {
			t.Fatalf("account/source column should be hidden when account filter is set: %#v", cols)
		}
	}
	collective := txColumnIndex(cols, txColumnCollective)
	category := txColumnIndex(cols, txColumnCategory)
	if collective < 0 || category < 0 {
		t.Fatalf("missing collective/category columns: %#v", cols)
	}
	if category != collective+1 {
		t.Fatalf("category column index = %d, want directly after collective %d", category, collective)
	}
	if cols[collective].MinWidth < len(cols[collective].Header)+2 || cols[category].MinWidth < len(cols[category].Header)+2 {
		t.Fatalf("collective/category min widths too small: %#v / %#v", cols[collective], cols[category])
	}
}

func TestTransactionTableSelectionMarker(t *testing.T) {
	tx := TransactionEntry{ID: "tx-1", Type: "CREDIT", Currency: "EUR", Timestamp: 1776254400}
	cols := transactionTableColumns(true, true)
	rows := buildStickerRowsForTable([]TransactionEntry{tx}, cols, map[string]bool{"tx-1": true})
	if got := strings.TrimSpace(rows[0][0]); got != "[x]" {
		t.Fatalf("selection marker = %q, want [x]", got)
	}

	rows = buildStickerRowsForTable([]TransactionEntry{tx}, cols, nil)
	if got := strings.TrimSpace(rows[0][0]); got != "[ ]" {
		t.Fatalf("unselected marker = %q, want [ ]", got)
	}
}

func TestToggleAllFilteredSelectionUsesCurrentView(t *testing.T) {
	txs := []TransactionEntry{
		{ID: "tx-1", Type: "CREDIT", Currency: "EUR", Category: "visible", Timestamp: 1},
		{ID: "tx-2", Type: "CREDIT", Currency: "EUR", Category: "visible", Timestamp: 2},
		{ID: "tx-3", Type: "CREDIT", Currency: "EUR", Category: "hidden", Timestamp: 3},
	}
	cols := transactionTableColumns(true, true)
	m := txBrowserModel{
		table:         newStickerTableForColumns(txs, 0, 0, cols, nil),
		columns:       cols,
		txs:           txs,
		selectedTxIDs: map[string]bool{},
		selectedCol:   txColumnIndex(cols, txColumnCategory),
		filterStr:     "visible",
		sortCol:       -1,
	}

	m.toggleAllFilteredSelection()
	if !m.selectedTxIDs["tx-1"] || !m.selectedTxIDs["tx-2"] || m.selectedTxIDs["tx-3"] {
		t.Fatalf("selection after select all visible = %#v", m.selectedTxIDs)
	}

	m.toggleAllFilteredSelection()
	if len(m.selectedTxIDs) != 0 {
		t.Fatalf("selection after deselect all visible = %#v", m.selectedTxIDs)
	}
}

func TestBulkCategoryAssignmentUpdatesSelectedTransactions(t *testing.T) {
	appDir := t.TempDir()
	dataDir := filepath.Join(appDir, "data")
	t.Setenv("APP_DATA_DIR", appDir)
	t.Setenv("DATA_DIR", dataDir)

	ts := time.Date(2026, 4, 15, 12, 0, 0, 0, time.UTC).Unix()
	txs := []TransactionEntry{
		{ID: "stripe:txn_1", Provider: "stripe", StripeChargeID: "txn_1", Type: "DEBIT", NormalizedAmount: 10, Currency: "EUR", Timestamp: ts},
		{ID: "stripe:txn_2", Provider: "stripe", StripeChargeID: "txn_2", Type: "DEBIT", NormalizedAmount: 20, Currency: "EUR", Timestamp: ts},
		{ID: "stripe:txn_3", Provider: "stripe", StripeChargeID: "txn_3", Type: "DEBIT", NormalizedAmount: 30, Currency: "EUR", Timestamp: ts},
	}
	txFile := TransactionsFile{Year: "2026", Month: "04", Transactions: append([]TransactionEntry(nil), txs...)}
	data, err := json.MarshalIndent(txFile, "", "  ")
	if err != nil {
		t.Fatalf("marshal transactions: %v", err)
	}
	writeJSONFixture(t, filepath.Join(dataDir, "2026", "04", "generated", "transactions.json"), string(data))

	cols := transactionTableColumns(true, true)
	m := txBrowserModel{
		table:         newStickerTableForColumns(txs, 0, 0, cols, map[string]bool{"stripe:txn_1": true, "stripe:txn_2": true}),
		columns:       cols,
		txs:           txs,
		selectedTxIDs: map[string]bool{"stripe:txn_1": true, "stripe:txn_2": true},
		mode:          modeEditCategory,
		bulkEdit:      true,
		editInput:     "accounting",
		sortCol:       -1,
	}
	cmd := m.commitInlineEdit()
	if cmd == nil {
		t.Fatal("bulk assignment did not start Nostr publish command")
	}
	if !strings.Contains(m.statusText, "Posting 2 Nostr event(s) to 0 relay(s)") {
		t.Fatalf("posting status = %q", m.statusText)
	}
	msg := cmd()
	result, ok := msg.(txPublishResultMsg)
	if !ok {
		t.Fatalf("publish msg = %#v, want txPublishResultMsg", msg)
	}
	if result.Events != 2 || result.Published != 0 || result.Err == nil {
		t.Fatalf("publish result = %#v, want 2 attempted events and no-key error", result)
	}

	if got := m.txs[0].Category; got != "accounting" {
		t.Fatalf("selected tx 1 category = %q, want accounting", got)
	}
	if got := m.txs[1].Category; got != "accounting" {
		t.Fatalf("selected tx 2 category = %q, want accounting", got)
	}
	if got := m.txs[2].Category; got != "" {
		t.Fatalf("unselected tx category = %q, want empty", got)
	}

	saved := LoadTransactionsWithPII(dataDir, "2026", "04")
	if saved == nil || len(saved.Transactions) != 3 {
		t.Fatalf("saved transactions = %#v", saved)
	}
	if saved.Transactions[0].Category != "accounting" || saved.Transactions[1].Category != "accounting" || saved.Transactions[2].Category != "" {
		t.Fatalf("saved categories = %#v", saved.Transactions)
	}

	annData, err := os.ReadFile(nostrsource.Path(dataDir, "2026", "04", nostrsource.AnnotationsFile))
	if err != nil {
		t.Fatalf("read nostr annotations: %v", err)
	}
	var cache NostrAnnotationCache
	if err := json.Unmarshal(annData, &cache); err != nil {
		t.Fatalf("unmarshal annotations: %v", err)
	}
	if cache.Annotations["stripe:txn:txn_1"].Category != "accounting" || cache.Annotations["stripe:txn:txn_2"].Category != "accounting" {
		t.Fatalf("annotations = %#v", cache.Annotations)
	}
	if cache.Annotations["stripe:txn:txn_3"] != nil {
		t.Fatalf("unselected tx should not have annotation: %#v", cache.Annotations["stripe:txn:txn_3"])
	}
}

func TestCategoryPickerReturnsToTableAndShowsPublishStatus(t *testing.T) {
	appDir := t.TempDir()
	dataDir := filepath.Join(appDir, "data")
	t.Setenv("APP_DATA_DIR", appDir)
	t.Setenv("DATA_DIR", dataDir)

	ts := time.Date(2026, 4, 15, 12, 0, 0, 0, time.UTC).Unix()
	tx := TransactionEntry{
		ID:             "stripe:txn_1",
		Provider:       "stripe",
		StripeChargeID: "txn_1",
		Type:           "DEBIT",
		Currency:       "EUR",
		Timestamp:      ts,
	}
	txFile := TransactionsFile{Year: "2026", Month: "04", Transactions: []TransactionEntry{tx}}
	data, err := json.MarshalIndent(txFile, "", "  ")
	if err != nil {
		t.Fatalf("marshal transactions: %v", err)
	}
	writeJSONFixture(t, filepath.Join(dataDir, "2026", "04", "generated", "transactions.json"), string(data))

	cols := transactionTableColumns(true, true)
	m := txBrowserModel{
		table:         newStickerTableForColumns([]TransactionEntry{tx}, 0, 0, cols, nil),
		columns:       cols,
		txs:           []TransactionEntry{tx},
		detailTx:      &tx,
		mode:          modeEditCategory,
		editInput:     "accounting",
		selectedTxIDs: map[string]bool{},
		sortCol:       -1,
	}

	model, cmd := m.updateInlineEdit(tea.KeyMsg{Type: tea.KeyEnter})
	got := model.(txBrowserModel)
	if got.mode != modeTable {
		t.Fatalf("mode after category picker enter = %v, want modeTable", got.mode)
	}
	if got.detailTx != nil {
		t.Fatalf("detailTx after category picker enter = %#v, want nil", got.detailTx)
	}
	if cmd == nil {
		t.Fatal("expected Nostr publish command")
	}
	if !strings.Contains(got.statusText, "Posting 1 Nostr event(s) to 0 relay(s)") {
		t.Fatalf("status after picker = %q", got.statusText)
	}

	msg := cmd()
	result := msg.(txPublishResultMsg)
	model, clearCmd := got.Update(result)
	got = model.(txBrowserModel)
	if !got.statusError || !strings.Contains(got.statusText, "no Nostr identity configured") {
		t.Fatalf("error status = %q error=%v", got.statusText, got.statusError)
	}
	if clearCmd == nil {
		t.Fatal("expected delayed clear command for error status")
	}
}

func TestEnsureTransactionAssignmentSettingsCreatesMissingCategoryAndCollective(t *testing.T) {
	appDir := t.TempDir()
	t.Setenv("APP_DATA_DIR", appDir)

	if err := SaveCategories(nil); err != nil {
		t.Fatalf("SaveCategories: %v", err)
	}
	if err := SaveCollectives(map[string]Collective{}); err != nil {
		t.Fatalf("SaveCollectives: %v", err)
	}

	tx := TransactionEntry{
		Type:             "DEBIT",
		NormalizedAmount: 12,
		Currency:         "EUR",
		Category:         "bookkeeping",
		Collective:       "new-collective",
	}
	ensureTransactionAssignmentSettings(tx)

	cats := LoadCategories()
	foundCategory := false
	for _, cat := range cats {
		if cat.Slug == "bookkeeping" && cat.Label == "Bookkeeping" && cat.Direction == "expense" {
			foundCategory = true
		}
	}
	if !foundCategory {
		t.Fatalf("missing persisted category in %#v", cats)
	}

	collectives := LoadCollectives()
	if _, ok := collectives["new-collective"]; !ok {
		t.Fatalf("missing persisted collective in %#v", collectives)
	}

	for _, path := range []string{
		filepath.Join(appDir, "settings", "categories.json"),
		filepath.Join(appDir, "settings", "collectives.json"),
	} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("expected settings file %s: %v", path, err)
		}
	}
}

func TestBuildTransactionAnnotationEventUsesNIP73Tags(t *testing.T) {
	tx := TransactionEntry{
		Provider:         "stripe",
		StripeChargeID:   "txn_123",
		Category:         "bookkeeping",
		Collective:       "new-collective",
		Event:            "evt_1",
		NormalizedAmount: 12.5,
		Currency:         "EUR",
	}

	ev := buildTransactionAnnotationEvent(tx)
	if ev == nil {
		t.Fatal("buildTransactionAnnotationEvent returned nil")
	}
	if ev.Kind != 1111 {
		t.Fatalf("event kind = %d, want 1111", ev.Kind)
	}

	for _, want := range [][]string{
		{"I", "stripe:txn:txn_123"},
		{"K", "stripe:txn"},
		{"i", "stripe:txn:txn_123"},
		{"k", "stripe:txn"},
		{"category", "bookkeeping"},
		{"collective", "new-collective"},
		{"event", "evt_1"},
		{"amount", "12.50", "EUR"},
	} {
		if !hasNostrTag(ev.Tags, want) {
			t.Fatalf("missing tag %#v in %#v", want, ev.Tags)
		}
	}
}

func TestPersistTransactionAnnotationToNostrSource(t *testing.T) {
	appDir := t.TempDir()
	dataDir := filepath.Join(appDir, "data")
	t.Setenv("APP_DATA_DIR", appDir)
	t.Setenv("DATA_DIR", dataDir)

	persistTransactionAnnotationToNostrSource(TransactionEntry{
		Provider:       "stripe",
		StripeChargeID: "txn_123",
		Category:       "bookkeeping",
		Collective:     "new-collective",
		Timestamp:      time.Date(2026, 4, 15, 12, 0, 0, 0, time.UTC).Unix(),
		Tags:           [][]string{{"category", "bookkeeping"}, {"collective", "new-collective"}},
	}, "event-id", "author-pubkey")

	data, err := os.ReadFile(nostrsource.Path(dataDir, "2026", "04", nostrsource.AnnotationsFile))
	if err != nil {
		t.Fatalf("read annotations file: %v", err)
	}
	var cache NostrAnnotationCache
	if err := json.Unmarshal(data, &cache); err != nil {
		t.Fatalf("unmarshal annotations file: %v", err)
	}
	ann := cache.Annotations["stripe:txn:txn_123"]
	if ann == nil {
		t.Fatalf("missing annotation in %#v", cache.Annotations)
	}
	if ann.Category != "bookkeeping" || ann.Collective != "new-collective" {
		t.Fatalf("annotation assignment = %#v", ann)
	}
	if ann.NostrEventID != "event-id" || ann.Author != "author-pubkey" {
		t.Fatalf("annotation nostr metadata = %#v", ann)
	}
}

func TestEditAssignmentUpdatesCategoryAndCollectiveWithOnePublishCommand(t *testing.T) {
	appDir := t.TempDir()
	dataDir := filepath.Join(appDir, "data")
	t.Setenv("APP_DATA_DIR", appDir)
	t.Setenv("DATA_DIR", dataDir)

	ts := time.Date(2026, 4, 15, 12, 0, 0, 0, time.UTC).Unix()
	tx := TransactionEntry{
		ID:             "stripe:txn_1",
		Provider:       "stripe",
		StripeChargeID: "txn_1",
		Type:           "DEBIT",
		Currency:       "EUR",
		Timestamp:      ts,
	}
	txFile := TransactionsFile{Year: "2026", Month: "04", Transactions: []TransactionEntry{tx}}
	data, err := json.MarshalIndent(txFile, "", "  ")
	if err != nil {
		t.Fatalf("marshal transactions: %v", err)
	}
	writeJSONFixture(t, filepath.Join(dataDir, "2026", "04", "generated", "transactions.json"), string(data))

	cols := transactionTableColumns(true, true)
	m := txBrowserModel{
		table:                newStickerTableForColumns([]TransactionEntry{tx}, 0, 0, cols, nil),
		columns:              cols,
		txs:                  []TransactionEntry{tx},
		detailTx:             &tx,
		mode:                 modeEditAssignment,
		editCollectiveInput:  "commons-hub",
		editCategoryInput:    "accounting",
		editCollectiveCursor: 0,
		editCategoryCursor:   0,
		selectedTxIDs:        map[string]bool{},
		sortCol:              -1,
	}

	cmd := m.commitInlineEdit()
	if cmd == nil {
		t.Fatal("combined edit did not start Nostr publish command")
	}
	if got := m.txs[0].Collective; got != "commons-hub" {
		t.Fatalf("collective = %q, want commons-hub", got)
	}
	if got := m.txs[0].Category; got != "accounting" {
		t.Fatalf("category = %q, want accounting", got)
	}
	if !strings.Contains(m.statusText, "Posting 1 Nostr event(s)") {
		t.Fatalf("status = %q", m.statusText)
	}

	result := cmd().(txPublishResultMsg)
	if result.Events != 1 {
		t.Fatalf("published events = %d, want 1", result.Events)
	}

	annData, err := os.ReadFile(nostrsource.Path(dataDir, "2026", "04", nostrsource.AnnotationsFile))
	if err != nil {
		t.Fatalf("read annotations: %v", err)
	}
	var cache NostrAnnotationCache
	if err := json.Unmarshal(annData, &cache); err != nil {
		t.Fatalf("unmarshal annotations: %v", err)
	}
	ann := cache.Annotations["stripe:txn:txn_1"]
	if ann == nil || ann.Category != "accounting" || ann.Collective != "commons-hub" {
		t.Fatalf("annotation = %#v", ann)
	}
}

func TestAssignmentEditTabSwitchesFieldsAndCompletes(t *testing.T) {
	tx := TransactionEntry{ID: "stripe:txn_1", Provider: "stripe", StripeChargeID: "txn_1", Type: "DEBIT", Currency: "EUR"}
	cols := transactionTableColumns(true, true)
	m := txBrowserModel{
		table:                 newStickerTableForColumns([]TransactionEntry{tx}, 0, 0, cols, nil),
		columns:               cols,
		txs:                   []TransactionEntry{tx},
		mode:                  modeEditAssignment,
		editField:             0,
		editCollectiveInput:   "com",
		editCollectiveOptions: []string{"commonshub"},
		editCategoryOptions:   []string{"accounting"},
		selectedTxIDs:         map[string]bool{},
		sortCol:               -1,
	}

	model, _ := m.updateAssignmentEdit(tea.KeyMsg{Type: tea.KeyTab})
	got := model.(txBrowserModel)
	if got.editField != 1 {
		t.Fatalf("edit field = %d, want category field", got.editField)
	}
	if got.editCollectiveInput != "commonshub" {
		t.Fatalf("collective input = %q, want completed commonshub", got.editCollectiveInput)
	}
}

func transactionIDs(txs []TransactionEntry) []string {
	out := make([]string, len(txs))
	for i, tx := range txs {
		out[i] = tx.ID
	}
	return out
}

func hasNostrTag(tags nostr.Tags, want []string) bool {
	for _, tag := range tags {
		if reflect.DeepEqual([]string(tag), want) {
			return true
		}
	}
	return false
}
