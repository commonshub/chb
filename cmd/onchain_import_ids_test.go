package cmd

import "testing"

func TestMatchMultiTransferLines(t *testing.T) {
	p := "gnosis:0x0492b5c6bd2564826ead6eeec9e1a9756603825e:"
	d18b := "0xd18b712205f8d69a3a4eeef6ca987ffa3a2765bee2a4d6f38dd4abfeccf46541"
	// 0xd18b: burn 4.00 (log 6) + mint 3.70 (log 9). Odoo: burn twice
	// (22166 ":0", 39832 ":1", the duplicate kept on purpose) and the
	// mint under a hand-made id with a truncated hash.
	events := []TransactionEntry{{LogIndex: 6, Amount: -4}, {LogIndex: 9, Amount: 3.7}}
	lines := []OdooCacheLine{
		{ID: 39832, Amount: -4, UniqueImportID: p + d18b + ":1"},
		{ID: 22166, Amount: -4, UniqueImportID: p + d18b + ":0"},
		{ID: 39511, Amount: 3.7, UniqueImportID: p + "0xd18b712205f8d69a:1"},
	}
	ids := matchMultiTransferLines(events, lines)
	if ids[0] != p+d18b+":0" || ids[1] != p+"0xd18b712205f8d69a:1" {
		t.Fatalf("d18b ids %v (39832 must stay unmatched)", ids)
	}
	// 0xa062 on commonshub-test: 9.83 (log 2), 3.93 (log 6), 9.83 (log 10);
	// Odoo: 22769 3.93 ":0", 27124 9.83 ":2", new 9.83 ":log10".
	a062 := "0xa062dcd8301516adeffa77ff7470049cea15c0f49b98991ed09648631334086e"
	events = []TransactionEntry{{LogIndex: 2, Amount: 9.83}, {LogIndex: 6, Amount: 3.93}, {LogIndex: 10, Amount: 9.83}}
	lines = []OdooCacheLine{
		{ID: 22769, Amount: 3.93, UniqueImportID: p + a062 + ":0"},
		{ID: 27124, Amount: 9.83, UniqueImportID: p + a062 + ":2"},
		{ID: 40700, Amount: 9.83, UniqueImportID: p + a062 + ":log10"},
	}
	ids = matchMultiTransferLines(events, lines)
	if ids[0] != p+a062+":2" || ids[1] != p+a062+":0" || ids[2] != p+a062+":log10" {
		t.Fatalf("a062 ids %v", ids)
	}
	// Without the :log10 line, the second 9.83 is unmatched (gets a log id).
	ids = matchMultiTransferLines(events, lines[:2])
	if ids[2] != "" {
		t.Fatalf("unmatched event should get no Odoo id: %v", ids)
	}
	if got := onchainLogImportID("gnosis", "0x0492B5C6BD2564826EAD6EEEC9E1A9756603825E", a062, 10); got != p+a062+":log10" {
		t.Errorf("log id %q", got)
	}
}
