package cmd

import (
	"path/filepath"
	"testing"
)

func TestMergeNostrMetadataKeepsHigherCreatedAt(t *testing.T) {
	base := NostrMetadataCache{
		FetchedAt: "old",
		ChainID:   100,
		Transactions: map[string]*TxMetadata{
			"0xtx1": {TxHash: "0xtx1", Description: "old desc", CreatedAt: 100},
		},
		Addresses: map[string]*AddressMetadata{
			"0xaddr1": {Address: "0xaddr1", Name: "Old Name", CreatedAt: 100},
			"0xaddr2": {Address: "0xaddr2", Name: "Only In Base", CreatedAt: 50},
		},
	}
	incoming := NostrMetadataCache{
		FetchedAt: "new",
		ChainID:   100,
		Transactions: map[string]*TxMetadata{
			"0xtx1": {TxHash: "0xtx1", Description: "newer desc", CreatedAt: 200},
			"0xtx2": {TxHash: "0xtx2", Description: "added", CreatedAt: 150},
		},
		Addresses: map[string]*AddressMetadata{
			"0xaddr1": {Address: "0xaddr1", Name: "Stale Name", CreatedAt: 50}, // older — should NOT win
			"0xaddr3": {Address: "0xaddr3", Name: "New Address", CreatedAt: 200},
		},
	}

	merged := MergeNostrMetadata(base, incoming)

	if merged.FetchedAt != "new" {
		t.Errorf("FetchedAt: got %q, want %q", merged.FetchedAt, "new")
	}
	if got := merged.Transactions["0xtx1"].Description; got != "newer desc" {
		t.Errorf("tx1 description: got %q, want %q", got, "newer desc")
	}
	if _, ok := merged.Transactions["0xtx2"]; !ok {
		t.Error("tx2 should be added from incoming")
	}
	if got := merged.Addresses["0xaddr1"].Name; got != "Old Name" {
		t.Errorf("addr1 name: got %q, want %q (older incoming should not overwrite)", got, "Old Name")
	}
	if _, ok := merged.Addresses["0xaddr2"]; !ok {
		t.Error("addr2 (only in base) should be preserved")
	}
	if _, ok := merged.Addresses["0xaddr3"]; !ok {
		t.Error("addr3 should be added from incoming")
	}
}

func TestLoadAndWriteNostrMetadataCache(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "metadata.json")

	// Loading a missing file returns an empty (but initialized) cache.
	empty := LoadNostrMetadataCache(path)
	if empty.Transactions == nil || empty.Addresses == nil {
		t.Fatal("missing-file load should return initialized maps, not nil")
	}

	cache := NostrMetadataCache{
		FetchedAt: "now",
		ChainID:   42220,
		Transactions: map[string]*TxMetadata{
			"0xtx": {TxHash: "0xtx", Description: "hi", CreatedAt: 1},
		},
		Addresses: map[string]*AddressMetadata{
			"0xa": {Address: "0xa", Name: "Alice", CreatedAt: 1},
		},
	}
	if err := WriteNostrMetadataCache(path, cache); err != nil {
		t.Fatalf("write: %v", err)
	}

	roundTrip := LoadNostrMetadataCache(path)
	if roundTrip.ChainID != cache.ChainID {
		t.Errorf("ChainID: got %d, want %d", roundTrip.ChainID, cache.ChainID)
	}
	if got := roundTrip.Transactions["0xtx"].Description; got != "hi" {
		t.Errorf("tx description: got %q, want %q", got, "hi")
	}
	if got := roundTrip.Addresses["0xa"].Name; got != "Alice" {
		t.Errorf("addr name: got %q, want %q", got, "Alice")
	}
}
