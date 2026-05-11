package nostr

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"

	"github.com/CommonsHub/chb/sources"
)

const (
	Source                = "nostr"
	MetadataFile          = "metadata.json"
	StripeAnnotationsFile = "stripe-annotations.json"
	AnnotationsFile       = "transaction-annotations.json"
)

type SourceProvider struct{}

func (SourceProvider) Name() string {
	return Source
}

func (SourceProvider) Files() []sources.File {
	return []sources.File{
		{Name: "<chain-id>/metadata.json", Description: "Monthly Nostr tx/address metadata.", Private: false},
		{Name: "stripe-annotations.json", Description: "Monthly Nostr annotations for Stripe transaction URIs.", Private: false},
		{Name: "transaction-annotations.json", Description: "Monthly Nostr annotations for transaction URIs.", Private: false},
	}
}

func RelPath(elems ...string) string {
	parts := append([]string{"sources", Source}, elems...)
	return filepath.Join(parts...)
}

func ChainMetadataRelPath(chainID int) string {
	return RelPath(strconv.Itoa(chainID), MetadataFile)
}

func Path(dataDir, year, month string, elems ...string) string {
	parts := []string{dataDir, year}
	if month != "" {
		parts = append(parts, month)
	}
	parts = append(parts, RelPath(elems...))
	return filepath.Join(parts...)
}

func ChainMetadataPath(dataDir, year, month string, chainID int) string {
	return filepath.Join(dataDir, year, month, ChainMetadataRelPath(chainID))
}

func WriteJSON(dataDir, year, month string, v interface{}, elems ...string) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	path := Path(dataDir, year, month, elems...)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}
