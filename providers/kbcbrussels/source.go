package kbcbrussels

import (
	"path/filepath"
	"strings"

	"github.com/CommonsHub/chb/providers"
)

type SourceProvider struct{}

func (SourceProvider) Name() string {
	return Source
}

func (SourceProvider) Files() []providers.File {
	return []providers.File{
		{
			Name:        "export_<IBAN>_<YYYYMMDD>_<HHMM>.csv",
			Description: "Manually-downloaded KBC Brussels transaction export.",
			Private:     true,
		},
	}
}

// RelPath returns the path inside DATA_DIR for this provider.
func RelPath(elems ...string) string {
	parts := append([]string{"providers", Source}, elems...)
	return filepath.Join(parts...)
}

// LatestDir is the directory where the operator drops new CSV exports.
// We don't split the CSV into monthly archives — it's a single rolling
// dump and the generator filters rows by date.
func LatestDir(dataDir string) string {
	return filepath.Join(dataDir, "latest", "providers", Source)
}

// NormalizeIBAN strips whitespace from an IBAN. KBC's CSV embeds spaces
// in the counterparty IBAN column ("IE30 CITI 9900 …") but never in the
// account-owner column. Normalize both so URIs and matching work.
func NormalizeIBAN(s string) string {
	return strings.ReplaceAll(strings.TrimSpace(s), " ", "")
}
