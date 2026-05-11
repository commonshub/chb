package lumastripe

import "path/filepath"

const (
	Name          = "luma-stripe"
	EventURLsFile = "event-urls.json"
)

func Path(dataDir, year, month string, elems ...string) string {
	parts := []string{dataDir, year}
	if month != "" {
		parts = append(parts, month)
	}
	parts = append(parts, RelPath(elems...))
	return filepath.Join(parts...)
}

func RelPath(elems ...string) string {
	parts := append([]string{"processors", Name}, elems...)
	return filepath.Join(parts...)
}
