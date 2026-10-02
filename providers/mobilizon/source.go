// Package mobilizon is the Mobilizon target: we pull the events of one group
// and push our public calendar events into it.
//
// Files, all under DATA_DIR/latest/providers/mobilizon/:
//
//	events.json          the group's events, as the API returned them (pull)
//	pending/events.json  what the next push will create, update or cancel (generate)
//	published.json       what we pushed: Mobilizon IDs and content hashes (push)
package mobilizon

import (
	"encoding/json"
	"os"
	"path/filepath"

	"github.com/CommonsHub/chb/providers"
)

const (
	Source        = "mobilizon"
	EventsFile    = "events.json"
	PendingFile   = "events.json"
	PublishedFile = "published.json"
)

type SourceProvider struct{}

func (SourceProvider) Name() string {
	return Source
}

func (SourceProvider) Files() []providers.File {
	return []providers.File{
		{Name: EventsFile, Description: "Events of the Mobilizon group, as last pulled.", Private: false},
		{Name: "pending/" + PendingFile, Description: "Creates, updates and cancellations for the next push.", Private: false},
		{Name: PublishedFile, Description: "Events we published: Mobilizon IDs and content hashes.", Private: false},
	}
}

// Path returns DATA_DIR/latest/providers/mobilizon/<elems...>.
func Path(dataDir string, elems ...string) string {
	parts := append([]string{dataDir, "latest", "providers", Source}, elems...)
	return filepath.Join(parts...)
}

func EventsPath(dataDir string) string    { return Path(dataDir, EventsFile) }
func PendingPath(dataDir string) string   { return Path(dataDir, "pending", PendingFile) }
func PublishedPath(dataDir string) string { return Path(dataDir, PublishedFile) }

// ReadJSON decodes path into v. A missing file leaves v untouched and
// returns os.ErrNotExist so callers can tell "never pulled" from "empty".
func ReadJSON(path string, v interface{}) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, v)
}

func WriteJSON(path string, v interface{}) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	return os.WriteFile(path, append(data, '\n'), 0644)
}
