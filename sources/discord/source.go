package discord

import (
	"encoding/json"
	"os"
	"path/filepath"

	"github.com/CommonsHub/chb/sources"
)

const (
	Source       = "discord"
	MessagesFile = "messages.json"
)

type SourceProvider struct{}

func (SourceProvider) Name() string {
	return Source
}

func (SourceProvider) Files() []sources.File {
	return []sources.File{
		{Name: "<channel-id>/messages.json", Description: "Monthly Discord messages for a monitored channel.", Private: false},
		{Name: "images/<attachment-id>.<ext>", Description: "Downloaded Discord image attachments referenced by generated/images.json.", Private: false},
	}
}

func RelPath(elems ...string) string {
	parts := append([]string{"sources", Source}, elems...)
	return filepath.Join(parts...)
}

func ChannelRelPath(channelID string) string {
	return RelPath(channelID, MessagesFile)
}

func ImageRelPath(fileName string) string {
	return RelPath("images", fileName)
}

func Path(dataDir, year, month string, elems ...string) string {
	parts := []string{dataDir, year}
	if month != "" {
		parts = append(parts, month)
	}
	parts = append(parts, RelPath(elems...))
	return filepath.Join(parts...)
}

func ChannelPath(dataDir, year, month, channelID string) string {
	return filepath.Join(dataDir, year, month, ChannelRelPath(channelID))
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
