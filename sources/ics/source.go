package ics

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/CommonsHub/chb/sources"
)

const (
	Source = "ics"
)

type SourceProvider struct{}

func (SourceProvider) Name() string {
	return Source
}

func (SourceProvider) Files() []sources.File {
	return []sources.File{
		{Name: "<calendar>.ics", Description: "Monthly room and configured calendar VEVENT archive.", Private: true},
	}
}

func RelPath(elems ...string) string {
	parts := append([]string{"sources", Source}, elems...)
	return filepath.Join(parts...)
}

func Path(dataDir, year, month string, elems ...string) string {
	parts := []string{dataDir, year}
	if month != "" {
		parts = append(parts, month)
	}
	parts = append(parts, RelPath(elems...))
	return filepath.Join(parts...)
}

func FileName(slug string) string {
	slug = strings.TrimSpace(slug)
	if slug == "" {
		slug = "calendar"
	}
	if strings.HasSuffix(strings.ToLower(slug), ".ics") {
		return slug
	}
	return slug + ".ics"
}

func Write(dataDir, year, month, slug, content string) error {
	path := Path(dataDir, year, month, FileName(slug))
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		return err
	}
	_ = os.Chmod(filepath.Dir(path), 0700)
	_ = os.Chmod(path, 0600)
	return nil
}
