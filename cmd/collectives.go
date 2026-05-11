package cmd

import (
	"encoding/json"
	"os"
	"path/filepath"
)

// Collective represents a collective/project.
type Collective struct {
	Name string `json:"name"`
	Icon string `json:"icon,omitempty"`
}

func collectivesPath() string {
	return settingsFilePath("collectives.json")
}

// LoadCollectives reads collectives from APP_DATA_DIR/settings/collectives.json.
// The file is seeded from the embedded defaults on first run and kept in
// sync by EnsureSettingsBootstrapped when the user hasn't edited it locally.
func LoadCollectives() map[string]Collective {
	data, err := os.ReadFile(collectivesPath())
	if err != nil {
		return map[string]Collective{}
	}
	var collectives map[string]Collective
	if json.Unmarshal(data, &collectives) != nil {
		return map[string]Collective{}
	}
	return collectives
}

// SaveCollectives writes collectives to APP_DATA_DIR/settings/collectives.json.
func SaveCollectives(collectives map[string]Collective) error {
	data, err := json.MarshalIndent(collectives, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(collectivesPath()), 0755); err != nil {
		return err
	}
	return os.WriteFile(collectivesPath(), data, 0644)
}

// AddCollective adds a new collective and saves.
func AddCollective(slug string) {
	collectives := LoadCollectives()
	if _, ok := collectives[slug]; !ok {
		collectives[slug] = Collective{Name: slug}
		SaveCollectives(collectives)
	}
}

// CollectiveSlugs returns a sorted list of collective slugs.
func CollectiveSlugs() []string {
	collectives := LoadCollectives()
	var slugs []string
	for slug := range collectives {
		slugs = append(slugs, slug)
	}
	return slugs
}

