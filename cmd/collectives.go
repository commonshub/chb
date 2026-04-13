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
	return filepath.Join(chbDir(), "collectives.json")
}

// LoadCollectives reads collectives from ~/.chb/collectives.json.
// On first load, migrates from settings.json if collectives.json doesn't exist.
func LoadCollectives() map[string]Collective {
	data, err := os.ReadFile(collectivesPath())
	if err != nil {
		if os.IsNotExist(err) {
			// Migrate from settings.json
			collectives := migrateCollectivesFromSettings()
			if len(collectives) > 0 {
				SaveCollectives(collectives)
			}
			return collectives
		}
		return map[string]Collective{}
	}
	var collectives map[string]Collective
	if json.Unmarshal(data, &collectives) != nil {
		return map[string]Collective{}
	}
	return collectives
}

// SaveCollectives writes collectives to ~/.chb/collectives.json.
func SaveCollectives(collectives map[string]Collective) error {
	data, err := json.MarshalIndent(collectives, "", "  ")
	if err != nil {
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

func migrateCollectivesFromSettings() map[string]Collective {
	settings, err := LoadSettings()
	if err != nil {
		return map[string]Collective{}
	}

	result := map[string]Collective{}
	for slug, raw := range settings.Finance.Collectives {
		var c Collective
		if json.Unmarshal(raw, &c) == nil {
			result[slug] = c
		} else {
			result[slug] = Collective{Name: slug}
		}
	}
	return result
}
