package cmd

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
)

func categoriesPath() string {
	return settingsFilePath("categories.json")
}

// LoadCategories reads categories from APP_DATA_DIR/settings/categories.json.
// On first load, migrates from settings.json if categories.json doesn't exist.
func LoadCategories() []CategoryDef {
	data, err := os.ReadFile(existingSettingsFilePath("categories.json"))
	if err != nil {
		if os.IsNotExist(err) {
			cats := migrateCategoriesFromSettings()
			if len(cats) > 0 {
				SaveCategories(cats)
			}
			return dedupeCategories(cats)
		}
		return DefaultAccountingSettings().Categories
	}
	var cats []CategoryDef
	if json.Unmarshal(data, &cats) != nil {
		return DefaultAccountingSettings().Categories
	}
	return dedupeCategories(cats)
}

// SaveCategories writes categories to APP_DATA_DIR/settings/categories.json.
func SaveCategories(cats []CategoryDef) error {
	cats = dedupeCategories(cats)
	data, err := json.MarshalIndent(cats, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(categoriesPath()), 0755); err != nil {
		return err
	}
	return os.WriteFile(categoriesPath(), data, 0644)
}

// AddCategory adds a new category and saves.
func AddCategory(cat CategoryDef) {
	cats := LoadCategories()
	// Check if slug already exists
	for _, c := range cats {
		if categoryKey(c) == categoryKey(cat) {
			return
		}
	}
	cats = append(cats, cat)
	SaveCategories(cats)
}

func dedupeCategories(cats []CategoryDef) []CategoryDef {
	if len(cats) == 0 {
		return cats
	}
	seen := map[string]bool{}
	out := make([]CategoryDef, 0, len(cats))
	for _, cat := range cats {
		key := categoryKey(cat)
		if key == "" || seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, cat)
	}
	return out
}

func categoryKey(cat CategoryDef) string {
	slug := strings.ToLower(strings.TrimSpace(cat.Slug))
	if slug == "" {
		return ""
	}
	direction := strings.ToLower(strings.TrimSpace(cat.Direction))
	return slug + "\x00" + direction
}

func migrateCategoriesFromSettings() []CategoryDef {
	settings, err := LoadSettings()
	if err != nil || settings.Accounting == nil {
		return DefaultAccountingSettings().Categories
	}
	if len(settings.Accounting.Categories) > 0 {
		return settings.Accounting.Categories
	}
	return DefaultAccountingSettings().Categories
}
