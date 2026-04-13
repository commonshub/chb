package cmd

import (
	"encoding/json"
	"os"
	"path/filepath"
)

func categoriesPath() string {
	return filepath.Join(chbDir(), "categories.json")
}

// LoadCategories reads categories from ~/.chb/categories.json.
// On first load, migrates from settings.json if categories.json doesn't exist.
func LoadCategories() []CategoryDef {
	data, err := os.ReadFile(categoriesPath())
	if err != nil {
		if os.IsNotExist(err) {
			cats := migrateCategoriesFromSettings()
			if len(cats) > 0 {
				SaveCategories(cats)
			}
			return cats
		}
		return DefaultAccountingSettings().Categories
	}
	var cats []CategoryDef
	if json.Unmarshal(data, &cats) != nil {
		return DefaultAccountingSettings().Categories
	}
	return cats
}

// SaveCategories writes categories to ~/.chb/categories.json.
func SaveCategories(cats []CategoryDef) error {
	data, err := json.MarshalIndent(cats, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(categoriesPath(), data, 0644)
}

// AddCategory adds a new category and saves.
func AddCategory(cat CategoryDef) {
	cats := LoadCategories()
	// Check if slug already exists
	for _, c := range cats {
		if c.Slug == cat.Slug {
			return
		}
	}
	cats = append(cats, cat)
	SaveCategories(cats)
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
