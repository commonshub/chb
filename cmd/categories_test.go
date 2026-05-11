package cmd

import (
	"path/filepath"
	"testing"
)

func TestLoadCategoriesDedupesBySlugAndDirection(t *testing.T) {
	appDir := t.TempDir()
	t.Setenv("APP_DATA_DIR", appDir)
	writeJSONFixture(t, filepath.Join(appDir, "settings", "categories.json"), `[
	  {"slug": "catering", "label": "Catering", "direction": "expense"},
	  {"slug": "utilities", "label": "Utilities", "direction": "expense"},
	  {"slug": "catering", "label": "Catering duplicate", "direction": "expense"},
	  {"slug": "catering", "label": "Catering", "direction": "income"}
	]`)

	cats := LoadCategories()
	if len(cats) != 3 {
		t.Fatalf("LoadCategories returned %d categories, want 3: %#v", len(cats), cats)
	}
	if cats[0].Slug != "catering" || cats[0].Direction != "expense" {
		t.Fatalf("first catering category was not preserved: %#v", cats[0])
	}
	if cats[2].Slug != "catering" || cats[2].Direction != "income" {
		t.Fatalf("income catering category was not preserved: %#v", cats[2])
	}
}
