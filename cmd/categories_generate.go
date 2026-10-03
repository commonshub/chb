package cmd

// The category taxonomy as open data: latest/<tier>/categories.json, the
// same in every tier. Every slug a transaction, a bill line or a rule can
// carry, with its label, direction, display group and the PCMN account
// prefixes that map to it (longest prefix wins; see
// cmd/transactions_odoo_category.go). Source: settings/categories.json.

const categoriesFile = "categories.json"

type CategoriesFile struct {
	// Directions: income, expense, or both (a slug used either way:
	// internal transfers, VAT paid or refunded, catering bought or sold).
	Categories []CategoryDef `json:"categories"`
}

func generateCategoriesFile(dataDir string) int {
	cats := LoadCategories()
	writeTiersNoMirror(dataDir, "latest", "", categoriesFile, func(a Audience) interface{} {
		return CategoriesFile{Categories: cats}
	})
	return len(cats)
}
