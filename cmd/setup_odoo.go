package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"

	"github.com/charmbracelet/huh"
)

// SetupOdoo runs the interactive Odoo setup wizard.
// It connects to Odoo, analyzes recent transactions to discover which
// accounts and analytic categories are actually used, then guides the
// user through mapping them to local categories.
func SetupOdoo() error {
	fmt.Printf("\n%s🏢 Odoo Setup Wizard%s\n", Fmt.Bold, Fmt.Reset)
	fmt.Println("─────────────────────")
	fmt.Printf(`
  This wizard connects to your Odoo instance, analyzes your recent
  transactions to discover how they are categorized, and helps you
  map Odoo's analytic accounts to local categories.

  These mappings are then used to automatically categorize transactions
  synced from Stripe, Monerium, and blockchain accounts.

`)

	// ── Step 1: Credentials ──
	fmt.Printf("%s1. Credentials%s\n\n", Fmt.Bold, Fmt.Reset)

	configPath := configEnvPath()
	existing := loadConfigEnv(configPath)

	odooKeys := []envKey{
		{"ODOO_URL", "Odoo instance URL", "e.g. https://mycompany.odoo.com"},
		{"ODOO_LOGIN", "Odoo login email", "The email you use to log into Odoo"},
		{"ODOO_PASSWORD", "Odoo password or API key", "Settings > API Keys (recommended)"},
	}
	for _, k := range odooKeys {
		if val, source, ok := resolveEnvValue(existing, k.Name); ok && val != "" {
			fmt.Printf("  %s✓%s %s: %s%s (%s)%s\n", Fmt.Green, Fmt.Reset, k.Name, Fmt.Dim, maskValue(val), source, Fmt.Reset)
		} else {
			fmt.Printf("  %s☐%s %s — %s\n", Fmt.Dim, Fmt.Reset, k.Name, k.Help)
			var value string
			runField(huh.NewInput().
				Title(k.Desc).
				Value(&value))
			if value != "" {
				existing[k.Name] = value
				saveConfigEnv(configPath, existing)
				os.Setenv(k.Name, value)
				fmt.Printf("  %s✓ Saved%s\n", Fmt.Green, Fmt.Reset)
			}
		}
	}

	odooURL := os.Getenv("ODOO_URL")
	odooLogin := os.Getenv("ODOO_LOGIN")
	odooPassword := os.Getenv("ODOO_PASSWORD")
	if odooURL == "" || odooLogin == "" || odooPassword == "" {
		Warnf("%s⚠ Credentials incomplete, cannot continue%s", Fmt.Yellow, Fmt.Reset)
		return nil
	}

	db := odooDBFromURL(odooURL)
	fmt.Printf("\n  Connecting to %s...\n", odooURL)
	uid, err := odooAuth(odooURL, db, odooLogin, odooPassword)
	if err != nil || uid == 0 {
		return fmt.Errorf("authentication failed: %v", err)
	}
	fmt.Printf("  %s✓ Connected%s\n", Fmt.Green, Fmt.Reset)

	// ── Step 2: Analyze recent transactions ──
	fmt.Printf("\n%s2. Analyzing recent transactions%s\n\n", Fmt.Bold, Fmt.Reset)
	fmt.Printf("  Fetching recent income and expense journal entries...\n")

	// Fetch analytic accounts for name lookup
	accountsResult, err := odooExec(odooURL, db, uid, odooPassword,
		"account.analytic.account", "search_read",
		[]interface{}{[]interface{}{
			[]interface{}{"active", "=", true},
		}},
		map[string]interface{}{"fields": []string{"id", "name", "code", "plan_id"}})
	if err != nil {
		return fmt.Errorf("failed to fetch analytic accounts: %v", err)
	}

	type odooAcct struct {
		ID     int         `json:"id"`
		Name   string      `json:"name"`
		Code   string      `json:"code"`
		PlanID interface{} `json:"plan_id"`
	}
	var analyticAccounts []odooAcct
	json.Unmarshal(accountsResult, &analyticAccounts)

	analyticNameByID := map[int]string{}
	analyticPlanByID := map[int]string{}  // account ID -> plan name
	analyticPlanIDByAcct := map[int]int{} // account ID -> plan ID
	for _, a := range analyticAccounts {
		analyticNameByID[a.ID] = a.Name
		analyticPlanByID[a.ID] = odooFieldName(a.PlanID)
		analyticPlanIDByAcct[a.ID] = odooFieldID(a.PlanID)
	}

	// Fetch recent posted journal items that have actual amounts
	// We look at the last 200 journal items with debit or credit > 0
	linesResult, err := odooExec(odooURL, db, uid, odooPassword,
		"account.move.line", "search_read",
		[]interface{}{[]interface{}{
			"&",
			[]interface{}{"parent_state", "=", "posted"},
			"|",
			[]interface{}{"debit", ">", 0},
			[]interface{}{"credit", ">", 0},
		}},
		map[string]interface{}{
			"fields": []string{
				"id", "name", "account_id", "debit", "credit",
				"analytic_distribution", "partner_id", "product_id", "date",
			},
			"limit": 200,
			"order": "date desc",
		})
	if err != nil {
		return fmt.Errorf("failed to fetch journal items: %v", err)
	}

	type journalLine struct {
		ID                   int             `json:"id"`
		Name                 string          `json:"name"`
		AccountID            interface{}     `json:"account_id"`
		Debit                float64         `json:"debit"`
		Credit               float64         `json:"credit"`
		AnalyticDistribution json.RawMessage `json:"analytic_distribution"`
		PartnerID            interface{}     `json:"partner_id"`
		ProductID            interface{}     `json:"product_id"`
		Date                 string          `json:"date"`
	}
	var lines []journalLine
	json.Unmarshal(linesResult, &lines)

	fmt.Printf("  %s✓ Fetched %d recent journal entries%s\n", Fmt.Green, len(lines), Fmt.Reset)

	// ── Step 3: Map analytic plans ──
	// Fetch plans
	plansResult, err := odooExec(odooURL, db, uid, odooPassword,
		"account.analytic.plan", "search_read",
		[]interface{}{[]interface{}{}},
		map[string]interface{}{"fields": []string{"id", "name"}})
	if err != nil {
		return fmt.Errorf("failed to fetch analytic plans: %v", err)
	}

	type odooPlan struct {
		ID   int    `json:"id"`
		Name string `json:"name"`
	}
	var plans []odooPlan
	json.Unmarshal(plansResult, &plans)

	// Count accounts per plan
	accountsByPlan := map[int]int{}
	for _, a := range analyticAccounts {
		accountsByPlan[odooFieldID(a.PlanID)]++
	}

	// Pre-compute usage stats per analytic account from journal lines
	type usageStats struct {
		Count       int
		TotalAmount float64
		Direction   string
		Examples    []string
	}
	usageByAcct := map[int]*usageStats{}
	for _, line := range lines {
		if len(line.AnalyticDistribution) == 0 || string(line.AnalyticDistribution) == "false" {
			continue
		}
		var dist map[string]float64
		if json.Unmarshal(line.AnalyticDistribution, &dist) != nil {
			continue
		}
		direction := "expense"
		amount := line.Debit
		if line.Credit > 0 {
			direction = "income"
			amount = line.Credit
		}
		for idStr := range dist {
			var id int
			fmt.Sscanf(idStr, "%d", &id)
			if id == 0 {
				continue
			}
			u, ok := usageByAcct[id]
			if !ok {
				u = &usageStats{Direction: direction}
				usageByAcct[id] = u
			}
			u.Count++
			u.TotalAmount += amount
			if len(u.Examples) < 3 {
				u.Examples = append(u.Examples, line.Name)
			}
		}
	}

	// Load previous Odoo mapping from settings
	settings, _ := LoadSettings()
	acctSettings := DefaultAccountingSettings()
	if settings != nil && settings.Accounting != nil {
		acctSettings = settings.Accounting
	}

	previousMapping := map[string]string{}
	if acctSettings.Odoo != nil && acctSettings.Odoo.CategoryMapping != nil {
		previousMapping = acctSettings.Odoo.CategoryMapping
	}

	allMappings := map[int]string{}
	for idStr, cat := range previousMapping {
		var id int
		fmt.Sscanf(idStr, "%d", &id)
		if id > 0 {
			allMappings[id] = cat
		}
	}

	// Helper to build category options from categories.json
	categories := LoadCategories()
	buildOptions := func(direction string) []huh.Option[string] {
		opts := []huh.Option[string]{huh.NewOption("(skip)", "")}
		for _, cat := range categories {
			if cat.Direction == direction {
				opts = append(opts, huh.NewOption(cat.Label, cat.Slug))
			}
		}
		opts = append(opts, huh.NewOption("+ New category...", "__new__"))
		return opts
	}

	fmt.Printf("\n%s3. Map analytic accounts to categories%s\n", Fmt.Bold, Fmt.Reset)
	fmt.Printf(`
  Select a plan to map its accounts to local categories.
  Each account is shown with recent transaction stats.

`)

	// Plan selection → account mapping loop
	for {
		// Build plan menu
		var planOptions []huh.Option[string]
		for _, p := range plans {
			count := accountsByPlan[p.ID]
			if count == 0 {
				continue
			}
			// Count how many accounts in this plan are already mapped
			mapped := 0
			for _, a := range analyticAccounts {
				if odooFieldID(a.PlanID) == p.ID {
					if _, ok := allMappings[a.ID]; ok {
						mapped++
					}
				}
			}
			label := fmt.Sprintf("%s (%d accounts", p.Name, count)
			if mapped > 0 {
				label += fmt.Sprintf(", %d mapped", mapped)
			}
			label += ")"
			planOptions = append(planOptions, huh.NewOption(label, fmt.Sprintf("%d", p.ID)))
		}
		planOptions = append(planOptions, huh.NewOption("Done — save and exit", "done"))

		var choice string
		runField(huh.NewSelect[string]().
			Title("Select a plan to map").
			Options(planOptions...).
			Value(&choice))

		if choice == "done" {
			break
		}

		var planID int
		fmt.Sscanf(choice, "%d", &planID)

		// Get accounts in this plan, sorted by usage
		type acctEntry struct {
			ID    int
			Name  string
			Usage *usageStats
		}
		var planAccounts []acctEntry
		for _, a := range analyticAccounts {
			if odooFieldID(a.PlanID) == planID {
				planAccounts = append(planAccounts, acctEntry{a.ID, a.Name, usageByAcct[a.ID]})
			}
		}
		sort.Slice(planAccounts, func(i, j int) bool {
			ui := planAccounts[i].Usage
			uj := planAccounts[j].Usage
			if ui == nil {
				return false
			}
			if uj == nil {
				return true
			}
			return ui.TotalAmount > uj.TotalAmount
		})

		if len(previousMapping) > 0 {
			fmt.Printf("\n  %s(Previous mappings pre-selected — Enter to keep)%s\n", Fmt.Dim, Fmt.Reset)
		}

		for i, a := range planAccounts {
			idStr := fmt.Sprintf("%d", a.ID)
			prevCat := previousMapping[idStr]

			fmt.Printf("\n  %s%d/%d%s  %s%s%s", Fmt.Dim, i+1, len(planAccounts), Fmt.Reset, Fmt.Bold, a.Name, Fmt.Reset)
			if prevCat != "" {
				fmt.Printf("  %s(current: %s)%s", Fmt.Dim, prevCat, Fmt.Reset)
			}
			fmt.Println()

			dir := "income"
			if a.Usage != nil {
				dir = a.Usage.Direction
				dirIcon := "↓"
				if dir == "expense" {
					dirIcon = "↑"
				}
				fmt.Printf("    %s %s%.2f EUR  •  %d transactions%s\n",
					dirIcon, Fmt.Dim, a.Usage.TotalAmount, a.Usage.Count, Fmt.Reset)
				for _, ex := range a.Usage.Examples {
					if len(ex) > 60 {
						ex = ex[:57] + "..."
					}
					fmt.Printf("    %s  → %s%s\n", Fmt.Dim, ex, Fmt.Reset)
				}
			} else {
				fmt.Printf("    %s(no recent transactions)%s\n", Fmt.Dim, Fmt.Reset)
			}

			category := prevCat
			options := buildOptions(dir)
			runField(huh.NewSelect[string]().
				Title("Category").
				Options(options...).
				Value(&category))

			if category == "__new__" {
				var newSlug, newLabel, newDirection string
				newDirection = dir
				runField(huh.NewInput().Title("Category slug (e.g. catering)").Value(&newSlug))
				runField(huh.NewInput().Title("Category label (e.g. Catering)").Value(&newLabel))
				runField(huh.NewSelect[string]().Title("Direction").
					Options(huh.NewOption("Income", "income"), huh.NewOption("Expense", "expense")).
					Value(&newDirection))
				if newSlug != "" && newLabel != "" {
					AddCategory(CategoryDef{Slug: newSlug, Label: newLabel, Direction: newDirection})
					categories = LoadCategories() // reload
					category = newSlug
					fmt.Printf("    %s✓ Created: %s%s\n", Fmt.Green, newLabel, Fmt.Reset)
				} else {
					category = ""
				}
			}

			if category != "" {
				allMappings[a.ID] = category
			}
		}

		fmt.Printf("\n  %s✓ Done with this plan%s\n", Fmt.Green, Fmt.Reset)
	}

	// ── Save ──
	odooMapping := map[string]string{}
	for aID, cat := range allMappings {
		odooMapping[fmt.Sprintf("%d", aID)] = cat
	}

	if acctSettings.Odoo == nil {
		acctSettings.Odoo = &OdooAccountingConfig{}
	}
	acctSettings.Odoo.CategoryMapping = odooMapping

	if err := SaveAccountingSettings(acctSettings); err != nil {
		Warnf("  %s⚠ Failed to save settings: %v%s", Fmt.Yellow, err, Fmt.Reset)
	} else {
		fmt.Printf("\n%s✓ Saved %d category mappings to settings.json%s\n", Fmt.Green, len(odooMapping), Fmt.Reset)
	}

	if len(allMappings) > 0 {
		fmt.Println()
		for aID, cat := range allMappings {
			fmt.Printf("  %s → %s%s%s\n", analyticNameByID[aID], Fmt.Green, cat, Fmt.Reset)
		}
	}

	fmt.Println()
	return nil
}
