package cmd

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// ── Types ───────────────────────────────────────────────────────────────────

type MemberAmount struct {
	Value    float64 `json:"value"`
	Decimals int     `json:"decimals"`
	Currency string  `json:"currency"`
}

type MemberPayment struct {
	Date   string       `json:"date"`
	Amount MemberAmount `json:"amount"`
	Status string       `json:"status"`
	URL    string       `json:"url"`
}

type MemberAccounts struct {
	EmailHash string  `json:"emailHash"`
	Discord   *string `json:"discord"`
}

type Member struct {
	ID                 string         `json:"id"`
	Source             string         `json:"source"`
	Accounts           MemberAccounts `json:"accounts"`
	FirstName          string         `json:"firstName"`
	Plan               string         `json:"plan"`
	Amount             MemberAmount   `json:"amount"`
	Interval           string         `json:"interval"`
	Status             string         `json:"status"`
	CurrentPeriodStart string         `json:"currentPeriodStart"`
	CurrentPeriodEnd   string         `json:"currentPeriodEnd"`
	LatestPayment      *MemberPayment `json:"latestPayment"`
	SubscriptionURL    string         `json:"subscriptionUrl,omitempty"`
	CreatedAt          string         `json:"createdAt"`
	IsOrganization     bool           `json:"isOrganization,omitempty"`
}

type MembersSummary struct {
	TotalMembers   int          `json:"totalMembers"`
	ActiveMembers  int          `json:"activeMembers"`
	MonthlyMembers int          `json:"monthlyMembers"`
	YearlyMembers  int          `json:"yearlyMembers"`
	MRR            MemberAmount `json:"mrr"`
}

type MembersOutputFile struct {
	Year        string         `json:"year"`
	Month       string         `json:"month"`
	ProductID   string         `json:"productId"`
	GeneratedAt string         `json:"generatedAt"`
	Summary     MembersSummary `json:"summary"`
	Members     []Member       `json:"members"`
}

// Stripe types
type stripeSubscription struct {
	ID                 string `json:"id"`
	Status             string `json:"status"`
	Customer           string `json:"customer"`
	CurrentPeriodStart int64  `json:"current_period_start"`
	CurrentPeriodEnd   int64  `json:"current_period_end"`
	Created            int64  `json:"created"`
	CanceledAt         *int64 `json:"canceled_at"`
	EndedAt            *int64 `json:"ended_at"`
	Items              struct {
		Data []struct {
			Price struct {
				ID         string `json:"id"`
				UnitAmount int64  `json:"unit_amount"`
				Currency   string `json:"currency"`
				Recurring  struct {
					Interval      string `json:"interval"`
					IntervalCount int    `json:"interval_count"`
				} `json:"recurring"`
				Product string `json:"product"`
			} `json:"price"`
		} `json:"data"`
	} `json:"items"`
	Metadata      map[string]string `json:"metadata"`
	LatestInvoice json.RawMessage   `json:"latest_invoice"`
}

type stripeCustomer struct {
	ID       string            `json:"id"`
	Email    string            `json:"email"`
	Name     *string           `json:"name"`
	Metadata map[string]string `json:"metadata"`
}

type stripeInvoice struct {
	ID                string `json:"id"`
	Status            string `json:"status"`
	AmountPaid        int64  `json:"amount_paid"`
	Currency          string `json:"currency"`
	Created           int64  `json:"created"`
	HostedInvoiceURL  string `json:"hosted_invoice_url"`
	StatusTransitions struct {
		PaidAt *int64 `json:"paid_at"`
	} `json:"status_transitions"`
}

type providerSubscription struct {
	ID                 string         `json:"id"`
	Source             string         `json:"source"`
	EmailHash          string         `json:"emailHash"`
	FirstName          string         `json:"firstName"`
	LastName           string         `json:"lastName"`
	Plan               string         `json:"plan"`
	Amount             MemberAmount   `json:"amount"`
	Interval           string         `json:"interval"`
	Status             string         `json:"status"`
	CurrentPeriodStart string         `json:"currentPeriodStart"`
	CurrentPeriodEnd   string         `json:"currentPeriodEnd"`
	LatestPayment      *MemberPayment `json:"latestPayment"`
	SubscriptionURL    string         `json:"subscriptionUrl,omitempty"`
	CreatedAt          string         `json:"createdAt"`
	Discord            *string        `json:"discord"`
	IsOrganization     bool           `json:"isOrganization,omitempty"`
	ProductID          interface{}    `json:"productId,omitempty"`
}

type providerSnapshot struct {
	Provider      string                 `json:"provider"`
	FetchedAt     string                 `json:"fetchedAt"`
	Subscriptions []providerSubscription `json:"subscriptions"`
}

// ── Command ─────────────────────────────────────────────────────────────────

func MembersSync(args []string) error {
	if HasFlag(args, "--help", "-h", "help") {
		printMembersSyncHelp()
		return nil
	}

	fmt.Printf("\n%s🔄 Fetching membership data%s\n\n", Fmt.Bold, Fmt.Reset)

	dataDir := DataDir()
	stripeKey := os.Getenv("STRIPE_SECRET_KEY")
	odooURL := os.Getenv("ODOO_URL")
	odooLogin := os.Getenv("ODOO_LOGIN")
	odooPassword := os.Getenv("ODOO_PASSWORD")
	salt := os.Getenv("EMAIL_HASH_SALT")

	if salt == "" {
		// Generate a random salt and persist it
		salt = generateAndSaveSalt()
		fmt.Printf("  %sGenerated EMAIL_HASH_SALT: %s%s\n", Fmt.Dim, salt, Fmt.Reset)
	}

	stripeOnly := HasFlag(args, "--stripe-only")
	odooOnly := HasFlag(args, "--odoo-only")
	doStripe := !odooOnly
	doOdoo := !stripeOnly

	// Determine months
	months := getMemberMonths(args)
	fmt.Printf("📆 %d month(s) to process\n", len(months))

	// Read settings
	settings, err := LoadSettings()
	if err != nil {
		return fmt.Errorf("failed to load settings: %w", err)
	}
	stripeProductID := settings.Membership.Stripe.ProductID

	// Fetch all Stripe subscriptions (once)
	var stripeSubscriptions []stripeSubscription
	customerCache := map[string]*stripeCustomer{}

	if doStripe && stripeKey != "" {
		fmt.Println("📥 Fetching Stripe subscriptions...")
		var err error
		stripeSubscriptions, err = fetchAllStripeMemberSubscriptions(stripeKey, stripeProductID)
		if err != nil {
			fmt.Printf("  %s⚠ Stripe error: %v%s\n", Fmt.Yellow, err, Fmt.Reset)
			doStripe = false
		} else {
			fmt.Printf("  %d Stripe subscriptions\n", len(stripeSubscriptions))
		}
	} else if doStripe {
		fmt.Printf("%s⚠ STRIPE_SECRET_KEY not set, skipping Stripe%s\n", Fmt.Yellow, Fmt.Reset)
		doStripe = false
	}

	if doOdoo && (odooURL == "" || odooLogin == "" || odooPassword == "") {
		fmt.Printf("%s⚠ ODOO_URL/ODOO_LOGIN/ODOO_PASSWORD not set, skipping Odoo%s\n", Fmt.Yellow, Fmt.Reset)
		doOdoo = false
	}

	for _, ym := range months {
		year := ym.year
		month := ym.month
		monthStr := fmt.Sprintf("%02d", month)
		yearStr := strconv.Itoa(year)
		monthDir := filepath.Join(dataDir, yearStr, monthStr)

		fmt.Printf("\n📅 %s-%s\n", yearStr, monthStr)

		var snapshots []providerSnapshot

		// Stripe
		if doStripe && len(stripeSubscriptions) > 0 {
			snap := buildStripeMonthSnapshot(stripeSubscriptions, year, month, salt, stripeProductID, stripeKey, customerCache)
			fmt.Printf("  Stripe: %d subscriptions\n", len(snap.Subscriptions))
			snapData, _ := json.MarshalIndent(snap, "", "  ")
			writeMonthFile(dataDir, yearStr, monthStr, filepath.Join("finance", "stripe", "subscriptions.json"), snapData)
			snapshots = append(snapshots, snap)
		} else {
			// Try loading cached
			snapPath := filepath.Join(monthDir, "finance", "stripe", "subscriptions.json")
			if data, err := os.ReadFile(snapPath); err == nil {
				var snap providerSnapshot
				if json.Unmarshal(data, &snap) == nil {
					snapshots = append(snapshots, snap)
					fmt.Printf("  Stripe: loaded from cache\n")
				}
			}
		}

		// Odoo (only current month — API returns current state, not historical)
		now := time.Now()
		isCurrentMonth := year == now.Year() && month == int(now.Month())
		if doOdoo && odooURL != "" && odooLogin != "" && odooPassword != "" && isCurrentMonth {
			snap, err := buildOdooSnapshot(settings, odooURL, odooLogin, odooPassword, salt)
			if err != nil {
				fmt.Printf("  %sOdoo: error: %v%s\n", Fmt.Red, err, Fmt.Reset)
			} else {
				fmt.Printf("  Odoo: %d subscriptions\n", len(snap.Subscriptions))
				snapData, _ := json.MarshalIndent(snap, "", "  ")
				writeMonthFile(dataDir, yearStr, monthStr, filepath.Join("finance", "odoo", "subscriptions.json"), snapData)
				snapshots = append(snapshots, snap)
			}
		}
		// Load cached Odoo if not just fetched
		if !isCurrentMonth || !doOdoo || odooURL == "" {
			odooSnapPath := filepath.Join(monthDir, "finance", "odoo", "subscriptions.json")
			if data, err := os.ReadFile(odooSnapPath); err == nil {
				var snap providerSnapshot
				if json.Unmarshal(data, &snap) == nil {
					snapshots = append(snapshots, snap)
					fmt.Printf("  Odoo: loaded from cache\n")
				}
			}
		}

		if len(snapshots) == 0 {
			fmt.Println("  No data for this month")
			continue
		}

		// Merge
		members := mergeProviderSnapshots(snapshots)
		summary := calculateMembersSummary(members)

		out := MembersOutputFile{
			Year:        yearStr,
			Month:       monthStr,
			ProductID:   "mixed",
			GeneratedAt: time.Now().UTC().Format(time.RFC3339),
			Summary:     summary,
			Members:     members,
		}

		membersData, _ := json.MarshalIndent(out, "", "  ")
		writeMonthFile(dataDir, yearStr, monthStr, filepath.Join("generated", "members.json"), membersData)
		fmt.Printf("  %s✅ %d members (active: %d, MRR: €%.2f)%s\n",
			Fmt.Green, len(members), summary.ActiveMembers, summary.MRR.Value, Fmt.Reset)
	}

	fmt.Printf("\n%s✅ Done!%s\n\n", Fmt.Green, Fmt.Reset)
	return nil
}

// ── Stripe helpers ──────────────────────────────────────────────────────────

func fetchAllStripeMemberSubscriptions(apiKey, productID string) ([]stripeSubscription, error) {
	var all []stripeSubscription
	startingAfter := ""

	for {
		url := "https://api.stripe.com/v1/subscriptions?limit=100&status=all&expand[]=data.latest_invoice"
		if startingAfter != "" {
			url += "&starting_after=" + startingAfter
		}

		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}

		if resp.StatusCode == 429 {
			resp.Body.Close()
			time.Sleep(2 * time.Second)
			continue
		}

		if resp.StatusCode != 200 {
			resp.Body.Close()
			return nil, fmt.Errorf("Stripe API %d", resp.StatusCode)
		}

		var result struct {
			Data    []stripeSubscription `json:"data"`
			HasMore bool                 `json:"has_more"`
		}
		json.NewDecoder(resp.Body).Decode(&result)
		resp.Body.Close()

		// Filter by product
		for _, sub := range result.Data {
			for _, item := range sub.Items.Data {
				if item.Price.Product == productID {
					all = append(all, sub)
					break
				}
			}
		}

		if !result.HasMore || len(result.Data) == 0 {
			break
		}
		startingAfter = result.Data[len(result.Data)-1].ID
		time.Sleep(200 * time.Millisecond)
	}

	return all, nil
}

func buildStripeMonthSnapshot(subs []stripeSubscription, year, month int, salt, productID, apiKey string, cache map[string]*stripeCustomer) providerSnapshot {
	monthStart := time.Date(year, time.Month(month), 1, 0, 0, 0, 0, time.UTC).Unix()
	lastDay := time.Date(year, time.Month(month)+1, 0, 23, 59, 59, 0, time.UTC).Unix()

	var result []providerSubscription

	for _, sub := range subs {
		if sub.Created > lastDay {
			continue
		}

		active := sub.Status == "active" || sub.Status == "trialing" || sub.Status == "past_due"
		if active {
			if sub.CurrentPeriodStart > lastDay || sub.CurrentPeriodEnd < monthStart {
				continue
			}
		} else if sub.Status == "canceled" {
			canceledAt := sub.CanceledAt
			if canceledAt == nil {
				canceledAt = sub.EndedAt
			}
			if canceledAt != nil && *canceledAt < monthStart {
				continue
			}
		} else {
			continue
		}

		// Fetch customer
		cust, ok := cache[sub.Customer]
		if !ok {
			cust = fetchStripeCustomer(apiKey, sub.Customer)
			cache[sub.Customer] = cust
		}
		if cust == nil {
			continue
		}

		var priceItem *struct {
			Price struct {
				ID         string `json:"id"`
				UnitAmount int64  `json:"unit_amount"`
				Currency   string `json:"currency"`
				Recurring  struct {
					Interval      string `json:"interval"`
					IntervalCount int    `json:"interval_count"`
				} `json:"recurring"`
				Product string `json:"product"`
			} `json:"price"`
		}
		for i := range sub.Items.Data {
			if sub.Items.Data[i].Price.Product == productID {
				priceItem = &sub.Items.Data[i]
				break
			}
		}

		currency := "EUR"
		unitAmount := float64(0)
		interval := "month"
		if priceItem != nil {
			currency = strings.ToUpper(priceItem.Price.Currency)
			unitAmount = float64(priceItem.Price.UnitAmount) / 100
			interval = priceItem.Price.Recurring.Interval
		}

		emailHash := hashEmail(cust.Email, salt)
		firstName, _ := splitName(cust.Name)

		plan := "monthly"
		if interval == "year" {
			plan = "yearly"
		}

		// Parse latest invoice
		var payment *MemberPayment
		var inv stripeInvoice
		if json.Unmarshal(sub.LatestInvoice, &inv) == nil && inv.Status == "paid" {
			paidAt := inv.Created
			if inv.StatusTransitions.PaidAt != nil {
				paidAt = *inv.StatusTransitions.PaidAt
			}
			d := time.Unix(paidAt, 0).UTC().Format("2006-01-02")
			payment = &MemberPayment{
				Date:   d,
				Amount: MemberAmount{Value: float64(inv.AmountPaid) / 100, Decimals: 2, Currency: strings.ToUpper(inv.Currency)},
				Status: "succeeded",
				URL:    inv.HostedInvoiceURL,
			}
		}

		discord := sub.Metadata["client_reference_id"]
		if discord == "" {
			discord = sub.Metadata["discord_username"]
		}
		if discord == "" && cust.Metadata != nil {
			discord = cust.Metadata["discord_username"]
		}

		var discordPtr *string
		if discord != "" {
			discordPtr = &discord
		}

		result = append(result, providerSubscription{
			ID:                 sub.ID[:Min(14, len(sub.ID))] + "...",
			Source:             "stripe",
			EmailHash:          emailHash,
			FirstName:          firstName,
			Plan:               plan,
			Amount:             MemberAmount{Value: unitAmount, Decimals: 2, Currency: currency},
			Interval:           interval,
			Status:             sub.Status,
			CurrentPeriodStart: time.Unix(sub.CurrentPeriodStart, 0).UTC().Format("2006-01-02"),
			CurrentPeriodEnd:   time.Unix(sub.CurrentPeriodEnd, 0).UTC().Format("2006-01-02"),
			LatestPayment:      payment,
			SubscriptionURL:    fmt.Sprintf("https://dashboard.stripe.com/subscriptions/%s", sub.ID),
			CreatedAt:          time.Unix(sub.Created, 0).UTC().Format("2006-01-02"),
			Discord:            discordPtr,
			ProductID:          productID,
		})

		time.Sleep(50 * time.Millisecond) // Be polite
	}

	return providerSnapshot{
		Provider:      "stripe",
		FetchedAt:     time.Now().UTC().Format(time.RFC3339),
		Subscriptions: result,
	}
}

func fetchStripeCustomer(apiKey, customerID string) *stripeCustomer {
	url := "https://api.stripe.com/v1/customers/" + customerID
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil
	}
	req.Header.Set("Authorization", "Bearer "+apiKey)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil
	}

	var cust stripeCustomer
	json.NewDecoder(resp.Body).Decode(&cust)
	return &cust
}

func generateAndSaveSalt() string {
	b := make([]byte, 16)
	rand.Read(b)
	salt := "prod-" + hex.EncodeToString(b)

	// Persist to config.env
	configPath := configEnvPath()
	existing := loadConfigEnv(configPath)
	existing["EMAIL_HASH_SALT"] = salt
	saveConfigEnv(configPath, existing)
	os.Setenv("EMAIL_HASH_SALT", salt)

	return salt
}

func hashEmail(email, salt string) string {
	h := sha256.Sum256([]byte(strings.ToLower(strings.TrimSpace(email)) + salt))
	return fmt.Sprintf("%x", h)
}

func splitName(name *string) (string, string) {
	if name == nil || *name == "" {
		return "Member", ""
	}
	parts := strings.Fields(*name)
	if len(parts) == 1 {
		return parts[0], ""
	}
	return parts[0], strings.Join(parts[1:], " ")
}

// ── Merge ───────────────────────────────────────────────────────────────────

func mergeProviderSnapshots(snapshots []providerSnapshot) []Member {
	seen := map[string]Member{}

	// Process stripe first (priority), then odoo
	sortedSnaps := make([]providerSnapshot, len(snapshots))
	copy(sortedSnaps, snapshots)
	for i := range sortedSnaps {
		for j := i + 1; j < len(sortedSnaps); j++ {
			if sortedSnaps[i].Provider != "stripe" && sortedSnaps[j].Provider == "stripe" {
				sortedSnaps[i], sortedSnaps[j] = sortedSnaps[j], sortedSnaps[i]
			}
		}
	}

	for _, snap := range sortedSnaps {
		for _, sub := range snap.Subscriptions {
			if _, ok := seen[sub.EmailHash]; ok {
				continue
			}
			seen[sub.EmailHash] = Member{
				ID:     sub.ID,
				Source: sub.Source,
				Accounts: MemberAccounts{
					EmailHash: sub.EmailHash,
					Discord:   sub.Discord,
				},
				FirstName:          sub.FirstName,
				Plan:               sub.Plan,
				Amount:             sub.Amount,
				Interval:           sub.Interval,
				Status:             sub.Status,
				CurrentPeriodStart: sub.CurrentPeriodStart,
				CurrentPeriodEnd:   sub.CurrentPeriodEnd,
				LatestPayment:      sub.LatestPayment,
				SubscriptionURL:    sub.SubscriptionURL,
				CreatedAt:          sub.CreatedAt,
				IsOrganization:     sub.IsOrganization,
			}
		}
	}

	var result []Member
	for _, m := range seen {
		result = append(result, m)
	}

	// Sort by createdAt
	for i := range result {
		for j := i + 1; j < len(result); j++ {
			if result[i].CreatedAt > result[j].CreatedAt {
				result[i], result[j] = result[j], result[i]
			}
		}
	}

	return result
}

func calculateMembersSummary(members []Member) MembersSummary {
	var active, monthly, yearly int
	var monthlyMRR, yearlyMRR float64

	for _, m := range members {
		if m.Status == "active" || m.Status == "trialing" {
			active++
			if m.Plan == "monthly" {
				monthly++
				monthlyMRR += m.Amount.Value
			} else {
				yearly++
				yearlyMRR += m.Amount.Value / 12
			}
		}
	}

	mrr := math.Round((monthlyMRR+yearlyMRR)*100) / 100

	return MembersSummary{
		TotalMembers:   len(members),
		ActiveMembers:  active,
		MonthlyMembers: monthly,
		YearlyMembers:  yearly,
		MRR:            MemberAmount{Value: mrr, Decimals: 2, Currency: "EUR"},
	}
}

// ── Month parsing ───────────────────────────────────────────────────────────

type yearMonth struct {
	year  int
	month int
}

func getMemberMonths(args []string) []yearMonth {
	now := time.Now()

	// Check --month=YYYY-MM
	monthArg := GetOption(args, "--month")
	if monthArg != "" {
		parts := strings.Split(monthArg, "-")
		if len(parts) == 2 {
			y, _ := strconv.Atoi(parts[0])
			m, _ := strconv.Atoi(parts[1])
			return []yearMonth{{y, m}}
		}
	}

	// Check --backfill
	if HasFlag(args, "--backfill") {
		var months []yearMonth
		y, m := 2024, 6
		for y < now.Year() || (y == now.Year() && m <= int(now.Month())) {
			months = append(months, yearMonth{y, m})
			m++
			if m > 12 {
				m = 1
				y++
			}
		}
		return months
	}

	return []yearMonth{{now.Year(), int(now.Month())}}
}

// ── Odoo JSON-RPC ───────────────────────────────────────────────────────────

type odooRPCResponse struct {
	Result json.RawMessage `json:"result"`
	Error  *struct {
		Data struct {
			Message string `json:"message"`
		} `json:"data"`
		Message string `json:"message"`
	} `json:"error"`
}

func odooRPC(url, service, method string, args []interface{}) (json.RawMessage, error) {
	payload := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      time.Now().UnixMilli(),
		"method":  "call",
		"params": map[string]interface{}{
			"service": service,
			"method":  method,
			"args":    args,
		},
	}

	body, _ := json.Marshal(payload)
	resp, err := http.Post(url+"/jsonrpc", "application/json", strings.NewReader(string(body)))
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	var rpcResp odooRPCResponse
	if err := json.NewDecoder(resp.Body).Decode(&rpcResp); err != nil {
		return nil, fmt.Errorf("decode failed: %w", err)
	}
	if rpcResp.Error != nil {
		msg := rpcResp.Error.Data.Message
		if msg == "" {
			msg = rpcResp.Error.Message
		}
		return nil, fmt.Errorf("odoo error: %s", msg)
	}

	return rpcResp.Result, nil
}

// odooDBFromURL derives the database name from the Odoo URL.
// e.g. "https://citizen-spring-vzw.odoo.com" → "citizen-spring-vzw"
func odooDBFromURL(odooURL string) string {
	u := strings.TrimPrefix(odooURL, "https://")
	u = strings.TrimPrefix(u, "http://")
	if idx := strings.Index(u, "."); idx > 0 {
		return u[:idx]
	}
	return u
}

func odooAuth(odooURL, db, login, password string) (int, error) {
	result, err := odooRPC(odooURL, "common", "authenticate", []interface{}{
		db, login, password, map[string]interface{}{},
	})
	if err != nil {
		return 0, err
	}

	var uid int
	if err := json.Unmarshal(result, &uid); err != nil || uid == 0 {
		return 0, fmt.Errorf("auth failed (uid=0)")
	}
	return uid, nil
}

func odooExec(odooURL, db string, uid int, password, model, method string, args []interface{}, kwargs map[string]interface{}) (json.RawMessage, error) {
	callArgs := []interface{}{db, uid, password, model, method, args}
	if kwargs == nil {
		kwargs = map[string]interface{}{}
	}
	callArgs = append(callArgs, kwargs)
	return odooRPC(odooURL, "object", "execute_kw", callArgs)
}

func buildOdooSnapshot(settings *Settings, odooURL, login, password, salt string) (providerSnapshot, error) {
	odoo := settings.Membership.Odoo
	db := odooDBFromURL(odooURL)
	empty := providerSnapshot{Provider: "odoo", FetchedAt: time.Now().UTC().Format(time.RFC3339)}

	uid, err := odooAuth(odooURL, db, login, password)
	if err != nil {
		return empty, err
	}

	// Get product template IDs from settings
	templateIDs := make([]interface{}, len(odoo.Products))
	for i, p := range odoo.Products {
		templateIDs[i] = p.ID
	}

	// Find product.product IDs for our templates
	ppResult, err := odooExec(odooURL, db, uid, password, "product.product", "search",
		[]interface{}{[]interface{}{[]interface{}{"product_tmpl_id", "in", templateIDs}}}, nil)
	if err != nil {
		return empty, fmt.Errorf("product search: %w", err)
	}
	var ppIDs []int
	json.Unmarshal(ppResult, &ppIDs)
	if len(ppIDs) == 0 {
		return empty, nil
	}

	// Search active subscriptions
	ppIDsIface := make([]interface{}, len(ppIDs))
	for i, id := range ppIDs {
		ppIDsIface[i] = id
	}
	orderResult, err := odooExec(odooURL, db, uid, password, "sale.order", "search",
		[]interface{}{[]interface{}{
			[]interface{}{"is_subscription", "=", true},
			[]interface{}{"order_line.product_id", "in", ppIDsIface},
			[]interface{}{"subscription_state", "in", []interface{}{"3_progress", "4_paused"}},
		}}, nil)
	if err != nil {
		return empty, fmt.Errorf("order search: %w", err)
	}
	var orderIDs []int
	json.Unmarshal(orderResult, &orderIDs)
	if len(orderIDs) == 0 {
		return empty, nil
	}

	// Read orders
	orderIDsIface := make([]interface{}, len(orderIDs))
	for i, id := range orderIDs {
		orderIDsIface[i] = id
	}
	ordersRaw, err := odooExec(odooURL, db, uid, password, "sale.order", "read",
		[]interface{}{orderIDsIface},
		map[string]interface{}{"fields": []string{
			"partner_id", "subscription_state", "recurring_monthly",
			"start_date", "next_invoice_date", "amount_total",
			"currency_id", "order_line", "plan_id", "invoice_ids",
		}})
	if err != nil {
		return empty, fmt.Errorf("order read: %w", err)
	}

	var orders []map[string]interface{}
	json.Unmarshal(ordersRaw, &orders)

	// Collect all invoice IDs
	allInvoiceIDs := map[int]bool{}
	for _, o := range orders {
		if invIDs, ok := o["invoice_ids"].([]interface{}); ok {
			for _, id := range invIDs {
				if fid, ok := id.(float64); ok {
					allInvoiceIDs[int(fid)] = true
				}
			}
		}
	}

	// Fetch paid invoices
	invoiceMap := map[int]map[string]interface{}{}
	if len(allInvoiceIDs) > 0 {
		invIDsIface := make([]interface{}, 0, len(allInvoiceIDs))
		for id := range allInvoiceIDs {
			invIDsIface = append(invIDsIface, id)
		}
		invRaw, err := odooExec(odooURL, db, uid, password, "account.move", "read",
			[]interface{}{invIDsIface},
			map[string]interface{}{"fields": []string{"payment_state", "invoice_date", "amount_total", "currency_id"}})
		if err == nil {
			var invoices []map[string]interface{}
			json.Unmarshal(invRaw, &invoices)
			for _, inv := range invoices {
				ps, _ := inv["payment_state"].(string)
				if ps == "paid" || ps == "in_payment" {
					if id, ok := inv["id"].(float64); ok {
						invoiceMap[int(id)] = inv
					}
				}
			}
		}
	}

	// Fetch partners
	partnerIDSet := map[int]bool{}
	for _, o := range orders {
		if pid, ok := o["partner_id"].([]interface{}); ok && len(pid) > 0 {
			if fid, ok := pid[0].(float64); ok {
				partnerIDSet[int(fid)] = true
			}
		}
	}
	partnerMap := map[int]map[string]interface{}{}
	if len(partnerIDSet) > 0 {
		pIDs := make([]interface{}, 0, len(partnerIDSet))
		for id := range partnerIDSet {
			pIDs = append(pIDs, id)
		}
		pRaw, err := odooExec(odooURL, db, uid, password, "res.partner", "read",
			[]interface{}{pIDs},
			map[string]interface{}{"fields": []string{"name", "email", "is_company"}})
		if err == nil {
			var partners []map[string]interface{}
			json.Unmarshal(pRaw, &partners)
			for _, p := range partners {
				if id, ok := p["id"].(float64); ok {
					partnerMap[int(id)] = p
				}
			}
		}
	}

	// Fetch order lines → product template mapping
	allLineIDs := []interface{}{}
	for _, o := range orders {
		if lines, ok := o["order_line"].([]interface{}); ok {
			allLineIDs = append(allLineIDs, lines...)
		}
	}

	orderToTemplate := map[int]int{}
	if len(allLineIDs) > 0 {
		linesRaw, err := odooExec(odooURL, db, uid, password, "sale.order.line", "read",
			[]interface{}{allLineIDs},
			map[string]interface{}{"fields": []string{"product_id", "order_id"}})
		if err == nil {
			var lines []map[string]interface{}
			json.Unmarshal(linesRaw, &lines)

			lineProductIDs := map[int]bool{}
			for _, l := range lines {
				if pid, ok := l["product_id"].([]interface{}); ok && len(pid) > 0 {
					if fid, ok := pid[0].(float64); ok {
						lineProductIDs[int(fid)] = true
					}
				}
			}

			ppToTmpl := map[int]int{}
			if len(lineProductIDs) > 0 {
				ppIDsList := make([]interface{}, 0, len(lineProductIDs))
				for id := range lineProductIDs {
					ppIDsList = append(ppIDsList, id)
				}
				ppRaw, err := odooExec(odooURL, db, uid, password, "product.product", "read",
					[]interface{}{ppIDsList},
					map[string]interface{}{"fields": []string{"product_tmpl_id"}})
				if err == nil {
					var ppProducts []map[string]interface{}
					json.Unmarshal(ppRaw, &ppProducts)
					for _, p := range ppProducts {
						if tmpl, ok := p["product_tmpl_id"].([]interface{}); ok && len(tmpl) > 0 {
							if id, ok := p["id"].(float64); ok {
								if tmplID, ok := tmpl[0].(float64); ok {
									ppToTmpl[int(id)] = int(tmplID)
								}
							}
						}
					}
				}
			}

			for _, l := range lines {
				pid, _ := l["product_id"].([]interface{})
				oid, _ := l["order_id"].([]interface{})
				if len(pid) > 0 && len(oid) > 0 {
					ppID := int(pid[0].(float64))
					orderID := int(oid[0].(float64))
					if tmplID, ok := ppToTmpl[ppID]; ok {
						for _, p := range odoo.Products {
							if p.ID == tmplID {
								orderToTemplate[orderID] = tmplID
								break
							}
						}
					}
				}
			}
		}
	}

	// Build subscriptions
	var subs []providerSubscription
	for _, order := range orders {
		orderID := int(order["id"].(float64))
		partnerID := 0
		if pid, ok := order["partner_id"].([]interface{}); ok && len(pid) > 0 {
			partnerID = int(pid[0].(float64))
		}
		partner := partnerMap[partnerID]
		if partner == nil {
			continue
		}

		email, _ := partner["email"].(string)
		emailHash := email
		if email != "" {
			emailHash = hashEmail(email, salt)
		} else {
			emailHash = fmt.Sprintf("odoo-noemail-%d", orderID)
		}

		partnerName, _ := partner["name"].(string)
		firstName, lastName := splitName(&partnerName)
		isCompany, _ := partner["is_company"].(bool)

		tmplID := orderToTemplate[orderID]
		var productConfig *OdooProduct
		for i := range odoo.Products {
			if odoo.Products[i].ID == tmplID {
				productConfig = &odoo.Products[i]
				break
			}
		}
		interval := "month"
		if productConfig != nil {
			interval = productConfig.Interval
		}

		subState, _ := order["subscription_state"].(string)
		status := "active"
		if subState == "4_paused" {
			status = "paused"
		}

		recurringMonthly, _ := order["recurring_monthly"].(float64)
		totalAmount := recurringMonthly
		if interval == "year" {
			totalAmount = recurringMonthly * 12
		}

		startDate, _ := order["start_date"].(string)
		nextInvoice, _ := order["next_invoice_date"].(string)

		// Find latest paid invoice
		var latestPayment *MemberPayment
		if invIDs, ok := order["invoice_ids"].([]interface{}); ok {
			var bestDate string
			for _, iid := range invIDs {
				inv := invoiceMap[int(iid.(float64))]
				if inv == nil {
					continue
				}
				invDate, _ := inv["invoice_date"].(string)
				if invDate > bestDate {
					bestDate = invDate
					invAmount, _ := inv["amount_total"].(float64)
					latestPayment = &MemberPayment{
						Date:   invDate,
						Amount: MemberAmount{Value: invAmount, Decimals: 2, Currency: "EUR"},
						Status: "succeeded",
						URL:    fmt.Sprintf("%s/web#id=%d&model=account.move&view_type=form", odooURL, int(iid.(float64))),
					}
				}
			}
		}

		isOrg := isCompany || tmplID == 104

		subs = append(subs, providerSubscription{
			ID:                 fmt.Sprintf("odoo-%d", orderID),
			Source:             "odoo",
			EmailHash:          emailHash,
			FirstName:          firstName,
			LastName:           lastName,
			Plan:               interval + "ly",
			Amount:             MemberAmount{Value: math.Round(totalAmount*100) / 100, Decimals: 2, Currency: "EUR"},
			Interval:           interval,
			Status:             status,
			CurrentPeriodStart: startDate,
			CurrentPeriodEnd:   nextInvoice,
			LatestPayment:      latestPayment,
			SubscriptionURL:    fmt.Sprintf("%s/web#id=%d&model=sale.order&view_type=form", odooURL, orderID),
			CreatedAt:          startDate,
			IsOrganization:     isOrg,
			ProductID:          tmplID,
		})
	}

	return providerSnapshot{
		Provider:      "odoo",
		FetchedAt:     time.Now().UTC().Format(time.RFC3339),
		Subscriptions: subs,
	}, nil
}

func printMembersSyncHelp() {
	f := Fmt
	fmt.Printf(`
%schb members sync%s — Fetch membership data from Stripe and Odoo

%sUSAGE%s
  %schb members sync%s [options]

%sOPTIONS%s
  %s--month%s <YYYY-MM>    Fetch specific month only
  %s--backfill%s           Process all months since 2024-06
  %s--stripe-only%s        Only fetch from Stripe
  %s--odoo-only%s          Only fetch from Odoo
  %s--help, -h%s           Show this help

%sENVIRONMENT%s
  %sSTRIPE_SECRET_KEY%s    Stripe secret key (required for Stripe)
  %sODOO_URL%s             Odoo instance URL (e.g. https://mycompany.odoo.com)
  %sODOO_LOGIN%s           Odoo login email
  %sODOO_PASSWORD%s        Odoo password or API key
  %sEMAIL_HASH_SALT%s      Salt for email hashing (required)
`,
		f.Bold, f.Reset,
		f.Bold, f.Reset,
		f.Cyan, f.Reset,
		f.Bold, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Bold, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
	)
}
