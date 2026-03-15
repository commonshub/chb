package cmd

import (
	"crypto/sha256"
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
	if HasFlag(args, "--help", "-h") {
		printMembersSyncHelp()
		return nil
	}

	fmt.Printf("\n%s🔄 Fetching membership data%s\n\n", Fmt.Bold, Fmt.Reset)

	dataDir := DataDir()
	stripeKey := os.Getenv("STRIPE_SECRET_KEY")
	odooKey := os.Getenv("ODOO_API_KEY")
	odooLogin := os.Getenv("ODOO_LOGIN")
	salt := os.Getenv("EMAIL_HASH_SALT")

	if salt == "" {
		return fmt.Errorf("EMAIL_HASH_SALT environment variable required")
	}

	stripeOnly := HasFlag(args, "--stripe-only")
	odooOnly := HasFlag(args, "--odoo-only")
	doStripe := !odooOnly
	doOdoo := !stripeOnly

	// Determine months
	months := getMemberMonths(args)
	fmt.Printf("📆 %d month(s) to process\n", len(months))

	// Read settings for product ID
	settingsData, _ := os.ReadFile(filepath.Join("src", "settings", "settings.json"))
	stripeProductID := ""
	if settingsData != nil {
		var s struct {
			Membership struct {
				Stripe struct {
					ProductID string `json:"productId"`
				} `json:"stripe"`
			} `json:"membership"`
		}
		if json.Unmarshal(settingsData, &s) == nil {
			stripeProductID = s.Membership.Stripe.ProductID
		}
	}

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

	if doOdoo && (odooKey == "" || odooLogin == "") {
		fmt.Printf("%s⚠ ODOO_API_KEY/ODOO_LOGIN not set, skipping Odoo%s\n", Fmt.Yellow, Fmt.Reset)
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
			snapDir := filepath.Join(monthDir, "stripe")
			os.MkdirAll(snapDir, 0755)
			writeJSONFile(filepath.Join(snapDir, "subscriptions.json"), snap)
			snapshots = append(snapshots, snap)
		} else {
			// Try loading cached
			snapPath := filepath.Join(monthDir, "stripe", "subscriptions.json")
			if data, err := os.ReadFile(snapPath); err == nil {
				var snap providerSnapshot
				if json.Unmarshal(data, &snap) == nil {
					snapshots = append(snapshots, snap)
					fmt.Printf("  Stripe: loaded from cache\n")
				}
			}
		}

		// Odoo (only current month)
		now := time.Now()
		isCurrentMonth := year == now.Year() && month == int(now.Month())
		if doOdoo && isCurrentMonth {
			// Odoo integration would require JSON-RPC calls
			// For now, just load from cache if available
			fmt.Printf("  Odoo: skipped (JSON-RPC not yet implemented in Go)\n")
		}
		// Load cached Odoo
		odooSnapPath := filepath.Join(monthDir, "odoo", "subscriptions.json")
		if data, err := os.ReadFile(odooSnapPath); err == nil {
			var snap providerSnapshot
			if json.Unmarshal(data, &snap) == nil {
				snapshots = append(snapshots, snap)
				fmt.Printf("  Odoo: loaded from cache\n")
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

		os.MkdirAll(monthDir, 0755)
		writeJSONFile(filepath.Join(monthDir, "members.json"), out)
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
  %sODOO_API_KEY%s         Odoo API key (required for Odoo)
  %sODOO_LOGIN%s           Odoo login email
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
	)
}
