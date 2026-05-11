package cmd

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	odoosource "github.com/CommonsHub/chb/sources/odoo"
	stripesource "github.com/CommonsHub/chb/sources/stripe"
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
	stripeNeedsFetch := false
	now := time.Now()
	for _, ym := range months {
		yearStr := strconv.Itoa(ym.year)
		monthStr := fmt.Sprintf("%02d", ym.month)
		isCurrent := ym.year == now.Year() && ym.month == int(now.Month())
		if isCurrent || !fileExists(stripesource.Path(dataDir, yearStr, monthStr, stripesource.SubscriptionsFile)) {
			stripeNeedsFetch = true
			break
		}
	}

	// Fetch all Stripe subscriptions (once)
	var stripeSubscriptions []stripesource.Subscription
	customerCache := map[string]*stripesource.Customer{}

	if doStripe && !stripeNeedsFetch {
		doStripe = false
	} else if doStripe && stripeKey != "" {
		fmt.Println("📥 Fetching Stripe subscriptions...")
		var err error
		stripeSubscriptions, err = stripesource.FetchSubscriptions(stripeKey, stripeProductID)
		if err != nil {
			Warnf("  %s⚠ Stripe error: %v%s", Fmt.Yellow, err, Fmt.Reset)
			doStripe = false
		} else {
			fmt.Printf("  %d Stripe subscriptions\n", len(stripeSubscriptions))
		}
	} else if doStripe {
		Warnf("%s⚠ STRIPE_SECRET_KEY not set, skipping Stripe%s", Fmt.Yellow, Fmt.Reset)
		doStripe = false
	}

	if doOdoo && (odooURL == "" || odooLogin == "" || odooPassword == "") {
		Warnf("%s⚠ ODOO_URL/ODOO_LOGIN/ODOO_PASSWORD not set, skipping Odoo%s", Fmt.Yellow, Fmt.Reset)
		doOdoo = false
	}

	for _, ym := range months {
		year := ym.year
		month := ym.month
		monthStr := fmt.Sprintf("%02d", month)
		yearStr := strconv.Itoa(year)
		fmt.Printf("\n📅 %s-%s\n", yearStr, monthStr)

		var snapshots []providerSnapshot

		// Stripe
		if doStripe && len(stripeSubscriptions) > 0 {
			snap := buildStripeMonthSnapshot(stripeSubscriptions, year, month, salt, stripeProductID, stripeKey, customerCache)
			fmt.Printf("  Stripe: %d subscriptions\n", len(snap.Subscriptions))
			_ = stripesource.WriteJSON(dataDir, yearStr, monthStr, snap, stripesource.SubscriptionsFile)
			snapshots = append(snapshots, snap)
		} else {
			// Try loading cached
			snapPath := stripesource.Path(dataDir, yearStr, monthStr, stripesource.SubscriptionsFile)
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
				writeMonthFile(dataDir, yearStr, monthStr, odoosource.RelPath(odoosource.SubscriptionsFile), snapData)
				snapshots = append(snapshots, snap)
			}
		}
		// Load cached Odoo if not just fetched
		if !isCurrentMonth || !doOdoo || odooURL == "" {
			odooSnapPath := odoosource.Path(dataDir, yearStr, monthStr, odoosource.SubscriptionsFile)
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

func buildStripeMonthSnapshot(subs []stripesource.Subscription, year, month int, salt, productID, apiKey string, cache map[string]*stripesource.Customer) providerSnapshot {
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
			cust = stripesource.FetchCustomer(apiKey, sub.Customer)
			cache[sub.Customer] = cust
		}
		if cust == nil {
			continue
		}

		var priceItem *stripesource.SubscriptionItem
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
		var inv stripesource.Invoice
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
	sanitized := ""
	if name != nil {
		sanitized = sanitizePersonName(*name)
	}
	if sanitized == "" {
		return "Member", ""
	}
	parts := strings.Fields(sanitized)
	if len(parts) == 1 {
		return parts[0], ""
	}
	return parts[0], strings.Join(parts[1:], " ")
}

// sanitizePersonName drops whitespace-separated tokens that contain email
// addresses (typically emails that end up in Stripe/Odoo "name" fields). Used to keep
// email addresses out of firstName/lastName/name in public outputs.
func sanitizePersonName(s string) string {
	if s == "" {
		return ""
	}
	parts := strings.Fields(s)
	kept := parts[:0]
	for _, p := range parts {
		if containsEmail(p) {
			continue
		}
		kept = append(kept, p)
	}
	return strings.Join(kept, " ")
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

	// Check --month=YYYY-MM (members-specific alias)
	monthArg := GetOption(args, "--month")
	if monthArg != "" {
		parts := strings.Split(monthArg, "-")
		if len(parts) == 2 {
			y, _ := strconv.Atoi(parts[0])
			m, _ := strconv.Atoi(parts[1])
			return []yearMonth{{y, m}}
		}
	}

	// Shared sync semantics: positional year[/month], --since, --history
	posYear, posMonth, posFound := ParseYearMonthArg(args)
	if posFound {
		var months []yearMonth
		if posMonth != "" {
			return []yearMonth{{mustAtoi(posYear), mustAtoi(posMonth)}}
		}
		year := mustAtoi(posYear)
		for month := 1; month <= 12; month++ {
			months = append(months, yearMonth{year, month})
		}
		return months
	}

	if sinceMonth, isSince := ResolveSinceMonth(args, filepath.Join("generated", "members.json")); isSince {
		start := parseYearMonthValue(sinceMonth)
		var months []yearMonth
		y, m := start.year, start.month
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

func parseYearMonthValue(ym string) yearMonth {
	parts := strings.SplitN(ym, "-", 2)
	if len(parts) != 2 {
		now := time.Now()
		return yearMonth{now.Year(), int(now.Month())}
	}
	return yearMonth{mustAtoi(parts[0]), mustAtoi(parts[1])}
}

func mustAtoi(value string) int {
	n, err := strconv.Atoi(value)
	if err != nil {
		return 0
	}
	return n
}

func buildOdooSnapshot(settings *Settings, odooURL, login, password, salt string) (providerSnapshot, error) {
	empty := providerSnapshot{Provider: "odoo", FetchedAt: time.Now().UTC().Format(time.RFC3339)}
	products := make([]odoosource.MembershipProduct, len(settings.Membership.Odoo.Products))
	for i, product := range settings.Membership.Odoo.Products {
		products[i] = odoosource.MembershipProduct{
			ID:       product.ID,
			Name:     product.Name,
			Interval: product.Interval,
		}
	}
	snap, err := odoosource.BuildMembershipSnapshot(products, odooURL, login, password, salt)
	if err != nil {
		return empty, err
	}
	data, err := json.Marshal(snap)
	if err != nil {
		return empty, err
	}
	var out providerSnapshot
	if err := json.Unmarshal(data, &out); err != nil {
		return empty, err
	}
	return out, nil
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
