package cmd

// Member recaps: each member's monthly / quarterly / yearly recap of their
// use of the hub (days at the hub, tokens, praise, photos), rendered by the
// website and sent by chb, which knows the members and their addresses.
//
//   - The website renders: GET <site>/api/recap?discordId=<id>&period=P
//     ({member, summary, subject, html, text}; 404 = not in the server).
//     Its footer carries the member's signed notification-settings link.
//   - The website holds the preferences: GET <site>/api/notifications/subscribers
//     ({default:{monthly,quarterly,yearly}, members:{<discordId>:{…}}});
//     all three false = opted out, never email.
//   - chb picks the recipients: active and grace members (latest members.json,
//     stewards tier) with a Discord id and an address. The Discord id comes
//     from the member's accounts.discord (when it is a Discord id) or from
//     the stewards' map settings/members-discord.json ("odoo:<partner id>"
//     or "stripe:<customer id>" → Discord id). Never guessed from names.
//   - Due periods: the previous month in the first days of a month, the
//     previous quarter in the first days of Jan/Apr/Jul/Oct, the previous
//     year in the first days of January (recapDueDays tolerates a missed run).
//   - Ledger latest/providers/members/recaps-sent.json, keyed
//     discordId:period: nobody gets the same period twice. At most
//     recapMaxPerRun emails per run; the rest go out on the next run.
//
// Both endpoints use Authorization: Bearer RENEW_LINK_SECRET. Mail goes
// through Resend (RESEND_API_KEY), from hello@, as the reminders do.

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	odoosource "github.com/CommonsHub/chb/providers/odoo"
	stripesource "github.com/CommonsHub/chb/providers/stripe"
)

const (
	recapSiteDefault = "https://commonshub.brussels"
	recapMaxPerRun   = 25
	recapDueDays     = 3 // a period is due from day 1 to day 3 of the next one
)

var discordIDPattern = regexp.MustCompile(`^\d{15,21}$`)

// RecapPrefs: which recaps a member wants.
type RecapPrefs struct {
	Monthly   bool `json:"monthly"`
	Quarterly bool `json:"quarterly"`
	Yearly    bool `json:"yearly"`
}

func (p RecapPrefs) wants(kind string) bool {
	switch kind {
	case "monthly":
		return p.Monthly
	case "quarterly":
		return p.Quarterly
	case "yearly":
		return p.Yearly
	}
	return false
}

type recapSubscribers struct {
	Default RecapPrefs            `json:"default"`
	Members map[string]RecapPrefs `json:"members"`
}

func (s recapSubscribers) prefs(discordID string) RecapPrefs {
	if p, ok := s.Members[discordID]; ok {
		return p
	}
	return s.Default
}

// recapPeriod is "2026-10" (monthly), "2026-Q4" (quarterly) or "2026" (yearly).
type recapPeriod struct {
	ID   string
	Kind string
}

func parseRecapPeriod(s string) (recapPeriod, error) {
	s = strings.TrimSpace(s)
	switch {
	case regexp.MustCompile(`^\d{4}-(0[1-9]|1[0-2])$`).MatchString(s):
		return recapPeriod{s, "monthly"}, nil
	case regexp.MustCompile(`^\d{4}-Q[1-4]$`).MatchString(s):
		return recapPeriod{s, "quarterly"}, nil
	case regexp.MustCompile(`^\d{4}$`).MatchString(s):
		return recapPeriod{s, "yearly"}, nil
	}
	return recapPeriod{}, fmt.Errorf("period %q: expected YYYY-MM, YYYY-Qn or YYYY", s)
}

// dueRecapPeriods: the periods that ended just before today (Brussels).
func dueRecapPeriods(today time.Time) []recapPeriod {
	if today.Day() > recapDueDays {
		return nil
	}
	prev := time.Date(today.Year(), today.Month(), 1, 0, 0, 0, 0, today.Location()).AddDate(0, -1, 0)
	out := []recapPeriod{{prev.Format("2006-01"), "monthly"}}
	if m := today.Month(); m == time.January || m == time.April || m == time.July || m == time.October {
		out = append(out, recapPeriod{fmt.Sprintf("%d-Q%d", prev.Year(), (int(prev.Month())-1)/3+1), "quarterly"})
	}
	if today.Month() == time.January {
		out = append(out, recapPeriod{strconv.Itoa(prev.Year()), "yearly"})
	}
	return out
}

// recapRecipient: a member we could email (no address until sending/preview).
type recapRecipient struct {
	DiscordID        string
	FirstName        string
	OdooPartnerID    int
	StripeCustomerID string
}

func (r recapRecipient) identity() string {
	if r.OdooPartnerID > 0 {
		return fmt.Sprintf("odoo:%d", r.OdooPartnerID)
	}
	if r.StripeCustomerID != "" {
		return "stripe:" + r.StripeCustomerID
	}
	return ""
}

func recapLinksPath() string { return settingsFilePath("members-discord.json") }

// loadRecapDiscordLinks: the stewards' map "odoo:<id>" / "stripe:<cus>" → Discord id.
func loadRecapDiscordLinks() map[string]string {
	out := map[string]string{}
	if data, err := os.ReadFile(recapLinksPath()); err == nil {
		_ = json.Unmarshal(data, &out)
	}
	return out
}

// recapRecipients: active and grace members with a Discord id. The others
// are counted in missing (no Discord id), never guessed.
func recapRecipients(members []Member, links map[string]string) (out []recapRecipient, missing int) {
	seen := map[string]bool{}
	for _, m := range members {
		if m.Status != "active" && m.Status != "grace" {
			continue
		}
		r := recapRecipient{FirstName: m.FirstName, OdooPartnerID: m.OdooPartnerID, StripeCustomerID: m.StripeCustomerID}
		if m.Accounts.Discord != nil && discordIDPattern.MatchString(*m.Accounts.Discord) {
			r.DiscordID = *m.Accounts.Discord
		}
		if r.DiscordID == "" && m.OdooPartnerID > 0 {
			r.DiscordID = links[fmt.Sprintf("odoo:%d", m.OdooPartnerID)]
		}
		if r.DiscordID == "" && m.StripeCustomerID != "" {
			r.DiscordID = links["stripe:"+m.StripeCustomerID]
		}
		if !discordIDPattern.MatchString(r.DiscordID) {
			missing++
			continue
		}
		if seen[r.DiscordID] {
			continue // one member, two memberships: one recap
		}
		seen[r.DiscordID] = true
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].DiscordID < out[j].DiscordID })
	return out, missing
}

func loadCurrentMembers(dataDir string) ([]Member, error) {
	data, err := os.ReadFile(filepath.Join(dataDir, "latest", stewardsDirName, "members.json"))
	if err != nil {
		return nil, fmt.Errorf("no members.json — run `chb generate` first")
	}
	var f MembersOutputFile
	if err := json.Unmarshal(data, &f); err != nil {
		return nil, err
	}
	return f.Members, nil
}

// recapAddress: the Odoo partner's address from the local partner
// snapshot, else the Stripe customer's (live).
func recapAddress(dataDir string, r recapRecipient, partners map[int]string) (string, error) {
	if em := partners[r.OdooPartnerID]; r.OdooPartnerID > 0 && em != "" {
		return em, nil
	}
	if r.StripeCustomerID != "" {
		if c := stripesource.FetchCustomer(os.Getenv("STRIPE_SECRET_KEY"), r.StripeCustomerID); c != nil && c.Email != "" {
			return c.Email, nil
		}
	}
	return "", fmt.Errorf("no email address")
}

func loadPartnerEmails(dataDir string) map[int]string {
	out := map[int]string{}
	data, err := os.ReadFile(odoosource.Path(dataDir, "latest", "", odoosource.PartnersFile))
	if err != nil {
		return out
	}
	var f struct {
		Partners []struct {
			ID    int    `json:"id"`
			Email string `json:"email"`
		} `json:"partners"`
	}
	if json.Unmarshal(data, &f) == nil {
		for _, p := range f.Partners {
			if em := strings.TrimSpace(p.Email); em != "" {
				out[p.ID] = em
			}
		}
	}
	return out
}

// ── website ────────────────────────────────────────────────────────────

func recapSite() string {
	if s := strings.TrimRight(os.Getenv("CHB_WEBSITE_URL"), "/"); s != "" {
		return s
	}
	return recapSiteDefault
}

var errRecapNotFound = fmt.Errorf("not in the Discord server")

var recapGet = func(secret, path string, out interface{}) error {
	req, err := http.NewRequest("GET", recapSite()+path, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+secret)
	resp, err := (&http.Client{Timeout: 60 * time.Second}).Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return errRecapNotFound
	}
	if resp.StatusCode >= 300 {
		return fmt.Errorf("%s: HTTP %d", path, resp.StatusCode)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func fetchRecapSubscribers(secret string) (recapSubscribers, error) {
	var s recapSubscribers
	err := recapGet(secret, "/api/notifications/subscribers", &s)
	return s, err
}

type recapMessage struct {
	Member struct {
		DiscordID string `json:"discordId"`
		Username  string `json:"username"`
		Name      string `json:"name"`
	} `json:"member"`
	Summary struct {
		Days     float64 `json:"days"`
		Received float64 `json:"received"`
		Spent    float64 `json:"spent"`
		Praise   float64 `json:"praise"`
	} `json:"summary"`
	Subject string `json:"subject"`
	HTML    string `json:"html"`
	Text    string `json:"text"`
}

func fetchRecap(secret, discordID, period string) (recapMessage, error) {
	var m recapMessage
	q := url.Values{"discordId": {discordID}, "period": {period}}
	if err := recapGet(secret, "/api/recap?"+q.Encode(), &m); err != nil {
		return m, err
	}
	if m.Subject == "" || m.HTML == "" {
		return m, fmt.Errorf("recap %s %s: empty subject or html", discordID, period)
	}
	return m, nil
}

// ── ledger ─────────────────────────────────────────────────────────────

type RecapLedgerEntry struct {
	SentAt    string `json:"sentAt"`
	MessageID string `json:"messageId,omitempty"`
}

func recapLedgerPath(dataDir string) string {
	return filepath.Join(remindersDir(dataDir), "recaps-sent.json")
}

func loadRecapLedger(dataDir string) map[string]RecapLedgerEntry {
	out := map[string]RecapLedgerEntry{}
	if data, err := os.ReadFile(recapLedgerPath(dataDir)); err == nil {
		_ = json.Unmarshal(data, &out)
	}
	return out
}

type recapSend struct {
	Recipient recapRecipient
	Period    recapPeriod
}

func (s recapSend) key() string { return s.Recipient.DiscordID + ":" + s.Period.ID }

// planRecaps: every (member, period) due, wanted and not yet sent.
func planRecaps(recipients []recapRecipient, periods []recapPeriod, subs recapSubscribers, ledger map[string]RecapLedgerEntry) (todo []recapSend, optedOut, alreadySent int) {
	for _, r := range recipients {
		p := subs.prefs(r.DiscordID)
		if !p.Monthly && !p.Quarterly && !p.Yearly {
			optedOut++
			continue
		}
		for _, per := range periods {
			if !p.wants(per.Kind) {
				continue
			}
			s := recapSend{r, per}
			if _, done := ledger[s.key()]; done {
				alreadySent++
				continue
			}
			todo = append(todo, s)
		}
	}
	return todo, optedOut, alreadySent
}

// MembersRecap is `chb members recap`.
func MembersRecap(args []string) error {
	if HasFlag(args, "--help", "-h", "help") {
		fmt.Print(`
chb members recap — monthly / quarterly / yearly recap email per member

USAGE
  chb members recap                    Preview the recaps due today
  chb members recap --period 2026-10   Preview one period (YYYY-MM, YYYY-Qn, YYYY)
  chb members recap --yes              Send (at most ` + strconv.Itoa(recapMaxPerRun) + ` per run; --limit N)
  chb members recap --send-test <email> --discord <id> [--period P]
                                       Send ONE member's recap to <email>
                                       (no ledger; default period: last month)

Recipients: active and grace members with a Discord id (accounts.discord,
or settings/members-discord.json: {"odoo:<partner id>": "<discord id>",
"stripe:<customer id>": "<discord id>"}). Preferences come from the
website; members who opted out are never emailed. Sent recaps are
recorded in latest/providers/members/recaps-sent.json and never sent twice.
Needs RENEW_LINK_SECRET (website) and, to send, RESEND_API_KEY.
`)
		return nil
	}
	secret := os.Getenv("RENEW_LINK_SECRET")
	if secret == "" {
		return fmt.Errorf("RENEW_LINK_SECRET must be set in config.env")
	}
	today := time.Now().In(BrusselsTZ())
	lastMonth := time.Date(today.Year(), today.Month(), 1, 0, 0, 0, 0, today.Location()).AddDate(0, -1, 0).Format("2006-01")

	if to := GetOption(args, "--send-test"); to != "" {
		discordID := GetOption(args, "--discord")
		if !discordIDPattern.MatchString(discordID) {
			return fmt.Errorf("--send-test needs --discord <discord id>")
		}
		per := GetOption(args, "--period")
		if per == "" {
			per = lastMonth
		}
		p, err := parseRecapPeriod(per)
		if err != nil {
			return err
		}
		apiKey := os.Getenv("RESEND_API_KEY")
		if apiKey == "" {
			return fmt.Errorf("RESEND_API_KEY must be set in config.env to send")
		}
		m, err := fetchRecap(secret, discordID, p.ID)
		if err != nil {
			return err
		}
		id, err := sendReminderEmail(apiKey, fmt.Sprintf("recap-test-%d", time.Now().UnixNano()), to, reminderEmail{Subject: m.Subject, HTML: m.HTML, Text: m.Text})
		if err != nil {
			return err
		}
		fmt.Printf("  ✓ test recap %s for %s sent to %s — Resend id %s\n", p.ID, m.Member.Username, maskEmail(to), id)
		return nil
	}

	periods := dueRecapPeriods(today)
	if per := GetOption(args, "--period"); per != "" {
		p, err := parseRecapPeriod(per)
		if err != nil {
			return err
		}
		periods = []recapPeriod{p}
	}
	dataDir := DataDir()
	members, err := loadCurrentMembers(dataDir)
	if err != nil {
		return err
	}
	recipients, missing := recapRecipients(members, loadRecapDiscordLinks())
	var ids []string
	for _, p := range periods {
		ids = append(ids, p.ID+" ("+p.Kind+")")
	}
	fmt.Printf("\n  Member recaps — due: %s\n", map[bool]string{true: "none today (periods are due on days 1–" + strconv.Itoa(recapDueDays) + "; use --period)", false: strings.Join(ids, ", ")}[len(periods) == 0])
	fmt.Printf("  %d active/grace member(s) with a Discord id, %d skipped without one\n", len(recipients), missing)
	if len(periods) == 0 || len(recipients) == 0 {
		fmt.Println()
		return nil
	}
	subs, err := fetchRecapSubscribers(secret)
	if err != nil {
		return fmt.Errorf("notification preferences: %v", err)
	}
	ledger := loadRecapLedger(dataDir)
	todo, optedOut, alreadySent := planRecaps(recipients, periods, subs, ledger)
	fmt.Printf("  %d opted out, %d already sent, %d to send\n\n", optedOut, alreadySent, len(todo))

	limit := recapMaxPerRun
	if n, err := strconv.Atoi(GetOption(args, "--limit")); err == nil && n > 0 {
		limit = n
	}
	send := HasFlag(args, "--yes")
	apiKey := os.Getenv("RESEND_API_KEY")
	if send && apiKey == "" {
		return fmt.Errorf("RESEND_API_KEY must be set in config.env to send")
	}
	partners := loadPartnerEmails(dataDir)
	sent, failed := 0, 0
	for i, s := range todo {
		if send && sent >= limit {
			fmt.Printf("\n  %d left for the next run (limit %d)\n", len(todo)-i, limit)
			break
		}
		to, err := recapAddress(dataDir, s.Recipient, partners)
		if err != nil {
			fmt.Printf("    ⚠ %-9s %-10s %-20s %s: %v\n", s.Period.Kind, s.Period.ID, s.Recipient.DiscordID, s.Recipient.FirstName, err)
			failed++
			continue
		}
		if !send {
			fmt.Printf("    %-9s %-10s %-20s %-12s %s\n", s.Period.Kind, s.Period.ID, s.Recipient.DiscordID, s.Recipient.FirstName, maskEmail(to))
			continue
		}
		m, err := fetchRecap(secret, s.Recipient.DiscordID, s.Period.ID)
		if err != nil {
			Warnf("⚠ %s: %v", s.key(), err)
			failed++
			continue
		}
		id, err := sendReminderEmail(apiKey, "recap:"+s.key(), to, reminderEmail{Subject: m.Subject, HTML: m.HTML, Text: m.Text})
		if err != nil {
			Warnf("⚠ %s: %v", s.key(), err)
			failed++
			continue
		}
		ledger[s.key()] = RecapLedgerEntry{SentAt: time.Now().UTC().Format(time.RFC3339), MessageID: id}
		if err := writePrivateJSON(recapLedgerPath(dataDir), ledger); err != nil {
			return fmt.Errorf("record %s: %v (sent; stop to avoid a duplicate)", s.key(), err)
		}
		sent++
		fmt.Printf("    ✓ %s %s → %s\n", s.Period.ID, s.Recipient.FirstName, maskEmail(to))
	}
	if !send {
		if len(todo) > 0 {
			fmt.Printf("\n  %s(preview — nothing sent; add --yes to send)%s\n", Fmt.Dim, Fmt.Reset)
		}
		fmt.Println()
		return nil
	}
	fmt.Printf("\n  %d recap(s) sent, %d failed\n\n", sent, failed)
	return nil
}
