package cmd

// Membership reminders: one email per status change, for members in grace
// (payment failed or paused) or lapsed in the last 30 days.
//
//   - `chb generate` writes the plan for the current month, offline:
//     latest/providers/members/pending-reminders.json (stewards-only area).
//     No email addresses: who, why and since.
//   - `chb members remind` previews the plan, and with --yes sends what the
//     ledger (latest/providers/members/reminders-sent.json) has not sent
//     yet. The address is looked up live (Odoo partner, or the Stripe
//     customer for Stripe-only members); the renew link is signed with
//     RENEW_LINK_SECRET; mail goes through Resend (RESEND_API_KEY).
//
// Templates: cmd/templates/emails, copied from the website repo
// (docs/emails/membership-reminder.*, regenerated there from
// buildReminderEmail). Refresh: cp <site>/docs/emails/membership-reminder.* cmd/templates/emails/

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"embed"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"html"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	stripesource "github.com/CommonsHub/chb/providers/stripe"
)

//go:embed templates/emails/membership-reminder.*
var reminderTemplatesFS embed.FS

const (
	reminderFrom       = "Commons Hub Brussels <hello@commonshub.brussels>"
	reminderReplyTo    = "hello@commonshub.brussels"
	reminderRenewBase  = "https://commonshub.brussels/membership/renew?t="
	reminderLapsedDays = 30
	reminderMaxPerRun  = 25
	renewLinkValidity  = 30 * 24 * time.Hour
)

// ReminderPlanEntry is one reminder to send (no email address).
type ReminderPlanEntry struct {
	Key              string `json:"reminderKey"` // identity:reason:since — one per status change
	Reason           string `json:"reason"`      // payment_failed, paused, ended
	Status           string `json:"status"`      // grace, lapsed
	Since            string `json:"since"`
	GraceEndsAt      string `json:"graceEndsAt,omitempty"`
	FirstName        string `json:"firstName"`
	Organisation     bool   `json:"organisation,omitempty"`
	OdooPartnerID    int    `json:"odooPartnerId,omitempty"`
	StripeCustomerID string `json:"stripeCustomerId,omitempty"`
	PaymentReference string `json:"paymentReference,omitempty"`
	Plan             string `json:"plan"`
}

type ReminderPlanFile struct {
	GeneratedAt string              `json:"generatedAt"`
	Reminders   []ReminderPlanEntry `json:"reminders"`
}

type ReminderLedgerEntry struct {
	SentAt    string `json:"sentAt"`
	Reason    string `json:"reason"`
	MessageID string `json:"messageId,omitempty"`
}

func remindersDir(dataDir string) string {
	return filepath.Join(dataDir, "latest", "providers", "members")
}

func reminderPlanPath(dataDir string) string {
	return filepath.Join(remindersDir(dataDir), "pending-reminders.json")
}

func reminderLedgerPath(dataDir string) string {
	return filepath.Join(remindersDir(dataDir), "reminders-sent.json")
}

// buildReminderPlan: members in grace, or lapsed within the last 30 days.
func buildReminderPlan(members []Member, today string) []ReminderPlanEntry {
	cutoff := addDays(today, -reminderLapsedDays)
	var out []ReminderPlanEntry
	for _, m := range members {
		if m.Status != "grace" && !(m.Status == "lapsed" && m.StatusSince != "" && m.StatusSince >= cutoff) {
			continue
		}
		reason := m.StatusReason
		if m.Status == "lapsed" {
			reason = "ended"
		}
		if reason == "" {
			reason = "payment_failed"
		}
		identity := m.ID
		if m.OdooPartnerID > 0 {
			identity = fmt.Sprintf("odoo:%d", m.OdooPartnerID)
		} else if m.StripeCustomerID != "" {
			identity = "stripe:" + m.StripeCustomerID
		}
		out = append(out, ReminderPlanEntry{
			Key:              identity + ":" + reason + ":" + m.StatusSince,
			Reason:           reason,
			Status:           m.Status,
			Since:            m.StatusSince,
			GraceEndsAt:      m.GraceEndsAt,
			FirstName:        m.FirstName,
			Organisation:     m.IsOrganization,
			OdooPartnerID:    m.OdooPartnerID,
			StripeCustomerID: m.StripeCustomerID,
			PaymentReference: m.PaymentReference,
			Plan:             m.Plan,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}

func writePrivateJSON(path string, v interface{}) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o600)
}

// writeReminderPlan is called by generate for the current month.
func writeReminderPlan(dataDir string, members []Member) {
	today := time.Now().In(BrusselsTZ()).Format("2006-01-02")
	plan := ReminderPlanFile{GeneratedAt: time.Now().UTC().Format(time.RFC3339), Reminders: buildReminderPlan(members, today)}
	if plan.Reminders == nil {
		plan.Reminders = []ReminderPlanEntry{}
	}
	if err := writePrivateJSON(reminderPlanPath(dataDir), plan); err != nil {
		Warnf("⚠ reminders plan: %v", err)
	}
}

func loadReminderLedger(dataDir string) map[string]ReminderLedgerEntry {
	out := map[string]ReminderLedgerEntry{}
	if data, err := os.ReadFile(reminderLedgerPath(dataDir)); err == nil {
		_ = json.Unmarshal(data, &out)
	}
	return out
}

// ── rendering ──────────────────────────────────────────────────────────

type reminderEmail struct {
	Subject, HTML, Text string
}

func reminderTemplateName(reason string, organisation bool) string {
	name := "membership-reminder." + reason
	if organisation {
		name += ".organisation"
	}
	return name
}

// longBrusselsDate: "Wednesday 21 October".
func longBrusselsDate(date string) string {
	t, err := time.ParseInLocation("2006-01-02", firstN(date, 10), BrusselsTZ())
	if err != nil {
		return date
	}
	return t.Format("Monday 2 January")
}

func renderReminder(e ReminderPlanEntry, email, renewURL string) (reminderEmail, error) {
	name := reminderTemplateName(e.Reason, e.Organisation)
	meta, err := reminderTemplatesFS.ReadFile("templates/emails/membership-reminder.json")
	if err != nil {
		return reminderEmail{}, err
	}
	var subjects struct {
		Templates map[string]struct {
			Subject string `json:"subject"`
		} `json:"templates"`
	}
	if err := json.Unmarshal(meta, &subjects); err != nil {
		return reminderEmail{}, err
	}
	subject := subjects.Templates[name].Subject
	if subject == "" {
		return reminderEmail{}, fmt.Errorf("no subject for %s", name)
	}
	htmlTpl, err := reminderTemplatesFS.ReadFile("templates/emails/" + name + ".html")
	if err != nil {
		return reminderEmail{}, err
	}
	textTpl, err := reminderTemplatesFS.ReadFile("templates/emails/" + name + ".txt")
	if err != nil {
		return reminderEmail{}, err
	}
	ref := e.PaymentReference
	if ref == "" {
		ref = "Membership " + e.FirstName
	}
	values := map[string]string{
		"firstName":        e.FirstName,
		"email":            email,
		"renewUrl":         renewURL,
		"paymentReference": ref,
		"graceEndsAtLong":  longBrusselsDate(e.GraceEndsAt),
	}
	fill := func(tpl string, escape bool) string {
		for k, v := range values {
			if escape {
				v = html.EscapeString(v)
			}
			tpl = strings.ReplaceAll(tpl, "{{"+k+"}}", v)
		}
		return tpl
	}
	out := reminderEmail{Subject: subject, HTML: fill(string(htmlTpl), true), Text: fill(string(textTpl), false)}
	if strings.Contains(out.HTML, "{{") || strings.Contains(out.Text, "{{") {
		return out, fmt.Errorf("%s: unfilled placeholder", name)
	}
	return out, nil
}

// renewToken: base64url(JSON) + "." + base64url(HMAC-SHA256(secret, the
// base64url JSON string)), as the website verifies it.
func renewToken(secret string, e ReminderPlanEntry, now time.Time) string {
	payload := map[string]interface{}{"n": e.FirstName, "x": now.Add(renewLinkValidity).Unix()}
	if e.StripeCustomerID != "" {
		payload["c"] = e.StripeCustomerID
	}
	if e.OdooPartnerID > 0 {
		payload["p"] = e.OdooPartnerID
	}
	if e.Organisation {
		payload["o"] = true
	}
	js, _ := json.Marshal(payload)
	body := base64.RawURLEncoding.EncodeToString(js)
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(body))
	return body + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func maskEmail(e string) string {
	at := strings.Index(e, "@")
	if at <= 1 {
		return "***"
	}
	return e[:1] + "***" + e[at:]
}

// ── delivery ───────────────────────────────────────────────────────────

var sendReminderEmail = func(apiKey, idempotencyKey, to string, m reminderEmail) (string, error) {
	body, _ := json.Marshal(map[string]interface{}{
		"from": reminderFrom, "to": []string{to}, "reply_to": reminderReplyTo,
		"subject": m.Subject, "html": m.HTML, "text": m.Text,
	})
	req, err := http.NewRequest("POST", "https://api.resend.com/emails", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", idempotencyKey)
	resp, err := (&http.Client{Timeout: 30 * time.Second}).Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	var out struct {
		ID      string `json:"id"`
		Message string `json:"message"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&out)
	if resp.StatusCode >= 300 {
		return "", fmt.Errorf("resend: HTTP %d %s", resp.StatusCode, out.Message)
	}
	return out.ID, nil
}

// reminderAddress looks the member's address up live: the Odoo partner,
// else the Stripe customer.
func reminderAddress(e ReminderPlanEntry) (string, error) {
	if e.OdooPartnerID > 0 {
		creds, err := ResolveOdooCredentials()
		if err != nil {
			return "", err
		}
		uid, err := odooAuth(creds.URL, creds.DB, creds.Login, creds.Password)
		if err != nil {
			return "", err
		}
		rows, err := odooReadMapsByIDs(creds, uid, "res.partner", []int{e.OdooPartnerID}, []string{"email"})
		if err != nil || len(rows) == 0 {
			return "", fmt.Errorf("partner %d: %v", e.OdooPartnerID, err)
		}
		if em := strings.TrimSpace(odooString(rows[0]["email"])); em != "" {
			return em, nil
		}
	}
	if e.StripeCustomerID != "" {
		if c := stripesource.FetchCustomer(os.Getenv("STRIPE_SECRET_KEY"), e.StripeCustomerID); c != nil && c.Email != "" {
			return c.Email, nil
		}
	}
	return "", fmt.Errorf("no email address")
}

// MembersRemind is `chb members remind`.
func MembersRemind(args []string) error {
	if HasFlag(args, "--help", "-h", "help") {
		fmt.Print(`
chb members remind — membership reminders (one per status change)

USAGE
  chb members remind                 Preview who would get which reminder
  chb members remind --yes           Send (needs RESEND_API_KEY, RENEW_LINK_SECRET)
  chb members remind --render-test [payment_failed|paused|ended] [--organisation]
                                     Print a rendered sample for a fictitious member

The plan is written by ` + "`chb generate`" + ` (latest/providers/members/pending-reminders.json);
sent reminders are recorded in reminders-sent.json and never sent twice.
`)
		return nil
	}
	if HasFlag(args, "--render-test") {
		reason := "payment_failed"
		for _, a := range args {
			if a == "paused" || a == "ended" || a == "payment_failed" {
				reason = a
			}
		}
		e := ReminderPlanEntry{Key: "test", Reason: reason, Status: "grace", FirstName: "Alex", Organisation: HasFlag(args, "--organisation"),
			GraceEndsAt: addDays(time.Now().In(BrusselsTZ()).Format("2006-01-02"), membershipGraceDays), PaymentReference: "+++000/0000/00097+++", OdooPartnerID: 1}
		secret := os.Getenv("RENEW_LINK_SECRET")
		if secret == "" {
			secret = "test-secret-not-for-production"
		}
		m, err := renderReminder(e, "alex@example.org", reminderRenewBase+renewToken(secret, e, time.Now()))
		if err != nil {
			return err
		}
		fmt.Printf("Subject: %s\nFrom: %s\nReply-To: %s\n\n--- text ---\n%s\n--- html ---\n%s\n", m.Subject, reminderFrom, reminderReplyTo, m.Text, m.HTML)
		return nil
	}

	dataDir := DataDir()
	data, err := os.ReadFile(reminderPlanPath(dataDir))
	if err != nil {
		return fmt.Errorf("no reminder plan — run `chb generate` first")
	}
	var plan ReminderPlanFile
	if err := json.Unmarshal(data, &plan); err != nil {
		return err
	}
	ledger := loadReminderLedger(dataDir)
	var todo []ReminderPlanEntry
	for _, e := range plan.Reminders {
		if _, done := ledger[e.Key]; !done {
			todo = append(todo, e)
		}
	}
	fmt.Printf("\n  Membership reminders: %d in the plan, %d already sent, %d to send\n\n", len(plan.Reminders), len(plan.Reminders)-len(todo), len(todo))
	for _, e := range todo {
		fmt.Printf("    %-15s %-7s since %-10s  %s%s\n", e.Reason, e.Status, e.Since, e.FirstName, map[bool]string{true: " (organisation)"}[e.Organisation])
	}
	if len(todo) == 0 || !HasFlag(args, "--yes") {
		if len(todo) > 0 {
			fmt.Printf("\n  %s(preview — nothing sent; add --yes to send)%s\n\n", Fmt.Dim, Fmt.Reset)
		}
		return nil
	}
	apiKey, secret := os.Getenv("RESEND_API_KEY"), os.Getenv("RENEW_LINK_SECRET")
	if apiKey == "" || secret == "" {
		return fmt.Errorf("RESEND_API_KEY and RENEW_LINK_SECRET must be set in config.env to send")
	}
	if len(todo) > reminderMaxPerRun && !HasFlag(args, "--all") {
		return fmt.Errorf("%d reminders to send, more than %d at once — check the plan, then re-run with --all", len(todo), reminderMaxPerRun)
	}
	sent := 0
	for _, e := range todo {
		to, err := reminderAddress(e)
		if err != nil {
			Warnf("⚠ %s: %v", e.Key, err)
			continue
		}
		m, err := renderReminder(e, to, reminderRenewBase+renewToken(secret, e, time.Now()))
		if err != nil {
			Warnf("⚠ %s: %v", e.Key, err)
			continue
		}
		id, err := sendReminderEmail(apiKey, e.Key, to, m)
		if err != nil {
			Warnf("⚠ %s: %v", e.Key, err)
			continue
		}
		ledger[e.Key] = ReminderLedgerEntry{SentAt: time.Now().UTC().Format(time.RFC3339), Reason: e.Reason, MessageID: id}
		if err := writePrivateJSON(reminderLedgerPath(dataDir), ledger); err != nil {
			return fmt.Errorf("record %s: %v (sent; stop to avoid a duplicate)", e.Key, err)
		}
		sent++
		fmt.Printf("    ✓ %s → %s\n", e.Reason, maskEmail(to))
	}
	fmt.Printf("\n  %d reminder(s) sent\n\n", sent)
	return nil
}
