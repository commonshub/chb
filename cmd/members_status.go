package cmd

// Member status, with Odoo subscriptions as the source of truth.
//
//   - active: an Odoo subscription in progress with no invoice unpaid past
//     its due date (Stripe: active / trialing);
//   - grace: an invoice unpaid past its due date, or the subscription
//     paused — a member for 15 more days (graceEndsAt), then lapsed
//     (Stripe: past_due / paused / unpaid);
//   - lapsed: churned or closed, or grace expired (Stripe: canceled).
//
// The status depends on the date, so it is computed at generate time from
// the raw snapshot (providers/odoo/…/subscriptions.json), for the month's
// last day (today for the current month). A member in both Odoo and Stripe
// (same email hash) is taken from Odoo; disagreements are listed for
// stewards in members.json "mismatches".

import (
	"sort"
	"strings"
	"time"
)

const membershipGraceDays = 15

// MemberMismatch: Odoo and Stripe disagree about a member (stewards only).
type MemberMismatch struct {
	Kind         string `json:"kind"` // status_differs, no_odoo_subscription
	Name         string `json:"name"`
	EmailHash    string `json:"emailHash"`
	OdooStatus   string `json:"odooStatus,omitempty"`
	StripeStatus string `json:"stripeStatus,omitempty"`
	OdooURL      string `json:"odooUrl,omitempty"`
	StripeURL    string `json:"stripeUrl,omitempty"`
}

func addDays(date string, days int) string {
	t, err := time.Parse("2006-01-02", firstN(date, 10))
	if err != nil {
		return ""
	}
	return t.AddDate(0, 0, days).Format("2006-01-02")
}

func firstN(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return s
}

// odooMemberStatus: status, since and graceEndsAt of an Odoo subscription
// on refDate (YYYY-MM-DD).
func odooMemberStatus(sub providerSubscription, refDate string) (status, since, graceEnds string) {
	status, since, graceEnds, _, _ = odooMemberStatusDetail(sub, refDate)
	return
}

// odooMemberStatusDetail also returns why (payment_failed, paused, ended)
// and the overdue invoice's structured communication.
func odooMemberStatusDetail(sub providerSubscription, refDate string) (status, since, graceEnds, reason, paymentRef string) {
	if sub.OdooState == "6_churn" || sub.Status == "churned" {
		return "lapsed", firstN(sub.EndDate, 10), "", "ended", latestPaymentReference(sub)
	}
	// The oldest posted invoice still unpaid past its due date.
	oldestDue := ""
	for _, inv := range sub.Invoices {
		if inv.State != "" && inv.State != "posted" {
			continue
		}
		switch inv.PaymentState {
		case "paid", "in_payment", "reversed":
			continue
		}
		if inv.Residual <= 0.005 {
			continue
		}
		due := firstN(inv.DueDate, 10)
		if due == "" {
			due = firstN(inv.Date, 10)
		}
		if due != "" && due < refDate && (oldestDue == "" || due < oldestDue) {
			oldestDue = due
			paymentRef = inv.PaymentReference
		}
	}
	start := oldestDue
	reason = "payment_failed"
	if start == "" && (sub.OdooState == "4_paused" || sub.Status == "paused") {
		reason = "paused"
		start = firstN(sub.CurrentPeriodEnd, 10) // next invoice date
		if start == "" || start > refDate {
			start = refDate
		}
	}
	if paymentRef == "" {
		paymentRef = latestPaymentReference(sub)
	}
	if start == "" {
		return "active", "", "", "", paymentRef
	}
	graceEnds = addDays(start, membershipGraceDays)
	if graceEnds != "" && refDate > graceEnds {
		return "lapsed", graceEnds, "", "ended", paymentRef
	}
	return "grace", start, graceEnds, reason, paymentRef
}

// latestPaymentReference: the structured communication of the newest
// invoice that has one (Odoo's partner-based reference stays the same).
func latestPaymentReference(sub providerSubscription) string {
	best, ref := "", ""
	for _, inv := range sub.Invoices {
		if inv.PaymentReference != "" && inv.Date >= best {
			best, ref = inv.Date, inv.PaymentReference
		}
	}
	return ref
}

func stripeMemberStatus(status string) string {
	switch status {
	case "active", "trialing":
		return "active"
	case "past_due", "paused", "unpaid", "incomplete":
		return "grace"
	}
	return "lapsed"
}

// mergeProviderSnapshotsAt builds the month's members (Odoo first, then
// Stripe-only members) and the Odoo/Stripe mismatches.
func mergeProviderSnapshotsAt(snapshots []providerSnapshot, year, month int) ([]Member, []MemberMismatch) {
	monthStart := time.Date(year, time.Month(month), 1, 0, 0, 0, 0, time.UTC)
	refDate := monthStart.AddDate(0, 1, -1).Format("2006-01-02")
	if today := time.Now().In(BrusselsTZ()).Format("2006-01-02"); today < refDate {
		refDate = today
	}
	monthStartStr := monthStart.Format("2006-01-02")

	var odooSubs, stripeSubs []providerSubscription
	for _, snap := range snapshots {
		for _, sub := range snap.Subscriptions {
			if snap.Provider == "stripe" || sub.Source == "stripe" {
				stripeSubs = append(stripeSubs, sub)
			} else {
				odooSubs = append(odooSubs, sub)
			}
		}
	}
	toMember := func(sub providerSubscription, status, since, graceEnds string) Member {
		// A full name only for a verified organisation: the Odoo partner
		// is a company, or the name carries a legal form. The plan alone
		// (an individual on the non-profit plan) does not make anyone an
		// organisation: individuals are never named in public, and the
		// members tier shows first names only.
		orgName := ""
		if full := strings.TrimSpace(sub.FirstName + " " + sub.LastName); sub.IsCompany || nameHasLegalForm(full) {
			orgName = full
		}
		return Member{
			OrganizationName:   orgName,
			ID:                 sub.ID,
			Source:             sub.Source,
			Accounts:           MemberAccounts{EmailHash: sub.EmailHash, Discord: sub.Discord},
			FirstName:          sub.FirstName,
			Plan:               sub.Plan,
			Amount:             sub.Amount,
			Interval:           sub.Interval,
			Status:             status,
			CurrentPeriodStart: sub.CurrentPeriodStart,
			CurrentPeriodEnd:   sub.CurrentPeriodEnd,
			LatestPayment:      sub.LatestPayment,
			SubscriptionURL:    sub.SubscriptionURL,
			CreatedAt:          sub.CreatedAt,
			IsOrganization:     sub.IsOrganization,
			StatusSince:        since,
			GraceEndsAt:        graceEnds,
			OdooPartnerID:      sub.OdooPartnerID,
		}
	}

	seen := map[string]Member{}
	odooStatus := map[string]string{}
	odooURL := map[string]string{}
	for _, sub := range odooSubs {
		status, since, graceEnds, reason, payRef := odooMemberStatusDetail(sub, refDate)
		// A member who lapsed before this month is not one of its members.
		if status == "lapsed" && since != "" && since < monthStartStr {
			continue
		}
		key := sub.EmailHash
		if prev, ok := seen[key]; ok && statusRank(prev.Status) >= statusRank(status) {
			continue // several subscriptions: keep the best standing
		}
		m := toMember(sub, status, since, graceEnds)
		m.StatusReason, m.PaymentReference = reason, payRef
		seen[key] = m
		odooStatus[key] = status
		odooURL[key] = sub.SubscriptionURL
	}

	var mismatches []MemberMismatch
	for _, sub := range stripeSubs {
		status := stripeMemberStatus(sub.Status)
		key := sub.EmailHash
		name := strings.TrimSpace(sub.FirstName + " " + sub.LastName)
		if _, ok := odooStatus[key]; ok && sub.StripeCustomerID != "" {
			// The member's Stripe customer, for the renew link.
			m := seen[key]
			if m.StripeCustomerID == "" {
				m.StripeCustomerID = sub.StripeCustomerID
				seen[key] = m
			}
		}
		if os, ok := odooStatus[key]; ok {
			if os != status {
				mismatches = append(mismatches, MemberMismatch{Kind: "status_differs", Name: name, EmailHash: key,
					OdooStatus: os, StripeStatus: status, OdooURL: odooURL[key], StripeURL: sub.SubscriptionURL})
			}
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		if status != "lapsed" {
			mismatches = append(mismatches, MemberMismatch{Kind: "no_odoo_subscription", Name: name, EmailHash: key,
				StripeStatus: status, StripeURL: sub.SubscriptionURL})
		}
		m := toMember(sub, status, "", "")
		m.StripeCustomerID = sub.StripeCustomerID
		switch sub.Status {
		case "paused":
			m.StatusReason = "paused"
		case "past_due", "unpaid", "incomplete":
			m.StatusReason = "payment_failed"
		case "canceled", "incomplete_expired":
			m.StatusReason = "ended"
			m.StatusSince = firstN(sub.CurrentPeriodEnd, 10)
		}
		seen[key] = m
	}

	var result []Member
	for _, m := range seen {
		result = append(result, m)
	}
	// Sort by createdAt, then id: map order must not leak into the file
	// (it would move the tier hashes in hashes.json between runs).
	sort.Slice(result, func(i, j int) bool {
		if result[i].CreatedAt != result[j].CreatedAt {
			return result[i].CreatedAt < result[j].CreatedAt
		}
		return result[i].ID < result[j].ID
	})
	sort.Slice(mismatches, func(i, j int) bool {
		if mismatches[i].Kind != mismatches[j].Kind {
			return mismatches[i].Kind < mismatches[j].Kind
		}
		return mismatches[i].EmailHash < mismatches[j].EmailHash
	})
	return result, mismatches
}

func statusRank(s string) int {
	switch s {
	case "active":
		return 3
	case "grace":
		return 2
	case "lapsed":
		return 1
	}
	return 0
}
