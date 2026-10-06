package cmd

import (
	"strings"
	"testing"
	"time"

	stripesource "github.com/CommonsHub/chb/providers/stripe"
)

func TestStripeMonthSnapshotHistory(t *testing.T) {
	ts := func(y, m, d int) int64 { return time.Date(y, time.Month(m), d, 12, 0, 0, 0, time.UTC).Unix() }
	sub := func(id, status string, created int64, canceled *int64) stripesource.Subscription {
		s := stripesource.Subscription{ID: id, Status: status, Customer: "cus_" + id, Created: created, CanceledAt: canceled,
			// a monthly subscription's current period is THIS month
			CurrentPeriodStart: ts(2026, 10, 1), CurrentPeriodEnd: ts(2026, 10, 31)}
		item := stripesource.SubscriptionItem{Price: stripesource.Price{ID: "price_m", UnitAmount: 1000, Currency: "eur", Product: "prod_x"}}
		item.Price.Recurring.Interval = "month"
		item.Price.Recurring.IntervalCount = 1
		s.Items.Data = []stripesource.SubscriptionItem{item}
		return s
	}
	gone := ts(2026, 4, 15)
	subs := []stripesource.Subscription{
		sub("a", "active", ts(2025, 6, 1), nil),     // member since June 2025
		sub("b", "active", ts(2026, 6, 1), nil),     // joined June 2026
		sub("c", "canceled", ts(2025, 1, 1), &gone), // left in April 2026
	}
	name := "N"
	cache := map[string]*stripesource.Customer{}
	for _, s := range subs {
		cache[s.Customer] = &stripesource.Customer{ID: s.Customer, Email: s.ID + "@x.org", Name: &name}
	}
	count := func(y, m int) int {
		return len(buildStripeMonthSnapshot(subs, y, m, "salt", "prod_x", "", cache).Subscriptions)
	}
	if n := count(2026, 5); n != 1 {
		t.Errorf("May 2026: %d members, want 1 (a; b not yet, c left in April)", n)
	}
	if n := count(2026, 3); n != 2 {
		t.Errorf("March 2026: %d members, want 2 (a, c)", n)
	}
	if n := count(2026, 10); n != 2 {
		t.Errorf("October 2026: %d members, want 2 (a, b)", n)
	}
}

func TestMembersSyncRefusesWithoutSalt(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("DATA_DIR", tmp+"/data")
	t.Setenv("APP_DATA_DIR", tmp+"/app")
	t.Setenv("EMAIL_HASH_SALT", "")
	err := MembersSync(nil)
	if err == nil || !strings.Contains(err.Error(), "EMAIL_HASH_SALT") {
		t.Fatalf("must refuse without a salt, got %v", err)
	}
}
