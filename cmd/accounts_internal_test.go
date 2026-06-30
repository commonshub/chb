package cmd

import (
	"math"
	"testing"
)

func leg(slug string, amount float64, ts int64, hash string) internalLeg {
	return internalLeg{
		slug:   slug,
		amount: amount,
		tx:     TransactionEntry{AccountSlug: slug, NormalizedAmount: amount, Timestamp: ts, TxHash: hash},
	}
}

func legNet(ls []internalLeg) float64 {
	var s float64
	for _, l := range ls {
		s += l.amount
	}
	return s
}

func TestPairInternalLegs(t *testing.T) {
	day := int64(86400)

	t.Run("on-chain legs sharing a hash cancel", func(t *testing.T) {
		legs := []internalLeg{
			leg("savings", -500, 100, "0xabc"),
			leg("checking", 500, 100, "0xabc"),
		}
		orphans, hashPairs, amountPairs := pairInternalLegs(legs)
		if len(orphans) != 0 || hashPairs != 1 || amountPairs != 0 {
			t.Errorf("orphans=%d hashPairs=%d amountPairs=%d; want 0/1/0", len(orphans), hashPairs, amountPairs)
		}
	})

	t.Run("cross-account equal-opposite within window cancels", func(t *testing.T) {
		legs := []internalLeg{
			leg("stripe", -1472.61, 10*day, ""),
			leg("checking", 1472.61, 12*day, ""), // 2 days later
		}
		orphans, _, amountPairs := pairInternalLegs(legs)
		if len(orphans) != 0 || amountPairs != 1 {
			t.Errorf("orphans=%d amountPairs=%d; want 0/1", len(orphans), amountPairs)
		}
	})

	t.Run("outside window stays orphan", func(t *testing.T) {
		legs := []internalLeg{
			leg("stripe", -1472.61, 0, ""),
			leg("checking", 1472.61, 90*day, ""), // far apart
		}
		orphans, _, amountPairs := pairInternalLegs(legs)
		if len(orphans) != 2 || amountPairs != 0 {
			t.Errorf("orphans=%d amountPairs=%d; want 2/0", len(orphans), amountPairs)
		}
	})

	t.Run("same-account leg does not pair across-account but reversal cancels", func(t *testing.T) {
		legs := []internalLeg{
			leg("stripe", -287.22, day, ""),
			leg("stripe", 287.22, day, ""), // payout + reversal on same account
		}
		orphans, _, amountPairs := pairInternalLegs(legs)
		if len(orphans) != 0 || amountPairs != 1 {
			t.Errorf("orphans=%d amountPairs=%d; want 0/1 (same-account reversal)", len(orphans), amountPairs)
		}
	})

	t.Run("orphan with no counterpart survives and net is preserved", func(t *testing.T) {
		legs := []internalLeg{
			leg("hacked", -5000, day, ""),       // theft, no counterpart
			leg("savings", -500, 2*day, "0xabc"), // on-chain pair
			leg("checking", 500, 2*day, "0xabc"),
			leg("stripe", -1000, 3*day, ""), // cross-account pair
			leg("checking", 1000, 4*day, ""),
		}
		full := legNet(legs)
		orphans, hashPairs, amountPairs := pairInternalLegs(legs)
		if len(orphans) != 1 || orphans[0].slug != "hacked" {
			t.Fatalf("want single hacked orphan, got %d: %+v", len(orphans), orphans)
		}
		if hashPairs != 1 || amountPairs != 1 {
			t.Errorf("hashPairs=%d amountPairs=%d; want 1/1", hashPairs, amountPairs)
		}
		if s := legNet(orphans); math.Abs(s-full) > 0.005 {
			t.Errorf("orphan net %.2f != full net %.2f", s, full)
		}
	})

	t.Run("prefers nearest-date counterpart", func(t *testing.T) {
		legs := []internalLeg{
			leg("a", -100, 10*day, ""),
			leg("b", 100, 30*day, ""), // 20 days off
			leg("c", 100, 11*day, ""), // 1 day off — should win
		}
		orphans, _, _ := pairInternalLegs(legs)
		// The 11-day leg (c) pairs with a; b is left orphan.
		if len(orphans) != 1 || orphans[0].slug != "b" {
			t.Errorf("want b orphan, got %+v", orphans)
		}
	})
}
