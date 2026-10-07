package cmd

// How a room booking was paid, from its calendar event description, for
// bookings.json "payment": "tokens" | "euros" | null (unknown). The
// community tablet lets token-paid bookings in without a steward.
//
//   - the structured line token-bot-de writes wins: "Paid: 2 CHT" →
//     tokens; "Paid: €80 (card)" / "Paid: invoice" → euros;
//   - /book's "Booked by X … for 2.00 CHT", a "Booking TX:" line (the
//     token transfer), or "paid in tokens" → tokens;
//   - an Odoo invoice / sale order reference (CHB/2026/…, MEM/…, S00123,
//     INV/…, +++000/0044/21681+++), "invoice"/"factuur"/"facture", or a
//     Stripe / card payment → euros;
//   - anything else → unknown.

import (
	"html"
	"regexp"
	"strings"
)

var (
	bookingPaidLine     = regexp.MustCompile(`(?im)^\s*paid\s*:\s*(.+)$`)
	bookingTokenAmount  = regexp.MustCompile(`(?i)\b\d+(?:[.,]\d+)?\s*(?:cht|tokens?)\b`)
	bookingBookedTokens = regexp.MustCompile(`(?i)\bbooked by\b.*\bfor\s+\d+(?:[.,]\d+)?\s*cht\b`)
	bookingPaidTokens   = regexp.MustCompile(`(?im)\bpaid (?:in|with) (?:cht|tokens?)\b|^\s*booking tx\s*:`)
	bookingEuroRef      = regexp.MustCompile(`(?i)\b(?:CHB|MEM|INV|RINV)/\d{4}/\d+|\bS\d{5}\b|\+\+\+\d{3}/\d{4}/\d{5}\+\+\+|\binvoice\b|\bfactu(?:ur|re)\b|\bstripe\b|\bpaid by card\b|€\s*\d|\d\s*€|\beur\s*\d`)
	bookingHTMLTag      = regexp.MustCompile(`<[^>]*>`)
)

// bookingDescriptionText: the description as plain text, one line per line.
func bookingDescriptionText(desc string) string {
	s := strings.NewReplacer(`\n`, "\n", `\N`, "\n", `\,`, ",", `\;`, ";", "<br>", "\n", "<br/>", "\n", "<br />", "\n").Replace(desc)
	s = bookingHTMLTag.ReplaceAllString(s, " ")
	return html.UnescapeString(s)
}

// bookingPayment returns "tokens", "euros" or "" (unknown).
func bookingPayment(desc string) string {
	text := bookingDescriptionText(desc)
	if m := bookingPaidLine.FindStringSubmatch(text); m != nil {
		v := strings.ToLower(m[1])
		switch {
		case bookingTokenAmount.MatchString(v) || strings.Contains(v, "token"):
			return "tokens"
		case strings.Contains(v, "€") || strings.Contains(v, "eur") || strings.Contains(v, "card") ||
			strings.Contains(v, "invoice") || strings.Contains(v, "stripe") || strings.Contains(v, "transfer"):
			return "euros"
		}
	}
	if bookingBookedTokens.MatchString(strings.ReplaceAll(text, "\n", " ")) || bookingPaidTokens.MatchString(text) {
		return "tokens"
	}
	if bookingEuroRef.MatchString(text) {
		return "euros"
	}
	return ""
}
