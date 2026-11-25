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
	"unicode/utf8"
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

var bookingStewardNeededRe = regexp.MustCompile(`(?i)\bstewards?\s+needed\b`)

// bookingNoStewardRe: the description says none is needed ("No steward
// needed", "Steward not needed", "no need for a steward"…).
var bookingNoStewardRe = regexp.MustCompile(`(?i)\b(?:no|not\s+a|without(?:\s+a)?)\s+stewards?\s+(?:is\s+|are\s+)?(?:needed|required)\b|\bstewards?\s+(?:is\s+|are\s+)?not\s+(?:needed|required)\b|\bno\s+need\s+(?:for|of)\s+(?:a\s+)?stewards?\b`)

// bookingStewardNeeded: the description asks for a steward ("Steward
// needed", "Stewards needed", any case), and doesn't say none is ("No
// steward needed, self organised").
func bookingStewardNeeded(desc string) bool {
	text := bookingDescriptionText(desc)
	return bookingStewardNeededRe.MatchString(text) && !bookingNoStewardRe.MatchString(text)
}

const bookingDescriptionMax = 1000

var (
	bookingAnchor     = regexp.MustCompile(`(?is)<a\s[^>]*href\s*=\s*["']([^"']+)["'][^>]*>(.*?)</a>`)
	bookingBlockClose = regexp.MustCompile(`(?i)</(?:p|div|li|h[1-6])>`)
	bookingBlankLines = regexp.MustCompile(`\n{3,}`)
	bookingPhone      = regexp.MustCompile(`(?:\+|\b00)\d{2}[\d .\-/()]{7,}\d|\b0\d{2,3}[ ./-]?\d{2}[ ./-]?\d{2}[ ./-]?\d{2}\b`)
)

// bookingPlainDescription: the calendar description as plain text, for
// bookings.json. Links keep their address ("text (url)"), newlines are
// kept, blank runs collapse, and it is cut at bookingDescriptionMax.
func bookingPlainDescription(desc string) string {
	s := bookingAnchor.ReplaceAllStringFunc(desc, func(m string) string {
		sm := bookingAnchor.FindStringSubmatch(m)
		url := html.UnescapeString(sm[1])
		text := strings.TrimSpace(html.UnescapeString(bookingHTMLTag.ReplaceAllString(sm[2], "")))
		if text == "" || text == url || strings.TrimPrefix(strings.TrimPrefix(url, "https://"), "http://") == text {
			return url
		}
		return text + " (" + url + ")"
	})
	s = bookingBlockClose.ReplaceAllString(s, "\n")
	s = bookingDescriptionText(s)
	lines := strings.Split(strings.ReplaceAll(s, "\r", ""), "\n")
	for i, l := range lines {
		lines[i] = strings.Join(strings.Fields(strings.ReplaceAll(l, "\u00a0", " ")), " ")
	}
	s = strings.TrimSpace(bookingBlankLines.ReplaceAllString(strings.Join(lines, "\n"), "\n\n"))
	if utf8.RuneCountInString(s) > bookingDescriptionMax {
		r := []rune(s)
		s = strings.TrimSpace(string(r[:bookingDescriptionMax-1])) + "…"
	}
	return s
}

// maskBookingDescription: what members may read of a description: no phone
// numbers, third-party IBANs or Stripe customer ids (emails are masked by
// the audience policy at write time).
func maskBookingDescription(s string) string {
	if s == "" {
		return s
	}
	s = maskBankDetails(s)
	s = stripeCustomerIDPattern.ReplaceAllString(s, "[customer id]")
	return bookingPhone.ReplaceAllString(s, "[phone removed]")
}
