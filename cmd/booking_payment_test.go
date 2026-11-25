package cmd

import (
	"strings"
	"testing"
	"unicode/utf8"
)

func TestBookingPayment(t *testing.T) {
	for desc, want := range map[string]string{
		`Booked by Marlene (@marlenesm) on Tuesday September 15th at 10:06am for 2.00 CHT\nhttps://discord.com/channels/1/2/3`:                                  "tokens",
		`<a href="https://luma.com/x">luma</a><br>Members booking <br>Booked by manuelpueyo (@manuelpueyo) on Tuesday September 8th at 2:29pm for 8.00 CHT<br>`: "tokens",
		`Miriam in contact with Nicolas\nPaid in tokens\, CHB official sponsor for the event.`:                                                                  "tokens",
		`Booked by X for 2.00 CHT\nPaid: 2 CHT`: "tokens",
		`Board meeting\nPaid: €80 (card)`:       "euros",
		`Paid: invoice`:                         "euros",
		`Paid: €80 (EURb)`:                      "euros",
		`Booked by Leen
Booking TX: https://celoscan.io/tx/0xabc`: "tokens",
		`Quote S00205 accepted`:                 "euros",
		`Invoice CHB/2026/00297 sent`:           "euros",
		`Booking request from Laure Verbruggen`: "",
		`14 pax\nKoffie & Thee\nBroodjes`:       "",
		`Gathering to wrap up the Commons Day`:  "",
		``:                                      "",
	} {
		if got := bookingPayment(desc); got != want {
			t.Errorf("%q → %q, want %q", desc, got, want)
		}
	}
}

func TestBookingStewardNeeded(t *testing.T) {
	for desc, want := range map[string]bool{
		"Steward needed":                    true,
		"<p>STEWARDS NEEDED</p>":            true,
		`Workshop\nstewards   needed today`: true,
		"No steward":                        false,
		"Contact Olivia\nNo steward needed, self organised": false,
		"Steward not needed":        false,
		"stewards are not required": false,
		"No need for a steward":     false,
		"stewardship needed":        false,
		"":                          false,
	} {
		if got := bookingStewardNeeded(desc); got != want {
			t.Errorf("%q: got %v", desc, got)
		}
	}
}

func TestBookingPlainDescription(t *testing.T) {
	desc := `<p>Workshop on <b>care</b></p><p>Steward needed &amp; coffee</p><br><br><br>` +
		`<a href="https://example.org/x">the form</a> <a href="https://luma.com/abc">https://luma.com/abc</a>\nPaid: 2 CHT`
	want := "Workshop on care\nSteward needed & coffee\n\nthe form (https://example.org/x) https://luma.com/abc\nPaid: 2 CHT"
	if got := bookingPlainDescription(desc); got != want {
		t.Errorf("got %q\nwant %q", got, want)
	}
	long := strings.Repeat("é", 1500)
	if got := bookingPlainDescription(long); utf8.RuneCountInString(got) != bookingDescriptionMax || !strings.HasSuffix(got, "…") {
		t.Errorf("cap: %d runes", utf8.RuneCountInString(got))
	}
	if bookingPlainDescription("  <p> </p> ") != "" {
		t.Error("empty description must stay empty")
	}
}

func TestBookingDescriptionTiers(t *testing.T) {
	b := BookingRow{Description: "Call +32 475 12 34 56 or 0475/12.34.56, IBAN BE68 5390 0754 7034, cus_ABCDEFGH123"}
	if bookingForAudience(b, AudiencePublic).Description != "" {
		t.Error("public must not carry descriptions")
	}
	m := bookingForAudience(b, AudienceMembers).Description
	for _, leak := range []string{"475", "BE68", "cus_"} {
		if strings.Contains(m, leak) {
			t.Errorf("members description leaks %q: %s", leak, m)
		}
	}
	if bookingForAudience(b, AudienceStewards).Description != b.Description {
		t.Error("stewards see the description as is")
	}
}
