package cmd

import "testing"

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
