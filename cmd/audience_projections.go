package cmd

// Per-audience projections of every processed artifact. Each function takes
// the full (stewards) value and returns what a lower tier may see. The rules
// are the ones in docs/audiences.md:
//
//	public:   no person. Display identity of community members (Discord id,
//	          username, avatar) is treated as published-by-consent (open
//	          decision 1 in the doc); everything else about a person stops
//	          at members.
//	members:  names, narration, who did what — but nothing that lets you
//	          contact or pay someone, and no wallet ↔ person mapping.
//	stewards: everything.

import (
	"encoding/json"
	"strings"
)

// ---- contributors -------------------------------------------------------

func contributorForAudience(c ContributorEntry, a Audience) ContributorEntry {
	if a == AudienceStewards {
		return c
	}
	c.Address = nil // wallet ↔ person mapping is stewards-only
	return c
}

func contributorsFileForAudience(f MonthlyContributorsFile, a Audience) MonthlyContributorsFile {
	if a == AudienceStewards {
		return f
	}
	out := f
	out.Contributors = make([]ContributorEntry, len(f.Contributors))
	for i, c := range f.Contributors {
		out.Contributors[i] = contributorForAudience(c, a)
	}
	return out
}

func yearlyUsersFileForAudience(f YearlyUsersFile, a Audience) YearlyUsersFile {
	if a == AudienceStewards {
		return f
	}
	out := f
	out.Contributors = make([]YearlyUsersEntry, len(f.Contributors))
	for i, c := range f.Contributors {
		c.Address = nil
		out.Contributors[i] = c
	}
	return out
}

func topContributorsFileForAudience(f TopContributorsFile, a Audience) TopContributorsFile {
	if a == AudienceStewards {
		return f
	}
	out := f
	out.Contributors = make([]TopContributor, len(f.Contributors))
	for i, c := range f.Contributors {
		c.WalletAddress = nil
		out.Contributors[i] = c
	}
	return out
}

// ---- members ------------------------------------------------------------

// membersFileForAudience: public sees the summary only (counts, MRR);
// members see who is a member and on which plan, without the email hash,
// Stripe dashboard links or payment urls; stewards everything.
func membersFileForAudience(f MembersOutputFile, a Audience) MembersOutputFile {
	switch a {
	case AudienceStewards:
		return f
	case AudienceMembers:
		out := f
		out.Members = make([]Member, len(f.Members))
		for i, m := range f.Members {
			m.Accounts.EmailHash = ""
			m.SubscriptionURL = ""
			if m.LatestPayment != nil {
				lp := *m.LatestPayment
				lp.URL = ""
				m.LatestPayment = &lp
			}
			out.Members[i] = m
		}
		return out
	default:
		out := f
		out.Members = []Member{}
		return out
	}
}

// ---- images -------------------------------------------------------------

// imagesFileForAudience: public keeps the photo and who posted it (display
// identity) but not the message text, which is free-form and may name or
// quote other people; members and stewards get the text.
func imagesFileForAudience(f ImagesFile, a Audience) ImagesFile {
	if a != AudiencePublic {
		return f
	}
	out := f
	out.Images = make([]ImageEntry, len(f.Images))
	for i, img := range f.Images {
		img.Message = ""
		out.Images[i] = img
	}
	return out
}

// ---- counterparties -----------------------------------------------------

// counterpartiesFileForAudience: public lists only our own tracked accounts
// (entries with a slug); members see every counterparty by name; stewards
// everything.
func counterpartiesFileForAudience(f CounterpartiesFile, a Audience) CounterpartiesFile {
	if a != AudiencePublic {
		return f
	}
	out := f
	out.Counterparties = map[string]CounterpartyEntry{}
	for uri, cp := range f.Counterparties {
		if cp.Slug != "" {
			out.Counterparties[uri] = cp
		}
	}
	return out
}

// ---- events -------------------------------------------------------------

// eventForAudience: attendee lists (guests) and the raw Luma payload are
// stewards-only; ticket sales, attendance, income and notes are members;
// public gets the event as it is published (name, time, place, host, cover).
func eventForAudience(ev FullEvent, a Audience) FullEvent {
	if a == AudienceStewards {
		return ev
	}
	ev.Guests = nil
	ev.LumaData = nil
	if a == AudienceMembers {
		return ev
	}
	ev.TicketSales = nil
	ev.Metadata.Attendance = nil
	ev.Metadata.FridgeIncome = nil
	ev.Metadata.RentalIncome = nil
	ev.Metadata.TicketsSold = nil
	ev.Metadata.TicketRevenue = nil
	ev.Metadata.Note = nil
	return ev
}

func eventsFileForAudience(f FullEventsFile, a Audience) FullEventsFile {
	if a == AudienceStewards {
		return f
	}
	out := f
	out.Events = make([]FullEvent, len(f.Events))
	for i, ev := range f.Events {
		out.Events[i] = eventForAudience(ev, a)
	}
	return out
}

// eventsCSVForAudience drops the attendance/revenue/income/note columns for
// public; members and stewards get the full sheet.
func eventsCSVForAudience(csv string, a Audience) string {
	if a != AudiencePublic {
		return csv
	}
	lines := strings.Split(strings.TrimRight(csv, "\n"), "\n")
	if len(lines) == 0 {
		return csv
	}
	drop := map[string]bool{"Attendance": true, "Tickets Sold": true, "Ticket Revenue": true, "Fridge Income": true, "Rental Income": true, "Note": true}
	header := splitCSVLine(lines[0])
	keep := make([]int, 0, len(header))
	for i, h := range header {
		if !drop[h] {
			keep = append(keep, i)
		}
	}
	var out []string
	for _, line := range lines {
		cols := splitCSVLine(line)
		row := make([]string, 0, len(keep))
		for _, i := range keep {
			if i < len(cols) {
				row = append(row, cols[i])
			}
		}
		out = append(out, strings.Join(row, ","))
	}
	return strings.Join(out, "\n") + "\n"
}

// splitCSVLine splits one CSV line honouring double-quoted fields.
func splitCSVLine(line string) []string {
	var out []string
	var cur strings.Builder
	inQuotes := false
	for i := 0; i < len(line); i++ {
		c := line[i]
		switch {
		case c == '"':
			if inQuotes && i+1 < len(line) && line[i+1] == '"' {
				cur.WriteByte('"')
				i++
			} else {
				inQuotes = !inQuotes
				cur.WriteByte(c)
			}
		case c == ',' && !inQuotes:
			out = append(out, cur.String())
			cur.Reset()
		default:
			cur.WriteByte(c)
		}
	}
	out = append(out, cur.String())
	return out
}

// ---- inbound spreads ----------------------------------------------------

func inboundSpreadsFileForAudience(f InboundSpreadsFile, a Audience) InboundSpreadsFile {
	if a != AudiencePublic {
		return f
	}
	out := f
	out.Inbound = make([]InboundSpread, len(f.Inbound))
	for i, sp := range f.Inbound {
		sp.Counterparty = ""
		out.Inbound[i] = sp
	}
	return out
}

// ---- helpers ------------------------------------------------------------

// tierJSON marshals one value per tier with the projection fn; legacy gets
// the stewards bytes unless overridden by the caller.
func tierJSON[T any](full T, project func(T, Audience) T) tierPayload {
	enc := func(v T) []byte {
		b, err := json.MarshalIndent(v, "", "  ")
		if err != nil {
			return nil
		}
		return b
	}
	stewards := enc(full)
	return tierPayload{
		Stewards: stewards,
		Members:  enc(project(full, AudienceMembers)),
		Public:   enc(project(full, AudiencePublic)),
		Legacy:   stewards,
	}
}
