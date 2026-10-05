package cmd

// Every month has the same files.
//
// Generators write a month's file when they have something to say, and a
// month that was last generated before a layout change (the audience tiers,
// v3.11) can lack a tier copy altogether. Consumers should not need to
// know which months are "special". ensureMonthFileSet runs at the end of
// `chb generate` and, for every month from the first to the last month of
// data (and every year in between):
//
//   - re-projects a public/members file that is missing although the
//     stewards copy exists (offline: the stewards file is the full data);
//   - writes an empty, schema-valid file in all three tiers when a month
//     genuinely has nothing (no transactions, no events, …).
//
// Files are written without the latest/ mirror: latest/ keeps its own
// meaning (newest month, upcoming events, lifetime rollups).

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/CommonsHub/chb/ical"
)

// monthFileSpec describes one per-month artifact.
type monthFileSpec struct {
	rel string
	// empty builds the stewards skeleton for a month with no data.
	empty func(year, month, now string) interface{}
	// project re-projects stewards bytes for tier a; nil = same bytes in
	// every tier.
	project func(data []byte, a Audience) (interface{}, error)
}

func decodeAndProject[T any](data []byte, a Audience, project func(T, Audience) T) (interface{}, error) {
	var v T
	if err := json.Unmarshal(data, &v); err != nil {
		return nil, err
	}
	return project(v, a), nil
}

var monthFileSpecs = []monthFileSpec{
	{rel: "transactions.json",
		empty: func(y, m, now string) interface{} {
			return TransactionsFile{Year: y, Month: m, GeneratedAt: now, Transactions: []TransactionEntry{}}
		},
		project: func(data []byte, a Audience) (interface{}, error) {
			return decodeAndProject(data, a, func(f TransactionsFile, a Audience) TransactionsFile {
				out := f
				out.Transactions = make([]TransactionEntry, len(f.Transactions))
				for i, tx := range f.Transactions {
					out.Transactions[i] = transactionForAudience(tx, a)
				}
				return out
			})
		}},
	{rel: "counterparties.json",
		empty: func(y, m, now string) interface{} {
			return CounterpartiesFile{Month: y + "-" + m, GeneratedAt: now, Counterparties: map[string]CounterpartyEntry{}}
		},
		project: func(data []byte, a Audience) (interface{}, error) {
			return decodeAndProject(data, a, counterpartiesFileForAudience)
		}},
	{rel: "summary.json",
		empty: func(y, m, now string) interface{} {
			return MonthlyReportFile{Year: y, Month: m, GeneratedAt: now, Accounts: []MonthlyReportAccount{}, Sources: []MonthlyReportSource{}}
		}},
	{rel: commissionsFile,
		empty: func(y, m, now string) interface{} {
			return CommissionsFile{Year: y, Month: m, UpdatedAt: now, Items: []Commission{}}
		}},
	{rel: "contributors.json",
		empty: func(y, m, now string) interface{} {
			return MonthlyContributorsFile{Year: y, Month: m, GeneratedAt: now, Contributors: []ContributorEntry{}}
		},
		project: func(data []byte, a Audience) (interface{}, error) {
			return decodeAndProject(data, a, contributorsFileForAudience)
		}},
	{rel: "images.json",
		empty: func(y, m, now string) interface{} {
			return ImagesFile{Year: y, Month: m, Images: []ImageEntry{}}
		},
		project: func(data []byte, a Audience) (interface{}, error) {
			return decodeAndProject(data, a, imagesFileForAudience)
		}},
	{rel: contributionsFile,
		empty: func(y, m, now string) interface{} {
			return ContributionsFile{GeneratedAt: now, ChannelID: contributionsChannelID(), Messages: []ContributionMessage{}}
		}},
	{rel: tokensIssuedFile,
		empty: func(y, m, now string) interface{} {
			sym := ""
			if t := ContributionTokenConfig(LoadTokenConfigs()); t != nil {
				sym = t.Symbol
			}
			return TokensIssuedFile{GeneratedAt: now, Token: sym, Issued: []TokenIssued{}}
		},
		project: func(data []byte, a Audience) (interface{}, error) {
			return decodeAndProject(data, a, tokensIssuedForAudience)
		}},
	{rel: "members.json",
		empty: func(y, m, now string) interface{} {
			return MembersOutputFile{Year: y, Month: m, ProductID: "mixed", GeneratedAt: now, Members: []Member{}}
		},
		project: func(data []byte, a Audience) (interface{}, error) {
			return decodeAndProject(data, a, membersFileForAudience)
		}},
	{rel: "door.json",
		empty: func(y, m, now string) interface{} {
			return DoorMonthFile{Month: y + "-" + m, GeneratedAt: now, Openers: []DoorOpener{}}
		},
		project: func(data []byte, a Audience) (interface{}, error) {
			var f DoorMonthFile
			if err := json.Unmarshal(data, &f); err != nil {
				return nil, err
			}
			return doorFileForAudience(f, a), nil
		}},
	{rel: "events.json",
		empty: func(y, m, now string) interface{} {
			return FullEventsFile{Month: y + "-" + m, GeneratedAt: now, Events: []FullEvent{}}
		},
		project: func(data []byte, a Audience) (interface{}, error) {
			return decodeAndProject(data, a, eventsFileForAudience)
		}},
	{rel: filepath.Join("calendars", "public.ics"),
		empty: func(y, m, now string) interface{} {
			return rawFile(ical.WrapICS(nil, "-//Commons Hub Brussels//Public Calendar Events//EN"))
		}},
	{rel: "expenses.json",
		empty: func(y, m, now string) interface{} {
			return ExpensesFile{GeneratedAt: now, Scope: "month", Period: y + "-" + m, Currency: "EUR", ByCategory: []CategoryTotal{}, Expenses: []Expense{}}
		}},
	{rel: "vendors.json",
		empty: func(y, m, now string) interface{} {
			return VendorsFile{GeneratedAt: now, Scope: "month", Period: y + "-" + m, Currency: "EUR", Vendors: []VendorRow{}}
		}},
	{rel: "customers.json",
		empty: func(y, m, now string) interface{} {
			return CustomersFile{GeneratedAt: now, Scope: "month", Period: y + "-" + m, Currency: "EUR", ByIncome: []CategoryTotal{}, Customers: []CustomerRow{}}
		}},
	{rel: "bookings.json",
		empty: func(y, m, now string) interface{} {
			return BookingsFile{GeneratedAt: now, Scope: "month", Period: y + "-" + m, Currency: "EUR", Rooms: []RoomSummary{}, Bookings: []BookingRow{}, Rentals: []RentalRow{}}
		}},
}

// yearFileSpecs: the per-year files every year must have. Year events are
// rebuilt from the month stewards files (offline) when a tier lacks them.
var yearFileSpecs = []monthFileSpec{
	{rel: ledgerBalancesFile,
		empty: func(y, _, now string) interface{} {
			return LedgerBalancesFile{GeneratedAt: now, Year: y, PeriodStart: y + "-01-01", PeriodEnd: y + "-12-31",
				FiscalStart: y + "-01-01", Currency: "EUR", Accounts: []LedgerOut{}}
		}},
	{rel: annualAccountsFile,
		empty: func(y, _, now string) interface{} {
			return AnnualAccountsFile{GeneratedAt: now, Scope: "year", Year: y,
				Entity:      AnnualEntity{Name: "Commons Hub Brussels ASBL", EnterpriseNumber: "0804.505.132", FormerName: "Citizen Spring ASBL"},
				FiscalYears: []AnnualFiscalYear{}}
		}},
	{rel: "activitygrid.json",
		empty: func(y, _, now string) interface{} {
			return ActivityGridYear{Year: y, Months: []ActivityGridMonth{}}
		}},
	{rel: "contributors.json",
		empty: func(y, _, now string) interface{} {
			return YearlyUsersFile{Year: y, GeneratedAt: now, Contributors: []YearlyUsersEntry{}}
		},
		project: func(data []byte, a Audience) (interface{}, error) {
			return decodeAndProject(data, a, yearlyUsersFileForAudience)
		}},
	{rel: "events.json",
		empty: func(y, _, now string) interface{} {
			return FullEventsFile{Month: y, GeneratedAt: now, Events: []FullEvent{}}
		},
		project: func(data []byte, a Audience) (interface{}, error) {
			return decodeAndProject(data, a, eventsFileForAudience)
		}},
	{rel: "events.csv",
		empty: func(y, _, now string) interface{} {
			return rawFile("Event ID,Calendar Source,Date,Time,Event Name,Host,Attendance,Tickets Sold,Ticket Revenue (EUR),Fridge Income (EUR),Rental Income (EUR),Location,URL,Note\n")
		},
		project: func(data []byte, a Audience) (interface{}, error) {
			return rawFile(eventsCSVForAudience(string(data), a)), nil
		}},
	{rel: "expenses.json",
		empty: func(y, _, now string) interface{} {
			return ExpensesFile{GeneratedAt: now, Scope: "year", Period: y, Currency: "EUR", ByCategory: []CategoryTotal{}, Expenses: []Expense{}}
		}},
	{rel: "vendors.json",
		empty: func(y, _, now string) interface{} {
			return VendorsFile{GeneratedAt: now, Scope: "year", Period: y, Currency: "EUR", Vendors: []VendorRow{}}
		}},
	{rel: "customers.json",
		empty: func(y, _, now string) interface{} {
			return CustomersFile{GeneratedAt: now, Scope: "year", Period: y, Currency: "EUR", ByIncome: []CategoryTotal{}, Customers: []CustomerRow{}}
		}},
	{rel: "bookings.json",
		empty: func(y, _, now string) interface{} {
			return BookingsFile{GeneratedAt: now, Scope: "year", Period: y, Currency: "EUR", Rooms: []RoomSummary{}, Months: []BookingsMonth{}, Bookings: []BookingRow{}, Rentals: []RentalRow{}}
		}},
}

// rawFile is a non-JSON payload (ics, csv) written as is.
type rawFile string

// dataMonthRange lists every month from the first to the last YYYY/MM
// directory holding data (providers/ or a tier), gaps included.
func dataMonthRange(dataDir string) []string {
	var found []string
	years, _ := os.ReadDir(dataDir)
	for _, y := range years {
		if !y.IsDir() || !isYearSegment(y.Name()) {
			continue
		}
		ms, _ := os.ReadDir(filepath.Join(dataDir, y.Name()))
		for _, m := range ms {
			if !m.IsDir() || !isMonthSegment(m.Name()) {
				continue
			}
			dir := filepath.Join(dataDir, y.Name(), m.Name())
			if dirExists(filepath.Join(dir, "providers")) || dirExists(filepath.Join(dir, stewardsDirName)) {
				found = append(found, y.Name()+"-"+m.Name())
			}
		}
	}
	if len(found) == 0 {
		return nil
	}
	sort.Strings(found)
	return expandMonthRange(found[0], found[len(found)-1])
}

// ensureMonthFileSet completes every month and year; returns how many files
// it wrote (re-projections + skeletons).
func ensureMonthFileSet(dataDir string) (int, error) {
	now := time.Now().UTC().Format(time.RFC3339)
	months := dataMonthRange(dataDir)
	written := 0
	years := map[string]bool{}
	for _, ym := range months {
		year, month := ym[:4], ym[5:]
		years[year] = true
		for _, spec := range monthFileSpecs {
			n, err := completeFile(dataDir, year, month, spec, now)
			if err != nil {
				return written, fmt.Errorf("%s %s: %w", ym, spec.rel, err)
			}
			written += n
		}
	}
	for year := range years {
		// Year events come from the month stewards files; build the stewards
		// copy offline when it is missing, then complete the tiers.
		if _, err := os.Stat(audiencePath(dataDir, year, "", AudienceStewards, "events.json")); err != nil {
			if n := writeYearEventsStewards(dataDir, year, now); n {
				written++
			}
		}
		for _, spec := range yearFileSpecs {
			n, err := completeFile(dataDir, year, "", spec, now)
			if err != nil {
				return written, fmt.Errorf("%s %s: %w", year, spec.rel, err)
			}
			written += n
		}
	}
	return written, nil
}

// writeYearEventsStewards collects the year's month events into the
// stewards year file (no latest/ mirror). Returns whether it wrote one.
func writeYearEventsStewards(dataDir, year, now string) bool {
	var all []FullEvent
	for m := 1; m <= 12; m++ {
		data, err := os.ReadFile(audiencePath(dataDir, year, fmt.Sprintf("%02d", m), AudienceStewards, "events.json"))
		if err != nil {
			continue
		}
		var ef FullEventsFile
		if json.Unmarshal(data, &ef) == nil {
			all = append(all, ef.Events...)
		}
	}
	if len(all) == 0 {
		return false
	}
	sort.Slice(all, func(i, j int) bool { return all[i].StartAt < all[j].StartAt })
	data, err := json.MarshalIndent(FullEventsFile{Month: year, GeneratedAt: now, Events: all}, "", "  ")
	if err != nil {
		return false
	}
	return writeTierBytes(dataDir, year, "", AudienceStewards, "events.json", data) == nil
}

// completeFile makes sure rel exists in all three tiers of a month (or a
// year, month ""). Returns the number of tier files written.
func completeFile(dataDir, year, month string, spec monthFileSpec, now string) (int, error) {
	stewards := audiencePath(dataDir, year, month, AudienceStewards, spec.rel)
	data, err := os.ReadFile(stewards)
	if err != nil {
		// Nothing at all: an empty file in every tier.
		written := 0
		for _, a := range Audiences {
			if fileExists(audiencePath(dataDir, year, month, a, spec.rel)) {
				continue
			}
			payload, err := encodeSpecPayload(spec.empty(year, month, now), spec, a)
			if err != nil {
				return written, err
			}
			if err := writeTierBytes(dataDir, year, month, a, spec.rel, payload); err != nil {
				return written, err
			}
			written++
		}
		return written, nil
	}
	written := 0
	for _, a := range []Audience{AudienceMembers, AudiencePublic} {
		if fileExists(audiencePath(dataDir, year, month, a, spec.rel)) {
			continue
		}
		var payload []byte
		if spec.project == nil {
			payload = data
		} else {
			v, err := spec.project(data, a)
			if err != nil {
				return written, err
			}
			if payload, err = marshalPayload(v); err != nil {
				return written, err
			}
		}
		if err := writeTierBytes(dataDir, year, month, a, spec.rel, payload); err != nil {
			Warnf("  %s⚠ %s/%s %s: %v%s", Fmt.Yellow, a, spec.rel, firstNonEmpty(year+"-"+month, year), err, Fmt.Reset)
			continue
		}
		written++
	}
	return written, nil
}

// encodeSpecPayload marshals a skeleton, projected for a when it has a
// projection (an empty door.json is a different shape below stewards).
func encodeSpecPayload(v interface{}, spec monthFileSpec, a Audience) ([]byte, error) {
	data, err := marshalPayload(v)
	if err != nil || spec.project == nil || a == AudienceStewards {
		return data, err
	}
	pv, err := spec.project(data, a)
	if err != nil {
		return nil, err
	}
	return marshalPayload(pv)
}

func marshalPayload(v interface{}) ([]byte, error) {
	if r, ok := v.(rawFile); ok {
		return []byte(r), nil
	}
	return json.MarshalIndent(v, "", "  ")
}

// writeTierBytes writes one tier file under the tier policy, without the
// latest/ mirror.
func writeTierBytes(dataDir, year, month string, a Audience, rel string, data []byte) error {
	cleaned, err := enforceAudiencePolicy(a, rel, data)
	if err != nil {
		return err
	}
	target := audiencePath(dataDir, year, month, a, rel)
	if err := os.MkdirAll(filepath.Dir(target), a.DirMode()); err != nil {
		return err
	}
	if err := os.WriteFile(target, cleaned, a.FileMode()); err != nil {
		return err
	}
	if base, ok := dataBaseForPath(target); ok {
		_ = applyDataPathPolicy(base, target, false)
	}
	return nil
}
