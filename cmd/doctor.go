package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

type doctorFinding struct {
	Severity string
	Scope    string
	Message  string
	Fix      string
}

type doctorReport struct {
	Findings      []doctorFinding
	ScopesChecked int
	ImagesChecked int
}

type doctorScope struct {
	Label string
	Year  string
	Month string
	Path  string
}

var unicodeEscapePattern = regexp.MustCompile(`\\u[0-9a-fA-F]{4}`)
var canonicalImagePathPattern = regexp.MustCompile(`^\d{4}/\d{2}/messages/discord/images/`)
var missingLocalFilePattern = regexp.MustCompile(`^image ([^ ]+) references missing local file "([^"]+)"$`)

func Doctor(args []string) error {
	if HasFlag(args, "--help", "-h", "help") {
		PrintDoctorHelp()
		return nil
	}

	dataDir := DataDir()
	report := runDoctorChecks(dataDir)

	fmt.Printf("\n%s🩺 Doctor%s\n", Fmt.Bold, Fmt.Reset)
	fmt.Printf("%sDATA_DIR: %s%s\n", Fmt.Dim, dataDir, Fmt.Reset)
	fmt.Printf("%sChecked %d scope(s), %d image reference(s)%s\n\n",
		Fmt.Dim, report.ScopesChecked, report.ImagesChecked, Fmt.Reset)

	if len(report.Findings) == 0 {
		fmt.Printf("%s✓ DATA_DIR looks healthy%s\n\n", Fmt.Green, Fmt.Reset)
		return nil
	}

	summary := summarizeDoctorFindings(report.Findings)
	errorCount := 0
	for _, finding := range summary {
		color := Fmt.Yellow
		symbol := "!"
		if finding.Severity == "error" {
			color = Fmt.Red
			symbol = "✗"
			errorCount++
		}
		fmt.Printf("%s%s%s [%s] %s\n", color, symbol, Fmt.Reset, finding.Scope, finding.Message)
		if finding.Fix != "" {
			fmt.Printf("    Fix: %s\n", finding.Fix)
		}
	}
	fmt.Println()

	if errorCount > 0 {
		return fmt.Errorf("%d issue group(s) found", errorCount)
	}
	return nil
}

func runDoctorChecks(dataDir string) doctorReport {
	report := doctorReport{}

	checkRoomChannelDirs(dataDir, &report)

	scopes := collectDoctorScopes(dataDir)
	report.ScopesChecked = len(scopes)
	for _, scope := range scopes {
		checkGeneratedFiles(scope, &report)
		report.ImagesChecked += checkImagesFile(dataDir, scope, &report)
	}
	checkLatestHomepageEvents(dataDir, &report)
	checkOdooJournalBalances(&report)

	return report
}

// checkOdooJournalBalances verifies that each Odoo journal linked to a local
// account has statements that satisfy the running-balance / chain-continuity
// invariants. Skipped silently when Odoo credentials aren't configured.
func checkOdooJournalBalances(report *doctorReport) {
	creds, err := ResolveOdooCredentials()
	if err != nil {
		return
	}
	uid, err := odooAuth(creds.URL, creds.DB, creds.Login, creds.Password)
	if err != nil || uid == 0 {
		return
	}

	seen := map[int]bool{}
	for _, acc := range LoadAccountConfigs() {
		if acc.OdooJournalID == 0 || seen[acc.OdooJournalID] {
			continue
		}
		seen[acc.OdooJournalID] = true

		issues, err := CheckOdooJournalStatements(creds, uid, acc.OdooJournalID)
		if err != nil {
			report.Findings = append(report.Findings, doctorFinding{
				Severity: "warning",
				Scope:    fmt.Sprintf("odoo/journal-%d", acc.OdooJournalID),
				Message:  fmt.Sprintf("could not check journal %q: %v", acc.OdooJournalName, err),
				Fix:      fmt.Sprintf("Run: chb odoo journals %d check", acc.OdooJournalID),
			})
			continue
		}
		for _, i := range issues {
			scope := fmt.Sprintf("odoo/journal-%d", acc.OdooJournalID)
			fix := fmt.Sprintf("Run: chb odoo journals %d fix", acc.OdooJournalID)
			var msg string
			switch i.Kind {
			case "balance_mismatch":
				msg = fmt.Sprintf("statement %q (#%d, %s): running balance %s ≠ balance_end_real %s (off by %s)",
					i.StatementName, i.StatementID, i.Date,
					fmtEUR(i.RunningBalance), fmtEUR(i.BalanceEndReal), fmtEURSigned(i.Diff()))
			case "chain_gap":
				msg = fmt.Sprintf("statement %q (#%d, %s): balance_start %s doesn't chain from previous end_real %s",
					i.StatementName, i.StatementID, i.Date,
					fmtEUR(i.BalanceStart), fmtEUR(i.PreviousEndReal))
				fix = fmt.Sprintf("Run: chb odoo journals %d fix (if gap persists, re-run: chb odoo journals %d sync)",
					acc.OdooJournalID, acc.OdooJournalID)
			}
			report.Findings = append(report.Findings, doctorFinding{
				Severity: "error",
				Scope:    scope,
				Message:  msg,
				Fix:      fix,
			})
		}
	}
}

func collectDoctorScopes(dataDir string) []doctorScope {
	var scopes []doctorScope

	latestPath := filepath.Join(dataDir, "latest")
	if st, err := os.Stat(latestPath); err == nil && st.IsDir() {
		scopes = append(scopes, doctorScope{
			Label: "latest",
			Year:  "latest",
			Path:  latestPath,
		})
	}

	for _, year := range getAvailableYears(dataDir) {
		for _, month := range getAvailableMonths(dataDir, year) {
			scopes = append(scopes, doctorScope{
				Label: year + "/" + month,
				Year:  year,
				Month: month,
				Path:  filepath.Join(dataDir, year, month),
			})
		}
	}

	return scopes
}

func checkRoomChannelDirs(dataDir string, report *doctorReport) {
	rooms, err := LoadRooms()
	if err != nil {
		report.Findings = append(report.Findings, doctorFinding{
			Severity: "warning",
			Scope:    "config",
			Message:  "could not load rooms.json; room Discord channel checks skipped",
			Fix:      "Ensure ~/.chb/rooms.json exists and is valid JSON",
		})
		return
	}

	for _, room := range rooms {
		if room.DiscordChannelID == "" {
			continue
		}
		dir := filepath.Join(dataDir, "latest", "messages", "discord", room.DiscordChannelID)
		if st, err := os.Stat(dir); err != nil || !st.IsDir() {
			name := room.Slug
			if name == "" {
				name = room.ID
			}
			report.Findings = append(report.Findings, doctorFinding{
				Severity: "error",
				Scope:    "latest",
				Message:  fmt.Sprintf("missing Discord channel directory for room %s (%s)", name, room.DiscordChannelID),
				Fix:      "Run: chb messages sync --history",
			})
		}
	}
}

func checkGeneratedFiles(scope doctorScope, report *doctorReport) {
	messagesDir := filepath.Join(scope.Path, "messages", "discord")
	generatedDir := filepath.Join(scope.Path, "generated")
	financeDir := filepath.Join(scope.Path, "finance")
	calendarsDir := filepath.Join(scope.Path, "calendars")
	eventsDir := filepath.Join(scope.Path, "events")

	if hasChannelMessages(messagesDir) {
		requireFile(scope, filepath.Join(generatedDir, "images.json"), "messages present but generated/images.json is missing", "Run: chb generate", report)
	}
	if hasMaterialData(financeDir) {
		requireFile(scope, filepath.Join(generatedDir, "transactions.json"), "finance data present but generated/transactions.json is missing", "Run: chb generate", report)
		requireFile(scope, filepath.Join(generatedDir, "counterparties.json"), "finance data present but generated/counterparties.json is missing", "Run: chb generate", report)
	}
	if hasPublicEventSourceData(calendarsDir, eventsDir) {
		requireFile(scope, filepath.Join(generatedDir, "events.json"), "calendar/event data present but generated/events.json is missing", "Run: chb events sync --history", report)
	}
}

func requireFile(scope doctorScope, path, message, fix string, report *doctorReport) {
	if _, err := os.Stat(path); err == nil {
		return
	}
	report.Findings = append(report.Findings, doctorFinding{
		Severity: "error",
		Scope:    scope.Label,
		Message:  message,
		Fix:      fix,
	})
}

func hasChannelMessages(messagesDir string) bool {
	entries, err := os.ReadDir(messagesDir)
	if err != nil {
		return false
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		msgPath := filepath.Join(messagesDir, entry.Name(), "messages.json")
		data, err := os.ReadFile(msgPath)
		if err != nil {
			continue
		}
		var cache cachedMessageFile
		if json.Unmarshal(data, &cache) == nil && len(cache.Messages) > 0 {
			return true
		}
	}
	return false
}

func hasMaterialData(dir string) bool {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return false
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".") {
			continue
		}
		return true
	}
	return false
}

func hasPublicEventSourceData(calendarsDir, eventsDir string) bool {
	publicICS := filepath.Join(calendarsDir, "ics", "public.ics")
	if st, err := os.Stat(publicICS); err == nil && !st.IsDir() && st.Size() > 0 {
		return true
	}
	return hasMaterialData(eventsDir)
}

func checkImagesFile(dataDir string, scope doctorScope, report *doctorReport) int {
	imagesPath := filepath.Join(scope.Path, "generated", "images.json")
	raw, err := os.ReadFile(imagesPath)
	if err != nil {
		return 0
	}

	checked := 0
	if strings.Contains(string(raw), `"proxyUrl"`) {
		report.Findings = append(report.Findings, doctorFinding{
			Severity: "error",
			Scope:    scope.Label,
			Message:  "generated/images.json still contains deprecated proxyUrl fields",
			Fix:      "Run: chb generate",
		})
	}
	if unicodeEscapePattern.Match(raw) {
		report.Findings = append(report.Findings, doctorFinding{
			Severity: "error",
			Scope:    scope.Label,
			Message:  "generated/images.json contains escaped unicode sequences",
			Fix:      "Run: chb generate",
		})
	}

	var imagesFile struct {
		Images []struct {
			ID        string `json:"id"`
			URL       string `json:"url"`
			FilePath  string `json:"filePath"`
			LocalPath string `json:"localPath"`
			Timestamp string `json:"timestamp"`
		} `json:"images"`
	}
	if err := json.Unmarshal(raw, &imagesFile); err != nil {
		report.Findings = append(report.Findings, doctorFinding{
			Severity: "error",
			Scope:    scope.Label,
			Message:  "generated/images.json is not valid JSON",
			Fix:      "Run: chb generate",
		})
		return 0
	}

	for _, img := range imagesFile.Images {
		checked++
		path := img.FilePath
		if img.LocalPath != "" {
			path = img.LocalPath
		}
		id := img.ID
		if id == "" {
			id = "unknown"
		}

		if img.URL == "" {
			report.Findings = append(report.Findings, doctorFinding{
				Severity: "error",
				Scope:    scope.Label,
				Message:  fmt.Sprintf("image %s is missing url", id),
				Fix:      "Run: chb generate",
			})
		}
		if path == "" {
			report.Findings = append(report.Findings, doctorFinding{
				Severity: "error",
				Scope:    scope.Label,
				Message:  fmt.Sprintf("image %s is missing filePath/localPath", id),
				Fix:      "Run: chb generate",
			})
			continue
		}
		if strings.HasPrefix(path, "latest/") {
			report.Findings = append(report.Findings, doctorFinding{
				Severity: "error",
				Scope:    scope.Label,
				Message:  fmt.Sprintf("image %s uses latest/ in filePath/localPath", id),
				Fix:      "Run: chb generate",
			})
		}
		if !canonicalImagePathPattern.MatchString(path) {
			report.Findings = append(report.Findings, doctorFinding{
				Severity: "error",
				Scope:    scope.Label,
				Message:  fmt.Sprintf("image %s has non-canonical filePath/localPath %q", id, path),
				Fix:      "Run: chb generate",
			})
		}
		if expectedPrefix := imageMonthPrefixFromTimestamp(img.Timestamp); expectedPrefix != "" && !strings.HasPrefix(path, expectedPrefix+"/") {
			report.Findings = append(report.Findings, doctorFinding{
				Severity: "error",
				Scope:    scope.Label,
				Message:  fmt.Sprintf("image %s path %q does not match timestamp month %s", id, path, expectedPrefix),
				Fix:      "Run: chb generate",
			})
		}
		fullPath := filepath.Join(dataDir, filepath.FromSlash(path))
		if _, err := os.Stat(fullPath); err != nil {
			fix := "Run: chb images sync --history"
			if scope.Label == "latest" {
				fix = "Run: chb messages sync --history && chb generate && chb images sync --history"
			}
			report.Findings = append(report.Findings, doctorFinding{
				Severity: "error",
				Scope:    scope.Label,
				Message:  fmt.Sprintf("image %s references missing local file %q", id, path),
				Fix:      fix,
			})
		}
	}

	return checked
}

func checkLatestHomepageEvents(dataDir string, report *doctorReport) {
	latestEventsPath := filepath.Join(dataDir, "latest", "generated", "events.json")
	raw, err := os.ReadFile(latestEventsPath)
	if err != nil {
		if hasAnyMonthlyGeneratedEvents(dataDir) {
			report.Findings = append(report.Findings, doctorFinding{
				Severity: "error",
				Scope:    "latest",
				Message:  "latest/generated/events.json is missing",
				Fix:      "Run: chb generate",
			})
		}
		return
	}

	var latest LatestEventsFile
	if err := json.Unmarshal(raw, &latest); err != nil {
		report.Findings = append(report.Findings, doctorFinding{
			Severity: "error",
			Scope:    "latest",
			Message:  "latest/generated/events.json is not valid JSON",
			Fix:      "Run: chb generate",
		})
		return
	}

	for i, ev := range latest.Events {
		label := doctorHomepageEventLabel(i, ev)

		if strings.TrimSpace(ev.Name) == "" {
			report.Findings = append(report.Findings, doctorFinding{
				Severity: "error",
				Scope:    "latest",
				Message:  fmt.Sprintf("homepage event %s is missing title", label),
				Fix:      "Run: chb events sync --history && chb generate",
			})
		}
		if strings.TrimSpace(ev.CoverImage) == "" {
			report.Findings = append(report.Findings, doctorFinding{
				Severity: "error",
				Scope:    "latest",
				Message:  fmt.Sprintf("homepage event %s is missing coverImage", label),
				Fix:      "Run: chb events sync --history",
			})
		}
		if strings.TrimSpace(ev.CoverImageLocal) == "" {
			report.Findings = append(report.Findings, doctorFinding{
				Severity: "error",
				Scope:    "latest",
				Message:  fmt.Sprintf("homepage event %s is missing coverImageLocal", label),
				Fix:      "Run: chb images sync --history && chb generate",
			})
		} else {
			fullPath := filepath.Join(dataDir, filepath.FromSlash(ev.CoverImageLocal))
			if _, err := os.Stat(fullPath); err != nil {
				report.Findings = append(report.Findings, doctorFinding{
					Severity: "error",
					Scope:    "latest",
					Message:  fmt.Sprintf("homepage event %s references missing local cover image %q", label, ev.CoverImageLocal),
					Fix:      "Run: chb images sync --history && chb generate",
				})
			}
		}
		if looksLikeThinEventDescription(ev.Description) {
			report.Findings = append(report.Findings, doctorFinding{
				Severity: "error",
				Scope:    "latest",
				Message:  fmt.Sprintf("homepage event %s has a thin description", label),
				Fix:      "Run: chb events sync --history",
			})
		}
	}
}

func hasAnyMonthlyGeneratedEvents(dataDir string) bool {
	for _, year := range getAvailableYears(dataDir) {
		for _, month := range getAvailableMonths(dataDir, year) {
			eventsPath := filepath.Join(dataDir, year, month, "generated", "events.json")
			if st, err := os.Stat(eventsPath); err == nil && !st.IsDir() && st.Size() > 0 {
				return true
			}
		}
	}
	return false
}

func doctorHomepageEventLabel(index int, ev LatestEvent) string {
	if id := strings.TrimSpace(ev.ID); id != "" {
		return id
	}
	if name := strings.TrimSpace(ev.Name); name != "" {
		return strconv.Itoa(index+1) + " (" + name + ")"
	}
	return strconv.Itoa(index + 1)
}

func imageMonthPrefixFromTimestamp(timestamp string) string {
	if timestamp == "" {
		return ""
	}
	t, err := time.Parse(time.RFC3339Nano, timestamp)
	if err != nil {
		t, err = time.Parse("2006-01-02T15:04:05+00:00", timestamp)
	}
	if err != nil {
		return ""
	}
	t = t.In(BrusselsTZ())
	return fmt.Sprintf("%d/%02d", t.Year(), t.Month())
}

func summarizeDoctorFindings(findings []doctorFinding) []doctorFinding {
	type imageSummary struct {
		severity string
		scope    string
		fix      string
		total    int
		byMonth  map[string]int
	}

	imageGroups := map[string]*imageSummary{}
	var summarized []doctorFinding

	for _, finding := range findings {
		if m := missingLocalFilePattern.FindStringSubmatch(finding.Message); m != nil {
			path := m[2]
			month := "unknown"
			if parts := strings.Split(path, "/"); len(parts) >= 2 {
				month = parts[0] + "/" + parts[1]
			}
			key := finding.Severity + "|" + finding.Scope + "|" + finding.Fix
			group := imageGroups[key]
			if group == nil {
				group = &imageSummary{
					severity: finding.Severity,
					scope:    finding.Scope,
					fix:      finding.Fix,
					byMonth:  map[string]int{},
				}
				imageGroups[key] = group
			}
			group.total++
			group.byMonth[month]++
			continue
		}
		summarized = append(summarized, finding)
	}

	for _, group := range imageGroups {
		var months []string
		for month := range group.byMonth {
			months = append(months, month)
		}
		sort.Strings(months)
		var parts []string
		for _, month := range months {
			parts = append(parts, fmt.Sprintf("%s (%d)", month, group.byMonth[month]))
		}
		summarized = append(summarized, doctorFinding{
			Severity: group.severity,
			Scope:    group.scope,
			Message:  fmt.Sprintf("%d image references point to missing local files: %s", group.total, strings.Join(parts, ", ")),
			Fix:      group.fix,
		})
	}

	sort.SliceStable(summarized, func(i, j int) bool {
		if summarized[i].Severity != summarized[j].Severity {
			return summarized[i].Severity < summarized[j].Severity
		}
		if summarized[i].Scope != summarized[j].Scope {
			return summarized[i].Scope < summarized[j].Scope
		}
		return summarized[i].Message < summarized[j].Message
	})

	return summarized
}
