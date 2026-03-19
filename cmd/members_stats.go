package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

func MembersStats(args []string) {
	if HasFlag(args, "--help", "-h", "help") {
		printMembersStatsHelp()
		return
	}

	jsonOut := GetOption(args, "--format") == "json"
	dataDir := DataDir()

	posYear, posMonth, posFound := ParseYearMonthArg(args)

	type monthData struct {
		Month          string  `json:"month"`
		TotalMembers   int     `json:"totalMembers"`
		ActiveMembers  int     `json:"activeMembers"`
		MonthlyMembers int     `json:"monthlyMembers"`
		YearlyMembers  int     `json:"yearlyMembers"`
		MRR            float64 `json:"mrr"`
		Sources        map[string]int `json:"sources"`
	}

	var months []monthData

	yearDirs, _ := os.ReadDir(dataDir)
	for _, yd := range yearDirs {
		if !yd.IsDir() || len(yd.Name()) != 4 {
			continue
		}
		year := yd.Name()
		if posFound && posMonth == "" && year != posYear {
			continue
		}

		monthDirs, _ := os.ReadDir(filepath.Join(dataDir, year))
		for _, md := range monthDirs {
			if !md.IsDir() || len(md.Name()) != 2 {
				continue
			}
			month := md.Name()
			ym := year + "-" + month

			if posFound && posMonth != "" && (year != posYear || month != posMonth) {
				continue
			}

			membersPath := filepath.Join(dataDir, year, month, "generated", "members.json")
			data, err := os.ReadFile(membersPath)
			if err != nil {
				continue
			}

			var mf MembersOutputFile
			if json.Unmarshal(data, &mf) != nil {
				continue
			}

			sources := map[string]int{}
			for _, m := range mf.Members {
				sources[m.Source]++
			}

			months = append(months, monthData{
				Month:          ym,
				TotalMembers:   mf.Summary.TotalMembers,
				ActiveMembers:  mf.Summary.ActiveMembers,
				MonthlyMembers: mf.Summary.MonthlyMembers,
				YearlyMembers:  mf.Summary.YearlyMembers,
				MRR:            mf.Summary.MRR.Value,
				Sources:        sources,
			})
		}
	}

	sort.Slice(months, func(i, j int) bool {
		return months[i].Month > months[j].Month
	})

	if jsonOut {
		data, _ := json.MarshalIndent(months, "", "  ")
		fmt.Println(string(data))
		return
	}

	if len(months) == 0 {
		fmt.Printf("\n%sNo member data found.%s Run %schb members sync%s first.\n\n", Fmt.Dim, Fmt.Reset, Fmt.Cyan, Fmt.Reset)
		return
	}

	// Show latest month as headline
	latest := months[0]
	f := Fmt

	fmt.Printf("\n%s👥 Members%s\n", f.Bold, f.Reset)
	fmt.Printf("   Active:  %s%d%s", f.Bold, latest.ActiveMembers, f.Reset)
	fmt.Printf("  (%d monthly, %d yearly)\n", latest.MonthlyMembers, latest.YearlyMembers)
	fmt.Printf("   MRR:     %s%s%s\n", f.Bold, fmtEUR(latest.MRR), f.Reset)
	fmt.Printf("   Total:   %d (incl. canceled)\n\n", latest.TotalMembers)

	// Per-month table
	fmt.Printf("  %s%-10s  %6s  %6s  %8s  %s%s\n", f.Dim, "MONTH", "ACTIVE", "TOTAL", "MRR", "SOURCES", f.Reset)
	for _, m := range months {
		srcParts := []string{}
		// Sort sources
		var srcNames []string
		for name := range m.Sources {
			srcNames = append(srcNames, name)
		}
		sort.Strings(srcNames)
		for _, name := range srcNames {
			srcParts = append(srcParts, fmt.Sprintf("%s:%d", name, m.Sources[name]))
		}
		srcStr := ""
		for i, p := range srcParts {
			if i > 0 {
				srcStr += "  "
			}
			srcStr += p
		}

		fmt.Printf("  %-10s  %6d  %6d  %8s  %s%s%s\n",
			m.Month, m.ActiveMembers, m.TotalMembers, fmtEUR(m.MRR),
			f.Dim, srcStr, f.Reset,
		)
	}
	fmt.Println()
}

func printMembersStatsHelp() {
	f := Fmt
	fmt.Printf(`
%sUSAGE%s
  %schb members%s [year[/month]] [options]

%sOPTIONS%s
  %s<year>%s              Show stats for a specific year
  %s<year/month>%s        Show stats for a specific month
  %s--format json%s       Output as JSON
  %s--help, -h%s          Show this help

%sEXAMPLES%s
  %schb members%s                All-time member stats
  %schb members 2025%s           2025 only
  %schb members 2025/03%s        March 2025 only
`,
		f.Bold, f.Reset,
		f.Cyan, f.Reset,
		f.Bold, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Bold, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
	)
}
