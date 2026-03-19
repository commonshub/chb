package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

type dirSize struct {
	path  string
	bytes int64
	files int
}

// Stats shows data directory statistics
func Stats(args []string) {
	if HasFlag(args, "--help", "-h", "help") {
		PrintStatsHelp()
		return
	}

	dataDir := DataDir()

	fmt.Printf("\n%sData Directory%s\n", Fmt.Bold, Fmt.Reset)
	fmt.Printf("  Location: %s\n", dataDir)

	totalSize, totalFiles := dirStats(dataDir)
	fmt.Printf("  Total:    %s (%d files)\n", formatBytes(totalSize), totalFiles)

	// Check if data dir exists
	if _, err := os.Stat(dataDir); os.IsNotExist(err) {
		fmt.Printf("\n  %sNo data yet. Run %schb sync%s to get started.%s\n\n", Fmt.Dim, Fmt.Bold, Fmt.Dim, Fmt.Reset)
		return
	}

	// Breakdown by year/month
	fmt.Printf("\n%sBy Month%s\n", Fmt.Bold, Fmt.Reset)

	years, _ := os.ReadDir(dataDir)
	type monthEntry struct {
		label string
		size  int64
		files int
	}
	var months []monthEntry

	for _, yd := range years {
		if !yd.IsDir() {
			continue
		}
		year := yd.Name()
		if _, err := strconv.Atoi(year); err != nil || len(year) != 4 {
			continue
		}

		monthDirs, _ := os.ReadDir(filepath.Join(dataDir, year))
		for _, md := range monthDirs {
			if !md.IsDir() {
				continue
			}
			month := md.Name()
			if _, err := strconv.Atoi(month); err != nil || len(month) != 2 {
				continue
			}

			mPath := filepath.Join(dataDir, year, month)
			size, files := dirStats(mPath)
			months = append(months, monthEntry{
				label: year + "/" + month,
				size:  size,
				files: files,
			})
		}
	}

	// Sort chronologically
	sort.Slice(months, func(i, j int) bool {
		return months[i].label < months[j].label
	})

	// Find max size for bar chart
	var maxSize int64
	for _, m := range months {
		if m.size > maxSize {
			maxSize = m.size
		}
	}

	for _, m := range months {
		bar := makeBar(m.size, maxSize, 20)
		fmt.Printf("  %s  %s %s%7s%s (%d files)\n", m.label, bar, Fmt.Dim, formatBytes(m.size), Fmt.Reset, m.files)
	}

	// Breakdown by data type
	fmt.Printf("\n%sBy Type%s\n", Fmt.Bold, Fmt.Reset)

	typeMap := make(map[string]dirSize)
	for _, yd := range years {
		if !yd.IsDir() {
			continue
		}
		year := yd.Name()
		if _, err := strconv.Atoi(year); err != nil || len(year) != 4 {
			continue
		}
		monthDirs, _ := os.ReadDir(filepath.Join(dataDir, year))
		for _, md := range monthDirs {
			if !md.IsDir() {
				continue
			}
			month := md.Name()
			if _, err := strconv.Atoi(month); err != nil || len(month) != 2 {
				continue
			}
			typeDirs, _ := os.ReadDir(filepath.Join(dataDir, year, month))
			for _, td := range typeDirs {
				if !td.IsDir() {
					continue
				}
				tName := td.Name()
				tPath := filepath.Join(dataDir, year, month, tName)
				size, files := dirStats(tPath)
				ds := typeMap[tName]
				ds.bytes += size
				ds.files += files
				typeMap[tName] = ds
			}
		}
	}

	// Also check top-level non-year dirs (e.g. "latest")
	for _, d := range years {
		if !d.IsDir() {
			continue
		}
		name := d.Name()
		if _, err := strconv.Atoi(name); err == nil && len(name) == 4 {
			continue // skip year dirs
		}
		size, files := dirStats(filepath.Join(dataDir, name))
		ds := typeMap[name]
		ds.bytes += size
		ds.files += files
		typeMap[name] = ds
	}

	// Sort by size descending
	type typeEntry struct {
		name  string
		size  int64
		files int
	}
	var types []typeEntry
	for name, ds := range typeMap {
		types = append(types, typeEntry{name, ds.bytes, ds.files})
	}
	sort.Slice(types, func(i, j int) bool {
		return types[i].size > types[j].size
	})

	for _, t := range types {
		icon := typeIcon(t.name)
		fmt.Printf("  %s %-14s %7s (%d files)\n", icon, t.name, formatBytes(t.size), t.files)
	}

	// Images breakdown
	fmt.Printf("\n%sImages%s\n", Fmt.Bold, Fmt.Reset)

	imageExts := map[string]bool{".jpg": true, ".jpeg": true, ".png": true, ".gif": true, ".webp": true}
	var imgSize int64
	var imgCount int
	var discordImgSize int64
	var discordImgCount int
	var eventImgSize int64
	var eventImgCount int

	filepath.Walk(dataDir, func(p string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		ext := strings.ToLower(filepath.Ext(info.Name()))
		if !imageExts[ext] {
			return nil
		}
		imgSize += info.Size()
		imgCount++

		rel, _ := filepath.Rel(dataDir, p)
		if strings.Contains(rel, filepath.Join("messages", "discord", "images")) {
			discordImgSize += info.Size()
			discordImgCount++
		} else if strings.Contains(rel, filepath.Join("events", "images")) {
			eventImgSize += info.Size()
			eventImgCount++
		}
		return nil
	})

	if imgCount > 0 {
		fmt.Printf("  📸 Total          %7s (%d files)\n", formatBytes(imgSize), imgCount)
		if discordImgCount > 0 {
			fmt.Printf("     Discord        %7s (%d files)\n", formatBytes(discordImgSize), discordImgCount)
		}
		if eventImgCount > 0 {
			fmt.Printf("     Event covers   %7s (%d files)\n", formatBytes(eventImgSize), eventImgCount)
		}
	} else {
		fmt.Printf("  %sNo images yet. Run %schb images sync%s to download.%s\n", Fmt.Dim, Fmt.Bold, Fmt.Dim, Fmt.Reset)
	}

	fmt.Println()
}

func dirStats(path string) (int64, int) {
	var totalSize int64
	var totalFiles int
	filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if !info.IsDir() {
			totalSize += info.Size()
			totalFiles++
		}
		return nil
	})
	return totalSize, totalFiles
}

func formatBytes(b int64) string {
	switch {
	case b >= 1<<30:
		return fmt.Sprintf("%.1f GB", float64(b)/float64(1<<30))
	case b >= 1<<20:
		return fmt.Sprintf("%.1f MB", float64(b)/float64(1<<20))
	case b >= 1<<10:
		return fmt.Sprintf("%.1f KB", float64(b)/float64(1<<10))
	default:
		return fmt.Sprintf("%d B", b)
	}
}

func makeBar(value, max int64, width int) string {
	if max == 0 {
		return strings.Repeat("░", width)
	}
	filled := int(float64(value) / float64(max) * float64(width))
	if filled < 1 && value > 0 {
		filled = 1
	}
	return strings.Repeat("█", filled) + strings.Repeat("░", width-filled)
}

func typeIcon(name string) string {
	switch name {
	case "events":
		return "📅"
	case "calendars":
		return "📆"
	case "transactions":
		return "💰"
	case "messages":
		return "💬"
	case "latest":
		return "📌"
	default:
		return "📁"
	}
}
