package cmd

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

var downloadHTTPClient = &http.Client{Timeout: 20 * time.Second}

type imageSyncScope struct {
	Year  string
	Month string
	Label string
}

type eventCoverSyncResult struct {
	Downloaded int
	Skipped    int
	Domains    map[string]int
}

// ImagesSync downloads images from Discord messages and public event covers
// to the local data directory.
func ImagesSync(args []string) (int, error) {
	if HasFlag(args, "--help", "-h", "help") {
		printImagesSyncHelp()
		return 0, nil
	}

	force := HasFlag(args, "--force")
	dataDir := DataDir()
	discordToken := os.Getenv("DISCORD_BOT_TOKEN")

	fmt.Printf("\n%s📸 Syncing images...%s\n", Fmt.Bold, Fmt.Reset)
	fmt.Printf("%sDATA_DIR: %s%s\n", Fmt.Dim, dataDir, Fmt.Reset)

	if discordToken == "" {
		Warnf("%s⚠ DISCORD_BOT_TOKEN not set — skipping Discord image downloads%s", Fmt.Yellow, Fmt.Reset)
	}

	// Determine time range
	startMonth, isHistory := ResolveSinceMonth(args, "messages")
	posYear, posMonth, posFound := ParseYearMonthArg(args)

	years := getAvailableYears(dataDir)
	if len(years) == 0 {
		Warnf("%s⚠ No data found. Run sync first.%s", Fmt.Yellow, Fmt.Reset)
		return 0, nil
	}

	totalDiscord := 0
	totalEventCovers := 0
	skippedDiscord := 0
	skippedEventCovers := 0
	eventCoverDomains := map[string]int{}

	scopes := collectImageSyncScopes(dataDir, years, posYear, posMonth, posFound, startMonth, isHistory)
	for _, scope := range scopes {
		if discordToken != "" {
			d, s := syncDiscordImages(dataDir, scope.Year, scope.Month, scope.Label, discordToken, force)
			totalDiscord += d
			skippedDiscord += s
		}

		res := syncLumaImages(dataDir, scope.Year, scope.Month, force)
		totalEventCovers += res.Downloaded
		skippedEventCovers += res.Skipped
		for domain, count := range res.Domains {
			eventCoverDomains[domain] += count
		}
	}

	fmt.Printf("\n%s✅ Images sync complete%s\n", Fmt.Green, Fmt.Reset)
	if totalDiscord > 0 || skippedDiscord > 0 {
		fmt.Printf("  Discord attachments: %d downloaded, %d skipped\n", totalDiscord, skippedDiscord)
	}
	if totalEventCovers > 0 || skippedEventCovers > 0 {
		fmt.Printf("  Event covers: %d downloaded, %d skipped", totalEventCovers, skippedEventCovers)
		if summary := formatDomainSummary(eventCoverDomains); summary != "" {
			fmt.Printf(" (%s)", summary)
		}
		fmt.Println()
	}
	if totalDiscord == 0 && totalEventCovers == 0 && skippedDiscord == 0 && skippedEventCovers == 0 {
		fmt.Printf("  No images found to sync\n")
	}
	fmt.Println()

	UpdateSyncSource("images", isHistory)
	UpdateSyncActivity(isHistory)
	return totalDiscord + totalEventCovers, nil
}

// syncDiscordImages reads images.json for a month and downloads Discord attachments.
func syncDiscordImages(dataDir, year, month, label, token string, force bool) (downloaded, skipped int) {
	imagesPath := filepath.Join(dataDir, year, month, "generated", "images.json")
	data, err := os.ReadFile(imagesPath)
	if err != nil {
		return 0, 0
	}

	var imf ImagesFile
	if json.Unmarshal(data, &imf) != nil || len(imf.Images) == 0 {
		return 0, 0
	}

	for _, img := range imf.Images {
		if img.ID == "" || img.ChannelID == "" || img.MessageID == "" {
			continue
		}

		outPath := resolveDiscordImagePath(dataDir, year, month, img)
		imagesDir := filepath.Dir(outPath)
		if err := mkdirAllManagedData(imagesDir); err != nil {
			Warnf("  %s⚠ Failed to create %s: %v%s", Fmt.Yellow, imagesDir, err, Fmt.Reset)
			continue
		}

		// Check if already downloaded (any extension)
		if !force && fileExistsWithPrefix(imagesDir, img.ID) {
			skipped++
			continue
		}

		// Fetch message from Discord API to get the real attachment URL
		attachmentURL, ext := fetchDiscordAttachmentURL(img.ChannelID, img.MessageID, img.ID, token)
		if attachmentURL == "" {
			continue
		}

		outPath = filepath.Join(imagesDir, img.ID+ext)
		if err := downloadFile(attachmentURL, outPath); err != nil {
			Warnf("  %s⚠ Failed to download %s: %v%s", Fmt.Yellow, img.ID, err, Fmt.Reset)
			continue
		}

		downloaded++
		time.Sleep(200 * time.Millisecond) // rate limit
	}

	if downloaded > 0 {
		fmt.Printf("  ✓ %s discord: %d downloaded\n", label, downloaded)
	}

	return downloaded, skipped
}

// fetchDiscordAttachmentURL calls the Discord API to get the original attachment URL.
// Returns (url, extension) or ("", "") on failure.
func fetchDiscordAttachmentURL(channelID, messageID, attachmentID, token string) (string, string) {
	url := fmt.Sprintf("https://discord.com/api/v10/channels/%s/messages/%s", channelID, messageID)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return "", ""
	}
	req.Header.Set("Authorization", "Bot "+token)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", ""
	}
	defer resp.Body.Close()

	// Handle rate limiting
	if resp.StatusCode == 429 {
		retryAfter := resp.Header.Get("Retry-After")
		waitSec := 1.0
		if retryAfter != "" {
			fmt.Sscanf(retryAfter, "%f", &waitSec)
		}
		fmt.Printf("  %s⏳ Rate limited, waiting %.0fs...%s\n", Fmt.Dim, waitSec, Fmt.Reset)
		time.Sleep(time.Duration(waitSec*1000) * time.Millisecond)

		// Retry once
		resp2, err := http.DefaultClient.Do(req)
		if err != nil {
			return "", ""
		}
		defer resp2.Body.Close()
		resp = resp2
	}

	if resp.StatusCode != 200 {
		return "", ""
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", ""
	}

	var msg struct {
		Attachments []struct {
			ID  string `json:"id"`
			URL string `json:"url"`
		} `json:"attachments"`
	}
	if json.Unmarshal(body, &msg) != nil {
		return "", ""
	}

	for _, att := range msg.Attachments {
		if att.ID == attachmentID {
			ext := extFromURL(att.URL, ".jpg")
			return att.URL, ext
		}
	}

	return "", ""
}

type eventCoverDownloadTask struct {
	Index        int
	EventID      string
	CoverImage   string
	LocalRelPath string
	OutPath      string
	Domain       string
}

type eventCoverDownloadResult struct {
	Index        int
	Downloaded   bool
	Skipped      bool
	LocalRelPath string
	Err          error
}

// syncLumaImages reads events.json for a month, downloads public event cover images,
// and writes coverImageLocal back into events.json.
func syncLumaImages(dataDir, year, month string, force bool) eventCoverSyncResult {
	eventsPath := filepath.Join(dataDir, year, month, "generated", "events.json")
	data, err := os.ReadFile(eventsPath)
	if err != nil {
		return eventCoverSyncResult{}
	}

	var eventsFile FullEventsFile
	if json.Unmarshal(data, &eventsFile) != nil || len(eventsFile.Events) == 0 {
		return eventCoverSyncResult{}
	}

	imagesDir := filepath.Join(dataDir, year, month, "events", "images")
	if err := mkdirAllManagedData(imagesDir); err != nil {
		return eventCoverSyncResult{}
	}

	var tasks []eventCoverDownloadTask
	changed := false
	result := eventCoverSyncResult{Domains: map[string]int{}}

	for i, evt := range eventsFile.Events {
		if evt.CoverImage == "" || evt.ID == "" {
			continue
		}

		domain := hostFromURL(evt.CoverImage)
		if domain == "" {
			domain = "<unknown>"
		}
		result.Domains[domain]++

		ext := extFromURL(evt.CoverImage, ".jpg")
		localRelPath := filepath.ToSlash(filepath.Join(year, month, "events", "images", evt.ID+ext))
		outPath := filepath.Join(imagesDir, evt.ID+ext)

		if !force && evt.CoverImageLocal != "" {
			existingLocalPath := filepath.Join(dataDir, filepath.FromSlash(evt.CoverImageLocal))
			if fileExists(existingLocalPath) {
				if evt.CoverImageLocal != localRelPath {
					eventsFile.Events[i].CoverImageLocal = localRelPath
					changed = true
				}
				result.Skipped++
				continue
			}
		}
		if !force && fileExists(outPath) {
			if evt.CoverImageLocal != localRelPath {
				eventsFile.Events[i].CoverImageLocal = localRelPath
				changed = true
			}
			result.Skipped++
			continue
		}
		tasks = append(tasks, eventCoverDownloadTask{
			Index:        i,
			EventID:      evt.ID,
			CoverImage:   evt.CoverImage,
			LocalRelPath: localRelPath,
			OutPath:      outPath,
			Domain:       hostFromURL(evt.CoverImage),
		})
	}

	if len(tasks) > 0 {
		domains := uniqueTaskDomains(tasks)
		workerCount := 4
		if len(domains) < workerCount {
			workerCount = len(domains)
		}
		if workerCount < 1 {
			workerCount = 1
		}
		fmt.Printf("  ↳ %s-%s event covers: %d queued across %d domain(s), %d worker(s)\n", year, month, len(tasks), len(domains), workerCount)

		for _, res := range downloadEventCoverTasks(tasks, workerCount) {
			if res.Err != nil {
				Warnf("  %s⚠ Failed to download cover for %s: %v%s", Fmt.Yellow, eventsFile.Events[res.Index].ID, res.Err, Fmt.Reset)
				continue
			}
			if res.Downloaded {
				result.Downloaded++
				eventsFile.Events[res.Index].CoverImageLocal = res.LocalRelPath
				changed = true
			}
			if res.Skipped {
				result.Skipped++
				if eventsFile.Events[res.Index].CoverImageLocal != res.LocalRelPath {
					eventsFile.Events[res.Index].CoverImageLocal = res.LocalRelPath
					changed = true
				}
			}
		}
	}

	if changed {
		updated, err := marshalIndentedNoHTMLEscape(eventsFile)
		if err == nil {
			_ = writeDataFile(eventsPath, updated)
		}
	}

	if result.Downloaded > 0 {
		fmt.Printf("  ✓ %s-%s event covers: %d downloaded\n", year, month, result.Downloaded)
	}

	return result
}

func downloadEventCoverTasks(tasks []eventCoverDownloadTask, workerCount int) []eventCoverDownloadResult {
	grouped := map[string][]eventCoverDownloadTask{}
	var domains []string
	for _, task := range tasks {
		domain := task.Domain
		if domain == "" {
			domain = "<unknown>"
		}
		if _, ok := grouped[domain]; !ok {
			domains = append(domains, domain)
		}
		grouped[domain] = append(grouped[domain], task)
	}
	sort.Strings(domains)

	domainCh := make(chan []eventCoverDownloadTask)
	resultCh := make(chan eventCoverDownloadResult, len(tasks))

	var wg sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for domainTasks := range domainCh {
				for _, task := range domainTasks {
					if err := downloadFile(task.CoverImage, task.OutPath); err != nil {
						resultCh <- eventCoverDownloadResult{Index: task.Index, Err: err}
					} else {
						resultCh <- eventCoverDownloadResult{
							Index:        task.Index,
							Downloaded:   true,
							LocalRelPath: task.LocalRelPath,
						}
					}
					time.Sleep(200 * time.Millisecond)
				}
			}
		}()
	}

	for _, domain := range domains {
		domainCh <- grouped[domain]
	}
	close(domainCh)

	wg.Wait()
	close(resultCh)

	var results []eventCoverDownloadResult
	for res := range resultCh {
		results = append(results, res)
	}
	return results
}

func uniqueTaskDomains(tasks []eventCoverDownloadTask) []string {
	seen := map[string]bool{}
	var domains []string
	for _, task := range tasks {
		domain := task.Domain
		if domain == "" {
			domain = "<unknown>"
		}
		if seen[domain] {
			continue
		}
		seen[domain] = true
		domains = append(domains, domain)
	}
	sort.Strings(domains)
	return domains
}

func hostFromURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	return strings.ToLower(u.Hostname())
}

// downloadFile downloads a URL to a local file path.
func downloadFile(url, destPath string) error {
	resp, err := downloadHTTPClient.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}

	if err := mkdirAllManagedData(filepath.Dir(destPath)); err != nil {
		return err
	}

	out, err := os.Create(destPath)
	if err != nil {
		return err
	}

	_, err = io.Copy(out, resp.Body)
	if closeErr := out.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}

	baseDir, ok := dataBaseForPath(destPath)
	if ok {
		return applyDataPathPolicy(baseDir, destPath, false)
	}
	return os.Chmod(destPath, dataFileMode)
}

// extFromURL extracts the file extension from a URL, defaulting to defaultExt.
func extFromURL(rawURL, defaultExt string) string {
	// Strip query params
	clean := strings.Split(rawURL, "?")[0]
	ext := strings.ToLower(filepath.Ext(clean))
	if ext == ".jpg" || ext == ".jpeg" || ext == ".png" || ext == ".gif" || ext == ".webp" {
		return ext
	}
	return defaultExt
}

// fileExistsWithPrefix checks if any file with the given prefix exists in dir.
func fileExistsWithPrefix(dir, prefix string) bool {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return false
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), prefix+".") || e.Name() == prefix {
			return true
		}
	}
	return false
}

func collectImageSyncScopes(dataDir string, years []string, posYear, posMonth string, posFound bool, startMonth string, isHistory bool) []imageSyncScope {
	var scopes []imageSyncScope
	seen := map[string]bool{}

	addScope := func(year, month, label string) {
		key := year + "/" + month
		if seen[key] {
			return
		}
		seen[key] = true
		scopes = append(scopes, imageSyncScope{Year: year, Month: month, Label: label})
	}

	if posFound || startMonth != "" {
		for _, year := range years {
			for _, month := range getAvailableMonths(dataDir, year) {
				ym := fmt.Sprintf("%s-%s", year, month)
				if posFound {
					if posMonth != "" && (year != posYear || month != posMonth) {
						continue
					}
					if posMonth == "" && year != posYear {
						continue
					}
				}
				if startMonth != "" && ym < startMonth {
					continue
				}
				addScope(year, month, year+"-"+month)
			}
		}
		if isHistory {
			addScope("latest", "", "latest")
		}
		return scopes
	}

	now := time.Now().In(BrusselsTZ())
	prev := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, BrusselsTZ()).AddDate(0, -1, 0)
	addScope(fmt.Sprintf("%d", prev.Year()), fmt.Sprintf("%02d", prev.Month()), prev.Format("2006-01"))
	addScope(fmt.Sprintf("%d", now.Year()), fmt.Sprintf("%02d", now.Month()), now.Format("2006-01"))
	addScope("latest", "", "latest")
	return scopes
}

func formatDomainSummary(domainCounts map[string]int) string {
	if len(domainCounts) == 0 {
		return ""
	}

	type domainCount struct {
		Domain string
		Count  int
	}
	var counts []domainCount
	for domain, count := range domainCounts {
		counts = append(counts, domainCount{Domain: domain, Count: count})
	}
	sort.Slice(counts, func(i, j int) bool {
		if counts[i].Count == counts[j].Count {
			return counts[i].Domain < counts[j].Domain
		}
		return counts[i].Count > counts[j].Count
	})

	parts := make([]string, 0, Min(len(counts), 4))
	for i, dc := range counts {
		if i >= 4 {
			break
		}
		parts = append(parts, fmt.Sprintf("%s:%d", dc.Domain, dc.Count))
	}
	if len(counts) > 4 {
		parts = append(parts, fmt.Sprintf("+%d more", len(counts)-4))
	}
	return strings.Join(parts, ", ")
}

func resolveDiscordImagePath(dataDir, year, month string, img ImageEntry) string {
	relPath := img.FilePath
	if relPath == "" {
		relPath = filepath.ToSlash(filepath.Join(year, month, "messages", "discord", "images", img.ID))
	}
	return filepath.Join(dataDir, filepath.FromSlash(relPath))
}

func printImagesSyncHelp() {
	f := Fmt
	fmt.Printf(`
%schb images sync%s — Download images from Discord and public event covers to local data directory

%sUSAGE%s
  %schb images sync%s [year[/month]] [options]

%sDESCRIPTION%s
  By default, syncs latest generated image references plus the current month
  and previous month.
  With %s--history%s, also processes all historical months.

  Downloads images from two sources:

  %sDiscord attachments%s
    Reads images.json files (generated by 'chb generate') and fetches the
    original image files from Discord via the API. Images are saved to:
      data/YYYY/MM/channels/discord/images/{attachmentId}.{ext}

  %sPublic event covers%s
    Reads events.json files, downloads cover images from their source domains,
    and saves them to:
      data/YYYY/MM/events/images/{eventId}.{ext}

  Existing files are skipped unless --force is used.

%sOPTIONS%s
  %s<year>%s               Process all months of the given year (e.g. 2025)
  %s<year/month>%s         Process a specific month (e.g. 2025/11)
  %s--since%s <YYYY/MM>    Process from a specific month to now
  %s--history%s            Process all available months
  %s--force%s              Re-download even if files already exist
  %s--help, -h%s           Show this help

%sENVIRONMENT%s
  %sDISCORD_BOT_TOKEN%s    Required for Discord image downloads

%sEXAMPLES%s
  %schb images sync%s                   Reconcile latest + previous/current month images
  %schb images sync --history%s         Download all historical images
  %schb images sync 2025/03%s           Download images for March 2025
  %schb images sync --force%s           Re-download all images
`,
		f.Bold, f.Reset,
		f.Bold, f.Reset,
		f.Cyan, f.Reset,
		f.Bold, f.Reset,
		f.Yellow, f.Reset,
		f.Bold, f.Reset,
		f.Bold, f.Reset,
		f.Bold, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Bold, f.Reset,
		f.Yellow, f.Reset,
		f.Bold, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
	)
}
