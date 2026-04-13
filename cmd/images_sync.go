package cmd

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// ImagesSync downloads images from Discord messages and Luma event covers
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
		fmt.Printf("%s⚠ DISCORD_BOT_TOKEN not set — skipping Discord image downloads%s\n", Fmt.Yellow, Fmt.Reset)
	}

	// Determine time range
	startMonth, _ := ResolveSinceMonth(args, "messages")
	posYear, posMonth, posFound := ParseYearMonthArg(args)

	years := getAvailableYears(dataDir)
	if len(years) == 0 {
		fmt.Println("⚠️  No data found. Run sync first.")
		return 0, nil
	}

	totalDiscord := 0
	totalLuma := 0
	skippedDiscord := 0
	skippedLuma := 0

	for _, year := range years {
		months := getAvailableMonths(dataDir, year)
		for _, month := range months {
			ym := fmt.Sprintf("%s-%s", year, month)

			// Filter by positional year/month
			if posFound {
				if posMonth != "" && (year != posYear || month != posMonth) {
					continue
				}
				if posMonth == "" && year != posYear {
					continue
				}
			}

			// Filter by --since / --history
			if startMonth != "" && ym < startMonth {
				continue
			}

			// Discord images
			if discordToken != "" {
				d, s := syncDiscordImages(dataDir, year, month, discordToken, force)
				totalDiscord += d
				skippedDiscord += s
			}

			// Luma event cover images
			l, s := syncLumaImages(dataDir, year, month, force)
			totalLuma += l
			skippedLuma += s
		}
	}

	fmt.Printf("\n%s✅ Images sync complete%s\n", Fmt.Green, Fmt.Reset)
	if totalDiscord > 0 || skippedDiscord > 0 {
		fmt.Printf("  Discord: %d downloaded, %d skipped\n", totalDiscord, skippedDiscord)
	}
	if totalLuma > 0 || skippedLuma > 0 {
		fmt.Printf("  Luma:    %d downloaded, %d skipped\n", totalLuma, skippedLuma)
	}
	if totalDiscord == 0 && totalLuma == 0 && skippedDiscord == 0 && skippedLuma == 0 {
		fmt.Printf("  No images found to sync\n")
	}
	fmt.Println()

	return totalDiscord + totalLuma, nil
}

// syncDiscordImages reads images.json for a month and downloads Discord attachments.
func syncDiscordImages(dataDir, year, month, token string, force bool) (downloaded, skipped int) {
	imagesPath := filepath.Join(dataDir, year, month, "generated", "images.json")
	data, err := os.ReadFile(imagesPath)
	if err != nil {
		return 0, 0
	}

	var imf ImagesFile
	if json.Unmarshal(data, &imf) != nil || len(imf.Images) == 0 {
		return 0, 0
	}

	imagesDir := filepath.Join(dataDir, year, month, "messages", "discord", "images")
	os.MkdirAll(imagesDir, 0755)

	for _, img := range imf.Images {
		if img.ID == "" || img.ChannelID == "" || img.MessageID == "" {
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

		outPath := filepath.Join(imagesDir, img.ID+ext)
		if err := downloadFile(attachmentURL, outPath); err != nil {
			fmt.Printf("  %s⚠ Failed to download %s: %v%s\n", Fmt.Yellow, img.ID, err, Fmt.Reset)
			continue
		}

		downloaded++
		time.Sleep(200 * time.Millisecond) // rate limit
	}

	if downloaded > 0 {
		fmt.Printf("  ✓ %s-%s discord: %d downloaded\n", year, month, downloaded)
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

// syncLumaImages reads events.json for a month and downloads event cover images.
func syncLumaImages(dataDir, year, month string, force bool) (downloaded, skipped int) {
	eventsPath := filepath.Join(dataDir, year, month, "generated", "events.json")
	data, err := os.ReadFile(eventsPath)
	if err != nil {
		return 0, 0
	}

	var eventsFile FullEventsFile
	if json.Unmarshal(data, &eventsFile) != nil || len(eventsFile.Events) == 0 {
		return 0, 0
	}

	imagesDir := filepath.Join(dataDir, year, month, "events", "images")
	os.MkdirAll(imagesDir, 0755)

	for _, evt := range eventsFile.Events {
		if evt.CoverImage == "" || evt.ID == "" {
			continue
		}

		ext := extFromURL(evt.CoverImage, ".jpg")

		outPath := filepath.Join(imagesDir, evt.ID+ext)
		if !force && fileExists(outPath) {
			skipped++
			continue
		}

		if err := downloadFile(evt.CoverImage, outPath); err != nil {
			fmt.Printf("  %s⚠ Failed to download cover for %s: %v%s\n", Fmt.Yellow, evt.ID, err, Fmt.Reset)
			continue
		}

		downloaded++
		time.Sleep(200 * time.Millisecond)
	}

	if downloaded > 0 {
		fmt.Printf("  ✓ %s-%s luma covers: %d downloaded\n", year, month, downloaded)
	}

	return downloaded, skipped
}

// downloadFile downloads a URL to a local file path.
func downloadFile(url, destPath string) error {
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}

	out, err := os.Create(destPath)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, resp.Body)
	return err
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

func printImagesSyncHelp() {
	f := Fmt
	fmt.Printf(`
%schb images sync%s — Download images from Discord and Luma to local data directory

%sUSAGE%s
  %schb images sync%s [year[/month]] [options]

%sDESCRIPTION%s
  Downloads images from two sources:

  %sDiscord attachments%s
    Reads images.json files (generated by 'chb generate') and fetches the
    original image files from Discord via the API. Images are saved to:
      data/YYYY/MM/channels/discord/images/{attachmentId}.{ext}

  %sLuma event covers%s
    Reads events.json files and downloads event cover images. Saved to:
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
  %schb images sync%s                   Download images for current month
  %schb images sync --history%s         Download all historical images
  %schb images sync 2025/03%s           Download images for March 2025
  %schb images sync --force%s           Re-download all images
`,
		f.Bold, f.Reset,
		f.Bold, f.Reset,
		f.Cyan, f.Reset,
		f.Bold, f.Reset,
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
