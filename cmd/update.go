package cmd

import (
	"archive/tar"
	"bufio"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"runtime"
	"strings"
	"time"
)

// ghRelease holds the fields we need from the GitHub releases API.
type ghRelease struct {
	TagName     string `json:"tag_name"`
	Name        string `json:"name"`
	PublishedAt string `json:"published_at"`
	Body        string `json:"body"`
}

// Update checks for the latest GitHub release and replaces the binary.
// If yes is true, skips the confirmation prompt.
func Update(yes bool) error {
	f := Fmt

	// Show current version
	fmt.Printf("%sCurrent version:%s\n", f.Cyan, f.Reset)
	fmt.Printf("  Version: %s\n", Version)
	if CommitSHA != "" {
		short := CommitSHA
		if len(short) > 7 {
			short = short[:7]
		}
		fmt.Printf("  Commit:  %s\n", short)
	}
	if CommitDate != "" {
		fmt.Printf("  Date:    %s\n", CommitDate)
	}

	// Fetch latest release
	fmt.Printf("\n%sChecking for updates...%s\n", f.Dim, f.Reset)
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get("https://api.github.com/repos/CommonsHub/chb/releases/latest")
	if err != nil {
		return fmt.Errorf("failed to check for updates: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return fmt.Errorf("GitHub API returned %d", resp.StatusCode)
	}

	var latest ghRelease
	if err := json.NewDecoder(resp.Body).Decode(&latest); err != nil {
		return fmt.Errorf("failed to parse release info: %w", err)
	}

	latestVersion := strings.TrimPrefix(latest.TagName, "v")
	currentVersion := strings.TrimPrefix(Version, "v")

	if latestVersion == currentVersion {
		fmt.Printf("\n%s✓ You're up to date.%s\n", f.Green, f.Reset)
		return nil
	}

	// Determine OS and arch
	goos := runtime.GOOS
	goarch := runtime.GOARCH
	tarball := fmt.Sprintf("chb_%s_%s.tar.gz", goos, goarch)
	downloadURL := fmt.Sprintf("https://github.com/CommonsHub/chb/releases/download/%s/%s", latest.TagName, tarball)

	// HEAD request to get size
	var sizeMB float64
	headResp, headErr := client.Head(downloadURL)
	if headErr == nil && headResp.StatusCode == 200 && headResp.ContentLength > 0 {
		sizeMB = float64(headResp.ContentLength) / (1024 * 1024)
	}
	if headResp != nil {
		headResp.Body.Close()
	}

	// Show latest version
	fmt.Printf("\n%sLatest version:%s\n", f.Cyan, f.Reset)
	fmt.Printf("  Version: %s\n", latest.TagName)
	if latest.PublishedAt != "" {
		fmt.Printf("  Date:    %s\n", latest.PublishedAt)
	}
	if latest.Name != "" {
		fmt.Printf("  Name:    %s\n", latest.Name)
	}
	fmt.Printf("  File:    %s\n", tarball)
	if sizeMB > 0 {
		fmt.Printf("  Size:    %.1f MB\n", sizeMB)
	}

	if !yes {
		fmt.Printf("\n%sUpdate to %s? [Y/n]%s ", f.Yellow, latest.TagName, f.Reset)
		reader := bufio.NewReader(os.Stdin)
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(strings.ToLower(input))
		if input != "" && input != "y" && input != "yes" {
			fmt.Println("Update cancelled.")
			return nil
		}
	}

	fmt.Printf("\n%sDownloading...%s\n", f.Dim, f.Reset)

	resp2, err := client.Get(downloadURL)
	if err != nil {
		return fmt.Errorf("download failed: %w", err)
	}
	defer resp2.Body.Close()

	if resp2.StatusCode != 200 {
		return fmt.Errorf("download failed: HTTP %d (no binary for %s/%s?)", resp2.StatusCode, goos, goarch)
	}

	// Extract the binary from the tarball
	gr, err := gzip.NewReader(resp2.Body)
	if err != nil {
		return fmt.Errorf("failed to decompress: %w", err)
	}
	defer gr.Close()

	newBinary, err := extractTarBinary(gr, "chb")
	if err != nil {
		return fmt.Errorf("failed to extract binary: %w", err)
	}

	// Find where the current binary lives
	execPath, err := os.Executable()
	if err != nil {
		return fmt.Errorf("cannot determine executable path: %w", err)
	}
	execPath, err = resolveSymlinks(execPath)
	if err != nil {
		return fmt.Errorf("cannot resolve executable path: %w", err)
	}

	// Write to a temp file next to the binary, then rename (atomic on same FS)
	tmpPath := execPath + ".tmp"
	if err := os.WriteFile(tmpPath, newBinary, 0755); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("cannot write to %s (try running with sudo): %w", execPath, err)
	}

	if err := os.Rename(tmpPath, execPath); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("cannot replace binary: %w", err)
	}

	fmt.Printf("\n%s✓ Updated to %s%s\n", f.Green, latest.TagName, f.Reset)

	// Refresh settings from GitHub
	fmt.Printf("\n%sRefreshing settings...%s\n", f.Dim, f.Reset)
	if err := DownloadSettings(chbDir()); err != nil {
		fmt.Printf("%sCould not refresh settings:%s %v\n", f.Yellow, f.Reset, err)
	}

	return nil
}

// extractTarBinary reads a tar stream and returns the contents of the named file.
func extractTarBinary(r io.Reader, name string) ([]byte, error) {
	tr := tar.NewReader(r)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if hdr.Name == name || strings.HasSuffix(hdr.Name, "/"+name) {
			return io.ReadAll(tr)
		}
	}
	return nil, fmt.Errorf("binary %q not found in archive", name)
}

// resolveSymlinks resolves a path through symlinks to the real file.
func resolveSymlinks(path string) (string, error) {
	resolved, err := os.Readlink(path)
	if err != nil {
		// Not a symlink
		return path, nil
	}
	if !strings.HasPrefix(resolved, "/") {
		dir := path[:strings.LastIndex(path, "/")+1]
		resolved = dir + resolved
	}
	return resolveSymlinks(resolved)
}
