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
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

// ghRelease holds the fields we need from the GitHub releases API.
type ghRelease struct {
	TagName     string    `json:"tag_name"`
	Name        string    `json:"name"`
	PublishedAt string    `json:"published_at"`
	Body        string    `json:"body"`
	Assets      []ghAsset `json:"assets"`
}

type ghAsset struct {
	Name               string `json:"name"`
	BrowserDownloadURL string `json:"browser_download_url"`
	Size               int64  `json:"size"`
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

	if resp.StatusCode == 404 {
		return fmt.Errorf("no GitHub releases found; chb update only installs published release binaries")
	}
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
	asset, hasBinary := selectReleaseAsset(latest, goos, goarch)
	var sizeMB float64
	if hasBinary && asset.Size > 0 {
		sizeMB = float64(asset.Size) / (1024 * 1024)
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

	if !hasBinary {
		return fmt.Errorf("no GitHub release binary available for %s/%s in %s; published assets: %s", goos, goarch, latest.TagName, strings.Join(releaseAssetNames(latest.Assets), ", "))
	}

	fmt.Printf("  File:    %s\n", asset.Name)
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

	resp2, err := client.Get(asset.BrowserDownloadURL)
	if err != nil {
		return fmt.Errorf("download failed: %w", err)
	}
	defer resp2.Body.Close()

	if resp2.StatusCode != 200 {
		return fmt.Errorf("download failed: HTTP %d", resp2.StatusCode)
	}

	// Extract the binary from the tarball
	gr, err := gzip.NewReader(resp2.Body)
	if err != nil {
		return fmt.Errorf("failed to decompress: %w", err)
	}
	defer gr.Close()

	newBinary, err := extractTarBinary(gr, releaseAssetBinaryName(asset.Name))
	if err != nil {
		return fmt.Errorf("failed to extract binary: %w", err)
	}

	// Find where the current binary lives.
	// In musl-based containers os.Executable may resolve through /proc/self/exe
	// to the loader, so prefer argv[0]/PATH when available.
	execPath, err := currentBinaryPath(os.Args)
	if err != nil {
		return fmt.Errorf("cannot determine executable path: %w", err)
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
	refreshSettings()
	return nil
}

func refreshSettings() {
	f := Fmt
	fmt.Printf("\n%sRefreshing settings...%s\n", f.Dim, f.Reset)
	if err := DownloadSettings(chbDir()); err != nil {
		fmt.Printf("%sCould not refresh settings:%s %v\n", f.Yellow, f.Reset, err)
	}
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
		if hdr.Name == name || strings.HasSuffix(hdr.Name, "/"+name) || hdr.Name == "chb" || strings.HasSuffix(hdr.Name, "/chb") {
			return io.ReadAll(tr)
		}
	}
	return nil, fmt.Errorf("binary %q not found in archive", name)
}

func selectReleaseAsset(release ghRelease, goos, goarch string) (ghAsset, bool) {
	version := normalizeVersion(release.TagName)
	wantNames := []string{
		fmt.Sprintf("chb_%s_%s_%s.tar.gz", version, goos, goarch),
		fmt.Sprintf("chb_%s_%s.tar.gz", goos, goarch),
	}

	for _, want := range wantNames {
		for _, asset := range release.Assets {
			if asset.Name == want && asset.BrowserDownloadURL != "" {
				return asset, true
			}
		}
	}

	suffix := fmt.Sprintf("_%s_%s.tar.gz", goos, goarch)
	for _, asset := range release.Assets {
		if strings.HasPrefix(asset.Name, "chb_") && strings.HasSuffix(asset.Name, suffix) && asset.BrowserDownloadURL != "" {
			return asset, true
		}
	}

	return ghAsset{}, false
}

func releaseAssetBinaryName(assetName string) string {
	return strings.TrimSuffix(assetName, ".tar.gz")
}

func releaseAssetNames(assets []ghAsset) []string {
	names := make([]string, 0, len(assets))
	for _, asset := range assets {
		if asset.Name != "" {
			names = append(names, asset.Name)
		}
	}
	if len(names) == 0 {
		return []string{"(none)"}
	}
	return names
}

func currentBinaryPath(args []string) (string, error) {
	if len(args) > 0 && args[0] != "" {
		candidate := args[0]
		if strings.ContainsRune(candidate, os.PathSeparator) {
			if !filepath.IsAbs(candidate) {
				absPath, err := filepath.Abs(candidate)
				if err == nil {
					candidate = absPath
				}
			}
			if resolved, err := resolveSymlinks(candidate); err == nil {
				return resolved, nil
			}
			return candidate, nil
		}
		if lookedUp, err := exec.LookPath(candidate); err == nil {
			if resolved, err := resolveSymlinks(lookedUp); err == nil {
				return resolved, nil
			}
			return lookedUp, nil
		}
	}

	execPath, err := os.Executable()
	if err != nil {
		return "", err
	}
	execPath, err = resolveSymlinks(execPath)
	if err != nil {
		return "", err
	}
	return execPath, nil
}

// resolveSymlinks resolves a path through symlinks to the real file.
func resolveSymlinks(path string) (string, error) {
	resolved, err := os.Readlink(path)
	if err != nil {
		return path, nil
	}
	if !strings.HasPrefix(resolved, "/") {
		dir := path[:strings.LastIndex(path, "/")+1]
		resolved = dir + resolved
	}
	return resolveSymlinks(resolved)
}
