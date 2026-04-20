package cmd

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSelectReleaseAssetMatchesVersionedLinuxAMD64(t *testing.T) {
	release := ghRelease{
		TagName: "v2.3.5",
		Assets: []ghAsset{
			{Name: "checksums.txt", BrowserDownloadURL: "https://example.com/checksums.txt"},
			{Name: "chb_2.3.5_linux_amd64.tar.gz", BrowserDownloadURL: "https://example.com/chb_2.3.5_linux_amd64.tar.gz"},
			{Name: "chb_2.3.5_linux_arm64.tar.gz", BrowserDownloadURL: "https://example.com/chb_2.3.5_linux_arm64.tar.gz"},
		},
	}

	asset, ok := selectReleaseAsset(release, "linux", "amd64")
	if !ok {
		t.Fatal("expected linux/amd64 asset to be found")
	}
	if asset.Name != "chb_2.3.5_linux_amd64.tar.gz" {
		t.Fatalf("unexpected asset: %q", asset.Name)
	}
}

func TestSelectReleaseAssetFallsBackToUnversionedName(t *testing.T) {
	release := ghRelease{
		TagName: "v2.3.5",
		Assets: []ghAsset{
			{Name: "chb_linux_amd64.tar.gz", BrowserDownloadURL: "https://example.com/chb_linux_amd64.tar.gz"},
		},
	}

	asset, ok := selectReleaseAsset(release, "linux", "amd64")
	if !ok {
		t.Fatal("expected fallback linux/amd64 asset to be found")
	}
	if asset.Name != "chb_linux_amd64.tar.gz" {
		t.Fatalf("unexpected fallback asset: %q", asset.Name)
	}
}

func TestReleaseAssetBinaryName(t *testing.T) {
	got := releaseAssetBinaryName("chb_2.3.5_linux_amd64.tar.gz")
	if got != "chb_2.3.5_linux_amd64" {
		t.Fatalf("unexpected binary name: %q", got)
	}
}

func TestCurrentBinaryPathPrefersLookPathForBareCommand(t *testing.T) {
	tmpDir := t.TempDir()
	binDir := filepath.Join(tmpDir, "bin")
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		t.Fatalf("mkdir bin: %v", err)
	}

	chbPath := filepath.Join(binDir, "chb")
	if err := os.WriteFile(chbPath, []byte("binary"), 0o755); err != nil {
		t.Fatalf("write chb: %v", err)
	}

	origPath := os.Getenv("PATH")
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+origPath)

	got, err := currentBinaryPath([]string{"chb"})
	if err != nil {
		t.Fatalf("currentBinaryPath returned error: %v", err)
	}
	if got != chbPath {
		t.Fatalf("expected %q, got %q", chbPath, got)
	}
}

func TestCurrentBinaryPathResolvesRelativeArgv0(t *testing.T) {
	tmpDir := t.TempDir()
	origWD, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	if err := os.Chdir(tmpDir); err != nil {
		t.Fatalf("chdir tmp: %v", err)
	}
	t.Cleanup(func() {
		_ = os.Chdir(origWD)
	})

	relPath := filepath.Join("bin", "chb")
	if err := os.MkdirAll("bin", 0o755); err != nil {
		t.Fatalf("mkdir rel bin: %v", err)
	}
	if err := os.WriteFile(relPath, []byte("binary"), 0o755); err != nil {
		t.Fatalf("write relative chb: %v", err)
	}

	got, err := currentBinaryPath([]string{"./" + relPath})
	if err != nil {
		t.Fatalf("currentBinaryPath returned error: %v", err)
	}

	want := filepath.Join(tmpDir, relPath)
	if got != want {
		t.Fatalf("expected %q, got %q", want, got)
	}
}
