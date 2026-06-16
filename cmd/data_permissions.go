package cmd

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

const (
	dataRootDirMode    = os.FileMode(0755)
	dataPublicDirMode  = os.FileMode(0755)
	dataPrivateDirMode = os.FileMode(0700)
	dataFileMode       = os.FileMode(0644)
)

var normalizedDataDirs sync.Map

func ensureManagedDataDir(dir string) string {
	cleanDir := filepath.Clean(dir)
	_ = os.MkdirAll(cleanDir, dataRootDirMode)
	normalizeDataDir(cleanDir)
	return cleanDir
}

func normalizeDataDir(baseDir string) {
	baseDir = filepath.Clean(baseDir)
	if _, loaded := normalizedDataDirs.LoadOrStore(baseDir, struct{}{}); loaded {
		return
	}

	migrateLegacySourceArchives(baseDir)
	migrateLegacyRootGenerated(baseDir)
	migrateLegacySourcePathReferences(baseDir)
	_ = applyDataPathPolicy(baseDir, baseDir, true)
	_ = filepath.Walk(baseDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info == nil {
			return nil
		}
		_ = applyDataPathPolicy(baseDir, path, info.IsDir())
		return nil
	})
}

func mkdirAllManagedData(path string) error {
	baseDir, ok := dataBaseForPath(path)
	if !ok {
		return os.MkdirAll(path, dataPublicDirMode)
	}
	if err := os.MkdirAll(path, dataPublicDirMode); err != nil {
		return err
	}
	return applyDataPathPolicy(baseDir, path, true)
}

func writeDataFile(path string, data []byte) error {
	data = enforcePIIPolicy(path, data)

	baseDir, ok := dataBaseForPath(path)
	if !ok {
		if err := os.MkdirAll(filepath.Dir(path), dataPublicDirMode); err != nil {
			return err
		}
		return os.WriteFile(path, data, dataFileMode)
	}
	if err := mkdirAllManagedData(filepath.Dir(path)); err != nil {
		return err
	}
	if err := os.WriteFile(path, data, dataFileMode); err != nil {
		return err
	}
	return applyDataPathPolicy(baseDir, path, false)
}

// enforcePIIPolicy scrubs name fields and warns about emails for JSON files
// written outside /private/ and monthly /providers/ archives. Non-JSON files,
// private paths, and provider archives are returned as-is.
func enforcePIIPolicy(path string, data []byte) []byte {
	if !strings.HasSuffix(path, ".json") {
		return data
	}
	if pathHasPrivateSegment(path) || pathHasProviderArchiveSegment(path) {
		return data
	}
	cleaned, scrubbed := scrubNameFields(data)
	for _, leak := range scrubbed {
		Warnf("⚠ PII guard: scrubbed %s in %s (%s)", leak.Kind, path, leak.String())
	}
	_, soft := validatePublicJSON(cleaned)
	for _, leak := range soft {
		Warnf("⚠ PII guard: possible email in %s — %s", path, leak.String())
	}
	return cleaned
}

func dataBaseForPath(path string) (string, bool) {
	baseDir := filepath.Clean(DataDir())
	targetPath := filepath.Clean(path)
	rel, err := filepath.Rel(baseDir, targetPath)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
		return "", false
	}
	return baseDir, true
}

func applyDataPathPolicy(baseDir, targetPath string, isDir bool) error {
	baseDir = filepath.Clean(baseDir)
	targetPath = filepath.Clean(targetPath)

	rel, err := filepath.Rel(baseDir, targetPath)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
		return nil
	}
	if rel == "." {
		return nil
	}

	dirRel := rel
	if !isDir {
		dirRel = filepath.Dir(rel)
	}

	if dirRel != "." {
		current := baseDir
		privateMode := false
		for _, part := range strings.Split(dirRel, string(os.PathSeparator)) {
			if part == "" || part == "." {
				continue
			}
			current = filepath.Join(current, part)
			if part == "private" || providerArchivesStartAt(baseDir, current) {
				privateMode = true
			}
			mode := dataPublicDirMode
			if privateMode {
				mode = dataPrivateDirMode
			}
			if err := os.Chmod(current, mode); err != nil && !os.IsNotExist(err) && !os.IsPermission(err) {
				return err
			}
		}
	}

	if !isDir {
		if err := os.Chmod(targetPath, dataFileMode); err != nil && !os.IsNotExist(err) && !os.IsPermission(err) {
			return err
		}
	}

	return nil
}

func providerArchivesStartAt(baseDir, currentPath string) bool {
	rel, err := filepath.Rel(baseDir, currentPath)
	if err != nil || rel == "." {
		return false
	}
	parts := strings.Split(rel, string(os.PathSeparator))
	if len(parts) == 3 && isYearSegment(parts[0]) && isMonthSegment(parts[1]) && parts[2] == "providers" {
		return true
	}
	if len(parts) == 2 && parts[0] == "latest" && parts[1] == "providers" {
		return true
	}
	return false
}

func migrateLegacySourceArchives(baseDir string) {
	var archives []string
	_ = filepath.WalkDir(baseDir, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry == nil || !entry.IsDir() || entry.Name() != "sources" {
			return nil
		}
		if legacySourcesArchiveDir(baseDir, path) {
			archives = append(archives, path)
			return filepath.SkipDir
		}
		return nil
	})
	for _, srcDir := range archives {
		migrateLegacySourceArchive(srcDir)
	}
}

func legacySourcesArchiveDir(baseDir, path string) bool {
	rel, err := filepath.Rel(baseDir, path)
	if err != nil || rel == "." {
		return false
	}
	parts := strings.Split(rel, string(os.PathSeparator))
	if len(parts) == 3 && isYearSegment(parts[0]) && isMonthSegment(parts[1]) && parts[2] == "sources" {
		return true
	}
	if len(parts) == 2 && parts[0] == "latest" && parts[1] == "sources" {
		return true
	}
	return false
}

func migrateLegacySourceArchive(srcDir string) {
	dstDir := filepath.Join(filepath.Dir(srcDir), "providers")
	if _, err := os.Stat(dstDir); os.IsNotExist(err) {
		if err := os.Rename(srcDir, dstDir); err == nil {
			return
		}
	}
	if err := os.MkdirAll(dstDir, dataPrivateDirMode); err != nil {
		return
	}
	if mergeArchiveDirs(srcDir, dstDir) {
		_ = os.Remove(srcDir)
	}
}

func migrateLegacyRootGenerated(baseDir string) {
	srcDir := filepath.Join(baseDir, "generated")
	if _, err := os.Stat(srcDir); err != nil {
		return
	}
	dstDir := filepath.Join(baseDir, "latest", "generated")
	if _, err := os.Stat(dstDir); os.IsNotExist(err) {
		if err := os.Rename(srcDir, dstDir); err == nil {
			return
		}
	}
	if err := os.MkdirAll(dstDir, dataPublicDirMode); err != nil {
		return
	}
	if mergeArchiveDirs(srcDir, dstDir) {
		_ = os.Remove(srcDir)
	}
}

func mergeArchiveDirs(srcDir, dstDir string) bool {
	entries, err := os.ReadDir(srcDir)
	if err != nil {
		return false
	}
	allMoved := true
	for _, entry := range entries {
		src := filepath.Join(srcDir, entry.Name())
		dst := filepath.Join(dstDir, entry.Name())
		if _, err := os.Stat(dst); os.IsNotExist(err) {
			if err := os.Rename(src, dst); err != nil {
				allMoved = false
			}
			continue
		}
		if entry.IsDir() {
			if mergeArchiveDirs(src, dst) {
				_ = os.Remove(src)
			} else {
				allMoved = false
			}
			continue
		}
		if sameFileContent(src, dst) {
			_ = os.Remove(src)
			continue
		}
		allMoved = false
	}
	return allMoved
}

func sameFileContent(a, b string) bool {
	left, err := os.ReadFile(a)
	if err != nil {
		return false
	}
	right, err := os.ReadFile(b)
	if err != nil {
		return false
	}
	return bytes.Equal(left, right)
}

func migrateLegacySourcePathReferences(baseDir string) {
	_ = filepath.WalkDir(baseDir, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry == nil || entry.IsDir() {
			return nil
		}
		switch filepath.Ext(path) {
		case ".json", ".md", ".csv":
		default:
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil || !bytes.Contains(data, []byte("sources/")) {
			return nil
		}
		updated := bytes.ReplaceAll(data, []byte("sources/"), []byte("providers/"))
		if bytes.Equal(data, updated) {
			return nil
		}
		mode := dataFileMode
		if info, err := entry.Info(); err == nil {
			mode = info.Mode().Perm()
		}
		_ = os.WriteFile(path, updated, mode)
		return nil
	})
}
