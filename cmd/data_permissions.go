package cmd

import (
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
// written outside /private/ and monthly /sources/ archives. Non-JSON files,
// private paths, and source dumps are returned as-is.
func enforcePIIPolicy(path string, data []byte) []byte {
	if !strings.HasSuffix(path, ".json") {
		return data
	}
	if pathHasPrivateSegment(path) || pathHasSourcesSegment(path) {
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
			if part == "private" || sourcesStartAt(baseDir, current) {
				privateMode = true
			}
			mode := dataPublicDirMode
			if privateMode {
				mode = dataPrivateDirMode
			}
			if err := os.Chmod(current, mode); err != nil && !os.IsNotExist(err) {
				return err
			}
		}
	}

	if !isDir {
		if err := os.Chmod(targetPath, dataFileMode); err != nil && !os.IsNotExist(err) {
			return err
		}
	}

	return nil
}

func sourcesStartAt(baseDir, currentPath string) bool {
	rel, err := filepath.Rel(baseDir, currentPath)
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
