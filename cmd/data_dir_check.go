package cmd

import (
	"fmt"
	"os"
	"path/filepath"
)

// EnsureWritableDataDir verifies that DATA_DIR can be created (if needed) and written to.
// It returns the resolved directory path or an error with context about why the preflight failed.
func EnsureWritableDataDir() (string, error) {
	dataDir := filepath.Clean(resolveDataDir())

	if info, err := os.Stat(dataDir); err == nil && !info.IsDir() {
		return dataDir, fmt.Errorf("DATA_DIR %s is not a directory (%s)", dataDir, info.Mode().Type())
	}

	if err := os.MkdirAll(dataDir, dataRootDirMode); err != nil {
		return dataDir, fmt.Errorf("cannot create DATA_DIR %s: %w%s", dataDir, err, dataDirAccessContext(filepath.Dir(dataDir)))
	}

	testFile, err := os.CreateTemp(dataDir, ".chb-write-test-*")
	if err != nil {
		return dataDir, fmt.Errorf("cannot write to DATA_DIR %s: %w%s", dataDir, err, dataDirAccessContext(dataDir))
	}

	testPath := testFile.Name()
	if _, err := testFile.WriteString("ok\n"); err != nil {
		testFile.Close()
		_ = os.Remove(testPath)
		return dataDir, fmt.Errorf("cannot write to DATA_DIR %s: %w%s", dataDir, err, dataDirAccessContext(dataDir))
	}
	if err := testFile.Close(); err != nil {
		_ = os.Remove(testPath)
		return dataDir, fmt.Errorf("cannot finalize write test in DATA_DIR %s: %w%s", dataDir, err, dataDirAccessContext(dataDir))
	}
	if err := os.Remove(testPath); err != nil {
		return dataDir, fmt.Errorf("cannot clean up write test file in DATA_DIR %s: %w%s", dataDir, err, dataDirAccessContext(dataDir))
	}

	return dataDir, nil
}

func dataDirAccessContext(path string) string {
	info, err := os.Stat(path)
	if err != nil {
		return ""
	}
	return fmt.Sprintf(" (path %s exists with mode %o)", path, info.Mode().Perm())
}
