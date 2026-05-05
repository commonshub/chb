package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"
)

var (
	diagnosticsMu       sync.Mutex
	diagnosticsFile     *os.File
	diagnosticsPath     string
	diagnosticsWarnings int
	diagnosticsErrors   int
	ansiPattern         = regexp.MustCompile(`\x1b\[[0-9;]*m`)
)

func Warnf(format string, args ...interface{}) {
	writeDiagnostic("warning", true, format, args...)
}

func LogWarningf(format string, args ...interface{}) {
	writeDiagnostic("warning", false, format, args...)
}

func Errorf(format string, args ...interface{}) {
	writeDiagnostic("error", true, format, args...)
}

func LogErrorf(format string, args ...interface{}) {
	writeDiagnostic("error", false, format, args...)
}

func DiagnosticsLogPath() string {
	diagnosticsMu.Lock()
	defer diagnosticsMu.Unlock()
	return diagnosticsPath
}

func DiagnosticsSummary() string {
	diagnosticsMu.Lock()
	defer diagnosticsMu.Unlock()
	if diagnosticsWarnings == 0 && diagnosticsErrors == 0 {
		return ""
	}
	return fmt.Sprintf("%s and %s, written in %s", pluralCount(diagnosticsErrors, "error"), pluralCount(diagnosticsWarnings, "warning"), diagnosticsPath)
}

func PrintDiagnosticsSummary() {
	summary := DiagnosticsSummary()
	if summary == "" {
		return
	}
	fmt.Fprintf(os.Stderr, "%s%s%s\n", Fmt.Dim, summary, Fmt.Reset)
}

func Fatalf(format string, args ...interface{}) {
	Errorf(format, args...)
	ExitWithDiagnostics(1)
}

func ExitWithDiagnostics(code int) {
	PrintDiagnosticsSummary()
	CloseDiagnosticsLog()
	os.Exit(code)
}

func pluralCount(n int, singular string) string {
	if n == 1 {
		return fmt.Sprintf("%d %s", n, singular)
	}
	return fmt.Sprintf("%d %ss", n, singular)
}

func CloseDiagnosticsLog() {
	diagnosticsMu.Lock()
	defer diagnosticsMu.Unlock()
	if diagnosticsFile != nil {
		_ = diagnosticsFile.Close()
		diagnosticsFile = nil
	}
}

func writeDiagnostic(level string, echo bool, format string, args ...interface{}) {
	message := fmt.Sprintf(format, args...)
	message = strings.TrimRight(message, "\n")

	diagnosticsMu.Lock()
	defer diagnosticsMu.Unlock()

	switch level {
	case "warning":
		diagnosticsWarnings++
	case "error":
		diagnosticsErrors++
	}

	if echo {
		fmt.Fprintf(os.Stderr, "%s\n", message)
	}

	if diagnosticsFile == nil {
		cwd, err := os.Getwd()
		if err != nil {
			cwd = "."
		}
		diagnosticsPath = filepath.Join(cwd, time.Now().Format("20060102-1504")+".log")
		diagnosticsFile, _ = os.OpenFile(diagnosticsPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	}
	if diagnosticsFile == nil {
		return
	}

	clean := ansiPattern.ReplaceAllString(message, "")
	_, _ = fmt.Fprintf(diagnosticsFile, "%s [%s] %s\n", time.Now().Format(time.RFC3339), strings.ToUpper(level), clean)
}
