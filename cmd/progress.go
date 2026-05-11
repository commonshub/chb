package cmd

import (
	"fmt"
	"os"
)

type statusLine struct {
	active bool
	tty    bool
}

func newStatusLine() *statusLine {
	info, err := os.Stdout.Stat()
	return &statusLine{tty: err == nil && (info.Mode()&os.ModeCharDevice) != 0}
}

func (s *statusLine) Update(format string, args ...interface{}) {
	if s == nil || !s.tty {
		return
	}
	fmt.Printf("\r\033[K  %s", fmt.Sprintf(format, args...))
	s.active = true
}

func (s *statusLine) Clear() {
	if s == nil || !s.active || !s.tty {
		return
	}
	fmt.Print("\r\033[K")
	s.active = false
}
