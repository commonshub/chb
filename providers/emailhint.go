package providers

import (
	"strings"
	"unicode/utf8"
)

// EmailHint masks an email so a member can recognise which address a
// membership uses without it being revealed: the first character of the
// local part, "…", then "@" and the full domain ("x…@gmail.com"). A
// one-character local part shows nothing of it ("…@gmail.com"). "" when
// the address has no local part or no domain.
func EmailHint(email string) string {
	email = strings.ToLower(strings.TrimSpace(email))
	at := strings.LastIndex(email, "@")
	if at <= 0 || at == len(email)-1 {
		return ""
	}
	local, domain := email[:at], email[at+1:]
	first, size := utf8.DecodeRuneInString(local)
	if size == len(local) {
		return "…@" + domain
	}
	return string(first) + "…@" + domain
}
