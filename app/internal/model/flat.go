package model

import (
	"encoding/json"
	"strings"
	"unicode/utf16"
	"unicode/utf8"
)

// Art eines JSON-Werts in einem Record.
const (
	KString = 's'
	KNumber = 'n'
	KBool   = 'b'
	KNull   = 'z'
	KNested = 'o' // Objekt oder Array (wird nicht ausgewertet)
)

// Field ist ein Schlüssel/Wert-Paar einer Logzeile. V ist bei Strings der dekodierte
// Inhalt, sonst der Rohtext des Werts. K und V verweisen nach Möglichkeit in die
// Eingabezeile (keine Kopie).
type Field struct {
	K, V string
	Kind byte
}

// Record ist eine geparste Logzeile (flaches JSON-Objekt).
type Record []Field

// Get liefert den Wert zu einem Schlüssel (bei Duplikaten den letzten, wie encoding/json).
func (r Record) Get(k string) (Field, bool) {
	for i := len(r) - 1; i >= 0; i-- {
		if r[i].K == k {
			return r[i], true
		}
	}
	return Field{}, false
}

// ParseFlat zerlegt ein JSON-Objekt in seine Felder der obersten Ebene. Traefik
// schreibt flache Objekte; dafür ist dieser Parser um ein Vielfaches schneller als
// encoding/json mit map[string]any. dst wird wiederverwendet. ok=false: kein
// gültiges JSON-Objekt.
func ParseFlat(s string, dst Record) (rec Record, ok bool) {
	rec = dst[:0]
	i := skipWS(s, 0)
	if i >= len(s) || s[i] != '{' {
		return rec, false
	}
	i = skipWS(s, i+1)
	if i < len(s) && s[i] == '}' {
		return rec, skipWS(s, i+1) == len(s)
	}
	for {
		if i >= len(s) || s[i] != '"' {
			return rec, false
		}
		var f Field
		if f.K, i, ok = parseString(s, i); !ok {
			return rec, false
		}
		i = skipWS(s, i)
		if i >= len(s) || s[i] != ':' {
			return rec, false
		}
		i = skipWS(s, i+1)
		if i >= len(s) {
			return rec, false
		}
		switch c := s[i]; {
		case c == '"':
			f.Kind = KString
			if f.V, i, ok = parseString(s, i); !ok {
				return rec, false
			}
		case c == '{' || c == '[':
			end, ok := skipNested(s, i)
			if !ok || !json.Valid([]byte(s[i:end])) { // selten: verschachtelte Werte streng prüfen
				return rec, false
			}
			f.Kind, f.V, i = KNested, s[i:end], end
		default:
			end := i
			for end < len(s) && s[end] != ',' && s[end] != '}' && s[end] > ' ' {
				end++
			}
			f.V = s[i:end]
			switch {
			case f.V == "true" || f.V == "false":
				f.Kind = KBool
			case f.V == "null":
				f.Kind = KNull
			case validNumber(f.V):
				f.Kind = KNumber
			default:
				return rec, false
			}
			i = end
		}
		rec = append(rec, f)
		i = skipWS(s, i)
		if i >= len(s) {
			return rec, false
		}
		if s[i] == '}' {
			return rec, skipWS(s, i+1) == len(s)
		}
		if s[i] != ',' {
			return rec, false
		}
		i = skipWS(s, i+1)
	}
}

func skipWS(s string, i int) int {
	for i < len(s) && (s[i] == ' ' || s[i] == '\t' || s[i] == '\n' || s[i] == '\r') {
		i++
	}
	return i
}

// validNumber prüft die JSON-Zahlengrammatik: -?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?
func validNumber(v string) bool {
	i := 0
	if i < len(v) && v[i] == '-' {
		i++
	}
	digits := func() bool {
		start := i
		for i < len(v) && v[i] >= '0' && v[i] <= '9' {
			i++
		}
		return i > start
	}
	if i < len(v) && v[i] == '0' {
		i++
	} else if !digits() {
		return false
	}
	if i < len(v) && v[i] == '.' {
		i++
		if !digits() {
			return false
		}
	}
	if i < len(v) && (v[i] == 'e' || v[i] == 'E') {
		i++
		if i < len(v) && (v[i] == '+' || v[i] == '-') {
			i++
		}
		if !digits() {
			return false
		}
	}
	return i == len(v)
}

// parseString liest den String ab s[i] == '"' und liefert Inhalt und Position danach.
func parseString(s string, i int) (string, int, bool) {
	start := i + 1
	end := strings.IndexByte(s[start:], '"')
	if end < 0 {
		return "", 0, false
	}
	v := s[start : start+end]
	if esc := strings.IndexByte(v, '\\'); esc >= 0 {
		return unescape(s, start, start+esc) // selten; dort wird das echte Ende gesucht
	}
	if hasControl(v) {
		return "", 0, false
	}
	return v, start + end + 1, true
}

// hasControl meldet Bytes < 0x20 (in JSON-Strings unzulässig); prüft 8 Bytes je Schritt.
func hasControl(s string) bool {
	const lo, hi = 0x0101010101010101, 0x8080808080808080
	i := 0
	for ; i+8 <= len(s); i += 8 {
		x := uint64(s[i]) | uint64(s[i+1])<<8 | uint64(s[i+2])<<16 | uint64(s[i+3])<<24 |
			uint64(s[i+4])<<32 | uint64(s[i+5])<<40 | uint64(s[i+6])<<48 | uint64(s[i+7])<<56
		if (x-0x20*lo)&^x&hi != 0 {
			return true
		}
	}
	for ; i < len(s); i++ {
		if s[i] < 0x20 {
			return true
		}
	}
	return false
}

// unescape dekodiert einen String mit Escape-Sequenzen; s[esc] ist der erste Backslash.
func unescape(s string, start, esc int) (string, int, bool) {
	var b strings.Builder
	b.Grow(esc - start + 32)
	b.WriteString(s[start:esc])
	for j := esc; j < len(s); {
		c := s[j]
		switch {
		case c == '"':
			return b.String(), j + 1, true
		case c < 0x20:
			return "", 0, false
		case c != '\\':
			b.WriteByte(c)
			j++
			continue
		}
		if j+1 >= len(s) {
			return "", 0, false
		}
		switch s[j+1] {
		case '"', '\\', '/':
			b.WriteByte(s[j+1])
		case 'b':
			b.WriteByte('\b')
		case 'f':
			b.WriteByte('\f')
		case 'n':
			b.WriteByte('\n')
		case 'r':
			b.WriteByte('\r')
		case 't':
			b.WriteByte('\t')
		case 'u':
			r, ok := hex4(s, j+2)
			if !ok {
				return "", 0, false
			}
			j += 6
			if utf16.IsSurrogate(r) {
				// Gültiges Paar zusammensetzen, sonst U+FFFD (wie encoding/json).
				if r2, ok := hex4(s, j+2); ok && j+1 < len(s) && s[j] == '\\' && s[j+1] == 'u' {
					if dec := utf16.DecodeRune(r, r2); dec != utf8.RuneError {
						b.WriteRune(dec)
						j += 6
						continue
					}
				}
				r = utf8.RuneError
			}
			b.WriteRune(r)
			continue
		default:
			return "", 0, false
		}
		j += 2
	}
	return "", 0, false
}

func hex4(s string, i int) (rune, bool) {
	if i+4 > len(s) {
		return 0, false
	}
	var r rune
	for _, c := range []byte(s[i : i+4]) {
		switch {
		case c >= '0' && c <= '9':
			r = r<<4 | rune(c-'0')
		case c >= 'a' && c <= 'f':
			r = r<<4 | rune(c-'a'+10)
		case c >= 'A' && c <= 'F':
			r = r<<4 | rune(c-'A'+10)
		default:
			return 0, false
		}
	}
	return r, true
}

// skipNested überspringt ein Objekt/Array ab s[i] und liefert die Position danach.
func skipNested(s string, i int) (int, bool) {
	depth := 0
	for j := i; j < len(s); j++ {
		switch s[j] {
		case '{', '[':
			depth++
		case '}', ']':
			if depth--; depth == 0 {
				return j + 1, true
			}
		case '"':
			for j++; j < len(s) && s[j] != '"'; j++ {
				if s[j] == '\\' {
					j++
				}
			}
			if j >= len(s) {
				return 0, false
			}
		}
	}
	return 0, false
}
