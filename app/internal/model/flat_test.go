package model

import (
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"strings"
	"testing"
)

// Referenz: encoding/json. Der schnelle Parser muss dieselben Werte liefern und darf
// nichts akzeptieren, was kein gültiges JSON ist.
func checkAgainstJSON(t *testing.T, in string) {
	t.Helper()
	rec, ok := ParseFlat(in, nil)
	valid := json.Valid([]byte(in)) && strings.HasPrefix(strings.TrimSpace(in), "{")
	if ok != valid && (ok || strings.ToValidUTF8(in, "") == in) {
		t.Fatalf("ParseFlat ok=%v, encoding/json gültig=%v: %q", ok, valid, in)
	}
	var ref map[string]any
	if !ok || json.Unmarshal([]byte(in), &ref) != nil { // z. B. Zahl außerhalb float64
		return
	}
	got := map[string]Field{}
	for _, f := range rec {
		got[f.K] = f
	}
	if len(got) != len(ref) {
		t.Fatalf("Feldanzahl %d, erwartet %d: %q", len(got), len(ref), in)
	}
	for k, want := range ref {
		f, found := got[k]
		if !found {
			t.Fatalf("Feld %q fehlt: %q", k, in)
		}
		switch w := want.(type) {
		case string:
			if f.Kind != KString || f.V != w {
				t.Fatalf("Feld %q = %q (%c), erwartet %q: %q", k, f.V, f.Kind, w, in)
			}
		case float64:
			if f.Kind != KNumber || f.num() != w {
				t.Fatalf("Feld %q = %q, erwartet %v", k, f.V, w)
			}
		case bool:
			if f.Kind != KBool || f.V != fmt.Sprint(w) {
				t.Fatalf("Feld %q = %q, erwartet %v", k, f.V, w)
			}
		case nil:
			if f.Kind != KNull {
				t.Fatalf("Feld %q: erwartet null", k)
			}
		default:
			if f.Kind != KNested {
				t.Fatalf("Feld %q: erwartet verschachtelt", k)
			}
		}
	}
}

func TestParseFlatMatchesEncodingJSON(t *testing.T) {
	for _, in := range []string{
		`{}`, ` { } `, `{"a":"b"}`, `{"a":1,"b":-2.5e3,"c":true,"d":false,"e":null}`,
		`{"p":"/a?x=1\u0026y=\u003c2\u003e","q":"a\"b\\c\/d\n\t\r\b\f"}`,
		`{"emoji":"\ud83d\ude00","lone":"\ud83d","bad":"\ud83dx","pair2":"\ud83d\u0041","é":"ü€😀"}`,
		`{"n":{"a":[1,2,{"b":"}"}]},"arr":["]",1],"after":"x"}`,
		`{"dup":1,"dup":2}`, "{\"ws\" :\t1 ,\n\"x\":\"y\"\r\n}",
		`{"a":1,}`, `{"a":01}`, `{"a":1.}`, `{"a":.5}`, `{"a":+1}`, `{"a":1e}`, `{"a":tru}`, `{"a":"x}`, `{"a":"\x"}`, `{"a":"\u12"}`,
		`{"a" 1}`, `{a:1}`, `{"a":1}x`, `{"a":1}{"b":2}`, `[1]`, `"x"`, `{"a":[1,,2]}`, `{"a":{"b":}}`, `{"a":"b" "c":1}`, `{`, ``, `{"a":1 "b":2}`,
		"{\"a\":\"line\nbreak\"}", `{"a":NaN}`, `{"a":-}`, `{"a":1e5,"b":0,"c":-0,"d":0.0,"e":1E+2}`,
	} {
		checkAgainstJSON(t, in)
	}
	// Zufällige Veränderungen einer echten Zeile.
	r := rand.New(rand.NewPCG(1, 2))
	alphabet := []byte(`{}[]",:\ue01-.tfn a/`)
	for range 20000 {
		b := []byte(line)
		for range 1 + r.IntN(3) {
			switch pos := r.IntN(len(b)); r.IntN(3) {
			case 0:
				b[pos] = alphabet[r.IntN(len(alphabet))]
			case 1:
				b = append(b[:pos], b[pos+1:]...)
			default:
				b = append(b[:pos], append([]byte{alphabet[r.IntN(len(alphabet))]}, b[pos:]...)...)
			}
		}
		checkAgainstJSON(t, string(b))
	}
}

func BenchmarkParseAccess(b *testing.B) {
	var rec Record
	b.ReportAllocs()
	for b.Loop() {
		var kind Kind
		kind, rec = Decode(line, rec)
		if kind != KindAccess || NewAccess(rec, "p", "DE", 0).Status != 200 {
			b.Fatal("falsch geparst")
		}
	}
}
