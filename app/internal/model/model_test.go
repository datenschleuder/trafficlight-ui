package model

import (
	"fmt"
	"math"
	"strings"
	"testing"
)

const line = `{"ClientAddr":"91.64.1.2:5123","ClientHost":"91.64.1.2","DownstreamContentSize":1234,"DownstreamStatus":200,` +
	`"Duration":2500000,"OriginDuration":2000000,"OriginStatus":200,"RequestHost":"WWW.Example.org:443","RequestMethod":"get",` +
	`"RequestPath":"/a/b?x=1","RequestProtocol":"HTTP/2.0","RouterName":"default-web-0123456789abcdef0123@kubernetescrd",` +
	`"ServiceName":"default-web-80@kubernetescrd","StartUTC":"2026-11-20T10:00:00.123456789Z","TLSVersion":"1.3",` +
	`"entryPointName":"websecure","level":"info","msg":"","time":"2026-11-20T11:00:00+01:00",` +
	`"request_User-Agent":"Mozilla/5.0 (compatible; Googlebot/2.1)","request_Referer":"https://example.com/",` +
	`"downstream_Content-Type":"text/html","RequestScheme":"https","custom":42,"nested":{"a":1}}`

func TestDecodeOnlyJSON(t *testing.T) {
	for in, want := range map[string]Kind{
		line: KindAccess,
		`{"level":"error","time":"2026-11-20T10:00:00Z","message":"boom","error":"x"}`: KindSystem,
		`{"level":"info","msg":"Configuration loaded","time":"2026-11-20T10:00:00Z"}`:  KindSystem,
		`{"foo":1}`: KindOther,
		`2026-11-20T10:00:00Z INF Starting provider`:                                               KindNotJSON,
		`91.64.1.2 - - [20/Nov/2026:10:00:00 +0000] "GET / HTTP/1.1" 200 12 "-" "-" 1 "r" "u" 3ms`: KindNotJSON,
		`{"DownstreamStatus":200`: KindNotJSON,
		`["RequestMethod"]`:       KindNotJSON,
		``:                        KindNotJSON,
	} {
		if got, _ := Decode(in, nil); got != want {
			t.Errorf("Decode(%.40q) = %v, erwartet %v", in, got, want)
		}
	}
}

func TestNewAccess(t *testing.T) {
	_, m := Decode(line, nil)
	a := NewAccess(m, "pod-a", "DE", 0)
	if a.Site != "example.org" || a.Host != "WWW.Example.org:443" || a.Route != "/a/b" || a.Path != "/a/b?x=1" || a.Method != "GET" {
		t.Errorf("Felder falsch: %+v", a)
	}
	if a.Router != "default-web@kubernetescrd" || a.Internal || !a.Bot || a.Status != 200 || a.Size != 1234 || a.Dur != 2.5 {
		t.Errorf("Felder falsch: %+v", a)
	}
	if want := ParseTime("2026-11-20T10:00:00.123456789Z") + 2; a.T != want {
		t.Errorf("T=%d, erwartet %d", a.T, want)
	}
	for in, want := range map[string]string{"a-0123456789abcdef0123@k": "a@k", "a-0123456789abcdef012@k": "a-0123456789abcdef012@k",
		"api@internal": "api@internal", "x-0123456789abcdefXXXX@k": "x-0123456789abcdefXXXX@k", "": ""} {
		if got := stripRouterHash(in); got != want {
			t.Errorf("stripRouterHash(%q) = %q, erwartet %q", in, got, want)
		}
	}
	// Dynamische Felder: konfigurierte Header und eigene Felder, keine festen/verschachtelten.
	got := map[string]string{}
	for _, kv := range a.Extra {
		got[kv.K] = kv.V
	}
	want := map[string]string{"request_Referer": "https://example.com/", "downstream_Content-Type": "text/html", "RequestScheme": "https", "custom": "42"}
	if len(got) != len(want) {
		t.Errorf("dynamische Felder: %v", got)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("dynamisches Feld %s=%q, erwartet %q", k, got[k], v)
		}
	}
	// Ohne Router werden Hosts nicht einzeln gezählt; unbekannte Methoden zusammengefasst.
	_, m = Decode(`{"RequestMethod":"FOO\u0001","RequestHost":"evil.example","DownstreamStatus":404,"request_User-Agent":"x\u001fy"}`, nil)
	b := NewAccess(m, "p", "", 1700000000000)
	if b.Site != NoRouterSite || b.Method != "OTHER" || b.T != 1700000000000 || b.UA != "x?y" {
		t.Errorf("Sonderfälle falsch: %+v", b)
	}
}

func TestHist(t *testing.T) {
	h := make([]uint64, HistBins)
	for i := 1; i <= 1000; i++ {
		h[HistBin(float64(i))]++
	}
	for q, want := range map[float64]float64{.5: 500, .95: 950, .99: 990} {
		if got := HistQuantile(h, q); math.Abs(got-want)/want > 0.15 {
			t.Errorf("q%.2f = %.1f, erwartet ~%.0f", q, got, want)
		}
	}
	if HistBin(0) != 0 || HistBin(1e12) != HistBins-1 || HistQuantile(make([]uint64, HistBins), .5) != 0 {
		t.Error("Randfälle falsch")
	}
}

// Die Liste der festen Felder (staticFields) und die Auswertung in FillAccess müssen
// übereinstimmen: kein festes Feld darf als dynamisches Feld auftauchen.
func TestStaticFieldsNotDynamic(t *testing.T) {
	var sb strings.Builder
	sb.WriteString(`{"RequestMethod":"GET"`)
	for k := range staticFields {
		if k != "RequestMethod" {
			fmt.Fprintf(&sb, `,%q:"1"`, k)
		}
	}
	sb.WriteString(`,"request_X-Test":"dyn"}`)
	kind, rec := Decode(sb.String(), nil)
	a := NewAccess(rec, "", "", 0)
	if kind != KindAccess || len(a.Extra) != 1 || a.Extra[0] != (KV{"request_X-Test", "dyn"}) || !IsDynamicField("request_X-Test") {
		t.Errorf("dynamische Felder: %+v", a.Extra)
	}
}

func TestSys(t *testing.T) {
	_, m := Decode(`{"level":"warn","time":"2026-11-20T10:00:00Z","message":"hc failed","providerName":"k8s","n":3}`, nil)
	s := NewSys(m, "pod", 0)
	if s.Level != "WRN" || !IsAlert(s.Level) || s.Msg != "hc failed  n=3 providerName=k8s" || s.T != ParseTime("2026-11-20T10:00:00Z") {
		t.Errorf("Sys falsch: %+v", s)
	}
}
