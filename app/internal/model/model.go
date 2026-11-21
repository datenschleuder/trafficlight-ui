// Package model parst Traefik-Logzeilen (ausschließlich JSON) in kompakte Strukturen.
package model

import (
	"math"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Kind beschreibt, was eine Logzeile enthält.
type Kind int

const (
	KindNotJSON Kind = iota // Text/CLF – wird nie verarbeitet
	KindOther               // gültiges JSON, aber weder Access- noch Systemlog
	KindAccess
	KindSystem
)

const (
	NoRouterSite = "(ohne Router)"
	maxExtra     = 32
	maxExtraLen  = 200
)

// KV ist ein dynamisches Feld (z. B. request_Referer) samt Wert.
type KV struct{ K, V string }

// Access ist ein Access-Log-Eintrag. T ist der Zeitpunkt, an dem Traefik die
// Zeile geschrieben hat (Request-Ende = StartUTC + Duration), in Unix-ms.
type Access struct {
	T               int64
	Dur, ODur       float64 // ms
	Size            int64
	Status, OStatus int
	Retries         int
	Host, Site      string
	Path, Route     string
	Method, Proto   string
	Client, UA      string
	Router, Service string
	Entry, TLS      string
	CC, Pod         string
	Internal, Bot   bool
	Extra           []KV
}

// Sys ist eine Traefik-Systemlogzeile (nur bei --log.format=json).
type Sys struct {
	T     int64  `json:"t"`
	Level string `json:"level"`
	Msg   string `json:"msg"`
	Pod   string `json:"pod"`
}

var (
	botWords = strings.Split("bot|crawl|spider|slurp|curl|wget|python|go-http|httpclient|scrapy|headless|monitor|uptime|java/|okhttp|axios|facebookexternalhit|preview", "|")
	methods  = map[string]bool{"GET": true, "HEAD": true, "POST": true, "PUT": true, "PATCH": true, "DELETE": true, "OPTIONS": true, "CONNECT": true, "TRACE": true, "PROPFIND": true}
)

// Felder, die fest ausgewertet werden bzw. redundant sind. Alles andere gilt als
// dynamisches Feld (konfigurierte Header wie request_*, downstream_*, origin_* oder
// eigene Felder) und wird filter- und auswertbar gespeichert.
var staticFields = func() map[string]bool {
	m := map[string]bool{}
	for _, k := range strings.Fields(`StartUTC StartLocal Duration RouterName ServiceName KubernetesServiceName
		ClientHost ClientAddr ClientPort RequestHost RequestAddr RequestPort RequestMethod RequestPath
		RequestProtocol RequestLine RequestCount RequestContentSize OriginDuration OriginStatus
		OriginStatusLine OriginContentSize DownstreamStatus DownstreamStatusLine DownstreamContentSize
		RetryAttempts TLSVersion entryPointName request_User-Agent Overhead GzipRatio ServiceURL
		TraceId SpanId time level msg message _t _pod _cc`) {
		m[k] = true
	}
	return m
}()

// IsDynamicField meldet, ob ein Feldname als dynamisches Feld behandelt wird.
func IsDynamicField(k string) bool { return !staticFields[k] }

// Decode liest eine Logzeile. Zeilen, die kein JSON-Objekt sind, liefern KindNotJSON.
// buf wird für das Ergebnis wiederverwendet (darf nil sein).
func Decode(line string, buf Record) (Kind, Record) {
	line = strings.TrimSpace(line)
	if len(line) < 2 || line[0] != '{' || line[len(line)-1] != '}' {
		return KindNotJSON, buf
	}
	rec, ok := ParseFlat(line, buf)
	if !ok {
		return KindNotJSON, rec
	}
	access, level, msg := false, false, false
	for i := range rec {
		switch rec[i].K {
		case "RequestMethod", "DownstreamStatus":
			access = true
		case "level":
			level = rec[i].Kind != KNull
		case "message", "msg":
			msg = msg || rec[i].Kind != KNull
		}
	}
	switch {
	case access:
		return KindAccess, rec
	case level && msg:
		return KindSystem, rec
	}
	return KindOther, rec
}

// text liefert den Wert als String (Zahlen/Bool als Rohtext), "" für null/verschachtelt.
func (f Field) text() string {
	switch f.Kind {
	case KString, KNumber, KBool:
		return f.V
	}
	return ""
}

func (f Field) num() float64 {
	if f.Kind == KNumber || f.Kind == KString {
		n, _ := strconv.ParseFloat(f.V, 64)
		return n
	}
	return 0
}

// clean entfernt Steuerzeichen und begrenzt die Länge (Rohdaten bleiben unverändert).
func clean(s string, max int) string {
	if len(s) > max {
		s = strings.ToValidUTF8(s[:max], "")
	}
	for i := 0; i < len(s); i++ {
		if s[i] < 0x20 || s[i] == 0x7f {
			return strings.Map(func(r rune) rune {
				if r < 0x20 || r == 0x7f {
					return '?'
				}
				return r
			}, s)
		}
	}
	return s
}

func or(s, def string) string {
	if s == "" {
		return def
	}
	return s
}

// ParseTime liest RFC3339 mit beliebiger Sekundenbruchteil-Genauigkeit -> Unix-ms.
func ParseTime(s string) int64 {
	if s == "" {
		return 0
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return 0
	}
	return t.UnixMilli()
}

// isBot erkennt automatisierte Clients am User-Agent. Ergebnisse werden gemerkt, da
// sich User-Agents stark wiederholen.
func isBot(ua string) bool {
	botMu.RLock()
	bot, ok := botCache[ua]
	botMu.RUnlock()
	if ok {
		return bot
	}
	low := strings.ToLower(ua)
	for _, w := range botWords {
		if strings.Contains(low, w) {
			bot = true
			break
		}
	}
	botMu.Lock()
	if len(botCache) >= 20_000 {
		clear(botCache)
	}
	botCache[strings.Clone(ua)] = bot
	botMu.Unlock()
	return bot
}

var (
	botMu    sync.RWMutex
	botCache = map[string]bool{}
)

// SiteOf normalisiert einen Hostnamen (klein, ohne Port, ohne "www.").
func SiteOf(host string) string {
	h := strings.ToLower(host)
	if i := strings.LastIndexByte(h, ':'); i > 0 && !strings.HasSuffix(h, "]") && strings.IndexByte(h[i+1:], ':') < 0 {
		if _, err := strconv.Atoi(h[i+1:]); err == nil {
			h = h[:i]
		}
	}
	return or(strings.TrimPrefix(h, "www."), "-")
}

// NewAccess baut den kompakten Eintrag. fallbackT (Unix-ms, z. B. Kubelet-Zeitstempel)
// wird genutzt, wenn der Eintrag selbst keine plausible Zeit enthält. pod/cc leer:
// aus den gespeicherten Feldern _pod/_cc übernehmen.
func NewAccess(rec Record, pod, cc string, fallbackT int64) *Access {
	a := &Access{}
	FillAccess(a, rec, pod, cc, fallbackT)
	return a
}

// FillAccess wie NewAccess, füllt aber einen vorhandenen (wiederverwendeten) Eintrag.
func FillAccess(a *Access, rec Record, pod, cc string, fallbackT int64) {
	extra := a.Extra[:0]
	*a = Access{Pod: pod, CC: cc, Extra: extra}
	var startUTC string
	var (
		durNs                                          float64
		reqHost, reqAddr, router, svc, k8sSvc, timeStr string
		storedT                                        int64
	)
	for i := range rec {
		f := &rec[i]
		switch f.K {
		case "StartUTC":
			startUTC = f.text()
		case "Duration":
			durNs = f.num()
		case "OriginDuration":
			a.ODur = math.Round(f.num()/1e3) / 1e3
		case "RequestHost":
			reqHost = f.text()
		case "RequestAddr":
			reqAddr = f.text()
		case "RequestPath":
			a.Path = clean(f.text(), 2048)
		case "RequestMethod":
			a.Method = f.text()
		case "RequestProtocol":
			a.Proto = f.text()
		case "DownstreamStatus":
			a.Status = int(f.num())
		case "OriginStatus":
			a.OStatus = int(f.num())
		case "DownstreamContentSize":
			a.Size = int64(f.num())
		case "ClientHost":
			a.Client = f.text()
		case "request_User-Agent":
			a.UA = f.text()
		case "RouterName":
			router = f.text()
		case "ServiceName":
			svc = f.text()
		case "KubernetesServiceName":
			k8sSvc = f.text()
		case "entryPointName":
			a.Entry = f.text()
		case "TLSVersion":
			a.TLS = f.text()
		case "RetryAttempts":
			a.Retries = int(f.num())
		case "time":
			timeStr = f.text()
		case "_t":
			storedT = int64(f.num())
		case "_pod":
			if a.Pod == "" {
				a.Pod = f.text()
			}
		case "_cc":
			if a.CC == "" {
				a.CC = f.text()
			}
		case "StartLocal", "ClientAddr", "ClientPort", "RequestPort", "RequestLine", "RequestCount", "RequestContentSize",
			"OriginStatusLine", "OriginContentSize", "DownstreamStatusLine", "Overhead", "GzipRatio", "ServiceURL",
			"TraceId", "SpanId", "level", "msg", "message":
			// fest, aber nicht ausgewertet (nur in den Rohdaten sichtbar)
		default:
			// Dynamisches Feld: konfigurierte Header (request_*, downstream_*, origin_*) oder eigene Felder.
			if v := f.text(); v != "" && v != "-" && len(a.Extra) < 4*maxExtra { // "-" = leer bei Traefik
				a.Extra = append(a.Extra, KV{clean(f.K, 128), clean(v, maxExtraLen)})
			}
		}
	}
	a.Dur = math.Round(durNs/1e3) / 1e3
	if storedT > 0 { // gespeicherter Eintrag: Zeit exakt wie beim Einlesen
		a.T = storedT
	} else {
		if start := ParseTime(startUTC); start > 0 {
			a.T = start + int64(durNs/1e6)
		} else if timeStr != "" {
			a.T = ParseTime(timeStr)
		}
		if fallbackT > 0 && (a.T == 0 || a.T > fallbackT+3600_000) {
			a.T = fallbackT
		}
	}
	a.Host = clean(or(or(reqHost, reqAddr), "-"), 255)
	a.Route = a.Path
	if i := strings.IndexByte(a.Path, '?'); i >= 0 {
		a.Route = a.Path[:i]
	}
	if a.Method == "" {
		a.Method = "-"
	} else if !methods[a.Method] {
		if up := strings.ToUpper(a.Method); methods[up] {
			a.Method = up
		} else {
			a.Method = "OTHER"
		}
	}
	a.Proto = clean(or(a.Proto, "-"), 16)
	if a.Status < 0 || a.Status > 999 {
		a.Status = 0
	}
	if a.OStatus < 0 || a.OStatus > 999 {
		a.OStatus = 0
	}
	if a.Size < 0 {
		a.Size = 0
	}
	a.Client = clean(or(a.Client, "-"), 64)
	a.UA = clean(or(a.UA, "-"), 512)
	a.Internal = strings.HasSuffix(router, "@internal")
	if router == "" {
		// Kein Router hat gegriffen: beliebige (oft gefälschte) Hostnamen nicht einzeln zählen.
		a.Site = NoRouterSite
	} else {
		a.Site = SiteOf(a.Host)
	}
	router = stripRouterHash(router)
	a.Router = clean(or(router, "-"), 255)
	a.Service = clean(or(or(k8sSvc, svc), "-"), 255)
	a.Entry = clean(or(a.Entry, "-"), 64)
	a.TLS = clean(or(a.TLS, "-"), 16)
	a.Bot = isBot(a.UA)
	if a.Pod == "" {
		a.Pod = "-"
	}
	if len(a.Extra) > 1 {
		slices.SortFunc(a.Extra, func(x, y KV) int { return strings.Compare(x.K, y.K) })
	}
	if len(a.Extra) > maxExtra {
		a.Extra = a.Extra[:maxExtra]
	}
}

// stripRouterHash entfernt den von Traefik angehängten Hash ("-<20 Hex-Zeichen>@provider").
func stripRouterHash(router string) string {
	at := strings.LastIndexByte(router, '@')
	if at < 21 || router[at-21] != '-' {
		return router
	}
	for _, c := range []byte(router[at-20 : at]) {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return router
		}
	}
	return router[:at-21] + router[at:]
}

var levels = map[string]string{"trace": "TRC", "debug": "DBG", "info": "INF", "warn": "WRN", "warning": "WRN",
	"error": "ERR", "fatal": "FTL", "panic": "PNC"}

// NewSys baut einen Systemlog-Eintrag; zusätzliche JSON-Felder werden als k=v angehängt.
func NewSys(rec Record, pod string, fallbackT int64) *Sys {
	s := &Sys{Pod: pod, T: fallbackT}
	var msg string
	var extra []string
	for _, f := range rec {
		switch f.K {
		case "time":
			if t := ParseTime(f.text()); t > 0 {
				s.T = t
			}
		case "level":
			lv := strings.ToLower(f.text())
			if s.Level = levels[lv]; s.Level == "" {
				s.Level = strings.ToUpper(clean(lv, 8))
			}
		case "message", "msg":
			if msg == "" {
				msg = f.text()
			}
		default:
			if f.Kind != KNull {
				extra = append(extra, f.K+"="+f.V)
			}
		}
	}
	sort.Strings(extra)
	if len(extra) > 0 {
		msg = strings.TrimSpace(msg + "  " + strings.Join(extra, " "))
	}
	s.Msg = strings.Clone(clean(msg, 4000))
	return s
}

// IsAlert meldet, ob ein Level als Warnung/Fehler zählt (Zeitleiste).
func IsAlert(level string) bool {
	switch level {
	case "WRN", "ERR", "FTL", "PNC":
		return true
	}
	return false
}

// ---- Latenz-Histogramm: 80 logarithmische Klassen (Faktor 1,3) ab 0,05 ms ----

const (
	HistBins   = 80
	histBase   = 0.05
	histFactor = 1.3
)

var histLog = math.Log(histFactor)

// HistBin liefert die Klasse für eine Dauer in ms.
func HistBin(ms float64) int {
	if ms <= histBase {
		return 0
	}
	b := int(math.Log(ms/histBase)/histLog) + 1
	if b >= HistBins {
		return HistBins - 1
	}
	return b
}

// HistUpper ist die Obergrenze der Klasse in ms.
func HistUpper(bin int) float64 { return histBase * math.Pow(histFactor, float64(bin)) }

// HistQuantile schätzt das Quantil q (0..1) aus den Klassen (geometrische Interpolation).
func HistQuantile(h []uint64, q float64) float64 {
	var total uint64
	for _, n := range h {
		total += n
	}
	if total == 0 {
		return 0
	}
	target := q * float64(total)
	var cum float64
	for i, n := range h {
		if n == 0 {
			continue
		}
		if cum+float64(n) >= target {
			if i == 0 {
				return histBase
			}
			frac := (target - cum) / float64(n)
			return HistUpper(i-1) * math.Pow(histFactor, frac)
		}
		cum += float64(n)
	}
	return HistUpper(len(h) - 1)
}
