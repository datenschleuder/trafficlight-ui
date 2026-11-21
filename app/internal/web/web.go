// Package web liefert die GUI aus und stellt API und SSE-Stream bereit.
package web

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"embed"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/fs"
	"log/slog"
	"mime"
	"net/http"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"traefikgui/internal/collector"
	"traefikgui/internal/model"
	"traefikgui/internal/store"
)

//go:embed static
var staticFS embed.FS

const (
	maxStreams   = 64
	maxLiveRows  = 300
	minSnapEvery = 2 * time.Second
	maxSnapEvery = 30 * time.Second
	exportLimit  = 1_000_000
)

type Info struct {
	Version    string   `json:"version"`
	Namespaces []string `json:"namespaces"`
	Selector   string   `json:"selector"`
	Geo        bool     `json:"geo"`
}

type Handler struct {
	st      *store.Store
	col     *collector.Collector
	info    Info
	log     *slog.Logger
	mux     *http.ServeMux
	assets  map[string]*asset
	streams atomic.Int32
}

type asset struct {
	raw, gz []byte
	ctype   string
	etag    string
}

func New(st *store.Store, col *collector.Collector, info Info, log *slog.Logger) *Handler {
	h := &Handler{st: st, col: col, info: info, log: log, mux: http.NewServeMux(), assets: map[string]*asset{}}
	sub, _ := fs.Sub(staticFS, "static")
	fs.WalkDir(sub, ".", func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		raw, _ := fs.ReadFile(sub, p)
		var buf bytes.Buffer
		zw, _ := gzip.NewWriterLevel(&buf, gzip.BestCompression)
		zw.Write(raw)
		zw.Close()
		sum := sha256.Sum256(raw)
		ctype := map[string]string{".html": "text/html; charset=utf-8", ".js": "text/javascript; charset=utf-8",
			".css": "text/css; charset=utf-8", ".svg": "image/svg+xml", ".txt": "text/plain; charset=utf-8"}[path.Ext(p)]
		if ctype == "" {
			ctype = mime.TypeByExtension(path.Ext(p))
		}
		h.assets["/"+p] = &asset{raw: raw, gz: buf.Bytes(), ctype: ctype, etag: `"` + hex.EncodeToString(sum[:8]) + `"`}
		return nil
	})
	h.assets["/"] = h.assets["/index.html"]
	h.mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok\n")) })
	h.mux.HandleFunc("GET /readyz", h.ready)
	h.mux.HandleFunc("GET /api/stream", h.stream)
	h.mux.HandleFunc("GET /api/snapshot", h.snapshot)
	h.mux.HandleFunc("GET /api/rows", h.rows)
	h.mux.HandleFunc("GET /api/entry", h.entry)
	h.mux.HandleFunc("GET /api/sys", h.sys)
	h.mux.HandleFunc("GET /api/status", h.status)
	h.mux.HandleFunc("GET /api/export.csv", h.export)
	h.mux.HandleFunc("GET /", h.static)
	return h
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	hd := w.Header()
	hd.Set("X-Content-Type-Options", "nosniff")
	hd.Set("X-Frame-Options", "DENY")
	hd.Set("Referrer-Policy", "no-referrer")
	hd.Set("Content-Security-Policy", "default-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; frame-ancestors 'none'; base-uri 'none'; form-action 'none'")
	h.mux.ServeHTTP(w, r)
}

func (h *Handler) static(w http.ResponseWriter, r *http.Request) {
	a := h.assets[r.URL.Path]
	if a == nil {
		http.NotFound(w, r)
		return
	}
	hd := w.Header()
	hd.Set("Content-Type", a.ctype)
	hd.Set("ETag", a.etag)
	hd.Set("Cache-Control", "no-cache")
	hd.Set("Vary", "Accept-Encoding")
	if r.Header.Get("If-None-Match") == a.etag {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	body := a.raw
	if strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
		hd.Set("Content-Encoding", "gzip")
		body = a.gz
	}
	hd.Set("Content-Length", strconv.Itoa(len(body)))
	w.Write(body)
}

func (h *Handler) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := h.st.Ping(ctx); err != nil {
		http.Error(w, "redis: "+err.Error(), http.StatusServiceUnavailable)
		return
	}
	w.Write([]byte("ok\n"))
}

func writeJSON(w http.ResponseWriter, r *http.Request, code int, v any) {
	body, err := json.Marshal(v)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	hd := w.Header()
	hd.Set("Content-Type", "application/json")
	hd.Set("Cache-Control", "no-store")
	if len(body) > 1024 && strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
		hd.Set("Content-Encoding", "gzip")
		w.WriteHeader(code)
		zw, _ := gzip.NewWriterLevel(w, gzip.BestSpeed)
		zw.Write(body)
		zw.Close()
		return
	}
	w.WriteHeader(code)
	w.Write(body)
}

func fail(w http.ResponseWriter, r *http.Request, code int, msg string) {
	writeJSON(w, r, code, map[string]string{"error": msg})
}

// params ist die aus der URL gelesene Auswahl (Zeitraum + Filter).
type params struct {
	F          store.Filter
	Range      int64 // > 0: gleitendes Fenster (ms) bis jetzt -> Live
	From, To   int64
	ZFrom, ZTo int64
	TZOff      int64
	XField     string
}

func num(q map[string][]string, k string) int64 {
	if v := q[k]; len(v) > 0 {
		n, _ := strconv.ParseInt(v[0], 10, 64)
		return n
	}
	return 0
}

func parse(r *http.Request) params {
	q := r.URL.Query()
	p := params{Range: num(q, "range") * 1000, From: num(q, "from"), To: num(q, "to"), ZFrom: num(q, "zfrom"), ZTo: num(q, "zto"),
		TZOff: -num(q, "tz") * 60_000, XField: q.Get("xf")}
	if p.Range <= 0 && p.From <= 0 {
		p.Range = 3600_000
	}
	if p.TZOff < -14*3600_000 || p.TZOff > 14*3600_000 {
		p.TZOff = 0
	}
	f := &p.F
	f.Text = strings.ToLower(strings.TrimSpace(q.Get("q")))
	f.Site, f.Router, f.Service, f.Method = q.Get("site"), q.Get("router"), q.Get("service"), q.Get("method")
	f.Host = strings.ToLower(strings.TrimSpace(q.Get("host")))
	f.Pod, f.Country, f.Client, f.Route, f.UA = q.Get("pod"), q.Get("country"), q.Get("client"), q.Get("route"), q.Get("ua")
	f.Status = int(num(q, "status"))
	if cls, ok := q["cls"]; ok {
		for _, c := range cls[0] {
			if c >= '0' && c <= '5' {
				f.Classes |= 1 << (c - '0')
			}
		}
		if f.Classes == 0 {
			f.Classes = 1 << 7 // nichts ausgewählt -> nichts passt
		}
	}
	f.MinDur, _ = strconv.ParseFloat(q.Get("mindur"), 64)
	f.NoInt, f.NoBots = q.Get("noint") == "1", q.Get("nobots") == "1"
	for _, x := range q["x"] {
		if k, v, ok := strings.Cut(x, "="); ok && k != "" {
			f.X = append(f.X, model.KV{K: k, V: v})
		}
	}
	return p
}

func (p *params) live() bool { return p.Range > 0 }

func (p *params) window(now int64) (from, to int64) {
	if p.Range > 0 {
		return now - p.Range, now + 1000
	}
	return p.From, p.To
}

func (p *params) query(now int64) store.Query {
	from, to := p.window(now)
	return store.Query{F: p.F, From: from, To: to, ZFrom: p.ZFrom, ZTo: p.ZTo, TZOff: p.TZOff, XField: p.XField}
}

func (h *Handler) snapshot(w http.ResponseWriter, r *http.Request) {
	p := parse(r)
	ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
	defer cancel()
	snap, err := h.st.Snapshot(ctx, p.query(time.Now().UnixMilli()))
	if err != nil {
		fail(w, r, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, r, http.StatusOK, snap)
}

func (h *Handler) rows(w http.ResponseWriter, r *http.Request) {
	p := parse(r)
	q := r.URL.Query()
	from, to := p.window(time.Now().UnixMilli())
	if p.ZFrom > 0 && p.ZTo > p.ZFrom {
		from, to = max(from, p.ZFrom), min(to, p.ZTo)
	}
	limit, _ := strconv.Atoi(q.Get("limit"))
	ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
	defer cancel()
	res, err := h.st.Rows(ctx, store.RowsQuery{F: p.F, From: from, To: to, Sort: q.Get("sort"), Asc: q.Get("dir") == "asc",
		Limit: limit, Cursor: q.Get("cursor"), XField: p.XField})
	if err != nil {
		fail(w, r, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, r, http.StatusOK, res)
}

func (h *Handler) entry(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	raw, err := h.st.RawEntry(ctx, r.URL.Query().Get("id"))
	switch {
	case err != nil:
		fail(w, r, http.StatusBadRequest, err.Error())
	case raw == nil:
		fail(w, r, http.StatusNotFound, "Eintrag nicht (mehr) vorhanden")
	default:
		// Dynamische Felder (konfigurierte Header, eigene Felder) getrennt ausweisen.
		var m map[string]json.RawMessage
		dynamic := []string{}
		if json.Unmarshal(raw, &m) == nil {
			for k, v := range m {
				if s := string(v); model.IsDynamicField(k) && s != `""` && s != `"-"` && s != "null" {
					dynamic = append(dynamic, k)
				}
			}
			sort.Strings(dynamic)
		}
		writeJSON(w, r, http.StatusOK, map[string]any{"id": r.URL.Query().Get("id"), "dynamic": dynamic, "entry": json.RawMessage(raw)})
	}
}

func (h *Handler) sys(w http.ResponseWriter, r *http.Request) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 || limit > 5000 {
		limit = 1000
	}
	recs, last := h.st.SysLogs(0, limit)
	if recs == nil {
		recs = []store.SysRec{}
	}
	writeJSON(w, r, http.StatusOK, map[string]any{"logs": recs, "seq": last})
}

type statusMsg struct {
	Now     int64                    `json:"now"`
	Info    Info                     `json:"info"`
	Pods    []collector.StreamStatus `json:"pods"`
	ListErr string                   `json:"listErr,omitempty"`
	Store   store.Stats              `json:"store"`
	Clients int32                    `json:"clients"`
}

func (h *Handler) statusMsg() statusMsg {
	pods, listErr := h.col.Status()
	if pods == nil {
		pods = []collector.StreamStatus{}
	}
	return statusMsg{Now: time.Now().UnixMilli(), Info: h.info, Pods: pods, ListErr: listErr, Store: h.st.Stats(), Clients: h.streams.Load()}
}

func (h *Handler) status(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, r, http.StatusOK, h.statusMsg())
}

func (h *Handler) export(w http.ResponseWriter, r *http.Request) {
	p := parse(r)
	from, to := p.window(time.Now().UnixMilli())
	if p.ZFrom > 0 && p.ZTo > p.ZFrom {
		from, to = max(from, p.ZFrom), min(to, p.ZTo)
	}
	w.Header().Set("Content-Type", "text/csv; charset=utf-8")
	w.Header().Set("Content-Disposition", `attachment; filename="traefik-access.csv"`)
	cw := csv.NewWriter(w)
	cw.Comma = ';'
	cw.Write([]string{"zeit", "status", "methode", "host", "pfad", "dauer_ms", "bytes", "client", "land", "protokoll", "router", "service", "pod", "user_agent"})
	n := 0
	err := h.st.Export(r.Context(), &p.F, from, to, exportLimit, func(a *model.Access, t int64) error {
		if n++; n%5000 == 0 {
			cw.Flush()
		}
		return cw.Write([]string{time.UnixMilli(t).UTC().Format("2006-01-02T15:04:05.000Z"), strconv.Itoa(a.Status), a.Method,
			csvSafe(a.Host), csvSafe(a.Path), strconv.FormatFloat(a.Dur, 'f', 3, 64), strconv.FormatInt(a.Size, 10), a.Client, a.CC,
			a.Proto, csvSafe(a.Router), csvSafe(a.Service), a.Pod, csvSafe(a.UA)})
	})
	cw.Flush()
	if err != nil && r.Context().Err() == nil {
		h.log.Warn("CSV-Export abgebrochen", "err", err)
	}
}

// csvSafe verhindert, dass Tabellenkalkulationen Logwerte als Formel ausführen.
func csvSafe(s string) string {
	if s != "" && strings.ContainsRune("=+-@\t\r", rune(s[0])) && s != "-" {
		return "'" + s
	}
	return s
}

// stream ist der SSE-Endpunkt: liefert sofort das Dashboard und danach laufend
// neue Daten (snapshot, rows, sys, status).
func (h *Handler) stream(w http.ResponseWriter, r *http.Request) {
	fl, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming nicht unterstützt", http.StatusInternalServerError)
		return
	}
	if h.streams.Add(1) > maxStreams {
		h.streams.Add(-1)
		http.Error(w, "zu viele Live-Verbindungen", http.StatusServiceUnavailable)
		return
	}
	defer h.streams.Add(-1)
	p := parse(r)
	ctx := r.Context()
	rc := http.NewResponseController(w)
	hd := w.Header()
	hd.Set("Content-Type", "text/event-stream")
	hd.Set("Cache-Control", "no-cache, no-transform")
	hd.Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)

	send := func(event string, v any) bool {
		body, err := json.Marshal(v)
		if err != nil {
			return true
		}
		rc.SetWriteDeadline(time.Now().Add(20 * time.Second))
		if _, err := fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, body); err != nil {
			return false
		}
		fl.Flush()
		return true
	}
	fmt.Fprint(w, "retry: 3000\n\n")

	ringSeq := h.st.RingSeq()
	_, sysSeq := h.st.SysLogs(0, 0)
	if !send("status", h.statusMsg()) {
		return
	}
	every := minSnapEvery
	snapshot := func() bool {
		sctx, cancel := context.WithTimeout(ctx, 60*time.Second)
		defer cancel()
		t0 := time.Now()
		snap, err := h.st.Snapshot(sctx, p.query(t0.UnixMilli()))
		if err != nil {
			if ctx.Err() != nil {
				return false
			}
			h.log.Warn("Auswertung fehlgeschlagen", "err", err)
			return send("fail", map[string]string{"error": err.Error()})
		}
		// Teure Auswertungen seltener wiederholen.
		every = min(max(8*time.Since(t0), minSnapEvery), maxSnapEvery)
		return send("snapshot", snap)
	}
	if !snapshot() {
		return
	}
	version := h.st.Version()
	lastSnap, lastStatus, lastBeat := time.Now(), time.Now(), time.Now()
	for {
		changed := h.st.Changed()
		select {
		case <-ctx.Done():
			return
		case <-changed:
		case <-time.After(5 * time.Second):
		}
		// Schreib-Bursts zusammenfassen.
		select {
		case <-ctx.Done():
			return
		case <-time.After(400 * time.Millisecond):
		}
		now := time.Now()
		if recs, last := h.st.SysLogs(sysSeq, 500); len(recs) > 0 {
			sysSeq = last
			if !send("sys", map[string]any{"logs": recs}) {
				return
			}
		}
		if p.live() {
			recents, next, gap := h.st.RecentSince(ringSeq)
			ringSeq = next
			rows := make([]store.RowOut, 0, 64)
			for i := len(recents) - 1; i >= 0 && len(rows) < maxLiveRows; i-- { // neueste zuerst
				rec := recents[i]
				if p.ZFrom > 0 && (rec.A.T < p.ZFrom || rec.A.T >= p.ZTo) {
					continue
				}
				if rec.ID != "" && p.F.MatchAccess(rec.A) {
					rows = append(rows, store.RowFromAccess(rec, p.XField))
				}
			}
			if len(rows) > 0 || gap {
				if !send("rows", map[string]any{"rows": rows, "gap": gap || len(rows) == maxLiveRows}) {
					return
				}
			}
			if v := h.st.Version(); (v != version && now.Sub(lastSnap) >= every) || now.Sub(lastSnap) >= maxSnapEvery {
				version, lastSnap = v, now
				if !snapshot() {
					return
				}
				lastBeat = now
			}
		}
		if now.Sub(lastStatus) >= 5*time.Second {
			lastStatus, lastBeat = now, now
			if !send("status", h.statusMsg()) {
				return
			}
		}
		if now.Sub(lastBeat) >= 15*time.Second {
			lastBeat = now
			rc.SetWriteDeadline(now.Add(20 * time.Second))
			if _, err := fmt.Fprint(w, ": ping\n\n"); err != nil {
				return
			}
			fl.Flush()
		}
	}
}
