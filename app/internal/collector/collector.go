// Package collector findet die Traefik-Pods und liest deren Logs fortlaufend ein.
// Verarbeitet werden ausschließlich Zeilen im JSON-Format.
package collector

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"sort"
	"sync"
	"time"

	"traefikgui/internal/geo"
	"traefikgui/internal/kube"
	"traefikgui/internal/model"
	"traefikgui/internal/store"
)

const (
	maxLine       = 1 << 20
	sniffLines    = 200             // so viele Zeilen ohne JSON -> Pod gilt als "kein JSON"
	noJSONRecheck = 5 * time.Minute // danach wird erneut geprüft
)

type Config struct {
	Namespaces []string
	Selector   string
	Container  string // leer = alle Container des Pods
	Backfill   bool   // beim ersten Kontakt vorhandene Logs vollständig einlesen
	Refresh    time.Duration
	Logger     *slog.Logger
}

// Sink nimmt gelesene Zeilen entgegen (der Store).
type Sink interface {
	Ingest() chan<- store.Item
	Cursor(ctx context.Context, key string) (int64, error)
}

// PodAPI ist der benötigte Teil der Kubernetes-API.
type PodAPI interface {
	ListPods(ctx context.Context, namespace, selector string) ([]kube.Pod, error)
	Logs(ctx context.Context, namespace, pod, container string, o kube.LogOptions) (io.ReadCloser, error)
}

// StreamStatus beschreibt den Zustand eines Pod/Container-Logstreams.
type StreamStatus struct {
	Namespace string `json:"namespace"`
	Pod       string `json:"pod"`
	Container string `json:"container"`
	Node      string `json:"node"`
	Phase     string `json:"phase"`
	Restarts  int    `json:"restarts"`
	State     string `json:"state"` // streaming | waiting | no-json | error | gone
	Format    string `json:"format"`
	Hint      string `json:"hint,omitempty"`
	Error     string `json:"error,omitempty"`
	Access    int64  `json:"access"`
	System    int64  `json:"system"`
	Skipped   int64  `json:"skipped"` // Zeilen, die kein JSON waren
	LastLine  int64  `json:"lastLine"`
	Since     int64  `json:"since"`
}

type stream struct {
	key    string
	cancel context.CancelFunc
	mu     sync.Mutex
	st     StreamStatus
	pod    kube.Pod
	ct     kube.Container
}

func (s *stream) update(fn func(st *StreamStatus)) {
	s.mu.Lock()
	fn(&s.st)
	s.mu.Unlock()
}

type Collector struct {
	cfg  Config
	api  PodAPI
	sink Sink
	geo  *geo.DB
	log  *slog.Logger

	mu      sync.Mutex
	streams map[string]*stream
	gone    []StreamStatus
	listErr string
	lastOK  time.Time
}

func New(cfg Config, api PodAPI, sink Sink, db *geo.DB) *Collector {
	if cfg.Refresh <= 0 {
		cfg.Refresh = 10 * time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Collector{cfg: cfg, api: api, sink: sink, geo: db, log: cfg.Logger, streams: map[string]*stream{}}
}

// Run gleicht regelmäßig die Pod-Liste ab und startet/stoppt Logstreams.
func (c *Collector) Run(ctx context.Context) {
	var wg sync.WaitGroup
	defer wg.Wait()
	t := time.NewTicker(c.cfg.Refresh)
	defer t.Stop()
	for {
		c.sync(ctx, &wg)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (c *Collector) sync(ctx context.Context, wg *sync.WaitGroup) {
	seen := map[string]bool{}
	var listErr error
	for _, ns := range c.cfg.Namespaces {
		pods, err := c.api.ListPods(ctx, ns, c.cfg.Selector)
		if err != nil {
			listErr = err
			c.log.Warn("Pod-Liste nicht abrufbar", "namespace", ns, "err", err)
			continue
		}
		for _, p := range pods {
			for _, ct := range p.Containers {
				if c.cfg.Container != "" && ct.Name != c.cfg.Container {
					continue
				}
				key := p.Namespace + "/" + p.UID + "/" + ct.Name
				seen[key] = true
				c.mu.Lock()
				s := c.streams[key]
				if s == nil {
					sctx, cancel := context.WithCancel(ctx)
					s = &stream{key: key, cancel: cancel, pod: p, ct: ct, st: StreamStatus{Namespace: p.Namespace, Pod: p.Name,
						Container: ct.Name, State: "waiting", Format: "unbekannt", Since: time.Now().UnixMilli()}}
					c.streams[key] = s
					wg.Add(1)
					go func() {
						defer wg.Done()
						c.follow(sctx, s)
					}()
					c.log.Info("Traefik-Pod gefunden", "pod", p.Name, "namespace", p.Namespace, "container", ct.Name)
				}
				c.mu.Unlock()
				s.mu.Lock()
				s.pod, s.ct = p, ct
				s.st.Node, s.st.Phase, s.st.Restarts, s.st.Hint = p.Node, p.Phase, ct.RestartCount, ct.JSONHint
				s.mu.Unlock()
			}
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if listErr != nil {
		c.listErr = listErr.Error()
		return // bei Fehlern keine Streams beenden
	}
	c.listErr, c.lastOK = "", time.Now()
	for key, s := range c.streams {
		if !seen[key] {
			s.cancel()
			delete(c.streams, key)
			s.mu.Lock()
			st := s.st
			s.mu.Unlock()
			st.State = "gone"
			c.gone = append(c.gone, st)
			if len(c.gone) > 20 {
				c.gone = c.gone[len(c.gone)-20:]
			}
			c.log.Info("Traefik-Pod verschwunden", "pod", st.Pod)
		}
	}
}

func (c *Collector) follow(ctx context.Context, s *stream) {
	backoff := time.Second
	var cursor int64 // Unix-ns der letzten gelesenen Zeile
	loaded, drainedRestart := false, -1
	for ctx.Err() == nil {
		s.mu.Lock()
		pod, ct := s.pod, s.ct
		s.mu.Unlock()
		if !loaded {
			cur, err := c.sink.Cursor(ctx, s.key)
			if err != nil {
				s.update(func(st *StreamStatus) { st.State, st.Error = "error", "Redis: "+err.Error() })
				sleep(ctx, backoff)
				continue
			}
			if cur == 0 && !c.cfg.Backfill {
				cur = time.Now().UnixNano()
			}
			cursor, loaded = cur, true
		}
		if !ct.Running {
			s.update(func(st *StreamStatus) { st.State, st.Error = "waiting", "Container läuft nicht" })
			sleep(ctx, 5*time.Second)
			continue
		}
		// Nach einem Container-Neustart zuerst den Rest der vorherigen Instanz holen.
		if ct.RestartCount > 0 && drainedRestart != ct.RestartCount && (cursor == 0 || time.Unix(0, cursor).Before(ct.StartedAt)) {
			if rc, err := c.api.Logs(ctx, pod.Namespace, pod.Name, ct.Name, kube.LogOptions{Since: sinceOf(cursor), Previous: true}); err == nil {
				cursor, _ = c.read(ctx, s, rc, cursor)
				rc.Close()
			}
		}
		drainedRestart = ct.RestartCount

		rc, err := c.api.Logs(ctx, pod.Namespace, pod.Name, ct.Name, kube.LogOptions{Since: sinceOf(cursor), Follow: true})
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			s.update(func(st *StreamStatus) { st.State, st.Error = "error", err.Error() })
			sleep(ctx, backoff)
			backoff = min(backoff*2, 30*time.Second)
			continue
		}
		s.update(func(st *StreamStatus) {
			if st.State != "no-json" { // bleibt bis zur ersten JSON-Zeile als "kein JSON" markiert
				st.State, st.Error = "streaming", ""
			}
		})
		started := time.Now()
		var noJSON bool
		cursor, noJSON = c.read(ctx, s, rc, cursor)
		rc.Close()
		if ctx.Err() != nil {
			return
		}
		if noJSON {
			s.update(func(st *StreamStatus) {
				st.State, st.Format = "no-json", "kein JSON"
				st.Error = "Logs liegen nicht im JSON-Format vor (--accesslog.format=json fehlt) – werden nicht gelesen"
			})
			c.log.Warn("Pod liefert kein JSON-Log, wird übersprungen", "pod", pod.Name, "erneut_in", noJSONRecheck.String())
			sleep(ctx, noJSONRecheck)
			continue
		}
		if time.Since(started) > time.Minute {
			backoff = time.Second
		}
		s.update(func(st *StreamStatus) {
			if st.State == "streaming" {
				st.State = "waiting"
			}
		})
		sleep(ctx, backoff)
		backoff = min(backoff*2, 30*time.Second)
	}
}

func sinceOf(cursor int64) time.Time {
	if cursor == 0 {
		return time.Time{}
	}
	return time.Unix(0, cursor)
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}

// read verarbeitet einen Logstream bis zu dessen Ende. Liefert den neuen Cursor und
// ob der Stream abgebrochen wurde, weil er kein JSON enthält.
func (c *Collector) read(ctx context.Context, s *stream, rc io.Reader, cursor int64) (int64, bool) {
	br := bufio.NewReaderSize(rc, 64<<10)
	in := c.sink.Ingest()
	var jsonSeen, textSeen int64
	s.mu.Lock()
	jsonSeen = s.st.Access + s.st.System
	podName := s.st.Pod
	s.mu.Unlock()
	var rec model.Record
	lastCursorSent := time.Now()
	skippedSinceCursor := 0
	resume := cursor // bis hierhin wurde bereits alles verarbeitet (sinceTime ist nur sekundengenau)
	send := func(it store.Item) bool {
		select {
		case in <- it:
			return true
		case <-ctx.Done():
			return false
		}
	}
	for {
		line, err := readLine(br)
		if len(line) > 0 {
			tsRaw, rest, ok := bytes.Cut(line, []byte{' '})
			ts, perr := time.Parse(time.RFC3339Nano, string(tsRaw))
			if !ok || perr != nil {
				goto next
			}
			tsn := ts.UnixNano()
			if tsn <= resume {
				goto next
			}
			cursor = max(cursor, tsn)
			// rest verweist in den Lesepuffer -> einmal kopieren; alle Felder zeigen in diese Kopie.
			text := string(bytes.TrimSpace(rest))
			var kind model.Kind
			switch kind, rec = model.Decode(text, rec); kind {
			case model.KindAccess:
				a := model.NewAccess(rec, podName, "", ts.UnixMilli())
				a.CC = c.geo.Lookup(a.Client)
				if !send(store.Item{Key: s.key, TS: tsn, Access: a, Raw: text}) {
					return cursor, false
				}
				jsonSeen++
				skippedSinceCursor = 0
				s.update(func(st *StreamStatus) {
					st.Access++
					st.LastLine, st.Format = ts.UnixMilli(), "json"
					if st.State == "no-json" {
						st.State, st.Error = "streaming", ""
					}
				})
			case model.KindSystem:
				if !send(store.Item{Key: s.key, TS: tsn, Sys: model.NewSys(rec, podName, ts.UnixMilli())}) {
					return cursor, false
				}
				jsonSeen++
				s.update(func(st *StreamStatus) {
					st.System++
					st.LastLine = ts.UnixMilli()
					if st.Format != "json" {
						st.Format = "json (nur Systemlog)"
					}
				})
			default:
				textSeen++
				skippedSinceCursor++
				s.update(func(st *StreamStatus) { st.Skipped++ })
				if jsonSeen == 0 && textSeen >= sniffLines {
					return cursor, true
				}
				// Cursor auch ohne verwertbare Zeilen gelegentlich sichern.
				if skippedSinceCursor >= 500 || time.Since(lastCursorSent) > 5*time.Second {
					if !send(store.Item{Key: s.key, TS: tsn}) {
						return cursor, false
					}
					skippedSinceCursor, lastCursorSent = 0, time.Now()
				}
			}
		}
	next:
		if err != nil {
			if !errors.Is(err, io.EOF) && ctx.Err() == nil {
				s.update(func(st *StreamStatus) { st.Error = err.Error() })
			}
			return cursor, false
		}
	}
}

// readLine liest eine Zeile beliebiger Länge; überlange Zeilen werden verworfen.
func readLine(br *bufio.Reader) ([]byte, error) {
	line, err := br.ReadSlice('\n')
	if err == nil || !errors.Is(err, bufio.ErrBufferFull) {
		return line, err
	}
	buf := append([]byte(nil), line...)
	for errors.Is(err, bufio.ErrBufferFull) {
		line, err = br.ReadSlice('\n')
		if len(buf) < maxLine {
			buf = append(buf, line...)
		}
	}
	if len(buf) >= maxLine {
		return nil, err
	}
	return buf, err
}

// Status liefert alle Streams (aktive zuerst, dann kürzlich verschwundene Pods).
func (c *Collector) Status() (streams []StreamStatus, listErr string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, s := range c.streams {
		s.mu.Lock()
		streams = append(streams, s.st)
		s.mu.Unlock()
	}
	sort.Slice(streams, func(i, j int) bool { return streams[i].Pod+streams[i].Container < streams[j].Pod+streams[j].Container })
	return append(streams, c.gone...), c.listErr
}
