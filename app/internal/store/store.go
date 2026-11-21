// Package store speichert Traefik-Logs in Redis und beantwortet Auswertungen.
//
// Redis-Schlüssel (Präfix konfigurierbar, Standard "tg:"):
//
//	raw:<stunde>        LIST   vollständige JSON-Einträge (inkl. dynamischer Felder), TTL = Detail-Aufbewahrung
//	rawz:<stunde>       LIST   dieselben Einträge abgeschlossener Stunden in gzip-Blöcken (siehe raw.go)
//	rawn                HASH   Anzahl der je Stunde nach rawz verschobenen Einträge
//	cube:<m|h|d>:<idx>  HASH   voraggregierter Würfel (Minute: 48 h, Stunde/Tag: Aufbewahrung)
//	cubez:<m|h|d>:<idx> STRING verdichteter (gzip) Würfel abgeschlossener Zeitabschnitte
//	tot                 HASH   Anzahl Requests je Stunde
//	sys:<tag>           LIST   Traefik-Systemlogs
//	cursor              HASH   letzter verarbeiteter Kubelet-Zeitstempel je Pod/Container
//	seq                 STRING laufende Nummer des letzten geschriebenen Batches
//
// Es gibt genau einen Schreiber (Run). Alle Schreibzugriffe eines Batches laufen in
// einer MULTI/EXEC-Transaktion, damit Rohdaten, Würfel und Cursor konsistent bleiben.
package store

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"traefikgui/internal/model"
)

const (
	hourMs      = int64(3600_000)
	dayMs       = 24 * hourMs
	MaxDays     = 90
	batchMax    = 2000
	batchWait   = 250 * time.Millisecond
	ringSize    = 4096
	sysRingSize = 10_000
	sysPerDay   = 50_000
	loadPage    = 4000
)

type Config struct {
	Prefix       string
	Retention    time.Duration // Würfel + Systemlogs, max. 90 Tage
	RawRetention time.Duration // Einzel-Requests, <= Retention
	DetailMax    int           // max. Einträge, die für Detail-Auswertungen im Speicher liegen
	MemHighPct   int           // ab dieser Redis-Auslastung werden die ältesten Rohdaten verworfen
	Logger       *slog.Logger
}

// Item ist eine vom Collector gelesene Logzeile.
type Item struct {
	Key    string // Cursor-Schlüssel des Streams
	TS     int64  // Kubelet-Zeitstempel (Unix-ns)
	Access *model.Access
	Raw    string // Original-JSON der Access-Zeile
	Sys    *model.Sys
}

// Recent ist ein frisch eingegangener Eintrag für den Live-Stream.
type Recent struct {
	Seq uint64
	ID  string
	A   *model.Access
}

type SysRec struct {
	Seq uint64 `json:"seq"`
	model.Sys
}

type bkKey struct {
	res   *Res
	index int64
}

type Store struct {
	rdb redis.UniversalClient
	cfg Config
	log *slog.Logger
	in  chan Item

	Minute, Hour, Day *Res
	tup               *tuples

	mu      sync.Mutex
	chunks  map[int64]*Chunk
	buckets map[bkKey]*Bucket
	totals  map[int64]int64
	ring    []Recent
	ringSeq uint64
	sysRing []SysRec
	sysSeq  uint64
	notify  chan struct{}

	sealMu   sync.RWMutex    // schützt den Austausch raw -> rawz gegen Leser
	sealed   map[int64]int64 // Stunde -> Anzahl versiegelter Einträge (unter mu)
	sealKick chan struct{}
	sealBusy atomic.Bool

	facetMu sync.Mutex
	facet   [4]map[string]struct{}

	lens map[int64]int // nur Writer
	seq  int64         // nur Writer

	rawFrom       atomic.Int64 // älteste Stunde mit Rohdaten
	cachedEntries atomic.Int64
	cachedRows    atomic.Int64
	version       atomic.Uint64

	stIngested, stDropped, stSys atomic.Int64
	stLastWrite                  atomic.Int64
	stRedisUsed, stRedisMax      atomic.Int64
	stRedisErr                   atomic.Value // string
	stPruned                     atomic.Int64

	now func() time.Time
}

func New(rdb redis.UniversalClient, cfg Config) *Store {
	if cfg.Prefix == "" {
		cfg.Prefix = "tg:"
	}
	if cfg.Retention <= 0 || cfg.Retention > MaxDays*24*time.Hour {
		cfg.Retention = MaxDays * 24 * time.Hour
	}
	if cfg.RawRetention <= 0 || cfg.RawRetention > cfg.Retention {
		cfg.RawRetention = cfg.Retention
	}
	if cfg.DetailMax <= 0 {
		cfg.DetailMax = 1_000_000
	}
	if cfg.MemHighPct <= 0 || cfg.MemHighPct > 100 {
		cfg.MemHighPct = 85
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	s := &Store{
		rdb: rdb, cfg: cfg, log: cfg.Logger, in: make(chan Item, 4*batchMax),
		Minute:   &Res{Name: "m", Step: 60_000, TTL: 48 * time.Hour},
		Hour:     &Res{Name: "h", Step: hourMs, TTL: cfg.Retention},
		Day:      &Res{Name: "d", Step: dayMs, TTL: cfg.Retention},
		tup:      &tuples{idx: map[string]uint32{}},
		chunks:   map[int64]*Chunk{},
		buckets:  map[bkKey]*Bucket{},
		totals:   map[int64]int64{},
		lens:     map[int64]int{},
		sealed:   map[int64]int64{},
		sealKick: make(chan struct{}, 1),
		notify:   make(chan struct{}),
		ring:     make([]Recent, ringSize),
		now:      time.Now,
	}
	s.stRedisErr.Store("")
	s.tup.note = func(d *Dims) { s.noteFacets(d.Site, d.Router, d.Method, d.Pod) }
	return s
}

func (s *Store) Config() Config { return s.cfg }

func (s *Store) key(parts ...string) string { return s.cfg.Prefix + strings.Join(parts, ":") }
func (s *Store) rawKey(hour int64) string   { return s.key("raw", strconv.FormatInt(hour, 10)) }
func (s *Store) cubeKey(b *Bucket) string {
	return s.key("cube", b.res.Name, strconv.FormatInt(b.index, 10))
}
func (s *Store) blobKey(b *Bucket) string {
	return s.key("cubez", b.res.Name, strconv.FormatInt(b.index, 10))
}
func (s *Store) sysKey(day int64) string { return s.key("sys", strconv.FormatInt(day, 10)) }

func (s *Store) nowMs() int64 { return s.now().UnixMilli() }

// Ingest ist der Eingang für den Collector (blockiert bei Rückstau).
func (s *Store) Ingest() chan<- Item { return s.in }

// Changed liefert einen Kanal, der beim nächsten Schreibvorgang geschlossen wird.
func (s *Store) Changed() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.notify
}

func (s *Store) Version() uint64 { return s.version.Load() }

// Cursor liefert den zuletzt gespeicherten Kubelet-Zeitstempel (Unix-ns) eines Streams.
func (s *Store) Cursor(ctx context.Context, key string) (int64, error) {
	v, err := s.rdb.HGet(ctx, s.key("cursor"), key).Result()
	if errors.Is(err, redis.Nil) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	n, _ := strconv.ParseInt(v, 10, 64)
	return n, nil
}

// Init lädt Zähler und die jüngsten Systemlogs. Muss vor Run aufgerufen werden.
func (s *Store) Init(ctx context.Context) error {
	if err := s.rdb.Ping(ctx).Err(); err != nil {
		return err
	}
	tot, err := s.rdb.HGetAll(ctx, s.key("tot")).Result()
	if err != nil {
		return err
	}
	rawn, err := s.rdb.HGetAll(ctx, s.key("rawn")).Result()
	if err != nil {
		return err
	}
	seq, err := s.rdb.Get(ctx, s.key("seq")).Int64()
	if err != nil && !errors.Is(err, redis.Nil) {
		return err
	}
	s.seq = seq
	s.mu.Lock()
	for k, v := range tot {
		h, e1 := strconv.ParseInt(k, 10, 64)
		n, e2 := strconv.ParseInt(v, 10, 64)
		if e1 == nil && e2 == nil {
			s.totals[h] = n
		}
	}
	for k, v := range rawn {
		h, e1 := strconv.ParseInt(k, 10, 64)
		n, e2 := strconv.ParseInt(v, 10, 64)
		if e1 == nil && e2 == nil {
			s.sealed[h] = n
		}
	}
	s.mu.Unlock()
	if err := s.refreshRawFrom(ctx); err != nil {
		return err
	}
	// Jüngste Systemlogs in den Ringpuffer laden.
	today := s.nowMs() / dayMs
	var recs []model.Sys
	for d := today; d > today-int64(s.cfg.Retention/(24*time.Hour))-1 && len(recs) < sysRingSize; d-- {
		vals, err := s.rdb.LRange(ctx, s.sysKey(d), int64(-(sysRingSize - len(recs))), -1).Result()
		if err != nil {
			return err
		}
		day := make([]model.Sys, 0, len(vals))
		for _, v := range vals {
			var e model.Sys
			if json.Unmarshal([]byte(v), &e) == nil {
				day = append(day, e)
			}
		}
		recs = append(day, recs...)
	}
	s.mu.Lock()
	for _, e := range recs {
		s.sysSeq++
		s.sysRing = append(s.sysRing, SysRec{s.sysSeq, e})
	}
	s.mu.Unlock()
	s.refreshRedisInfo(ctx)
	return nil
}

func (s *Store) scanKeys(ctx context.Context, pattern string, fn func(key string)) error {
	it := s.rdb.Scan(ctx, 0, pattern, 1000).Iterator()
	for it.Next(ctx) {
		fn(it.Val())
	}
	return it.Err()
}

func (s *Store) refreshRawFrom(ctx context.Context) error {
	hours, err := s.rawHours(ctx)
	if err != nil {
		return err
	}
	from := s.nowMs()/hourMs + 1
	if len(hours) > 0 {
		from = hours[0]
	}
	s.rawFrom.Store(from)
	return nil
}

// Run verarbeitet eingehende Zeilen, bis ctx endet.
func (s *Store) Run(ctx context.Context) {
	go s.sealer(ctx)
	janitor := time.NewTicker(time.Minute)
	defer janitor.Stop()
	mem := time.NewTicker(5 * time.Second)
	defer mem.Stop()
	batch := make([]Item, 0, batchMax)
	timer := time.NewTimer(time.Hour)
	timer.Stop()
	lastCompact, lastMem := time.Time{}, time.Now()
	for {
		select {
		case <-ctx.Done():
			return
		case <-mem.C:
			s.relieve(ctx, false)
			s.evictBuckets()
			lastMem = time.Now()
		case <-janitor.C:
			s.janitor(ctx, time.Since(lastCompact) > 10*time.Minute)
			if time.Since(lastCompact) > 10*time.Minute {
				lastCompact = time.Now()
			}
		case it := <-s.in:
			batch = append(batch[:0], it)
			timer.Reset(batchWait)
		fill:
			for len(batch) < batchMax {
				select {
				case it := <-s.in:
					batch = append(batch, it)
				case <-timer.C:
					break fill
				case <-ctx.Done():
					return
				}
			}
			timer.Stop()
			if err := s.apply(ctx, batch); err != nil {
				return // nur bei ctx-Ende
			}
			// Unter Dauerlast kommt der Ticker nicht zum Zug -> Speicher hier prüfen.
			if time.Since(lastMem) > 3*time.Second {
				s.relieve(ctx, false)
				s.evictBuckets()
				lastMem = time.Now()
			}
		}
	}
}

// retry wiederholt fn mit Backoff, bis es gelingt oder ctx endet.
func (s *Store) retry(ctx context.Context, what string, fn func() error) error {
	delay := 500 * time.Millisecond
	for {
		err := fn()
		if err == nil {
			s.stRedisErr.Store("")
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		s.stRedisErr.Store(err.Error())
		s.log.Warn("Redis-Fehler, neuer Versuch", "bei", what, "err", err, "in", delay.String())
		if msg := err.Error(); strings.Contains(msg, "OOM") || strings.Contains(msg, "EXECABORT") {
			s.relieve(ctx, true) // Redis ist voll: Platz schaffen, sonst käme der Writer nie weiter
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
		delay = min(delay*2, 10*time.Second)
	}
}

type idxAccess struct {
	idx int
	a   *model.Access
}

// spliceRaw stellt dem Original-JSON die abgeleiteten Felder _t (Zeit in ms), _pod
// und _cc voran, damit ein Eintrag beim Neuladen identisch rekonstruiert wird.
func spliceRaw(raw string, t int64, pod, cc string) string {
	p, _ := json.Marshal(pod)
	c, _ := json.Marshal(cc)
	var b strings.Builder
	b.Grow(len(raw) + len(p) + len(c) + 40)
	b.WriteString(`{"_t":`)
	b.WriteString(strconv.FormatInt(t, 10))
	b.WriteString(`,"_pod":`)
	b.Write(p)
	b.WriteString(`,"_cc":`)
	b.Write(c)
	rest := strings.TrimSpace(raw)
	rest = strings.TrimSpace(rest[1:])
	if rest != "}" {
		b.WriteByte(',')
	}
	b.WriteString(rest)
	return b.String()
}

func (s *Store) apply(ctx context.Context, batch []Item) error {
	now := s.nowMs()
	minT := now - s.cfg.Retention.Milliseconds()
	minRaw := now - s.cfg.RawRetention.Milliseconds()

	// Phase A: alles laden, was der Batch berührt (wiederholbar, verändert nichts).
	type target struct {
		raw     bool
		buckets []*Bucket
	}
	targets := make([]target, len(batch))
	needLen := map[int64]struct{}{}
	needBk := map[*Bucket]struct{}{}
	bucketsFor := func(t int64) []*Bucket {
		var bs []*Bucket
		for _, res := range []*Res{s.Minute, s.Hour, s.Day} {
			if t >= now-res.TTL.Milliseconds() {
				b := s.bucket(res, floorDiv(t, res.Step))
				bs = append(bs, b)
				needBk[b] = struct{}{}
			}
		}
		return bs
	}
	for i := range batch {
		it := &batch[i]
		switch {
		case it.Access != nil:
			a := it.Access
			if a.T < minT || a.T > now+dayMs {
				s.stDropped.Add(1)
				it.Access = nil
				continue
			}
			targets[i].raw = a.T >= minRaw && len(it.Raw) > 1
			if targets[i].raw {
				needLen[a.T/hourMs] = struct{}{}
			}
			targets[i].buckets = bucketsFor(a.T)
		case it.Sys != nil:
			if it.Sys.T < minT || it.Sys.T > now+dayMs {
				it.Sys = nil
				continue
			}
			targets[i].buckets = bucketsFor(it.Sys.T)
		}
	}
	for h := range needLen {
		if _, ok := s.lens[h]; ok {
			continue
		}
		if err := s.retry(ctx, "LLEN", func() error {
			s.sealMu.RLock() // nicht mitten in einer Verdichtung zählen
			defer s.sealMu.RUnlock()
			n, err := s.rdb.LLen(ctx, s.rawKey(h)).Result()
			if err == nil {
				s.lens[h] = int(s.sealedN(h) + n)
			}
			return err
		}); err != nil {
			return err
		}
	}
	for b := range needBk {
		if err := s.retry(ctx, "Würfel laden", func() error { return s.loadBucket(ctx, b) }); err != nil {
			return err
		}
	}

	// Phase B: Würfel im Speicher fortschreiben, Redis-Kommandos vorbereiten.
	raw := map[int64][]any{}
	accs := map[int64][]idxAccess{}
	sysByDay := map[int64][]any{}
	cursors := map[string]int64{}
	totHours := map[int64]struct{}{}
	var recents []Recent
	var sysRecs []model.Sys
	for i := range batch {
		it := &batch[i]
		if it.Key != "" && it.TS > cursors[it.Key] {
			cursors[it.Key] = it.TS
		}
		switch {
		case it.Access != nil:
			a := it.Access
			h := a.T / hourMs
			id := ""
			if targets[i].raw {
				idx := s.lens[h]
				s.lens[h]++
				raw[h] = append(raw[h], spliceRaw(it.Raw, a.T, a.Pod, a.CC))
				accs[h] = append(accs[h], idxAccess{idx, a})
				id = entryID(h, idx)
			}
			for _, b := range targets[i].buckets {
				b.mu.Lock()
				before := b.size()
				b.addAccess(a)
				s.cachedRows.Add(int64(b.size() - before))
				b.mu.Unlock()
			}
			totHours[h] = struct{}{}
			s.mu.Lock()
			s.totals[h]++
			s.mu.Unlock()
			recents = append(recents, Recent{ID: id, A: a})
		case it.Sys != nil:
			e := it.Sys
			for _, b := range targets[i].buckets {
				b.mu.Lock()
				before := b.size()
				b.addSys(e)
				s.cachedRows.Add(int64(b.size() - before))
				b.mu.Unlock()
			}
			js, _ := json.Marshal(e)
			sysByDay[e.T/dayMs] = append(sysByDay[e.T/dayMs], string(js))
			sysRecs = append(sysRecs, *e)
		}
	}

	// Phase C: eine Transaktion; bei Fehlern wiederholen (seq erkennt bereits angewandte Batches).
	s.seq++
	seq := s.seq
	type bkWrite struct {
		b      *Bucket
		blob   []byte
		fields []any
	}
	var writes []bkWrite
	for b := range needBk {
		b.mu.RLock()
		if b.isDirty() {
			w := bkWrite{b: b}
			if b.blob {
				w.blob, _ = b.encodeBlob()
			} else {
				w.fields = b.dirtyFields()
			}
			writes = append(writes, w)
		}
		b.mu.RUnlock()
	}
	s.mu.Lock()
	totVals := make([]any, 0, 2*len(totHours))
	for h := range totHours {
		totVals = append(totVals, strconv.FormatInt(h, 10), s.totals[h])
	}
	s.mu.Unlock()
	exec := func() error {
		_, err := s.rdb.TxPipelined(ctx, func(p redis.Pipeliner) error {
			for h, vals := range raw {
				k := s.rawKey(h)
				p.RPush(ctx, k, vals...)
				p.PExpireAt(ctx, k, time.UnixMilli((h+1)*hourMs).Add(s.cfg.RawRetention))
			}
			for _, w := range writes {
				exp := time.UnixMilli(w.b.start() + w.b.res.Step).Add(w.b.res.TTL)
				if w.blob != nil {
					p.Set(ctx, s.blobKey(w.b), w.blob, 0)
					p.PExpireAt(ctx, s.blobKey(w.b), exp)
				} else {
					p.HSet(ctx, s.cubeKey(w.b), w.fields...)
					p.PExpireAt(ctx, s.cubeKey(w.b), exp)
				}
			}
			if len(totVals) > 0 {
				p.HSet(ctx, s.key("tot"), totVals...)
			}
			for d, vals := range sysByDay {
				k := s.sysKey(d)
				p.RPush(ctx, k, vals...)
				p.LTrim(ctx, k, -sysPerDay, -1)
				p.PExpireAt(ctx, k, time.UnixMilli((d+1)*dayMs).Add(s.cfg.Retention))
			}
			if len(cursors) > 0 {
				cv := make([]any, 0, 2*len(cursors))
				for k, ts := range cursors {
					cv = append(cv, k, ts)
				}
				p.HSet(ctx, s.key("cursor"), cv...)
			}
			p.Set(ctx, s.key("seq"), seq, 0)
			return nil
		})
		return err
	}
	first := true
	if err := s.retry(ctx, "Batch schreiben", func() error {
		if !first {
			// Antwort ging evtl. verloren, obwohl EXEC lief: nicht doppelt schreiben.
			cur, err := s.rdb.Get(ctx, s.key("seq")).Int64()
			if err != nil && !errors.Is(err, redis.Nil) {
				return err
			}
			if cur == seq {
				return nil
			}
		}
		first = false
		return exec()
	}); err != nil {
		return err
	}
	for _, w := range writes {
		w.b.mu.Lock()
		w.b.clearDirty()
		w.b.mu.Unlock()
	}

	// Phase D: geladene Chunks, Live-Ringpuffer und Zähler aktualisieren.
	for h, list := range accs {
		s.mu.Lock()
		c := s.chunks[h]
		s.mu.Unlock()
		if c == nil {
			continue
		}
		c.loadMu.Lock()
		gap := false
		if c.loaded {
			c.mu.Lock()
			for _, ia := range list {
				next := c.base + len(c.ents)
				if ia.idx == next {
					c.add(ia.a)
					c.n.Add(1)
					s.cachedEntries.Add(1)
				} else if ia.idx > next {
					gap = true // sollte nicht vorkommen: Chunk verwerfen, wird neu geladen
					break
				}
			}
			c.mu.Unlock()
		}
		c.loadMu.Unlock()
		if gap {
			s.dropChunk(c)
		}
	}
	for h := range raw {
		if rf := s.rawFrom.Load(); h < rf {
			s.rawFrom.Store(h)
		}
	}
	s.stIngested.Add(int64(len(recents)))
	s.stSys.Add(int64(len(sysRecs)))
	s.stLastWrite.Store(now)
	s.mu.Lock()
	for _, r := range recents {
		s.ringSeq++
		r.Seq = s.ringSeq
		s.ring[r.Seq%ringSize] = r
	}
	for _, e := range sysRecs {
		s.sysSeq++
		s.sysRing = append(s.sysRing, SysRec{s.sysSeq, e})
	}
	if len(s.sysRing) > sysRingSize+1000 {
		s.sysRing = append([]SysRec(nil), s.sysRing[len(s.sysRing)-sysRingSize:]...)
	}
	if len(recents) > 0 || len(sysRecs) > 0 {
		s.version.Add(1)
		close(s.notify)
		s.notify = make(chan struct{})
	}
	s.mu.Unlock()
	return nil
}

func floorDiv(a, b int64) int64 {
	q := a / b
	if a%b < 0 {
		q--
	}
	return q
}

// ---- Würfel-Buckets ----

func (s *Store) bucket(res *Res, index int64) *Bucket {
	k := bkKey{res, index}
	s.mu.Lock()
	defer s.mu.Unlock()
	b := s.buckets[k]
	if b == nil {
		b = &Bucket{res: res, index: index, reg: s.tup}
		s.buckets[k] = b
	}
	b.used.Store(s.nowMs())
	return b
}

func (s *Store) loadBucket(ctx context.Context, b *Bucket) error {
	b.loadMu.Lock()
	defer b.loadMu.Unlock()
	if b.loaded {
		return nil
	}
	p := s.rdb.Pipeline()
	blob := p.Get(ctx, s.blobKey(b))
	hash := p.HGetAll(ctx, s.cubeKey(b))
	if _, err := p.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return err
	}
	return s.fillBucket(b, blob, hash)
}

func (s *Store) fillBucket(b *Bucket, blob *redis.StringCmd, hash *redis.MapStringStringCmd) error {
	data, err := blob.Bytes()
	if err != nil && !errors.Is(err, redis.Nil) {
		return err
	}
	fields, err := hash.Result()
	if err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(data) > 0 {
		if err := b.decodeBlob(data); err != nil {
			s.log.Error("verdichteter Würfel nicht lesbar", "key", s.blobKey(b), "err", err)
		}
		b.blob = true
	}
	for k, v := range fields {
		b.setField(k, v)
	}
	b.loaded = true
	s.cachedRows.Add(int64(b.size()))
	return nil
}

// loadBuckets liefert die geladenen Buckets [i0, i1] einer Auflösung. Für die
// Stundenauflösung werden Stunden ohne Requests übersprungen.
func (s *Store) loadBuckets(ctx context.Context, res *Res, i0, i1 int64) ([]*Bucket, error) {
	var all, missing []*Bucket
	for i := i0; i <= i1; i++ {
		if res == s.Hour {
			s.mu.Lock()
			n := s.totals[i]
			_, known := s.buckets[bkKey{res, i}]
			s.mu.Unlock()
			if n == 0 && !known {
				continue
			}
		}
		b := s.bucket(res, i)
		all = append(all, b)
		b.loadMu.Lock()
		if !b.loaded {
			missing = append(missing, b)
		}
		b.loadMu.Unlock()
	}
	for len(missing) > 0 {
		part := missing[:min(len(missing), 256)]
		missing = missing[len(part):]
		for _, b := range part {
			b.loadMu.Lock()
		}
		p := s.rdb.Pipeline()
		blobs := make([]*redis.StringCmd, len(part))
		hashes := make([]*redis.MapStringStringCmd, len(part))
		for i, b := range part {
			if !b.loaded {
				blobs[i] = p.Get(ctx, s.blobKey(b))
				hashes[i] = p.HGetAll(ctx, s.cubeKey(b))
			}
		}
		_, err := p.Exec(ctx)
		if err != nil && errors.Is(err, redis.Nil) {
			err = nil
		}
		// Dekodieren (gzip + JSON) parallel: bei großen Zeiträumen der teuerste Teil.
		errs := make([]error, len(part))
		var wg sync.WaitGroup
		for i, b := range part {
			if err == nil && blobs[i] != nil {
				wg.Add(1)
				go func() {
					defer wg.Done()
					errs[i] = s.fillBucket(b, blobs[i], hashes[i])
				}()
			}
		}
		wg.Wait()
		for i, b := range part {
			if err == nil {
				err = errs[i]
			}
			b.loadMu.Unlock()
		}
		if err != nil {
			return nil, err
		}
	}
	return all, nil
}

// ---- Chunks ----

func (s *Store) dropChunk(c *Chunk) {
	s.mu.Lock()
	if s.chunks[c.Hour] == c {
		delete(s.chunks, c.Hour)
		s.cachedEntries.Add(-c.n.Load())
	}
	s.mu.Unlock()
}

// chunk liefert den (bei Bedarf aus Redis geladenen) Chunk einer Stunde.
func (s *Store) chunk(ctx context.Context, hour int64) (*Chunk, error) {
	s.mu.Lock()
	c := s.chunks[hour]
	if c == nil {
		c = newChunk(hour)
		s.chunks[hour] = c
	}
	c.used.Store(s.nowMs())
	s.mu.Unlock()

	c.loadMu.Lock()
	defer c.loadMu.Unlock()
	if c.loaded {
		return c, nil
	}
	fresh := newChunk(hour)
	var rec model.Record
	var acc model.Access // wird je Zeile wiederverwendet; add kopiert alle Strings
	base, err := s.readHour(ctx, hour, int64(s.cfg.DetailMax), func(v string) {
		var kind model.Kind
		if kind, rec = model.Decode(v, rec); kind == model.KindAccess {
			model.FillAccess(&acc, rec, "", "", 0)
			fresh.add(&acc)
		} else {
			fresh.add(nil)
		}
	})
	if err != nil {
		return nil, err
	}
	fresh.base = int(base)
	fresh.compact()
	c.mu.Lock()
	c.strs, c.idx, c.ents, c.xpool, c.base = fresh.strs, fresh.idx, fresh.ents, fresh.xpool, fresh.base
	c.mu.Unlock()
	c.loaded = true
	c.n.Store(int64(len(fresh.ents)))
	s.cachedEntries.Add(int64(len(fresh.ents)))
	s.evictChunks(hour)
	return c, nil
}

// evictChunks hält den Detail-Cache unter dem Budget (am längsten ungenutzte zuerst).
func (s *Store) evictChunks(keep int64) {
	limit := int64(s.cfg.DetailMax) * 5 / 4
	if s.cachedEntries.Load() <= limit {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	list := make([]*Chunk, 0, len(s.chunks))
	for _, c := range s.chunks {
		if c.Hour != keep {
			list = append(list, c)
		}
	}
	sort.Slice(list, func(i, j int) bool { return list[i].used.Load() < list[j].used.Load() })
	for _, c := range list {
		if s.cachedEntries.Load() <= limit {
			break
		}
		delete(s.chunks, c.Hour)
		s.cachedEntries.Add(-c.n.Load())
	}
}

// ---- Wartung (läuft im Writer, daher ohne Konkurrenz zu apply) ----

func (s *Store) refreshRedisInfo(ctx context.Context) (used, max int64) {
	info, err := s.rdb.Info(ctx, "memory").Result()
	if err != nil {
		return s.stRedisUsed.Load(), s.stRedisMax.Load()
	}
	for _, line := range strings.Split(info, "\n") {
		k, v, ok := strings.Cut(strings.TrimSpace(line), ":")
		if !ok {
			continue
		}
		switch k {
		case "used_memory":
			used, _ = strconv.ParseInt(v, 10, 64)
		case "maxmemory":
			max, _ = strconv.ParseInt(v, 10, 64)
		}
	}
	s.stRedisUsed.Store(used)
	s.stRedisMax.Store(max)
	return used, max
}

func (s *Store) janitor(ctx context.Context, compact bool) {
	now := s.nowMs()
	curHour := now / hourMs

	s.relieve(ctx, false)
	if err := s.refreshRawFrom(ctx); err != nil {
		s.log.Warn("Rohdaten-Schlüssel nicht lesbar", "err", err)
	}

	// 2. Abgelaufenes aus dem Speicher entfernen.
	minHour := (now - s.cfg.Retention.Milliseconds()) / hourMs
	minRawHour := (now - s.cfg.RawRetention.Milliseconds()) / hourMs
	var stale []string
	s.mu.Lock()
	for h := range s.totals {
		if h < minHour-1 {
			delete(s.totals, h)
			stale = append(stale, strconv.FormatInt(h, 10))
		}
	}
	for h, c := range s.chunks {
		if h < minRawHour-1 {
			delete(s.chunks, h)
			s.cachedEntries.Add(-c.n.Load())
		}
	}
	var staleSealed []string
	for h := range s.sealed {
		if h < minRawHour-1 {
			delete(s.sealed, h)
			staleSealed = append(staleSealed, strconv.FormatInt(h, 10))
		}
	}
	s.mu.Unlock()
	for h := range s.lens {
		if h < curHour-48 {
			delete(s.lens, h)
		}
	}
	if len(stale) > 0 {
		s.rdb.HDel(ctx, s.key("tot"), stale...)
	}
	if len(staleSealed) > 0 {
		s.rdb.HDel(ctx, s.key("rawn"), staleSealed...)
	}

	s.compactCubes(ctx)
	if !compact {
		return
	}
	// 4. Cursor verschwundener Pods aufräumen.
	if cur, err := s.rdb.HGetAll(ctx, s.key("cursor")).Result(); err == nil {
		limit := (now - s.cfg.Retention.Milliseconds() - dayMs) * 1e6
		for k, v := range cur {
			if ts, _ := strconv.ParseInt(v, 10, 64); ts < limit {
				s.rdb.HDel(ctx, s.key("cursor"), k)
			}
		}
	}
}

// ---- Status ----

type Stats struct {
	Ingested     int64  `json:"ingested"`
	Dropped      int64  `json:"dropped"`
	SysLines     int64  `json:"sysLines"`
	LastWrite    int64  `json:"lastWrite"`
	Queue        int    `json:"queue"`
	Stored       int64  `json:"stored"`
	OldestHour   int64  `json:"oldest"`
	RawFrom      int64  `json:"rawFrom"`
	RetentionD   int    `json:"retentionDays"`
	RawRetD      int    `json:"rawRetentionDays"`
	DetailMax    int    `json:"detailMax"`
	CachedEnts   int64  `json:"cachedEntries"`
	CachedRows   int64  `json:"cachedRows"`
	RedisUsed    int64  `json:"redisUsed"`
	RedisMax     int64  `json:"redisMax"`
	RedisErr     string `json:"redisErr,omitempty"`
	PrunedHours  int64  `json:"prunedHours"`
	StoreVersion uint64 `json:"version"`
}

func (s *Store) Stats() Stats {
	st := Stats{
		Ingested: s.stIngested.Load(), Dropped: s.stDropped.Load(), SysLines: s.stSys.Load(),
		LastWrite: s.stLastWrite.Load(), Queue: len(s.in), RawFrom: s.rawFrom.Load() * hourMs,
		RetentionD: int(s.cfg.Retention / (24 * time.Hour)), RawRetD: int(s.cfg.RawRetention / (24 * time.Hour)),
		DetailMax: s.cfg.DetailMax, CachedEnts: s.cachedEntries.Load(), CachedRows: s.cachedRows.Load(),
		RedisUsed: s.stRedisUsed.Load(), RedisMax: s.stRedisMax.Load(), RedisErr: s.stRedisErr.Load().(string),
		PrunedHours: s.stPruned.Load(), StoreVersion: s.version.Load(),
	}
	s.mu.Lock()
	for h, n := range s.totals {
		st.Stored += n
		if n > 0 && (st.OldestHour == 0 || h*hourMs < st.OldestHour) {
			st.OldestHour = h * hourMs
		}
	}
	s.mu.Unlock()
	return st
}

// Ping prüft die Redis-Verbindung (Readiness).
func (s *Store) Ping(ctx context.Context) error { return s.rdb.Ping(ctx).Err() }

// RecentSince liefert Live-Einträge mit Seq > seq (älteste zuerst). gap meldet, dass
// der Ringpuffer zwischenzeitlich überschrieben wurde.
func (s *Store) RecentSince(seq uint64) (out []Recent, next uint64, gap bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	next = s.ringSeq
	if seq >= next {
		return nil, next, false
	}
	first := uint64(1)
	if next > ringSize {
		first = next - ringSize + 1
	}
	if seq+1 < first {
		gap = true
		seq = first - 1
	}
	for q := seq + 1; q <= next; q++ {
		out = append(out, s.ring[q%ringSize])
	}
	return out, next, gap
}

// RingSeq ist die Sequenznummer des jüngsten Live-Eintrags.
func (s *Store) RingSeq() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ringSeq
}

// SysLogs liefert die jüngsten Systemlogs (neueste zuerst), optional ab Seq > since.
func (s *Store) SysLogs(since uint64, limit int) (out []SysRec, last uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	last = s.sysSeq
	for i := len(s.sysRing) - 1; i >= 0 && len(out) < limit; i-- {
		if s.sysRing[i].Seq <= since {
			break
		}
		out = append(out, s.sysRing[i])
	}
	return out, last
}

// compactCubes verdichtet abgeschlossene Würfel-Buckets (Hash -> gzip-Blob, ca. 10x
// kleiner). Läuft im Writer, daher ohne Konkurrenz zu apply.
func (s *Store) compactCubes(ctx context.Context) {
	now := s.nowMs()
	for _, res := range []*Res{s.Day, s.Hour, s.Minute} {
		prefix := s.key("cube", res.Name) + ":"
		limit := now/res.Step - 2
		var idxs []int64
		_ = s.scanKeys(ctx, prefix+"*", func(k string) {
			if i, err := strconv.ParseInt(strings.TrimPrefix(k, prefix), 10, 64); err == nil && i < limit {
				idxs = append(idxs, i)
			}
		})
		for _, i := range idxs {
			if ctx.Err() != nil {
				return
			}
			b := s.bucket(res, i)
			if err := s.loadBucket(ctx, b); err != nil {
				return
			}
			b.mu.Lock()
			blob, err := b.encodeBlob()
			if err == nil {
				exp := time.UnixMilli(b.start() + b.res.Step).Add(b.res.TTL)
				_, err = s.rdb.TxPipelined(ctx, func(p redis.Pipeliner) error {
					p.Set(ctx, s.blobKey(b), blob, 0)
					p.PExpireAt(ctx, s.blobKey(b), exp)
					p.Del(ctx, s.cubeKey(b))
					return nil
				})
			}
			if err == nil {
				b.blob = true
			}
			b.mu.Unlock()
			if err != nil {
				s.log.Warn("Würfel verdichten fehlgeschlagen", "err", err)
				return
			}
		}
	}
}

// maxCubeRows begrenzt die im Prozess gehaltenen Würfel-Zeilen (ca. 115 Byte je Zeile).
const maxCubeRows = 800_000

// evictBuckets entfernt abgelaufene Buckets und hält den Würfel-Cache unter der Grenze
// (abgeschlossene, unveränderte Buckets; am längsten ungenutzte zuerst). Läuft nur im
// Writer: so kann kein Bucket verschwinden, den apply gerade fortschreibt.
func (s *Store) evictBuckets() {
	now := s.nowMs()
	s.mu.Lock()
	defer s.mu.Unlock()
	var cand []*Bucket
	for k, b := range s.buckets {
		expired := b.start()+b.res.Step+b.res.TTL.Milliseconds() < now
		hot := b.start()+3*b.res.Step > now
		b.mu.RLock()
		size, idle := b.size(), b.loaded && !b.isDirty()
		b.mu.RUnlock()
		if expired {
			delete(s.buckets, k)
			if idle {
				s.cachedRows.Add(-int64(size))
			}
		} else if !hot && idle {
			cand = append(cand, b)
		}
	}
	if s.cachedRows.Load() <= maxCubeRows {
		return
	}
	sort.Slice(cand, func(i, j int) bool { return cand[i].used.Load() < cand[j].used.Load() })
	for _, b := range cand {
		if s.cachedRows.Load() <= maxCubeRows*3/4 {
			break
		}
		b.mu.RLock()
		size := b.size()
		b.mu.RUnlock()
		delete(s.buckets, bkKey{b.res, b.index})
		s.cachedRows.Add(-int64(size))
	}
}
