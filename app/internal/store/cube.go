package store

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"traefikgui/internal/model"
)

// Res ist eine Auflösung des voraggregierten Würfels.
type Res struct {
	Name string
	Step int64 // ms
	TTL  time.Duration
}

const (
	sep          = "\x1f"
	sysPrefix    = "!"
	maxRowsPerBk = 50_000
	maxTuples    = 500_000
	overflowSite = "(sonstige)"
)

// Dims ist eine Kombination aller Dimensionen mit geringer Kardinalität. Jede
// Kombination wird prozessweit nur einmal gehalten; Würfel-Zeilen verweisen per Index.
type Dims struct {
	Site, Service, Router, Method, Proto, CC, Pod string
	Status                                        uint16
	Flags                                         uint8
	key                                           string // Redis-Hash-Feld
}

type tuples struct {
	mu   sync.RWMutex
	idx  map[string]uint32
	list []*Dims
	note func(d *Dims)
}

func dimsKey(site, service, router, method string, status int, proto, cc, pod string, flags uint8) string {
	return strings.Join([]string{site, service, router, method, strconv.Itoa(status), proto, cc, pod,
		strconv.Itoa(int(flags))}, sep)
}

func parseDims(k string) (*Dims, bool) {
	p := strings.Split(k, sep)
	if len(p) != 9 {
		return nil, false
	}
	st, err1 := strconv.Atoi(p[4])
	fl, err2 := strconv.Atoi(p[8])
	if err1 != nil || err2 != nil {
		return nil, false
	}
	return &Dims{Site: p[0], Service: p[1], Router: p[2], Method: p[3], Status: uint16(st), Proto: p[5], CC: p[6],
		Pod: p[7], Flags: uint8(fl), key: k}, true
}

// id liefert den Index einer Kombination (legt sie bei Bedarf an).
func (t *tuples) id(key string) (uint32, bool) {
	t.mu.RLock()
	id, ok := t.idx[key]
	t.mu.RUnlock()
	if ok {
		return id, true
	}
	d, ok := parseDims(strings.Clone(key))
	if !ok {
		return 0, false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if id, ok := t.idx[key]; ok {
		return id, true
	}
	if len(t.list) >= maxTuples {
		return 0, false
	}
	id = uint32(len(t.list))
	t.list = append(t.list, d)
	t.idx[d.key] = id
	if t.note != nil {
		t.note(d)
	}
	return id, true
}

// snapshot liefert alle bisher bekannten Kombinationen (Index = Tupel-ID).
func (t *tuples) snapshot() []*Dims {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.list
}

// HistBin ist eine belegte Klasse des Latenz-Histogramms.
type HistBin struct {
	I uint8
	N uint32
}

// Row ist eine Zeile des Würfels: Verweis auf die Dimensionen plus Messwerte.
type Row struct {
	T               uint32 // Index in tuples
	N, Bytes, DurUs uint64
	Hist            []HistBin
}

func (r *Row) add(a *model.Access) {
	r.N++
	r.Bytes += uint64(a.Size)
	r.DurUs += uint64(a.Dur * 1000)
	bin := uint8(model.HistBin(a.Dur))
	for i := range r.Hist {
		if r.Hist[i].I == bin {
			r.Hist[i].N++
			return
		}
	}
	r.Hist = append(r.Hist, HistBin{bin, 1})
}

func (r *Row) encode() string {
	b := make([]byte, 0, 32+8*len(r.Hist))
	b = strconv.AppendUint(b, r.N, 10)
	b = append(b, ' ')
	b = strconv.AppendUint(b, r.Bytes, 10)
	b = append(b, ' ')
	b = strconv.AppendUint(b, r.DurUs, 10)
	for _, h := range r.Hist {
		b = append(b, ' ')
		b = strconv.AppendUint(b, uint64(h.I), 10)
		b = append(b, ':')
		b = strconv.AppendUint(b, uint64(h.N), 10)
	}
	return string(b)
}

func (r *Row) decode(v string) bool {
	p := strings.Fields(v)
	if len(p) < 3 {
		return false
	}
	var err error
	if r.N, err = strconv.ParseUint(p[0], 10, 64); err != nil {
		return false
	}
	r.Bytes, _ = strconv.ParseUint(p[1], 10, 64)
	r.DurUs, _ = strconv.ParseUint(p[2], 10, 64)
	r.Hist = make([]HistBin, 0, len(p)-3)
	for _, hb := range p[3:] {
		i, n, ok := strings.Cut(hb, ":")
		bi, e1 := strconv.ParseUint(i, 10, 8)
		bn, e2 := strconv.ParseUint(n, 10, 32)
		if ok && e1 == nil && e2 == nil && bi < model.HistBins {
			r.Hist = append(r.Hist, HistBin{uint8(bi), uint32(bn)})
		}
	}
	return true
}

// Bucket ist ein Zeitabschnitt des Würfels (Redis-Hash cube:<res>:<index> bzw. nach
// der Verdichtung ein gzip-Blob cubez:<res>:<index>).
type Bucket struct {
	res   *Res
	index int64 // Startzeit / Step
	reg   *tuples

	loadMu sync.Mutex
	loaded bool

	mu       sync.RWMutex
	rows     []Row
	pos      map[uint32]int32  // Tupel-ID -> Position in rows (erst beim Schreiben aufgebaut)
	sys      map[string]uint32 // "LEVEL\x1fpod" -> Anzahl Systemlogzeilen
	blob     bool              // liegt verdichtet in Redis
	dirty    map[uint32]struct{}
	dirtySys map[string]struct{}

	used atomic.Int64
}

func (b *Bucket) start() int64 { return b.index * b.res.Step }

func (b *Bucket) size() int { return len(b.rows) + len(b.sys) + 1 }

func (b *Bucket) isDirty() bool { return len(b.dirty) > 0 || len(b.dirtySys) > 0 }

func (b *Bucket) row(id uint32) *Row {
	if b.pos == nil {
		b.pos = make(map[uint32]int32, len(b.rows)+8)
		for i := range b.rows {
			b.pos[b.rows[i].T] = int32(i)
		}
	}
	if i, ok := b.pos[id]; ok {
		return &b.rows[i]
	}
	b.pos[id] = int32(len(b.rows))
	b.rows = append(b.rows, Row{T: id})
	return &b.rows[len(b.rows)-1]
}

// addAccess zählt einen Eintrag (Aufrufer hält b.mu).
func (b *Bucket) addAccess(a *model.Access) {
	var flags uint8
	if a.Internal {
		flags |= FlagInternal
	}
	if a.Bot {
		flags |= FlagBot
	}
	id, ok := b.reg.id(dimsKey(a.Site, a.Service, a.Router, a.Method, a.Status, a.Proto, a.CC, a.Pod, flags))
	if _, known := b.pos[id]; !ok || (!known && len(b.rows) >= maxRowsPerBk && b.pos != nil) {
		// Schutz vor Kardinalitäts-Explosion: Rest in einer Sammelzeile zählen.
		if id, ok = b.reg.id(dimsKey(overflowSite, "-", "-", a.Method, a.Status, "-", "", a.Pod, flags)); !ok {
			id, _ = b.reg.id(dimsKey(overflowSite, "-", "-", "OTHER", 0, "-", "", "-", 0))
		}
	}
	b.row(id).add(a)
	if b.dirty == nil {
		b.dirty = map[uint32]struct{}{}
	}
	b.dirty[id] = struct{}{}
}

func (b *Bucket) addSys(s *model.Sys) {
	key := s.Level + sep + s.Pod
	if b.sys == nil {
		b.sys = map[string]uint32{}
	}
	b.sys[key]++
	if b.dirtySys == nil {
		b.dirtySys = map[string]struct{}{}
	}
	b.dirtySys[key] = struct{}{}
}

// dirtyFields liefert die geänderten Redis-Hash-Felder als (Feld, Wert)-Paare.
func (b *Bucket) dirtyFields() []any {
	dims := b.reg.snapshot()
	out := make([]any, 0, 2*(len(b.dirty)+len(b.dirtySys)))
	for id := range b.dirty {
		out = append(out, dims[id].key, b.rows[b.pos[id]].encode())
	}
	for k := range b.dirtySys {
		out = append(out, sysPrefix+k, strconv.FormatUint(uint64(b.sys[k]), 10))
	}
	return out
}

func (b *Bucket) clearDirty() {
	b.dirty, b.dirtySys = nil, nil
}

func (b *Bucket) setField(field, value string) {
	if k, ok := strings.CutPrefix(field, sysPrefix); ok {
		if n, err := strconv.ParseUint(value, 10, 32); err == nil {
			if b.sys == nil {
				b.sys = map[string]uint32{}
			}
			b.sys[k] = uint32(n)
		}
		return
	}
	if id, ok := b.reg.id(field); ok {
		r := Row{T: id}
		if r.decode(value) {
			*b.row(id) = r
		}
	}
}

func (b *Bucket) fields() map[string]string {
	dims := b.reg.snapshot()
	m := make(map[string]string, len(b.rows)+len(b.sys))
	for i := range b.rows {
		m[dims[b.rows[i].T].key] = b.rows[i].encode()
	}
	for k, n := range b.sys {
		m[sysPrefix+k] = strconv.FormatUint(uint64(n), 10)
	}
	return m
}

func (b *Bucket) encodeBlob() ([]byte, error) {
	var buf bytes.Buffer
	zw, _ := gzip.NewWriterLevel(&buf, gzip.BestCompression)
	if err := json.NewEncoder(zw).Encode(b.fields()); err != nil {
		return nil, err
	}
	if err := zw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (b *Bucket) decodeBlob(data []byte) error {
	zr, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return err
	}
	raw, err := io.ReadAll(zr)
	if err != nil {
		return err
	}
	var m map[string]string
	if err := json.Unmarshal(raw, &m); err != nil {
		return err
	}
	b.rows = make([]Row, 0, len(m))
	for k, v := range m {
		b.setField(k, v)
	}
	return nil
}
