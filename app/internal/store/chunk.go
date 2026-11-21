package store

import (
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"traefikgui/internal/model"
)

const (
	FlagInternal uint8 = 1
	FlagBot      uint8 = 2
	flagDead     uint8 = 128 // nicht lesbarer Rohdatensatz; hält nur den Listenindex frei
)

// Entry ist ein Access-Log-Eintrag in kompakter Form; Strings sind Indizes in das
// Wörterbuch des Chunks.
type Entry struct {
	T, Size                           int64
	Dur, ODur                         float32
	Host, Site, Path, Route           uint32
	Method, Proto, Client, UA         uint32
	Router, Service, EP, TLS, CC, Pod uint32
	XOff                              uint32
	Status, OStatus                   uint16
	XLen, Retries, Flags, Bin         uint8 // Bin: Klasse des Latenz-Histogramms
}

// Chunk hält alle Einträge einer Stunde (Redis-Liste raw:<stunde>). Einträge werden
// nur angehängt; Leser arbeiten auf einem Schnappschuss (View).
type Chunk struct {
	Hour int64

	loadMu sync.Mutex // serialisiert Laden und Anhängen durch den Writer
	loaded bool

	mu    sync.RWMutex
	strs  []string
	idx   map[string]uint32
	ents  []Entry
	xpool []uint32 // Paare (Feld, Wert) der dynamischen Felder
	base  int      // Listenindex von ents[0] (>0, wenn nur das Ende der Stunde geladen ist)

	used atomic.Int64
	n    atomic.Int64 // Anzahl Einträge (für die Cache-Buchhaltung)
}

func newChunk(hour int64) *Chunk {
	return &Chunk{Hour: hour, strs: []string{""}, idx: map[string]uint32{"": 0}}
}

func (c *Chunk) sym(s string) uint32 {
	if c.idx == nil { // nach dem Laden verworfen (siehe compact) -> für weitere Einträge neu aufbauen
		c.idx = make(map[string]uint32, len(c.strs)+64)
		for i, v := range c.strs {
			c.idx[v] = uint32(i)
		}
	}
	if id, ok := c.idx[s]; ok {
		return id
	}
	// Kopieren: s verweist meist in eine ganze Logzeile, die sonst im Speicher bliebe.
	s = strings.Clone(s)
	id := uint32(len(c.strs))
	c.strs = append(c.strs, s)
	c.idx[s] = id
	return id
}

// compact legt alle Wörterbuch-Strings in einen zusammenhängenden Speicherblock und
// verwirft den Suchindex (spart bei vielen eindeutigen Werten den Großteil des
// Speichers). Aufrufer hält c.mu bzw. besitzt den Chunk exklusiv.
func (c *Chunk) compact() {
	total := 0
	for _, s := range c.strs {
		total += len(s)
	}
	var sb strings.Builder
	sb.Grow(total)
	for _, s := range c.strs {
		sb.WriteString(s)
	}
	all, off := sb.String(), 0
	strs := make([]string, len(c.strs))
	for i, s := range c.strs {
		strs[i] = all[off : off+len(s)]
		off += len(s)
	}
	c.strs, c.idx = strs, nil
	c.ents = slices.Clip(c.ents)
	c.xpool = slices.Clip(c.xpool)
}

// add hängt einen Eintrag an (Aufrufer hält c.mu). a == nil erzeugt einen Platzhalter.
func (c *Chunk) add(a *model.Access) {
	if a == nil {
		c.ents = append(c.ents, Entry{Flags: flagDead})
		return
	}
	e := Entry{
		T: a.T, Size: a.Size, Dur: float32(a.Dur), ODur: float32(a.ODur),
		Host: c.sym(a.Host), Site: c.sym(a.Site), Path: c.sym(a.Path), Route: c.sym(a.Route),
		Method: c.sym(a.Method), Proto: c.sym(a.Proto), Client: c.sym(a.Client), UA: c.sym(a.UA),
		Router: c.sym(a.Router), Service: c.sym(a.Service), EP: c.sym(a.Entry), TLS: c.sym(a.TLS),
		CC: c.sym(a.CC), Pod: c.sym(a.Pod),
		Status: uint16(a.Status), OStatus: uint16(a.OStatus), Retries: uint8(min(max(a.Retries, 0), 255)),
		Bin: uint8(model.HistBin(a.Dur)),
	}
	if a.Internal {
		e.Flags |= FlagInternal
	}
	if a.Bot {
		e.Flags |= FlagBot
	}
	if n := len(a.Extra); n > 0 {
		e.XOff, e.XLen = uint32(len(c.xpool)), uint8(n)
		for _, kv := range a.Extra {
			c.xpool = append(c.xpool, c.sym(kv.K), c.sym(kv.V))
		}
	}
	c.ents = append(c.ents, e)
}

// View ist ein unveränderlicher Schnappschuss eines Chunks.
type View struct {
	Hour int64
	Base int
	Ents []Entry
	Strs []string
	X    []uint32
	c    *Chunk
}

func (c *Chunk) view() View {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return View{Hour: c.Hour, Base: c.base, Ents: c.ents, Strs: c.strs, X: c.xpool, c: c}
}

// lookup liefert den Wörterbuchindex eines Strings, sofern er im Schnappschuss vorkommt.
func (v *View) lookup(s string) (uint32, bool) {
	v.c.mu.RLock()
	idx := v.c.idx
	if idx != nil {
		id, ok := idx[s]
		v.c.mu.RUnlock()
		return id, ok && int(id) < len(v.Strs)
	}
	v.c.mu.RUnlock()
	for i, x := range v.Strs { // ohne Index: lineare Suche (einmal je Filterwert und Chunk)
		if x == s {
			return uint32(i), true
		}
	}
	return 0, false
}

func (v *View) extras(e *Entry) []uint32 {
	if e.XLen == 0 {
		return nil
	}
	return v.X[e.XOff : e.XOff+2*uint32(e.XLen)]
}

// ID ist die stabile Kennung eines Eintrags: "<stunde>-<listenindex>".
func entryID(hour int64, idx int) string {
	return strconv.FormatInt(hour, 10) + "-" + strconv.Itoa(idx)
}
