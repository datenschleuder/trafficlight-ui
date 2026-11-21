package store

import (
	"sort"
	"sync"

	"traefikgui/internal/model"
)

// Series ist eine Zeitreihe mit fester Schrittweite. C[0..4] = sonstige, 2xx, 3xx, 4xx, 5xx.
type Series struct {
	T0    int64       `json:"t0"`
	Step  int64       `json:"step"`
	N     int         `json:"n"`
	C     [5][]uint64 `json:"c"`
	Bytes []uint64    `json:"bytes,omitempty"`
	P50   []float64   `json:"p50,omitempty"` // -1 = keine Daten
	P95   []float64   `json:"p95,omitempty"`
	P99   []float64   `json:"p99,omitempty"`
	Sys   []uint64    `json:"sys,omitempty"`

	i0, off int64
	hist    [][]uint64
}

var stepsMs = func() []int64 {
	secs := []int64{1, 5, 10, 30, 60, 120, 300, 600, 900, 1800, 3600, 7200, 10800, 21600, 43200, 86400}
	out := make([]int64, len(secs))
	for i, s := range secs {
		out[i] = s * 1000
	}
	return out
}()

func pickStep(span int64, maxBars int64, minStep int64) int64 {
	for _, s := range stepsMs {
		if s >= minStep && span/s <= maxBars {
			return s
		}
	}
	return stepsMs[len(stepsMs)-1]
}

// newSeries legt eine Reihe über [from, to) an; off verschiebt die Bucket-Grenzen auf
// die lokale Zeitzone des Browsers.
func newSeries(from, to, step, off int64, detail bool) *Series {
	i0 := floorDiv(from+off, step)
	n := int(floorDiv(to-1+off, step)-i0) + 1
	if n < 1 {
		n = 1
	}
	if n > 4000 {
		n = 4000
	}
	s := &Series{T0: i0*step - off, Step: step, N: n, i0: i0, off: off}
	for i := range s.C {
		s.C[i] = make([]uint64, n)
	}
	if detail {
		s.Bytes = make([]uint64, n)
		s.hist = make([][]uint64, n)
	}
	return s
}

// shape liefert eine leere Reihe mit gleichem Raster.
func (s *Series) shape() *Series {
	o := &Series{T0: s.T0, Step: s.Step, N: s.N, i0: s.i0, off: s.off}
	for i := range o.C {
		o.C[i] = make([]uint64, s.N)
	}
	if s.Bytes != nil {
		o.Bytes = make([]uint64, s.N)
		o.hist = make([][]uint64, s.N)
	}
	return o
}

func (s *Series) index(t int64) int {
	i := int(floorDiv(t+s.off, s.Step) - s.i0)
	if i < 0 || i >= s.N {
		return -1
	}
	return i
}

func classIdx(status int) int {
	if c := status / 100; c >= 2 && c <= 5 {
		return c - 1
	}
	return 0
}

func (s *Series) add(t int64, status int, n, bytes uint64) int {
	i := s.index(t)
	if i < 0 {
		return -1
	}
	s.C[classIdx(status)][i] += n
	if s.Bytes != nil {
		s.Bytes[i] += bytes
	}
	return i
}

func (s *Series) addHist(i, bin int, n uint64) {
	if s.hist[i] == nil {
		s.hist[i] = make([]uint64, model.HistBins)
	}
	s.hist[i][bin] += n
}

func (s *Series) merge(o *Series) {
	for c := range s.C {
		for i, n := range o.C[c] {
			s.C[c][i] += n
		}
	}
	for i, n := range o.Bytes {
		s.Bytes[i] += n
	}
	for i, h := range o.hist {
		if h == nil {
			continue
		}
		if s.hist[i] == nil {
			s.hist[i] = h
			continue
		}
		for b, n := range h {
			s.hist[i][b] += n
		}
	}
}

func (s *Series) finish() {
	if s.hist == nil {
		return
	}
	s.P50, s.P95, s.P99 = make([]float64, s.N), make([]float64, s.N), make([]float64, s.N)
	for i, h := range s.hist {
		if h == nil {
			s.P50[i], s.P95[i], s.P99[i] = -1, -1, -1
			continue
		}
		s.P50[i], s.P95[i], s.P99[i] = round3(model.HistQuantile(h, .5)), round3(model.HistQuantile(h, .95)), round3(model.HistQuantile(h, .99))
	}
}

func round3(f float64) float64 { return float64(int64(f*1000+.5)) / 1000 }

const (
	slowBins    = model.HistBins / 2
	maxSlowKeys = 200_000
)

type slowAgg struct {
	n    uint64
	sum  float64
	hist [slowBins]uint32
}

type errKey struct {
	status int
	route  string
}

// agg sammelt alle Kennzahlen einer Auswertung. Jeder Worker füllt ein eigenes agg;
// am Ende werden sie zusammengeführt.
type agg struct {
	tl, se     *Series // Zeitleiste (äußeres Fenster) und Detailreihe (Zoom-Fenster); je optional
	zFrom, zTo int64

	n, s4, s5, bytes uint64
	hist             [model.HistBins]uint64
	tMin, tMax       int64

	status                                 map[int]uint64
	methods, protos, sites, countries, pod map[string]uint64
	services                               map[[2]string]uint64

	detail        bool // auch Einzel-Request-Dimensionen sammeln
	routes        map[string]uint64
	clients       map[string]*clientAgg
	uas           map[string]uint64
	errs          map[errKey]uint64
	slow          map[[2]string]*slowAgg
	fields, xvals map[string]uint64
	xfield        string
	scanned       int64
	lost          int      // aus der Client-Liste entfernte seltene Werte
	tupN          []uint64 // Würfel: Anzahl je Dimensions-Kombination
}

func newAgg(detail bool) *agg {
	g := &agg{detail: detail, status: map[int]uint64{}, methods: map[string]uint64{}, protos: map[string]uint64{},
		sites: map[string]uint64{}, countries: map[string]uint64{}, pod: map[string]uint64{}, services: map[[2]string]uint64{}}
	if detail {
		g.routes, g.clients, g.uas = map[string]uint64{}, map[string]*clientAgg{}, map[string]uint64{}
		g.errs, g.slow = map[errKey]uint64{}, map[[2]string]*slowAgg{}
		g.fields, g.xvals = map[string]uint64{}, map[string]uint64{}
	}
	return g
}

func (g *agg) like() *agg {
	o := newAgg(g.detail)
	o.zFrom, o.zTo, o.xfield = g.zFrom, g.zTo, g.xfield
	if g.tl != nil {
		o.tl = g.tl.shape()
	}
	if g.se != nil {
		o.se = g.se.shape()
	}
	return o
}

func addMap[K comparable](dst, src map[K]uint64) {
	for k, n := range src {
		dst[k] += n
	}
}

func (g *agg) merge(o *agg) {
	if g.tl != nil && o.tl != nil {
		g.tl.merge(o.tl)
	}
	if g.se != nil && o.se != nil {
		g.se.merge(o.se)
	}
	g.n, g.s4, g.s5, g.bytes = g.n+o.n, g.s4+o.s4, g.s5+o.s5, g.bytes+o.bytes
	g.scanned += o.scanned
	for i, n := range o.hist {
		g.hist[i] += n
	}
	if o.tMin != 0 && (g.tMin == 0 || o.tMin < g.tMin) {
		g.tMin = o.tMin
	}
	g.tMax = max(g.tMax, o.tMax)
	if len(o.tupN) > len(g.tupN) {
		g.tupN = append(g.tupN, make([]uint64, len(o.tupN)-len(g.tupN))...)
	}
	for id, n := range o.tupN {
		g.tupN[id] += n
	}
	addMap(g.status, o.status)
	addMap(g.methods, o.methods)
	addMap(g.protos, o.protos)
	addMap(g.sites, o.sites)
	addMap(g.countries, o.countries)
	addMap(g.pod, o.pod)
	addMap(g.services, o.services)
	if !g.detail {
		return
	}
	addMap(g.routes, o.routes)
	addMap(g.uas, o.uas)
	for k, v := range o.clients {
		if cur := g.clients[k]; cur != nil {
			cur.n += v.n
		} else {
			g.clients[k] = v
		}
	}
	g.lost += o.lost
	g.limit()
	addMap(g.errs, o.errs)
	addMap(g.fields, o.fields)
	addMap(g.xvals, o.xvals)
	for k, v := range o.slow {
		if cur := g.slow[k]; cur != nil {
			cur.n += v.n
			cur.sum += v.sum
			for i, n := range v.hist {
				cur.hist[i] += n
			}
		} else if len(g.slow) < maxSlowKeys {
			g.slow[k] = v
		}
	}
}

func (g *agg) inZoom(t int64) bool { return t >= g.zFrom && t < g.zTo }

// addRow zählt eine Würfel-Zeile, deren Bucket bei t beginnt. Die Aufteilung nach
// Dimensionen wird je Kombination gesammelt (tupN) und erst am Ende aufgelöst.
func (g *agg) addRow(r *Row, status int, t int64, timeline, panels bool) {
	if timeline && g.tl != nil {
		g.tl.add(t, status, r.N, 0)
	}
	if !panels || !g.inZoom(t) {
		return
	}
	g.n += r.N
	g.bytes += r.Bytes
	switch status / 100 {
	case 4:
		g.s4 += r.N
	case 5:
		g.s5 += r.N
	}
	if g.tMin == 0 || t < g.tMin {
		g.tMin = t
	}
	g.tMax = max(g.tMax, t)
	i := -1
	if g.se != nil {
		i = g.se.add(t, status, r.N, r.Bytes)
	}
	for _, h := range r.Hist {
		g.hist[h.I] += uint64(h.N)
		if i >= 0 {
			g.se.addHist(i, int(h.I), uint64(h.N))
		}
	}
	if int(r.T) >= len(g.tupN) {
		g.tupN = append(g.tupN, make([]uint64, int(r.T)+1-len(g.tupN))...)
	}
	g.tupN[r.T] += r.N
}

// foldTuples löst die je Kombination gesammelten Zähler in die Dimensionen auf.
func (g *agg) foldTuples(dims []*Dims) {
	for id, n := range g.tupN {
		if n == 0 {
			continue
		}
		d := dims[id]
		g.status[int(d.Status)] += n
		g.methods[d.Method] += n
		g.protos[d.Proto] += n
		g.sites[d.Site] += n
		g.countries[d.CC] += n
		g.pod[d.Pod] += n
		g.services[[2]string{d.Service, d.Router}] += n
	}
	g.tupN = nil
}

// scan wertet einen Chunk-Schnappschuss aus.
func (g *agg) scan(v *View, c *compiled) {
	nstr := len(v.Strs)
	// Zähler je Wörterbucheintrag; die Puffer werden über alle Chunks wiederverwendet.
	sc := scratchPool.Get().(*scratch)
	defer scratchPool.Put(sc)
	var sites, methods, protos, ccs, pods, routes, clients, uas, clientCC []uint32
	sites, methods, protos, ccs, pods = sc.get(0, nstr), sc.get(1, nstr), sc.get(2, nstr), sc.get(3, nstr), sc.get(4, nstr)
	if g.detail {
		routes, clients, uas, clientCC = sc.get(5, nstr), sc.get(6, nstr), sc.get(7, nstr), sc.get(8, nstr)
	}
	services := map[uint64]uint32{}
	errs := map[uint64]uint32{}
	slow := map[uint64]*slowAgg{}
	var fields, xvals []uint32
	var status [1000]uint32
	if g.detail {
		fields, xvals = sc.get(9, nstr), sc.get(10, nstr)
	}
	xf, hasXF := uint32(0), false
	if g.detail && g.xfield != "" {
		xf, hasXF = v.lookup(g.xfield)
	}
	ents := v.Ents
	for i := range ents {
		e := &ents[i]
		if !c.match(e) {
			continue
		}
		g.scanned++
		st := int(e.Status)
		if g.tl != nil {
			g.tl.add(e.T, st, 1, 0)
		}
		if !g.inZoom(e.T) {
			continue
		}
		g.n++
		g.bytes += uint64(e.Size)
		switch st / 100 {
		case 4:
			g.s4++
		case 5:
			g.s5++
		}
		if g.tMin == 0 || e.T < g.tMin {
			g.tMin = e.T
		}
		if e.T > g.tMax {
			g.tMax = e.T
		}
		bin := int(e.Bin)
		g.hist[bin]++
		if g.se != nil {
			if si := g.se.add(e.T, st, 1, uint64(e.Size)); si >= 0 {
				g.se.addHist(si, bin, 1)
			}
		}
		status[st]++
		sites[e.Site]++
		methods[e.Method]++
		protos[e.Proto]++
		ccs[e.CC]++
		pods[e.Pod]++
		services[uint64(e.Service)<<32|uint64(e.Router)]++
		if !g.detail {
			continue
		}
		routes[e.Route]++
		clients[e.Client]++
		clientCC[e.Client] = e.CC
		uas[e.UA]++
		if st >= 400 {
			errs[uint64(st)<<32|uint64(e.Route)]++
		}
		sk := uint64(e.Site)<<32 | uint64(e.Route)
		sa := slow[sk]
		if sa == nil && len(slow) < maxSlowKeys {
			sa = &slowAgg{}
			slow[sk] = sa
		}
		if sa != nil {
			sa.n++
			sa.sum += float64(e.Dur)
			sa.hist[bin/2]++
		}
		if e.XLen > 0 {
			xs := v.extras(e)
			for j := 0; j < len(xs); j += 2 {
				fields[xs[j]]++
				if hasXF && xs[j] == xf {
					xvals[xs[j+1]]++
				}
			}
		}
	}
	for st, n := range status {
		if n > 0 {
			g.status[st] += uint64(n)
		}
	}
	str := v.Strs
	fold := func(dst map[string]uint64, src []uint32) {
		for id, n := range src {
			if n > 0 {
				dst[str[id]] += uint64(n)
			}
		}
	}
	fold(g.sites, sites)
	fold(g.methods, methods)
	fold(g.protos, protos)
	fold(g.countries, ccs)
	fold(g.pod, pods)
	for k, n := range services {
		g.services[[2]string{str[k>>32], str[uint32(k)]}] += uint64(n)
	}
	if !g.detail {
		return
	}
	fold(g.routes, routes)
	fold(g.uas, uas)
	for id, n := range clients {
		if n > 0 {
			if cur := g.clients[str[id]]; cur != nil {
				cur.n += uint64(n)
			} else {
				g.clients[str[id]] = &clientAgg{n: uint64(n), cc: str[clientCC[id]]}
			}
		}
	}
	for k, n := range errs {
		g.errs[errKey{int(k >> 32), str[uint32(k)]}] += uint64(n)
	}
	for k, sa := range slow {
		key := [2]string{str[k>>32], str[uint32(k)]}
		if cur := g.slow[key]; cur != nil {
			cur.n += sa.n
			cur.sum += sa.sum
			for i, n := range sa.hist {
				cur.hist[i] += n
			}
		} else if len(g.slow) < maxSlowKeys {
			g.slow[key] = sa
		}
	}
	fold(g.fields, fields)
	fold(g.xvals, xvals)
	g.limit()
}

type clientAgg struct {
	n  uint64
	cc string
}

type scratch struct{ bufs [11][]uint32 }

var scratchPool = sync.Pool{New: func() any { return new(scratch) }}

func (s *scratch) get(i, n int) []uint32 {
	if cap(s.bufs[i]) < n {
		s.bufs[i] = make([]uint32, n, n+n/4)
	}
	b := s.bufs[i][:n]
	clear(b)
	return b
}

// maxKeys begrenzt die Listen mit potenziell unbegrenzt vielen Werten (Pfade,
// Clients, User-Agents …). Wird die Grenze überschritten, fallen die seltensten Werte
// heraus; die Top-Listen bleiben damit praktisch exakt, der Speicher beschränkt.
const maxKeys = 250_000

func pruneMap[K comparable, V any](m map[K]V, count func(V) uint64) (removed int) {
	for t := uint64(1); len(m) > maxKeys*3/4; t *= 2 {
		for k, v := range m {
			if count(v) <= t {
				delete(m, k)
				removed++
			}
		}
	}
	return removed
}

func (g *agg) limit() {
	id := func(n uint64) uint64 { return n }
	if len(g.routes) > maxKeys {
		pruneMap(g.routes, id)
	}
	if len(g.uas) > maxKeys {
		pruneMap(g.uas, id)
	}
	if len(g.xvals) > maxKeys {
		pruneMap(g.xvals, id)
	}
	if len(g.errs) > maxKeys {
		pruneMap(g.errs, id)
	}
	if len(g.clients) > maxKeys {
		g.lost += pruneMap(g.clients, func(c *clientAgg) uint64 { return c.n })
	}
}

// ---- Ergebnis ----

type Top struct {
	K string  `json:"k"`
	N uint64  `json:"n"`
	X string  `json:"x,omitempty"` // Zusatz: Router, Land oder Host
	S int     `json:"s,omitempty"` // Statuscode
	A float64 `json:"a,omitempty"` // Ø-Dauer ms
	P float64 `json:"p,omitempty"` // p95 ms
}

const topN = 50

func topOf(m map[string]uint64, limit int) []Top {
	out := make([]Top, 0, len(m))
	for k, n := range m {
		out = append(out, Top{K: k, N: n})
	}
	return cut(out, limit)
}

func cut(out []Top, limit int) []Top {
	sort.Slice(out, func(i, j int) bool {
		if out[i].N != out[j].N {
			return out[i].N > out[j].N
		}
		return out[i].K < out[j].K
	})
	if len(out) > limit {
		out = out[:limit]
	}
	return out
}

func (sa *slowAgg) p95() float64 {
	h := make([]uint64, model.HistBins)
	for i, n := range sa.hist {
		h[2*i+1] = uint64(n)
	}
	return model.HistQuantile(h, .95)
}
