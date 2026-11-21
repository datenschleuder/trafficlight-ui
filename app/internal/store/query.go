package store

import (
	"context"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"traefikgui/internal/model"
)

// Query beschreibt eine Dashboard-Auswertung. [From, To) ist das äußere Fenster
// (Zeitleiste); [ZFrom, ZTo) optional ein per Zeitleiste gewählter Ausschnitt.
type Query struct {
	F          Filter
	From, To   int64
	ZFrom, ZTo int64
	TZOff      int64  // Offset der Browser-Zeitzone in ms (Ost = positiv)
	XField     string // dynamisches Feld, dessen Top-Werte geliefert werden
}

type KPI struct {
	Count     uint64  `json:"count"`
	Total     uint64  `json:"total"` // ungefiltert im selben Zeitraum
	S4        uint64  `json:"s4"`
	S5        uint64  `json:"s5"`
	Bytes     uint64  `json:"bytes"`
	P50       float64 `json:"p50"`
	P95       float64 `json:"p95"`
	P99       float64 `json:"p99"`
	Clients   int     `json:"clients"`
	Countries int     `json:"countries"`
	TMin      int64   `json:"tMin"`
	TMax      int64   `json:"tMax"`
}

type Hist struct {
	Edges  []float64 `json:"edges"` // Obergrenzen in ms; letzter Wert 0 = offen
	Counts []uint64  `json:"counts"`
}

type Tops struct {
	Site    []Top `json:"site"`
	Service []Top `json:"service"`
	Country []Top `json:"country"`
	Pod     []Top `json:"pod"`
	Status  []Top `json:"status"`
	Method  []Top `json:"method"`
	Proto   []Top `json:"proto"`
	Route   []Top `json:"route"`
	Client  []Top `json:"client"`
	UA      []Top `json:"ua"`
	Err     []Top `json:"err"`
	Slow    []Top `json:"slow"`
	Fields  []Top `json:"fields"`
	X       []Top `json:"x"`
}

type Facets struct {
	Sites   []string `json:"sites"`
	Routers []string `json:"routers"`
	Methods []string `json:"methods"`
	Pods    []string `json:"pods"`
}

// Snapshot ist das komplette Dashboard für eine Query.
type Snapshot struct {
	Now      int64   `json:"now"`
	From     int64   `json:"from"`
	To       int64   `json:"to"`
	ZFrom    int64   `json:"zfrom"`
	ZTo      int64   `json:"zto"`
	Zoomed   bool    `json:"zoomed"`
	Mode     string  `json:"mode"`               // "detail": alles exakt aus Einzel-Requests; "rollup": Würfel + begrenzte Details
	ClipFrom int64   `json:"clipFrom,omitempty"` // Einzel-Request-Auswertungen decken nur [ClipFrom, ZTo) ab
	Scanned  int64   `json:"scanned"`
	Timeline *Series `json:"timeline"`
	Series   *Series `json:"series"`
	KPI      KPI     `json:"kpi"`
	Hist     Hist    `json:"hist"`
	Top      Tops    `json:"top"`
	Facets   Facets  `json:"facets"`
	XField   string  `json:"xfield,omitempty"`
	TookMs   float64 `json:"tookMs"`
}

type scanPlan struct {
	hours    []int64 // neueste zuerst
	clipped  bool
	clipFrom int64
}

// planScan wählt die Stunden, deren Einzel-Requests ausgewertet werden: von neu nach
// alt, bis das Budget (DetailMax) erreicht ist oder keine Rohdaten mehr vorliegen.
func (s *Store) planScan(from, to int64) scanPlan {
	p := scanPlan{clipFrom: from}
	rawFrom := s.rawFrom.Load()
	budget := int64(s.cfg.DetailMax)
	var sum int64
	s.mu.Lock()
	defer s.mu.Unlock()
	for h := floorDiv(to-1, hourMs); h >= floorDiv(from, hourMs); h-- {
		n := s.totals[h]
		if n == 0 {
			continue
		}
		if h < rawFrom || (sum+n > budget && len(p.hours) > 0) {
			p.clipped = true
			p.clipFrom = (h + 1) * hourMs
			break
		}
		p.hours = append(p.hours, h)
		sum += n
	}
	return p
}

// views lädt die Chunks der Stunden (parallel) und liefert Schnappschüsse.
func (s *Store) views(ctx context.Context, hours []int64) ([]View, error) {
	views := make([]View, len(hours))
	errs := make([]error, len(hours))
	sem := make(chan struct{}, max(2, runtime.GOMAXPROCS(0)))
	var wg sync.WaitGroup
	for i, h := range hours {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			c, err := s.chunk(ctx, h)
			if err != nil {
				errs[i] = err
				return
			}
			views[i] = c.view()
		}()
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return nil, err
		}
	}
	return views, nil
}

// scanViews wertet die Schnappschüsse parallel aus und führt die Teilergebnisse in g zusammen.
func scanViews(ctx context.Context, views []View, f *Filter, from, to int64, g *agg) {
	workers := min(len(views), max(1, runtime.GOMAXPROCS(0)))
	if workers == 0 {
		return
	}
	jobs := make(chan *View)
	parts := make([]*agg, workers)
	var wg sync.WaitGroup
	for w := range workers {
		parts[w] = g.like()
		wg.Add(1)
		go func() {
			defer wg.Done()
			for v := range jobs {
				if ctx.Err() != nil {
					continue
				}
				if c, ok := f.compile(v, from, to); ok {
					parts[w].scan(v, c)
				}
			}
		}()
	}
	for i := range views {
		jobs <- &views[i]
	}
	close(jobs)
	wg.Wait()
	for _, p := range parts {
		g.merge(p)
	}
}

func (s *Store) clampWindow(from, to int64) (int64, int64) {
	now := s.nowMs()
	if to <= 0 || to > now+60_000 {
		to = now + 1000
	}
	if lo := now - s.cfg.Retention.Milliseconds(); from < lo {
		from = lo
	}
	if from >= to {
		from = to - 60_000
	}
	return from, to
}

// Snapshot berechnet das Dashboard.
//
// Liegt der Zeitraum vollständig im Detail-Budget, wird alles exakt aus den
// Einzel-Requests berechnet ("detail"). Sonst kommen Zeitreihen, KPIs und die
// Dimensionen des Würfels aus den Voraggregaten ("rollup") und nur die
// Einzel-Request-Dimensionen (Pfade, Clients, User-Agents, dynamische Felder) aus dem
// jüngsten Teil des Zeitraums (ClipFrom).
func (s *Store) Snapshot(ctx context.Context, q Query) (*Snapshot, error) {
	t0 := time.Now()
	now := s.nowMs()
	from, to := s.clampWindow(q.From, q.To)
	zFrom, zTo := from, to
	zoomed := false
	if q.ZFrom > 0 && q.ZTo > q.ZFrom {
		zFrom, zTo = max(q.ZFrom, from), min(q.ZTo, to)
		zoomed = zTo > zFrom && (zFrom != from || zTo != to)
		if !zoomed {
			zFrom, zTo = from, to
		}
	}
	cubeable := !q.F.NeedsDetail()
	snap := &Snapshot{Now: now, From: from, To: to, ZFrom: zFrom, ZTo: zTo, Zoomed: zoomed, XField: q.XField}

	// Plan für die Einzel-Requests: Zoom-Fenster; ohne Würfel-fähigen Filter muss auch
	// die Zeitleiste gescannt werden, dann das äußere Fenster.
	scanFrom, scanTo := zFrom, zTo
	cubeTimeline := cubeable && zoomed
	plan := s.planScan(zFrom, zTo)
	if !cubeable && zoomed {
		scanFrom, scanTo = from, to
		plan = s.planScan(from, to)
	}
	cubePanels := cubeable && plan.clipped
	if cubePanels {
		cubeTimeline = true
	}

	// Auflösung des Würfels: so fein wie möglich, ohne zu viele Abschnitte zu laden.
	res := s.Hour
	switch {
	case from >= now-s.Minute.TTL.Milliseconds()+hourMs && to-from <= 50*hourMs:
		res = s.Minute
	case to-from > 14*dayMs:
		res = s.Day
	}
	tlStep := pickStep(to-from, 240, 0)
	seStep := pickStep(zTo-zFrom, 120, 0)
	if cubeTimeline {
		tlStep = pickStep(to-from, 240, res.Step)
	}
	if cubePanels {
		seStep = pickStep(zTo-zFrom, 120, res.Step)
	}

	g := newAgg(false)
	g.zFrom, g.zTo = zFrom, zTo
	g.tl = newSeries(from, to, tlStep, q.TZOff, false)
	g.se = newSeries(zFrom, zTo, seStep, q.TZOff, true)
	// Einzel-Requests schon laden, während der Würfel ausgewertet wird.
	type viewsResult struct {
		views []View
		err   error
	}
	viewsCh := make(chan viewsResult, 1)
	go func() {
		v, err := s.views(ctx, plan.hours)
		viewsCh <- viewsResult{v, err}
	}()

	buckets, err := s.loadBuckets(ctx, res, floorDiv(from, res.Step), floorDiv(to-1, res.Step))
	if err != nil {
		return nil, err
	}
	// Würfel parallel auswerten: je Worker ein eigenes Teilergebnis.
	workers := max(1, min(runtime.GOMAXPROCS(0), len(buckets)/64+1))
	type cubePart struct {
		g     *agg
		sys   []uint64
		total uint64
	}
	parts := make([]cubePart, workers)
	// Filter je Dimensions-Kombination nur einmal auswerten (0 = offen, 1 = passt, 2 = passt nicht).
	dims := s.tup.snapshot()
	var wg sync.WaitGroup
	for w := range workers {
		parts[w] = cubePart{g: g.like(), sys: make([]uint64, g.tl.N)}
		wg.Add(1)
		go func() {
			defer wg.Done()
			p := &parts[w]
			match := make([]uint8, len(dims))
			for i := w; i < len(buckets); i += workers {
				b := buckets[i]
				t := b.start()
				inZoom := t+res.Step > zFrom && t < zTo
				tt := max(t, from)
				if inZoom {
					tt = max(t, zFrom)
				}
				b.mu.RLock()
				for ri := range b.rows {
					r := &b.rows[ri]
					if inZoom {
						p.total += r.N
					}
					if !(cubeTimeline || cubePanels) || int(r.T) >= len(dims) {
						continue // Kombination kam erst während der Abfrage hinzu
					}
					if match[r.T] == 0 {
						match[r.T] = 2
						if q.F.MatchDims(dims[r.T]) {
							match[r.T] = 1
						}
					}
					if match[r.T] == 1 {
						p.g.addRow(r, int(dims[r.T].Status), tt, cubeTimeline, cubePanels)
					}
				}
				for k, n := range b.sys {
					if lv, _, _ := strings.Cut(k, sep); model.IsAlert(lv) {
						if i := g.tl.index(max(t, from)); i >= 0 {
							p.sys[i] += uint64(n)
						}
					}
				}
				b.mu.RUnlock()
				b.used.Store(now)
			}
		}()
	}
	wg.Wait()
	sys := make([]uint64, g.tl.N)
	var total uint64
	for i := range parts {
		g.merge(parts[i].g)
		total += parts[i].total
		for j, n := range parts[i].sys {
			sys[j] += n
		}
	}

	g.foldTuples(dims)

	// Einzel-Requests.
	d := newAgg(true)
	d.zFrom, d.zTo, d.xfield = zFrom, zTo, q.XField
	if !cubeTimeline {
		d.tl = g.tl.shape()
	}
	if !cubePanels {
		d.se = g.se.shape()
	}
	vr := <-viewsCh
	if vr.err != nil {
		return nil, vr.err
	}
	views := vr.views
	clipFrom := int64(0)
	if plan.clipped {
		clipFrom = plan.clipFrom
	}
	for i := range views {
		if v := &views[i]; v.Base > 0 && len(v.Ents) > 0 { // übergroße Stunde: nur das Ende geladen
			clipFrom = max(clipFrom, v.Ents[0].T)
		}
	}
	scanViews(ctx, views, &q.F, max(scanFrom, clipFrom), scanTo, d)
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// Zusammenführen: Würfel-Teile aus g, alles Übrige aus d.
	src := d
	if cubePanels {
		src = g
		snap.Mode = "rollup"
	} else {
		snap.Mode = "detail"
		g.se = d.se
	}
	if !cubeTimeline {
		g.tl = d.tl
	}
	if clipFrom > 0 {
		snap.ClipFrom = clipFrom
		if !cubePanels && (clipFrom > zFrom || !cubeTimeline && clipFrom > from) {
			snap.Mode = "clipped" // nicht Würfel-fähiger Filter: alles nur ab ClipFrom
		}
	}
	g.tl.Sys = sys
	g.se.finish()
	snap.Timeline, snap.Series = g.tl, g.se
	snap.Scanned = int64(d.n)

	hist := src.hist[:]
	countries := 0
	for cc := range src.countries {
		if cc != "" && cc != "LAN" && cc != "??" {
			countries++
		}
	}
	snap.KPI = KPI{Count: src.n, Total: max(total, src.n), S4: src.s4, S5: src.s5, Bytes: src.bytes,
		P50: round3(model.HistQuantile(hist, .5)), P95: round3(model.HistQuantile(hist, .95)),
		P99: round3(model.HistQuantile(hist, .99)), Clients: len(d.clients) + d.lost, Countries: countries,
		TMin: src.tMin, TMax: src.tMax}
	if cubePanels && src.n > 0 { // Würfel kennt nur Bucket-Grenzen
		snap.KPI.TMin, snap.KPI.TMax = max(src.tMin, zFrom), min(src.tMax+res.Step, zTo, now)
	}
	snap.Hist = displayHist(hist)

	tp := &snap.Top
	tp.Site = topOf(src.sites, topN)
	tp.Country = topOf(src.countries, topN)
	tp.Pod = topOf(src.pod, topN)
	tp.Method = topOf(src.methods, 12)
	tp.Proto = topOf(src.protos, 12)
	for st, n := range src.status {
		tp.Status = append(tp.Status, Top{S: st, N: n})
	}
	sort.Slice(tp.Status, func(i, j int) bool { return tp.Status[i].S < tp.Status[j].S })
	for k, n := range src.services {
		tp.Service = append(tp.Service, Top{K: k[0], X: k[1], N: n})
	}
	tp.Service = cut(tp.Service, topN)
	tp.Route = topOf(d.routes, topN)
	tp.UA = topOf(d.uas, topN)
	tp.Fields = topOf(d.fields, 200)
	tp.X = topOf(d.xvals, topN)
	for k, c := range d.clients {
		tp.Client = append(tp.Client, Top{K: k, N: c.n, X: c.cc})
	}
	tp.Client = cut(tp.Client, topN)
	for k, n := range d.errs {
		tp.Err = append(tp.Err, Top{K: k.route, S: k.status, N: n})
	}
	tp.Err = cut(tp.Err, topN)
	for k, sa := range d.slow {
		if sa.n >= 3 {
			tp.Slow = append(tp.Slow, Top{K: k[1], X: k[0], N: sa.n, A: round3(sa.sum / float64(sa.n)), P: round3(sa.p95())})
		}
	}
	sort.Slice(tp.Slow, func(i, j int) bool {
		if tp.Slow[i].P != tp.Slow[j].P {
			return tp.Slow[i].P > tp.Slow[j].P
		}
		return tp.Slow[i].K < tp.Slow[j].K
	})
	if len(tp.Slow) > topN {
		tp.Slow = tp.Slow[:topN]
	}

	snap.Facets = s.facets()
	snap.TookMs = float64(time.Since(t0).Microseconds()) / 1000
	return snap, nil
}

// displayHist fasst die feinen Klassen zu den Anzeige-Klassen zusammen.
func displayHist(h []uint64) Hist {
	edges := []float64{1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 0}
	counts := make([]uint64, len(edges))
	for bin, n := range h {
		if n == 0 {
			continue
		}
		// Mitte der Klasse (geometrisch) entscheidet über die Anzeige-Klasse.
		mid := model.HistUpper(bin) / 1.14
		i := len(edges) - 1
		for j, e := range edges[:len(edges)-1] {
			if mid < e {
				i = j
				break
			}
		}
		counts[i] += n
	}
	return Hist{Edges: edges, Counts: counts}
}

// ---- Auswahllisten (alle in geladenen Würfeln gesehenen Werte) ----

const maxFacet = 2000

func (s *Store) noteFacets(site, router, method, pod string) {
	s.facetMu.Lock()
	for i, v := range [4]string{site, router, method, pod} {
		if s.facet[i] == nil {
			s.facet[i] = map[string]struct{}{}
		}
		if _, ok := s.facet[i][v]; !ok && len(s.facet[i]) < maxFacet {
			s.facet[i][v] = struct{}{}
		}
	}
	s.facetMu.Unlock()
}

func (s *Store) facets() Facets {
	keys := func(m map[string]struct{}) []string {
		out := make([]string, 0, len(m))
		for k := range m {
			out = append(out, k)
		}
		sort.Strings(out)
		return out
	}
	s.facetMu.Lock()
	defer s.facetMu.Unlock()
	return Facets{Sites: keys(s.facet[0]), Routers: keys(s.facet[1]), Methods: keys(s.facet[2]), Pods: keys(s.facet[3])}
}
