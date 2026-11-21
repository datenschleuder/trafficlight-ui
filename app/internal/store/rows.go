package store

import (
	"container/heap"
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"traefikgui/internal/model"
)

// RowsQuery fragt eine Seite der Request-Tabelle ab.
type RowsQuery struct {
	F        Filter
	From, To int64
	Sort     string // t (Standard), dur, size, status
	Asc      bool
	Limit    int
	Cursor   string // von der vorherigen Seite geliefert
	XField   string
}

// RowOut ist eine Tabellenzeile.
type RowOut struct {
	ID  string  `json:"id"`
	T   int64   `json:"t"`
	St  int     `json:"st"`
	M   string  `json:"m"`
	H   string  `json:"h"`
	P   string  `json:"p"`
	D   float32 `json:"d"`
	S   int64   `json:"s"`
	C   string  `json:"c"`
	CC  string  `json:"cc"`
	Pr  string  `json:"pr"`
	Sv  string  `json:"sv"`
	R   string  `json:"r"`
	UA  string  `json:"ua"`
	Pod string  `json:"pod"`
	X   string  `json:"x,omitempty"`
}

type RowsResult struct {
	Rows     []RowOut `json:"rows"`
	Next     string   `json:"next,omitempty"`
	ClipFrom int64    `json:"clipFrom,omitempty"`
}

const maxSortedRows = 10_000

func rowOut(v *View, i int, xfield uint32, hasX bool) RowOut {
	e := &v.Ents[i]
	s := v.Strs
	r := RowOut{ID: entryID(v.Hour, v.Base+i), T: e.T, St: int(e.Status), M: s[e.Method], H: s[e.Host], P: s[e.Path],
		D: e.Dur, S: e.Size, C: s[e.Client], CC: s[e.CC], Pr: s[e.Proto], Sv: s[e.Service], R: s[e.Router],
		UA: s[e.UA], Pod: s[e.Pod]}
	if hasX {
		xs := v.extras(e)
		for j := 0; j < len(xs); j += 2 {
			if xs[j] == xfield {
				r.X = s[xs[j+1]]
				break
			}
		}
	}
	return r
}

// RowFromAccess baut die Tabellenzeile eines Live-Eintrags.
func RowFromAccess(r Recent, xfield string) RowOut {
	a := r.A
	out := RowOut{ID: r.ID, T: a.T, St: a.Status, M: a.Method, H: a.Host, P: a.Path, D: float32(a.Dur), S: a.Size,
		C: a.Client, CC: a.CC, Pr: a.Proto, Sv: a.Service, R: a.Router, UA: a.UA, Pod: a.Pod}
	if xfield != "" {
		for _, kv := range a.Extra {
			if kv.K == xfield {
				out.X = kv.V
			}
		}
	}
	return out
}

type rowRef struct {
	v   *View
	i   int
	key float64
}

// Rows liefert eine Seite der Tabelle. Nach Zeit sortiert wird stundenweise gelesen
// (Keyset-Paginierung, beliebig tief); andere Sortierungen liefern die Top 10.000.
func (s *Store) Rows(ctx context.Context, q RowsQuery) (*RowsResult, error) {
	from, to := s.clampWindow(q.From, q.To)
	if q.Limit <= 0 || q.Limit > 1000 {
		q.Limit = 200
	}
	plan := s.planScan(from, to)
	res := &RowsResult{Rows: []RowOut{}}
	if plan.clipped {
		res.ClipFrom = plan.clipFrom
		from = max(from, plan.clipFrom)
	}
	hours := plan.hours // neueste zuerst
	if q.Sort == "" || q.Sort == "t" {
		if q.Asc {
			hours = slices.Clone(hours)
			slices.Reverse(hours)
		}
		var cT, cH, cI int64 = 0, 0, 0
		hasCur := false
		if p := strings.Split(q.Cursor, ":"); len(p) == 3 {
			cT, _ = strconv.ParseInt(p[0], 10, 64)
			cH, _ = strconv.ParseInt(p[1], 10, 64)
			cI, _ = strconv.ParseInt(p[2], 10, 64)
			hasCur = true
		}
		// before meldet, ob (t,h,i) in Sortierrichtung hinter dem Cursor liegt.
		after := func(t, h, i int64) bool {
			if !hasCur {
				return true
			}
			c := cmp3(t, h, i, cT, cH, cI)
			if q.Asc {
				return c > 0
			}
			return c < 0
		}
		for _, h := range hours {
			if hasCur && (q.Asc && h < cH || !q.Asc && h > cH) {
				continue
			}
			if len(res.Rows) >= q.Limit {
				break
			}
			c, err := s.chunk(ctx, h)
			if err != nil {
				return nil, err
			}
			v := c.view()
			cf, ok := q.F.compile(&v, from, to)
			if !ok {
				continue
			}
			var idx []int
			for i := range v.Ents {
				if cf.match(&v.Ents[i]) && after(v.Ents[i].T, h, int64(v.Base+i)) {
					idx = append(idx, i)
				}
			}
			slices.SortFunc(idx, func(a, b int) int {
				c := cmp3(v.Ents[a].T, h, int64(a), v.Ents[b].T, h, int64(b))
				if q.Asc {
					return c
				}
				return -c
			})
			xf, hasX := uint32(0), false
			if q.XField != "" {
				xf, hasX = v.lookup(q.XField)
			}
			for _, i := range idx {
				if len(res.Rows) >= q.Limit {
					res.Next = fmt.Sprintf("%d:%d:%d", res.Rows[len(res.Rows)-1].T, lastHour(res), lastIdx(res))
					return res, nil
				}
				res.Rows = append(res.Rows, rowOut(&v, i, xf, hasX))
			}
		}
		if len(res.Rows) >= q.Limit {
			res.Next = fmt.Sprintf("%d:%d:%d", res.Rows[len(res.Rows)-1].T, lastHour(res), lastIdx(res))
		}
		return res, nil
	}

	// Sortierung nach Messwert: Top-K über alle Stunden des Plans.
	offset, _ := strconv.Atoi(q.Cursor)
	if offset < 0 || offset >= maxSortedRows {
		return res, nil
	}
	k := min(offset+q.Limit, maxSortedRows)
	views, err := s.views(ctx, plan.hours)
	if err != nil {
		return nil, err
	}
	h := &rowHeap{asc: q.Asc}
	for vi := range views {
		v := &views[vi]
		cf, ok := q.F.compile(v, from, to)
		if !ok {
			continue
		}
		for i := range v.Ents {
			e := &v.Ents[i]
			if !cf.match(e) {
				continue
			}
			var key float64
			switch q.Sort {
			case "dur":
				key = float64(e.Dur)
			case "size":
				key = float64(e.Size)
			default:
				key = float64(e.Status)
			}
			ref := rowRef{v, i, key}
			if h.Len() < k {
				heap.Push(h, ref)
			} else if h.better(ref, h.refs[0]) {
				h.refs[0] = ref
				heap.Fix(h, 0)
			}
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
	}
	refs := h.refs
	slices.SortFunc(refs, func(a, b rowRef) int {
		if h.better(a, b) {
			return -1
		}
		if h.better(b, a) {
			return 1
		}
		return 0
	})
	for i := offset; i < len(refs) && i < k; i++ {
		xf, hasX := uint32(0), false
		if q.XField != "" {
			xf, hasX = refs[i].v.lookup(q.XField)
		}
		res.Rows = append(res.Rows, rowOut(refs[i].v, refs[i].i, xf, hasX))
	}
	if len(refs) >= k && k < maxSortedRows {
		res.Next = strconv.Itoa(k)
	}
	return res, nil
}

func lastHour(r *RowsResult) int64 {
	h, _, _ := strings.Cut(r.Rows[len(r.Rows)-1].ID, "-")
	n, _ := strconv.ParseInt(h, 10, 64)
	return n
}

func lastIdx(r *RowsResult) int64 {
	_, i, _ := strings.Cut(r.Rows[len(r.Rows)-1].ID, "-")
	n, _ := strconv.ParseInt(i, 10, 64)
	return n
}

func cmp3(t1, h1, i1, t2, h2, i2 int64) int {
	switch {
	case t1 != t2:
		if t1 < t2 {
			return -1
		}
		return 1
	case h1 != h2:
		if h1 < h2 {
			return -1
		}
		return 1
	case i1 != i2:
		if i1 < i2 {
			return -1
		}
		return 1
	}
	return 0
}

// rowHeap hält die besten k Zeilen; an der Wurzel liegt die schlechteste.
type rowHeap struct {
	refs []rowRef
	asc  bool
}

// better: a steht in der Ausgabe vor b.
func (h *rowHeap) better(a, b rowRef) bool {
	if a.key != b.key {
		return (a.key < b.key) == h.asc
	}
	ta, tb := a.v.Ents[a.i].T, b.v.Ents[b.i].T
	if ta != tb {
		return ta > tb
	}
	if a.v.Hour != b.v.Hour {
		return a.v.Hour > b.v.Hour
	}
	return a.i > b.i
}
func (h *rowHeap) Len() int           { return len(h.refs) }
func (h *rowHeap) Less(i, j int) bool { return h.better(h.refs[j], h.refs[i]) }
func (h *rowHeap) Swap(i, j int)      { h.refs[i], h.refs[j] = h.refs[j], h.refs[i] }
func (h *rowHeap) Push(x any)         { h.refs = append(h.refs, x.(rowRef)) }
func (h *rowHeap) Pop() any {
	x := h.refs[len(h.refs)-1]
	h.refs = h.refs[:len(h.refs)-1]
	return x
}

// Export ruft fn für alle passenden Einträge in zeitlicher Reihenfolge auf (max. limit).
func (s *Store) Export(ctx context.Context, f *Filter, from, to int64, limit int, fn func(a *model.Access, t int64) error) error {
	from, to = s.clampWindow(from, to)
	plan := s.planScan(from, to)
	if plan.clipped {
		from = max(from, plan.clipFrom)
	}
	n := 0
	for hi := len(plan.hours) - 1; hi >= 0 && n < limit; hi-- {
		c, err := s.chunk(ctx, plan.hours[hi])
		if err != nil {
			return err
		}
		v := c.view()
		cf, ok := f.compile(&v, from, to)
		if !ok {
			continue
		}
		var idx []int
		for i := range v.Ents {
			if cf.match(&v.Ents[i]) {
				idx = append(idx, i)
			}
		}
		slices.SortStableFunc(idx, func(a, b int) int { return cmp3(v.Ents[a].T, 0, 0, v.Ents[b].T, 0, 0) })
		str := v.Strs
		for _, i := range idx {
			if n >= limit {
				break
			}
			e := &v.Ents[i]
			a := &model.Access{T: e.T, Status: int(e.Status), Method: str[e.Method], Host: str[e.Host], Path: str[e.Path],
				Dur: float64(e.Dur), Size: e.Size, Client: str[e.Client], CC: str[e.CC], Proto: str[e.Proto],
				Router: str[e.Router], Service: str[e.Service], UA: str[e.UA], Pod: str[e.Pod]}
			if err := fn(a, e.T); err != nil {
				return err
			}
			n++
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
	return nil
}
