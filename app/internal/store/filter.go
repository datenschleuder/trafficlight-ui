package store

import (
	"strconv"
	"strings"

	"traefikgui/internal/model"
)

// Filter beschreibt die Auswahl von Access-Log-Einträgen. Leere Felder filtern nicht.
type Filter struct {
	Text    string // Volltext (klein geschrieben) über Pfad, UA, IP, Host, Land, Status, dynamische Felder
	Site    string // exakt (normalisierter Host)
	Host    string // Teilstring des normalisierten Hosts (klein geschrieben)
	Router  string
	Service string
	Method  string
	Pod     string
	Country string
	Client  string
	Route   string
	UA      string
	Status  int   // exakter Statuscode, 0 = egal
	Classes uint8 // Bitmaske erlaubter Statusklassen (Bit n = nxx); 0 = alle
	MinDur  float64
	NoInt   bool // interne Router (…@internal) ausblenden
	NoBots  bool
	X       []model.KV // dynamische Felder: Feld = Wert
}

const allClasses = 0x3f

func statusClass(st int) uint8 {
	c := st / 100
	if c < 0 || c > 5 {
		c = 0
	}
	return uint8(c)
}

// NeedsDetail meldet, ob der Filter Felder nutzt, die nur in den Einzel-Requests
// (nicht im voraggregierten Würfel) vorhanden sind.
func (f *Filter) NeedsDetail() bool {
	return f.Text != "" || f.Client != "" || f.Route != "" || f.UA != "" || f.MinDur > 0 || len(f.X) > 0
}

func (f *Filter) classOK(st int) bool {
	return f.Classes == 0 || f.Classes&allClasses == allClasses || f.Classes&(1<<statusClass(st)) != 0
}

// MatchDims prüft eine Dimensions-Kombination des Würfels (nur gültig, wenn !NeedsDetail()).
func (f *Filter) MatchDims(r *Dims) bool {
	if f.NoInt && r.Flags&FlagInternal != 0 || f.NoBots && r.Flags&FlagBot != 0 {
		return false
	}
	if !f.classOK(int(r.Status)) || f.Status != 0 && int(r.Status) != f.Status {
		return false
	}
	return (f.Site == "" || r.Site == f.Site) && (f.Host == "" || strings.Contains(r.Site, f.Host)) && (f.Router == "" || r.Router == f.Router) &&
		(f.Service == "" || r.Service == f.Service) && (f.Method == "" || r.Method == f.Method) &&
		(f.Pod == "" || r.Pod == f.Pod) && (f.Country == "" || r.CC == f.Country)
}

// MatchAccess prüft einen einzelnen Eintrag (Live-Stream).
func (f *Filter) MatchAccess(a *model.Access) bool {
	if f.NoInt && a.Internal || f.NoBots && a.Bot {
		return false
	}
	if !f.classOK(a.Status) || f.Status != 0 && a.Status != f.Status {
		return false
	}
	if f.Site != "" && a.Site != f.Site || f.Host != "" && !strings.Contains(a.Site, f.Host) || f.Router != "" && a.Router != f.Router ||
		f.Service != "" && a.Service != f.Service || f.Method != "" && a.Method != f.Method ||
		f.Pod != "" && a.Pod != f.Pod || f.Country != "" && a.CC != f.Country ||
		f.Client != "" && a.Client != f.Client || f.Route != "" && a.Route != f.Route ||
		f.UA != "" && a.UA != f.UA || a.Dur < f.MinDur {
		return false
	}
	for _, want := range f.X {
		ok := false
		for _, kv := range a.Extra {
			if kv.K == want.K && kv.V == want.V {
				ok = true
				break
			}
		}
		if !ok {
			return false
		}
	}
	if f.Text != "" {
		hit := func(s string) bool { return strings.Contains(strings.ToLower(s), f.Text) }
		if !(hit(a.Path) || hit(a.UA) || hit(a.Client) || hit(a.Host) || hit(a.CC) || hit(a.Service) ||
			hit(a.Router) || a.Status >= 100 && strings.Contains(strconv.Itoa(a.Status), f.Text)) {
			for _, kv := range a.Extra {
				if hit(kv.V) {
					return true
				}
			}
			return false
		}
	}
	return true
}

// compiled ist ein Filter, übersetzt in die Wörterbuch-Indizes eines Chunks.
type compiled struct {
	f                                         *Filter
	v                                         *View
	site, router, service, method, pod, cc    uint32
	client, route, ua                         uint32
	hasSite, hasRouter, hasService, hasMethod bool
	hasPod, hasCC, hasClient, hasRoute, hasUA bool
	minDur                                    float32
	x                                         [][2]uint32
	text, host                                []bool // je Wörterbucheintrag: enthält den Suchtext bzw. Host-Teilstring
	statusText                                [1000]bool
	from, to                                  int64
}

// compile übersetzt den Filter; ok=false heißt: in diesem Chunk kann nichts passen.
func (f *Filter) compile(v *View, from, to int64) (*compiled, bool) {
	c := &compiled{f: f, v: v, minDur: float32(f.MinDur), from: from, to: to}
	ok := true
	set := func(s string, id *uint32, has *bool) {
		if s == "" {
			return
		}
		*has = true
		if i, found := v.lookup(s); found {
			*id = i
		} else {
			ok = false
		}
	}
	set(f.Site, &c.site, &c.hasSite)
	set(f.Router, &c.router, &c.hasRouter)
	set(f.Service, &c.service, &c.hasService)
	set(f.Method, &c.method, &c.hasMethod)
	set(f.Pod, &c.pod, &c.hasPod)
	set(f.Country, &c.cc, &c.hasCC)
	set(f.Client, &c.client, &c.hasClient)
	set(f.Route, &c.route, &c.hasRoute)
	set(f.UA, &c.ua, &c.hasUA)
	for _, kv := range f.X {
		k, ok1 := v.lookup(kv.K)
		val, ok2 := v.lookup(kv.V)
		if !ok1 || !ok2 {
			ok = false
		}
		c.x = append(c.x, [2]uint32{k, val})
	}
	if !ok {
		return nil, false
	}
	if f.Host != "" {
		c.host = make([]bool, len(v.Strs))
		for i, s := range v.Strs {
			c.host[i] = strings.Contains(s, f.Host)
		}
	}
	if f.Text != "" {
		c.text = make([]bool, len(v.Strs))
		any := false
		for i, s := range v.Strs {
			if containsFold(s, f.Text) {
				c.text[i] = true
				any = true
			}
		}
		for st := range c.statusText {
			if st >= 100 && strings.Contains(strconv.Itoa(st), f.Text) {
				c.statusText[st] = true
				any = true
			}
		}
		if !any {
			return nil, false
		}
	}
	return c, true
}

// containsFold: Teilstring-Suche ohne Beachtung der Groß-/Kleinschreibung (needle ist klein).
func containsFold(s, needle string) bool {
	if len(needle) > len(s) {
		return false
	}
	for i := 0; i < len(s); i++ {
		if c := s[i]; c >= 'A' && c <= 'Z' || c >= 0x80 {
			return strings.Contains(strings.ToLower(s), needle)
		}
	}
	return strings.Contains(s, needle)
}

func (c *compiled) match(e *Entry) bool {
	if e.Flags&flagDead != 0 || e.T < c.from || e.T >= c.to {
		return false
	}
	f := c.f
	if f.NoInt && e.Flags&FlagInternal != 0 || f.NoBots && e.Flags&FlagBot != 0 {
		return false
	}
	if !f.classOK(int(e.Status)) || f.Status != 0 && int(e.Status) != f.Status {
		return false
	}
	if c.hasSite && e.Site != c.site || c.hasRouter && e.Router != c.router ||
		c.hasService && e.Service != c.service || c.hasMethod && e.Method != c.method ||
		c.hasPod && e.Pod != c.pod || c.hasCC && e.CC != c.cc ||
		c.hasClient && e.Client != c.client || c.hasRoute && e.Route != c.route ||
		c.hasUA && e.UA != c.ua || e.Dur < c.minDur {
		return false
	}
	if c.host != nil && !c.host[e.Site] {
		return false
	}
	if len(c.x) > 0 {
		xs := c.v.extras(e)
		for _, want := range c.x {
			ok := false
			for i := 0; i < len(xs); i += 2 {
				if xs[i] == want[0] && xs[i+1] == want[1] {
					ok = true
					break
				}
			}
			if !ok {
				return false
			}
		}
	}
	if c.text != nil {
		t := c.text
		if t[e.Path] || t[e.UA] || t[e.Client] || t[e.Host] || t[e.CC] || t[e.Service] || t[e.Router] ||
			c.statusText[e.Status] {
			return true
		}
		xs := c.v.extras(e)
		for i := 1; i < len(xs); i += 2 {
			if t[xs[i]] {
				return true
			}
		}
		return false
	}
	return true
}
