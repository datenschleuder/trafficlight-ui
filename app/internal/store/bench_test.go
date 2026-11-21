package store

import (
	"testing"
	"time"

	"traefikgui/internal/fakekube"
	"traefikgui/internal/model"
)

func benchLines(n int) []string {
	fk := fakekube.New(fakekube.Options{Pods: 2, Rate: 2})
	var lines []string
	for k := int64(3_000_000_000); len(lines) < n; k++ {
		for _, l := range fk.Lines(int(k%2), k) {
			if l[0] == '{' {
				lines = append(lines, spliceRaw(l, k*int64(time.Second/2)/1e6, "pod-x", "DE"))
			}
		}
	}
	return lines
}

// Kosten je Eintrag beim Laden eines Chunks aus Redis (ohne Netzwerk).
func BenchmarkDecode(b *testing.B) {
	lines := benchLines(20000)
	b.ReportAllocs()
	for b.Loop() {
		c := newChunk(1)
		var rec model.Record
		var acc model.Access
		for _, l := range lines {
			_, rec = model.Decode(l, rec)
			model.FillAccess(&acc, rec, "", "", 0)
			c.add(&acc)
		}
		c.compact()
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(len(lines)), "ns/entry")
}

// Kosten je Eintrag bei der Auswertung eines geladenen Chunks.
func BenchmarkScan(b *testing.B) {
	lines := benchLines(20000)
	c := newChunk(1)
	for _, l := range lines {
		_, m := model.Decode(l, nil)
		c.add(model.NewAccess(m, "", "", 0))
	}
	v := c.view()
	f := &Filter{NoInt: true}
	b.ReportAllocs()
	for b.Loop() {
		g := newAgg(true)
		g.zTo = 1 << 62
		g.se = newSeries(v.Ents[0].T, v.Ents[len(v.Ents)-1].T+1, 60_000, 0, true)
		cf, _ := f.compile(&v, 0, 1<<62)
		g.scan(&v, cf)
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(len(lines)), "ns/entry")
}
