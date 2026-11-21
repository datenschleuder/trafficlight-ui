package store

import (
	"runtime"
	"testing"

	"traefikgui/internal/model"
)

// Speicherbedarf geladener Chunks je Eintrag (Richtwert für DETAIL_MAX_ENTRIES).
func TestChunkMemoryPerEntry(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	const hours, perHour = 20, 7000
	lines := benchLines(hours * perHour)
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	chunks := make([]*Chunk, hours)
	var rec model.Record
	var acc model.Access
	for h := range chunks {
		c := newChunk(int64(h))
		for _, l := range lines[h*perHour : (h+1)*perHour] {
			_, rec = model.Decode(l, rec)
			model.FillAccess(&acc, rec, "", "", 0)
			c.add(&acc)
		}
		c.compact()
		chunks[h] = c
	}
	runtime.GC()
	runtime.ReadMemStats(&after)
	per := float64(after.HeapAlloc-before.HeapAlloc) / float64(hours*perHour)
	strs := 0
	for _, c := range chunks {
		strs += len(c.strs)
	}
	t.Logf("%.0f Byte je Eintrag (%d Einträge, %.1f Wörterbucheinträge je 100 Einträge)", per, hours*perHour, float64(strs)*100/float64(hours*perHour))
	if per > 400 {
		t.Errorf("Chunk-Speicher je Eintrag zu hoch: %.0f Byte", per)
	}
	runtime.KeepAlive(chunks)
	runtime.KeepAlive(lines)
}
