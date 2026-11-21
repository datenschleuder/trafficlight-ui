package store

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"traefikgui/internal/fakekube"
	"traefikgui/internal/model"
)

var t0 = time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)

func newStore(t *testing.T, mr *miniredis.Miniredis, detailMax int, now time.Time) *Store {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rdb.Close() })
	s := New(rdb, Config{DetailMax: detailMax, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	s.now = func() time.Time { return now }
	if err := s.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	return s
}

// gen erzeugt die Zeilen zweier Pods über die letzten hours Stunden vor t0.
func gen(hours int) (items []Item, accs []*model.Access, sys int) {
	fk := fakekube.New(fakekube.Options{Pods: 2, Rate: 0.4})
	step := int64(time.Second * 5 / 2)
	for k := t0.Add(-time.Duration(hours)*time.Hour).UnixNano()/step + 1; k*step < t0.UnixNano(); k++ {
		ts := k * step
		for pod := range 2 {
			for _, line := range fk.Lines(pod, k) {
				name := fmt.Sprintf("pod-%d", pod)
				switch kind, m := model.Decode(line, nil); kind {
				case model.KindAccess:
					a := model.NewAccess(m, name, []string{"DE", "US", "FR", "LAN"}[len(accs)%4], ts/1e6)
					accs = append(accs, a)
					items = append(items, Item{Key: name, TS: ts, Access: a, Raw: line})
				case model.KindSystem:
					sys++
					items = append(items, Item{Key: name, TS: ts, Sys: model.NewSys(m, name, ts/1e6)})
				}
			}
		}
	}
	return
}

func ingest(t *testing.T, s *Store, items []Item) {
	t.Helper()
	for i := 0; i < len(items); i += 700 {
		batch := append([]Item(nil), items[i:min(i+700, len(items))]...)
		if err := s.apply(context.Background(), batch); err != nil {
			t.Fatal(err)
		}
	}
}

func count(accs []*model.Access, f *Filter, from, to int64) (n, bytes uint64) {
	for _, a := range accs {
		if a.T >= from && a.T < to && f.MatchAccess(a) {
			n++
			bytes += uint64(a.Size)
		}
	}
	return
}

func sum(s *Series) (n uint64) {
	for _, c := range s.C {
		for _, v := range c {
			n += v
		}
	}
	return
}

func TestSnapshotDetailAndRollup(t *testing.T) {
	mr := miniredis.RunT(t)
	ctx := context.Background()
	items, accs, nsys := gen(6)
	if len(accs) < 5000 || nsys == 0 {
		t.Fatalf("zu wenig Testdaten: %d access, %d sys", len(accs), nsys)
	}
	s := newStore(t, mr, 1_000_000, t0)
	ingest(t, s, items)

	from, to := t0.Add(-6*time.Hour).UnixMilli(), t0.UnixMilli()+1000
	filters := map[string]Filter{
		"alle":      {},
		"site":      {Site: "radio.example.org"},
		"host":      {Host: "radio"},
		"5xx":       {Classes: 1 << 5},
		"ohne bots": {NoBots: true, NoInt: true},
		"pod+land":  {Pod: "pod-1", Country: "DE"},
		"status":    {Status: 404},
		"text":      {Text: "feed"},
		"route":     {Route: "/api/now-playing", MinDur: 5},
		"dynamisch": {X: []model.KV{{K: "request_Referer", V: "https://www.google.com/"}}},
		"client":    {Client: accs[10].Client},
	}
	// Zweite Instanz mit kleinem Budget -> Würfel ("rollup") statt Einzel-Requests.
	small := newStore(t, mr, 3000, t0)
	for name, f := range filters {
		wantN, wantB := count(accs, &f, from, to)
		snap, err := s.Snapshot(ctx, Query{F: f, From: from, To: to, XField: "request_Referer"})
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("%-10s n=%d detail=%.1fms", name, wantN, snap.TookMs)
		if snap.Mode != "detail" || snap.KPI.Count != wantN || snap.KPI.Bytes != wantB {
			t.Errorf("%s: detail mode=%s count=%d bytes=%d, erwartet %d/%d", name, snap.Mode, snap.KPI.Count, snap.KPI.Bytes, wantN, wantB)
		}
		if sum(snap.Series) != wantN || sum(snap.Timeline) != wantN {
			t.Errorf("%s: Reihen %d/%d, erwartet %d", name, sum(snap.Series), sum(snap.Timeline), wantN)
		}
		if snap.KPI.Total != uint64(len(accs)) {
			t.Errorf("%s: total=%d, erwartet %d", name, snap.KPI.Total, len(accs))
		}
		roll, err := small.Snapshot(ctx, Query{F: f, From: from, To: to})
		if err != nil {
			t.Fatal(err)
		}
		if f.NeedsDetail() {
			if roll.Mode != "clipped" || roll.ClipFrom <= from || roll.KPI.Count >= wantN && wantN > 50 {
				t.Errorf("%s: erwartet begrenzte Auswertung, mode=%s clip=%d count=%d/%d", name, roll.Mode, roll.ClipFrom, roll.KPI.Count, wantN)
			}
			cn, _ := count(accs, &f, roll.ClipFrom, to)
			if roll.KPI.Count != cn {
				t.Errorf("%s: clipped count=%d, erwartet %d", name, roll.KPI.Count, cn)
			}
			continue
		}
		if roll.Mode != "rollup" || roll.ClipFrom <= from {
			t.Errorf("%s: erwartet rollup mit ClipFrom, mode=%s clip=%d", name, roll.Mode, roll.ClipFrom)
		}
		if roll.KPI.Count != wantN || roll.KPI.Bytes != wantB || sum(roll.Series) != wantN || sum(roll.Timeline) != wantN {
			t.Errorf("%s: rollup count=%d bytes=%d reihen=%d/%d, erwartet %d/%d", name, roll.KPI.Count, roll.KPI.Bytes,
				sum(roll.Series), sum(roll.Timeline), wantN, wantB)
		}
		if roll.KPI.P95 != snap.KPI.P95 || roll.KPI.S5 != snap.KPI.S5 || len(roll.Top.Site) != len(snap.Top.Site) {
			t.Errorf("%s: rollup weicht ab: p95 %v/%v s5 %d/%d", name, roll.KPI.P95, snap.KPI.P95, roll.KPI.S5, snap.KPI.S5)
		}
		for i := range roll.Top.Site {
			if roll.Top.Site[i] != snap.Top.Site[i] {
				t.Errorf("%s: Top-Sites weichen ab: %v / %v", name, roll.Top.Site[i], snap.Top.Site[i])
			}
		}
		if wantN > 0 && len(roll.Top.Route) == 0 {
			t.Errorf("%s: rollup ohne Detail-Listen", name)
		}
	}

	// Große Zeiträume nutzen den Stunden- bzw. Tages-Würfel und liefern dieselben Summen.
	for name, days := range map[string]int{"stunde": 10, "tag": 30} {
		for fname, f := range map[string]Filter{"alle": {}, "site+4xx": {Site: "radio.example.org", Classes: 1 << 4}} {
			wantN, wantB := count(accs, &f, from, to)
			wide, err := small.Snapshot(ctx, Query{F: f, From: t0.Add(-time.Duration(days) * 24 * time.Hour).UnixMilli(), To: to})
			if err != nil {
				t.Fatal(err)
			}
			wantStep := map[string]int64{"stunde": hourMs, "tag": dayMs}[name]
			if wide.Mode != "rollup" || wide.KPI.Count != wantN || wide.KPI.Bytes != wantB || sum(wide.Timeline) != wantN ||
				sum(wide.Series) != wantN || wide.Series.Step < wantStep || wide.Series.Step%wantStep != 0 {
				t.Errorf("%s/%s: mode=%s count=%d bytes=%d reihen=%d/%d step=%d, erwartet %d/%d", name, fname, wide.Mode,
					wide.KPI.Count, wide.KPI.Bytes, sum(wide.Timeline), sum(wide.Series), wide.Series.Step, wantN, wantB)
			}
		}
	}

	// Details der ungefilterten Auswertung.
	snap, _ := s.Snapshot(ctx, Query{From: from, To: to, XField: "request_Referer"})
	routes, clients := map[string]uint64{}, map[string]bool{}
	var sysAlerts uint64
	for _, a := range accs {
		routes[a.Route]++
		clients[a.Client] = true
	}
	for _, it := range items {
		if it.Sys != nil && model.IsAlert(it.Sys.Level) {
			sysAlerts++
		}
	}
	if snap.Top.Route[0].N != routes[snap.Top.Route[0].K] || snap.KPI.Clients != len(clients) {
		t.Errorf("Top-Route/Clients falsch: %+v clients=%d/%d", snap.Top.Route[0], snap.KPI.Clients, len(clients))
	}
	var gotSys uint64
	for _, n := range snap.Timeline.Sys {
		gotSys += n
	}
	if gotSys != sysAlerts {
		t.Errorf("Systemlog-Warnungen in der Zeitleiste: %d, erwartet %d", gotSys, sysAlerts)
	}
	hasField := false
	for _, f := range snap.Top.Fields {
		hasField = hasField || f.K == "request_Referer"
		if !model.IsDynamicField(f.K) {
			t.Errorf("statisches Feld als dynamisch gelistet: %s", f.K)
		}
	}
	if !hasField || len(snap.Top.X) == 0 || len(snap.Top.Slow) == 0 || len(snap.Top.Err) == 0 {
		t.Errorf("dynamische Felder/Listen fehlen: fields=%v x=%d slow=%d err=%d", snap.Top.Fields, len(snap.Top.X), len(snap.Top.Slow), len(snap.Top.Err))
	}
	if len(snap.Facets.Pods) != 2 || len(snap.Facets.Sites) < 5 {
		t.Errorf("Auswahllisten unvollständig: %+v", snap.Facets)
	}

	// Zoom: Zeitleiste bleibt vollständig, Kennzahlen nur für den Ausschnitt.
	zf, zt := t0.Add(-3*time.Hour).UnixMilli(), t0.Add(-2*time.Hour).UnixMilli()
	zs, _ := s.Snapshot(ctx, Query{From: from, To: to, ZFrom: zf, ZTo: zt})
	zn, _ := count(accs, &Filter{}, zf, zt)
	if !zs.Zoomed || zs.KPI.Count != zn || sum(zs.Timeline) != uint64(len(accs)) || sum(zs.Series) != zn {
		t.Errorf("Zoom: count=%d (erwartet %d) timeline=%d series=%d", zs.KPI.Count, zn, sum(zs.Timeline), sum(zs.Series))
	}

	// Neustart: alles kommt aus Redis zurück.
	re := newStore(t, mr, 1_000_000, t0)
	rs, err := re.Snapshot(ctx, Query{From: from, To: to})
	if err != nil || rs.KPI.Count != uint64(len(accs)) || rs.KPI.P99 != snap.KPI.P99 {
		t.Errorf("nach Neustart: %v count=%d, erwartet %d", err, rs.KPI.Count, len(accs))
	}
	if logs, _ := re.SysLogs(0, 100000); len(logs) != nsys {
		t.Errorf("Systemlogs nach Neustart: %d, erwartet %d", len(logs), nsys)
	}
	if cur, _ := re.Cursor(ctx, "pod-1"); cur != items[len(items)-1].TS && cur != items[len(items)-2].TS {
		t.Errorf("Cursor falsch: %d", cur)
	}

	// Verdichtung abgeschlossener Stunden-Würfel ändert die Ergebnisse nicht.
	later := newStore(t, mr, 3000, t0.Add(4*time.Hour))
	later.janitor(ctx, true)
	blobs, hashes := 0, 0
	for _, k := range mr.Keys() {
		switch {
		case len(k) > 11 && k[:11] == "tg:cubez:h:":
			blobs++
		case len(k) > 10 && k[:10] == "tg:cube:h:":
			hashes++
		}
	}
	if blobs < 6 || hashes != 0 {
		t.Errorf("Verdichtung: %d Blobs, %d Hashes", blobs, hashes)
	}
	after := newStore(t, mr, 3000, t0.Add(4*time.Hour))
	as, err := after.Snapshot(ctx, Query{From: from, To: to})
	if err != nil || as.Mode != "rollup" || as.KPI.Count != uint64(len(accs)) || as.KPI.P95 != snap.KPI.P95 {
		t.Errorf("nach Verdichtung: %v mode=%s count=%d p95=%v/%v", err, as.Mode, as.KPI.Count, as.KPI.P95, snap.KPI.P95)
	}
	// Nachzügler in eine verdichtete Stunde.
	late := *accs[0]
	raw := `{"RequestMethod":"GET"}`
	if err := after.apply(ctx, []Item{{Key: "pod-0", TS: 1, Access: &late, Raw: raw}}); err != nil {
		t.Fatal(err)
	}
	final := newStore(t, mr, 3000, t0.Add(4*time.Hour))
	if fs, _ := final.Snapshot(ctx, Query{From: from, To: to}); fs.KPI.Count != uint64(len(accs))+1 {
		t.Errorf("Nachzügler: count=%d, erwartet %d", fs.KPI.Count, len(accs)+1)
	}
}

func TestRowsAndRaw(t *testing.T) {
	mr := miniredis.RunT(t)
	ctx := context.Background()
	items, accs, _ := gen(3)
	s := newStore(t, mr, 1_000_000, t0)
	ingest(t, s, items)
	from, to := t0.Add(-3*time.Hour).UnixMilli(), t0.UnixMilli()+1000
	f := Filter{NoInt: true}
	want, _ := count(accs, &f, from, to)

	for _, asc := range []bool{false, true} {
		seen := map[string]bool{}
		var last int64
		cursor := ""
		for page := 0; ; page++ {
			res, err := s.Rows(ctx, RowsQuery{F: f, From: from, To: to, Asc: asc, Limit: 333, Cursor: cursor})
			if err != nil {
				t.Fatal(err)
			}
			for _, r := range res.Rows {
				if seen[r.ID] {
					t.Fatalf("doppelte Zeile %s", r.ID)
				}
				seen[r.ID] = true
				if last != 0 && (asc && r.T < last || !asc && r.T > last) {
					t.Fatalf("Sortierung verletzt (asc=%v): %d nach %d", asc, r.T, last)
				}
				last = r.T
			}
			if cursor = res.Next; cursor == "" {
				break
			}
		}
		if uint64(len(seen)) != want {
			t.Errorf("asc=%v: %d Zeilen, erwartet %d", asc, len(seen), want)
		}
	}

	// Sortierung nach Dauer: Top 5 stimmen mit der Referenz überein.
	durs := []float64{}
	for _, a := range accs {
		if f.MatchAccess(a) {
			durs = append(durs, a.Dur)
		}
	}
	sort.Sort(sort.Reverse(sort.Float64Slice(durs)))
	res, err := s.Rows(ctx, RowsQuery{F: f, From: from, To: to, Sort: "dur", Limit: 5})
	if err != nil || len(res.Rows) != 5 {
		t.Fatalf("Rows(dur): %v %d", err, len(res.Rows))
	}
	for i, r := range res.Rows {
		if float32(durs[i]) != r.D {
			t.Errorf("dur[%d]=%v, erwartet %v", i, r.D, durs[i])
		}
	}
	// Rohdaten inkl. dynamischer Felder.
	raw, err := s.RawEntry(ctx, res.Rows[0].ID)
	var m map[string]any
	if err != nil || json.Unmarshal(raw, &m) != nil || m["RequestPath"] != res.Rows[0].P || m["_pod"] != res.Rows[0].Pod {
		t.Fatalf("RawEntry: %v %s", err, raw)
	}
	if _, ok := m["request_User-Agent"]; !ok {
		t.Errorf("dynamisches Feld fehlt in den Rohdaten: %s", raw)
	}
	if raw, _ := s.RawEntry(ctx, "1-999999"); raw != nil {
		t.Errorf("unbekannte id liefert Daten")
	}
}

func TestLiveAppendAndRetention(t *testing.T) {
	mr := miniredis.RunT(t)
	ctx := context.Background()
	items, accs, _ := gen(2)
	s := newStore(t, mr, 1_000_000, t0)
	half := len(items) / 2
	ingest(t, s, items[:half])
	from, to := t0.Add(-2*time.Hour).UnixMilli(), t0.UnixMilli()+1000
	// Erst abfragen (Chunks/Würfel sind dann im Cache), danach weiter einlesen.
	if _, err := s.Snapshot(ctx, Query{From: from, To: to}); err != nil {
		t.Fatal(err)
	}
	v := s.Version()
	ingest(t, s, items[half:])
	snap, _ := s.Snapshot(ctx, Query{From: from, To: to})
	if snap.KPI.Count != uint64(len(accs)) || s.Version() == v {
		t.Errorf("Live-Anhängen: count=%d, erwartet %d", snap.KPI.Count, len(accs))
	}
	recents, next, gap := s.RecentSince(0)
	if len(recents) != min(len(accs), ringSize) || next != uint64(len(accs)) || gap != (len(accs) > ringSize) {
		t.Errorf("Ringpuffer: %d Einträge, next=%d gap=%v", len(recents), next, gap)
	}
	if recents[len(recents)-1].A != accs[len(accs)-1] {
		t.Errorf("Ringpuffer liefert nicht den jüngsten Eintrag")
	}
	// Zu alte Einträge werden verworfen.
	old := *accs[0]
	old.T = t0.Add(-100 * 24 * time.Hour).UnixMilli()
	if err := s.apply(ctx, []Item{{Access: &old, Raw: `{}`}}); err != nil {
		t.Fatal(err)
	}
	if st := s.Stats(); st.Dropped != 1 || st.Stored != int64(len(accs)) {
		t.Errorf("Aufbewahrung: dropped=%d stored=%d", st.Dropped, st.Stored)
	}
	if New(nil, Config{Retention: 400 * 24 * time.Hour}).cfg.Retention != MaxDays*24*time.Hour {
		t.Errorf("Aufbewahrung nicht auf %d Tage begrenzt", MaxDays)
	}
}

func TestSealRawBlocks(t *testing.T) {
	mr := miniredis.RunT(t)
	ctx := context.Background()
	items, accs, _ := gen(3)
	s := newStore(t, mr, 1_000_000, t0)
	ingest(t, s, items)
	from, to := t0.Add(-3*time.Hour).UnixMilli(), t0.UnixMilli()+1000
	all, err := s.Rows(ctx, RowsQuery{From: from, To: to, Limit: 1000})
	if err != nil || len(all.Rows) != 1000 {
		t.Fatal(err, len(all.Rows))
	}
	before := map[string]string{}
	for i := 0; i < len(all.Rows); i += 37 {
		raw, _ := s.RawEntry(ctx, all.Rows[i].ID)
		before[all.Rows[i].ID] = string(raw)
	}
	sizeBefore := 0
	for _, k := range mr.Keys() {
		if len(k) > 7 && k[:7] == "tg:raw:" {
			l, _ := mr.List(k)
			for _, v := range l {
				sizeBefore += len(v)
			}
		}
	}

	// Vier Stunden später sind alle Stunden abgeschlossen und werden versiegelt.
	s.now = func() time.Time { return t0.Add(4 * time.Hour) }
	s.sealOnce(ctx)
	sizeAfter, lists := 0, 0
	for _, k := range mr.Keys() {
		switch {
		case len(k) > 8 && k[:8] == "tg:rawz:":
			l, _ := mr.List(k)
			for _, v := range l {
				sizeAfter += len(v)
			}
		case len(k) > 7 && k[:7] == "tg:raw:":
			lists++
		}
	}
	if lists != 0 || sizeAfter == 0 || sizeAfter*4 > sizeBefore {
		t.Fatalf("Versiegeln: %d Listen übrig, %d -> %d Bytes", lists, sizeBefore, sizeAfter)
	}
	t.Logf("Rohdaten %d -> %d Bytes (%.1fx)", sizeBefore, sizeAfter, float64(sizeBefore)/float64(sizeAfter))

	// Gleiche IDs liefern dieselben Daten – im laufenden und in einem neu gestarteten Prozess.
	fresh := newStore(t, mr, 1_000_000, t0.Add(4*time.Hour))
	for _, st := range []*Store{s, fresh} {
		for id, want := range before {
			if got, err := st.RawEntry(ctx, id); err != nil || string(got) != want {
				t.Fatalf("RawEntry(%s) nach Versiegeln: %v\n%s\n%s", id, err, got, want)
			}
		}
		snap, err := st.Snapshot(ctx, Query{From: from, To: to})
		if err != nil || snap.Mode != "detail" || snap.KPI.Count != uint64(len(accs)) || len(snap.Top.Route) == 0 {
			t.Fatalf("Snapshot nach Versiegeln: %v mode=%s count=%d/%d", err, snap.Mode, snap.KPI.Count, len(accs))
		}
	}
	again, _ := fresh.Rows(ctx, RowsQuery{From: from, To: to, Limit: 1000})
	for i := range all.Rows {
		if again.Rows[i] != all.Rows[i] {
			t.Fatalf("Zeile %d weicht nach Versiegeln ab: %+v / %+v", i, again.Rows[i], all.Rows[i])
		}
	}

	// Nachzügler in eine versiegelte Stunde: Index läuft weiter, Daten bleiben lesbar.
	late := *accs[len(accs)-1]
	late.Path, late.Route = "/nachzuegler", "/nachzuegler"
	if err := fresh.apply(ctx, []Item{{Key: "pod-0", TS: 5, Access: &late, Raw: `{"RequestMethod":"GET","RequestPath":"/nachzuegler"}`}}); err != nil {
		t.Fatal(err)
	}
	res, _ := fresh.Rows(ctx, RowsQuery{F: Filter{Route: "/nachzuegler"}, From: from, To: to})
	if len(res.Rows) != 1 {
		t.Fatalf("Nachzügler nicht gefunden: %+v", res.Rows)
	}
	raw, err := fresh.RawEntry(ctx, res.Rows[0].ID)
	var m map[string]any
	if err != nil || json.Unmarshal(raw, &m) != nil || m["RequestPath"] != "/nachzuegler" {
		t.Fatalf("Nachzügler-Rohdaten: %v %s", err, raw)
	}
	// Viele Nachzügler werden nachträglich in die Blöcke aufgenommen (angebrochener Block wird aufgefüllt).
	var lateItems []Item
	for i := range 700 {
		a := *accs[len(accs)-1]
		a.Path, a.Route = fmt.Sprintf("/spaet?i=%d", i), "/spaet"
		lateItems = append(lateItems, Item{Access: &a, Raw: fmt.Sprintf(`{"RequestMethod":"GET","RequestPath":"/spaet?i=%d"}`, i)})
	}
	if err := fresh.apply(ctx, lateItems); err != nil {
		t.Fatal(err)
	}
	if n := fresh.sealOnce(ctx); n != 1 {
		t.Fatalf("erneutes Versiegeln: %d Stunden", n)
	}
	if n := fresh.sealOnce(ctx); n != 0 {
		t.Fatalf("Versiegeln nicht idempotent: %d", n)
	}
	lr, _ := fresh.Rows(ctx, RowsQuery{F: Filter{Route: "/spaet"}, From: from, To: to, Limit: 1000})
	if len(lr.Rows) != 700 {
		t.Fatalf("Nachzügler nach erneutem Versiegeln: %d", len(lr.Rows))
	}
	for _, r := range lr.Rows[:50] {
		raw, err := fresh.RawEntry(ctx, r.ID)
		if err != nil || json.Unmarshal(raw, &m) != nil || m["RequestPath"] != r.P {
			t.Fatalf("Rohdaten %s passen nicht zu %s: %s", r.ID, r.P, raw)
		}
	}
	third := newStore(t, mr, 1_000_000, t0.Add(4*time.Hour))
	if snap, _ := third.Snapshot(ctx, Query{From: from, To: to}); snap.KPI.Count != uint64(len(accs))+701 {
		t.Fatalf("nach Nachzügler: %d, erwartet %d", snap.KPI.Count, len(accs)+701)
	}
	if tr, _ := third.Rows(ctx, RowsQuery{F: Filter{Route: "/spaet"}, From: from, To: to, Limit: 1000}); len(tr.Rows) != 700 || tr.Rows[0].ID != lr.Rows[0].ID || tr.Rows[0].P != lr.Rows[0].P || tr.Rows[0].T != lr.Rows[0].T {
		t.Fatalf("Nachzügler nach Neustart: %d", len(tr.Rows))
	}
	for id, want := range before {
		if got, _ := third.RawEntry(ctx, id); string(got) != want {
			t.Fatalf("RawEntry(%s) nach Nachzügler verändert", id)
		}
	}
}
