package collector

import (
	"context"
	"io"
	"log/slog"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"traefikgui/internal/fakekube"
	"traefikgui/internal/kube"
	"traefikgui/internal/model"
	"traefikgui/internal/store"
)

const rate = 20.0

// expected zählt, was fakekube für einen Pod bis einschließlich Cursor geliefert hat.
func expected(fk *fakekube.Server, pod int, start time.Time, cursor int64) (access, sys int64) {
	step := int64(float64(time.Second) / rate)
	for k := start.UnixNano()/step + 1; k*step <= cursor; k++ {
		for i, line := range fk.Lines(pod, k) {
			if k*step+int64(i) > cursor {
				break
			}
			switch kind, _ := model.Decode(line, nil); kind {
			case model.KindAccess:
				access++
			case model.KindSystem:
				sys++
			}
		}
	}
	return
}

func TestCollectorMultiPodJSONOnly(t *testing.T) {
	mr := miniredis.RunT(t)
	begin := time.Now()
	history := 10 * time.Minute
	opts := fakekube.Options{Pods: 2, TextPods: 1, History: history, Rate: rate, Now: func() time.Time { return begin }}
	fk := fakekube.New(opts)
	srv := httptest.NewServer(fk)
	defer srv.Close()
	api, err := kube.New(kube.Config{URL: srv.URL})
	if err != nil {
		t.Fatal(err)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	// run startet Store + Collector, wartet bis cond erfüllt ist und fährt beides herunter.
	run := func(cond func(st *store.Store, col *Collector) bool) (*store.Store, []StreamStatus) {
		rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
		defer rdb.Close()
		st := store.New(rdb, store.Config{Logger: log})
		ctx, cancel := context.WithCancel(context.Background())
		if err := st.Init(ctx); err != nil {
			t.Fatal(err)
		}
		col := New(Config{Namespaces: []string{"default"}, Selector: "app=traefik", Backfill: true, Refresh: 200 * time.Millisecond, Logger: log}, api, st, nil)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); st.Run(ctx) }()
		go func() { defer wg.Done(); col.Run(ctx) }()
		deadline := time.Now().Add(30 * time.Second)
		for !cond(st, col) {
			if time.Now().After(deadline) {
				pods, _ := col.Status()
				t.Fatalf("Zeitüberschreitung; Status: %+v, Store: %+v", pods, st.Stats())
			}
			time.Sleep(50 * time.Millisecond)
		}
		pods, _ := col.Status()
		// Erst den Collector anhalten, dann den Store leerlaufen lassen.
		time.Sleep(600 * time.Millisecond)
		cancel()
		wg.Wait()
		return st, pods
	}

	check := func(phase string) {
		t.Helper()
		ctx := context.Background()
		// Neu aus Redis laden, damit wirklich der gespeicherte Stand geprüft wird.
		rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
		defer rdb.Close()
		fresh := store.New(rdb, store.Config{Logger: log})
		if err := fresh.Init(ctx); err != nil {
			t.Fatal(err)
		}
		var wantAccess int64
		for pod := range 2 {
			key := "default/" + podUID(pod) + "/traefik"
			cur, err := fresh.Cursor(ctx, key)
			if err != nil || cur == 0 {
				t.Fatalf("%s: kein Cursor für %s (%v)", phase, key, err)
			}
			a, _ := expected(fk, pod, begin.Add(-history), cur)
			wantAccess += a
		}
		snap, err := fresh.Snapshot(ctx, store.Query{From: begin.Add(-time.Hour).UnixMilli(), To: time.Now().Add(time.Minute).UnixMilli()})
		if err != nil {
			t.Fatal(err)
		}
		if int64(snap.KPI.Count) != wantAccess || fresh.Stats().Stored != wantAccess {
			t.Errorf("%s: gespeichert %d (Würfel %d), laut Cursor erwartet %d", phase, snap.KPI.Count, fresh.Stats().Stored, wantAccess)
		}
		if len(snap.Top.Pod) != 2 {
			t.Errorf("%s: erwartet Daten von 2 Pods, erhalten %+v", phase, snap.Top.Pod)
		}
	}

	histAccess, histSys := int64(0), int64(0)
	for pod := range 2 {
		a, s := expected(fk, pod, begin.Add(-history), begin.UnixNano())
		histAccess, histSys = histAccess+a, histSys+s
	}
	if histAccess < 5000 || histSys == 0 {
		t.Fatalf("zu wenig Testdaten: %d/%d", histAccess, histSys)
	}

	// Phase 1: Historie beider JSON-Pods einlesen; der Text-Pod wird erkannt und ignoriert.
	st, pods := run(func(st *store.Store, col *Collector) bool {
		pods, _ := col.Status()
		noJSON := false
		for _, p := range pods {
			noJSON = noJSON || p.State == "no-json"
		}
		return st.Stats().Ingested >= histAccess && noJSON
	})
	if len(pods) != 3 {
		t.Fatalf("erwartet 3 Streams, erhalten %d", len(pods))
	}
	for _, p := range pods {
		switch p.Pod {
		case podName(fk, 2):
			if p.State != "no-json" || p.Access != 0 || p.System != 0 || p.Skipped < sniffLines {
				t.Errorf("Text-Pod nicht ignoriert: %+v", p)
			}
		default:
			if p.State != "streaming" || p.Format != "json" || p.Access == 0 {
				t.Errorf("JSON-Pod: %+v", p)
			}
		}
	}
	if st.Stats().SysLines < histSys {
		t.Errorf("Systemlogs: %d, erwartet >= %d", st.Stats().SysLines, histSys)
	}
	check("Phase 1")

	// Phase 2: Neustart – setzt am Cursor fort, ohne Lücken und ohne Duplikate.
	time.Sleep(1200 * time.Millisecond)
	run(func(st *store.Store, col *Collector) bool { return st.Stats().Ingested >= 20 })
	check("Phase 2")
}

func podUID(i int) string { return "00000000-0000-4000-8000-00000000000" + string(rune('0'+i)) }

func podName(fk *fakekube.Server, i int) string {
	srv := httptest.NewServer(fk)
	defer srv.Close()
	api, _ := kube.New(kube.Config{URL: srv.URL})
	pods, _ := api.ListPods(context.Background(), "default", "")
	return pods[i].Name
}
