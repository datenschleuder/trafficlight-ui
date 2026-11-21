package web

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"traefikgui/internal/collector"
	"traefikgui/internal/fakekube"
	"traefikgui/internal/kube"
	"traefikgui/internal/store"
)

// setup startet Store, Collector (gegen simulierte Traefik-Pods) und den HTTP-Handler.
func setup(t *testing.T) *httptest.Server {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	st := store.New(rdb, store.Config{Logger: log})
	ctx, cancel := context.WithCancel(context.Background())
	if err := st.Init(ctx); err != nil {
		t.Fatal(err)
	}
	fk := httptest.NewServer(fakekube.New(fakekube.Options{Pods: 2, TextPods: 1, History: 5 * time.Minute, Rate: 20}))
	api, err := kube.New(kube.Config{URL: fk.URL})
	if err != nil {
		t.Fatal(err)
	}
	col := collector.New(collector.Config{Namespaces: []string{"default"}, Selector: "app=traefik", Backfill: true, Refresh: 200 * time.Millisecond, Logger: log}, api, st, nil)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); st.Run(ctx) }()
	go func() { defer wg.Done(); col.Run(ctx) }()
	srv := httptest.NewServer(New(st, col, Info{Version: "test", Namespaces: []string{"default"}, Selector: "app=traefik"}, log))
	t.Cleanup(func() { srv.Close(); cancel(); wg.Wait(); fk.Close(); rdb.Close() })
	return srv
}

type event struct {
	name string
	data map[string]any
}

// readEvents liest SSE-Ereignisse, bis stop true liefert.
func readEvents(t *testing.T, url string, stop func(ev event) bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("Content-Type %q", ct)
	}
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 1<<20), 16<<20)
	var ev event
	for sc.Scan() {
		line := sc.Text()
		switch {
		case strings.HasPrefix(line, "event: "):
			ev.name = line[7:]
		case strings.HasPrefix(line, "data: "):
			if err := json.Unmarshal([]byte(line[6:]), &ev.data); err != nil {
				t.Fatalf("ungültiges JSON im Ereignis %s: %v", ev.name, err)
			}
		case line == "" && ev.name != "":
			if stop(ev) {
				return
			}
			ev = event{}
		}
	}
	t.Fatalf("Stream endete vorzeitig: %v", sc.Err())
}

func TestStreamDeliversLiveData(t *testing.T) {
	srv := setup(t)
	var names []string
	var counts []float64
	liveRows := 0
	readEvents(t, srv.URL+"/api/stream?range=900&tz=-60", func(ev event) bool {
		names = append(names, ev.name)
		switch ev.name {
		case "snapshot":
			counts = append(counts, ev.data["kpi"].(map[string]any)["count"].(float64))
		case "rows":
			liveRows += len(ev.data["rows"].([]any))
		}
		// Ende: zwei Auswertungen mit wachsender Anzahl und live gelieferte Zeilen.
		return len(counts) >= 2 && counts[len(counts)-1] > counts[0] && liveRows > 0
	})
	if names[0] != "status" || names[1] != "snapshot" {
		t.Errorf("Reihenfolge der ersten Ereignisse: %v", names[:2])
	}
	// Gefilterter Stream liefert nur passende Zeilen.
	readEvents(t, srv.URL+"/api/stream?range=900&cls=4", func(ev event) bool {
		if ev.name != "rows" {
			return false
		}
		rows := ev.data["rows"].([]any)
		for _, r := range rows {
			if st := r.(map[string]any)["st"].(float64); st < 400 || st > 499 {
				t.Errorf("Zeile mit Status %v trotz Filter 4xx", st)
			}
		}
		return len(rows) > 0
	})
}

func TestAPI(t *testing.T) {
	srv := setup(t)
	get := func(path string, into any) *http.Response {
		t.Helper()
		resp, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		if into != nil {
			if err := json.Unmarshal(body, into); err != nil {
				t.Fatalf("%s: %v\n%.200s", path, err, body)
			}
		}
		return resp
	}
	// Warten, bis Daten eingelesen sind und der Text-Pod als "kein JSON" erkannt ist.
	var status struct {
		Pods  []collector.StreamStatus
		Store store.Stats
	}
	for deadline := time.Now().Add(20 * time.Second); ; time.Sleep(100 * time.Millisecond) {
		get("/api/status", &status)
		noJSON := 0
		for _, p := range status.Pods {
			if p.State == "no-json" {
				noJSON++
			}
		}
		if status.Store.Ingested > 500 && noJSON == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("keine Daten: %+v", status)
		}
	}
	var rows store.RowsResult
	get("/api/rows?range=900&limit=50&xf=request_Referer", &rows)
	if len(rows.Rows) != 50 || rows.Next == "" {
		t.Fatalf("rows: %d next=%q", len(rows.Rows), rows.Next)
	}
	var entry struct {
		ID      string
		Dynamic []string
		Entry   map[string]any
	}
	if resp := get("/api/entry?id="+rows.Rows[0].ID, &entry); resp.StatusCode != 200 || entry.Entry["RequestPath"] != rows.Rows[0].P {
		t.Fatalf("entry: %d %+v", resp.StatusCode, entry)
	}
	for _, k := range entry.Dynamic {
		if k == "DownstreamStatus" || k == "_pod" {
			t.Errorf("festes Feld als dynamisch gemeldet: %s", k)
		}
	}
	if resp := get("/api/entry?id=1-99999999", nil); resp.StatusCode != 404 {
		t.Errorf("unbekannter Eintrag: %d", resp.StatusCode)
	}
	if resp := get("/api/entry?id=x", nil); resp.StatusCode != 400 {
		t.Errorf("ungültige id: %d", resp.StatusCode)
	}
	resp, err := http.Get(srv.URL + "/api/export.csv?range=900&cls=2")
	if err != nil {
		t.Fatal(err)
	}
	csv, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	lines := strings.Split(strings.TrimSpace(string(csv)), "\n")
	if len(lines) < 100 || !strings.HasPrefix(lines[0], "zeit;status;") || !strings.Contains(lines[1], ";200;") && !strings.Contains(lines[1], ";20") {
		t.Errorf("CSV: %d Zeilen, Kopf %q", len(lines), lines[0])
	}
	// GUI und Sicherheits-Header.
	for _, p := range []string{"/", "/app.js", "/i18n.js", "/app.css", "/chart.umd.min.js"} {
		if resp := get(p, nil); resp.StatusCode != 200 || resp.Header.Get("Content-Security-Policy") == "" || resp.Header.Get("X-Content-Type-Options") != "nosniff" {
			t.Errorf("%s: %d %v", p, resp.StatusCode, resp.Header)
		}
	}
	if resp := get("/nicht-da", nil); resp.StatusCode != 404 {
		t.Errorf("unbekannter Pfad: %d", resp.StatusCode)
	}
	if resp := get("/readyz", nil); resp.StatusCode != 200 {
		t.Errorf("readyz: %d", resp.StatusCode)
	}
}

func TestCSVSafe(t *testing.T) {
	for in, want := range map[string]string{"=cmd()": "'=cmd()", "+1": "'+1", "@x": "'@x", "-": "-", "-x": "'-x", "/ok": "/ok", "": ""} {
		if got := csvSafe(in); got != want {
			t.Errorf("csvSafe(%q) = %q, erwartet %q", in, got, want)
		}
	}
}
