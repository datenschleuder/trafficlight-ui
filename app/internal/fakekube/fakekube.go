// Package fakekube simuliert die Kubernetes-API (Pod-Liste, Log-Streams) mit
// deterministisch erzeugten Traefik-Logs – für Tests, lokale Entwicklung und Screenshots.
package fakekube

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"math"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type Options struct {
	Pods     int           // Pods mit JSON-Access-Log
	TextPods int           // zusätzliche Pods im Common-Log-Format (müssen ignoriert werden)
	History  time.Duration // so weit reichen die Logs zurück
	Rate     float64       // Requests pro Sekunde und Pod
	Now      func() time.Time
}

type Server struct {
	o    Options
	step time.Duration
}

func New(o Options) *Server {
	if o.Now == nil {
		o.Now = time.Now
	}
	if o.Rate <= 0 {
		o.Rate = 2
	}
	return &Server{o: o, step: time.Duration(float64(time.Second) / o.Rate)}
}

func (s *Server) podName(i int) string {
	return fmt.Sprintf("traefik-%s-%s", [...]string{"7c9d8f6b5d", "7c9d8f6b5d", "7c9d8f6b5d", "64f5b9c8d7"}[i%4],
		[...]string{"x2k9p", "m4tzq", "b7hw3", "q8n5v", "j6r2c", "d9s4f"}[i%6])
}

func (s *Server) isText(i int) bool { return i >= s.o.Pods }

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	// /api/v1/namespaces/<ns>/pods[/<name>/log]
	if len(parts) < 5 || parts[0] != "api" || parts[2] != "namespaces" || parts[4] != "pods" {
		http.NotFound(w, r)
		return
	}
	ns := parts[3]
	total := s.o.Pods + s.o.TextPods
	if len(parts) == 5 {
		type m = map[string]any
		items := []m{}
		for i := range total {
			format := "json"
			if s.isText(i) {
				format = "common"
			}
			items = append(items, m{
				"metadata": m{"name": s.podName(i), "namespace": ns, "uid": fmt.Sprintf("00000000-0000-4000-8000-%012d", i)},
				"spec": m{"nodeName": fmt.Sprintf("pool-fra1-%d", i%3+1), "containers": []m{{"name": "traefik",
					"args": []string{"--accesslog", "--accesslog.format=" + format}}}},
				"status": m{"phase": "Running", "containerStatuses": []m{{"name": "traefik", "restartCount": 0,
					"state": m{"running": m{"startedAt": s.o.Now().Add(-s.o.History).UTC().Format(time.RFC3339)}}}}},
			})
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(m{"kind": "PodList", "items": items})
		return
	}
	if len(parts) != 7 || parts[6] != "log" {
		http.NotFound(w, r)
		return
	}
	pod := -1
	for i := range total {
		if s.podName(i) == parts[5] {
			pod = i
		}
	}
	if pod < 0 {
		http.Error(w, `{"message":"pod not found"}`, http.StatusNotFound)
		return
	}
	q := r.URL.Query()
	if q.Get("previous") == "true" {
		http.Error(w, `{"message":"previous terminated container not found"}`, http.StatusBadRequest)
		return
	}
	now := s.o.Now()
	from := now.Add(-s.o.History)
	if since, err := time.Parse(time.RFC3339, q.Get("sinceTime")); err == nil && since.After(from) {
		from = since
	}
	follow := q.Get("follow") == "true"
	w.Header().Set("Content-Type", "text/plain")
	fl, _ := w.(http.Flusher)
	k := from.UnixNano()/int64(s.step) + 1
	for n := 0; ; k++ {
		t := time.Unix(0, k*int64(s.step))
		if t.After(s.o.Now()) {
			if !follow {
				return
			}
			if fl != nil {
				fl.Flush()
			}
			select {
			case <-r.Context().Done():
				return
			case <-time.After(time.Until(t)):
			}
		}
		for i, line := range s.Lines(pod, k) {
			if _, err := fmt.Fprintf(w, "%s %s\n", t.Add(time.Duration(i)).UTC().Format(time.RFC3339Nano), line); err != nil {
				return
			}
		}
		if n++; n%2000 == 0 && r.Context().Err() != nil {
			return
		}
	}
}

type site struct {
	host, router, service, addr string
	weight                      float64
	paths                       []string
}

var sites = []site{
	{"radio.example.org", "default-ingressrouteradio-1a2b3c4d5e6f7a8b9c0d@kubernetescrd", "default-webradio-8080@kubernetescrd", "10.244.1.17:8080", 5,
		[]string{"/", "/", "/live", "/api/now-playing", "/api/now-playing", "/api/stations", "/assets/app.js", "/assets/app.css", "/favicon.ico", "/stream/main.mp3"}},
	{"www.radio.example.org", "default-ingressrouteradio-1a2b3c4d5e6f7a8b9c0d@kubernetescrd", "default-webradio-8080@kubernetescrd", "10.244.2.9:8080", 2,
		[]string{"/", "/live", "/assets/app.js", "/sitemap.xml"}},
	{"podcast.example.org", "default-ingressroutepodcast-0f9e8d7c6b5a4f3e2d1c@kubernetescrd", "default-podcast-8080@kubernetescrd", "10.244.1.33:8080", 3,
		[]string{"/", "/feed.xml", "/feed.xml", "/episodes", "/episodes/42", "/episodes/43", "/media/ep42.mp3", "/media/ep43.mp3", "/api/chapters/42"}},
	{"api.example.org", "default-ingressrouteradios-5e4d3c2b1a0f9e8d7c6b@kubernetescrd", "default-radio-api-8080@kubernetescrd", "10.244.3.5:8080", 3,
		[]string{"/api/radios/search", "/api/radios/top", "/api/radios/country/de", "/api/radios/playlist", "/api/health"}},
	{"shop.example.org", "default-ingressroutegrowv-9c8b7a6f5e4d3c2b1a0f@kubernetescrd", "default-growv-8080@kubernetescrd", "10.244.2.21:8080", 1.5,
		[]string{"/", "/login", "/dashboard", "/api/plants", "/api/sensors", "/static/main.js"}},
}

var scanPaths = []string{"/.env", "/wp-login.php", "/wp-admin/setup-config.php", "/.git/config", "/admin", "/phpmyadmin/",
	"/cgi-bin/luci", "/vendor/phpunit/phpunit/src/Util/PHP/eval-stdin.php", "/actuator/health", "/config.json", "/owa/"}

var agents = []string{
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36",
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36",
	"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/19.0 Safari/605.1.15",
	"Mozilla/5.0 (iPhone; CPU iPhone OS 19_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/19.0 Mobile/15E148 Safari/604.1",
	"Mozilla/5.0 (Linux; Android 16; Pixel 9) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Mobile Safari/537.36",
	"Mozilla/5.0 (X11; Linux x86_64; rv:144.0) Gecko/20100101 Firefox/144.0",
	"AppleCoreMedia/1.0.0.22G86 (iPhone; U; CPU OS 19_1 like Mac OS X; de_de)",
	"VLC/3.0.21 LibVLC/3.0.21",
	"Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
	"Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)",
	"curl/8.11.1",
	"python-requests/2.32.3",
	"Go-http-client/2.0",
}

// Präfixe öffentlicher Netze (das Land bestimmt die GeoIP-Datenbank).
var nets = []string{"91.64", "91.64", "217.160", "84.132", "84.132", "87.123", "93.220", "73.15", "98.137", "8.8", "90.1",
	"195.154", "81.2", "86.1", "145.100", "77.160", "85.1", "178.197", "77.116", "213.33", "83.0", "88.0", "79.0", "78.64",
	"126.0", "177.0", "49.36", "203.2", "41.0"}

var referers = []string{"https://www.google.com/", "https://www.google.de/", "https://duckduckgo.com/", "https://radio.example.org/",
	"https://podcast.example.org/episodes", "https://t.co/abc123", "https://www.bing.com/"}

var langs = []string{"de-DE,de;q=0.9", "de-DE,de;q=0.9,en;q=0.8", "en-US,en;q=0.9", "fr-FR,fr;q=0.9", "en-GB,en;q=0.8", "nl-NL,nl;q=0.9"}

func pick[T any](r *rand.Rand, xs []T) T { return xs[r.IntN(len(xs))] }

// Lines liefert die Logzeilen von Pod pod im Zeitschritt k (0..n Zeilen; deterministisch).
func (s *Server) Lines(pod int, k int64) []string {
	h := fnv.New64a()
	fmt.Fprintf(h, "%d/%d", pod, k)
	r := rand.New(rand.NewPCG(h.Sum64(), uint64(pod)))
	t := time.Unix(0, k*int64(s.step)).UTC()
	// Tagesgang: nachts weniger Requests.
	hour := float64(t.Hour()) + float64(t.Minute())/60
	load := 0.35 + 0.65*math.Pow(math.Sin(math.Pi*(hour-4)/24), 2)
	if r.Float64() > load {
		return nil
	}
	if s.isText(pod) {
		return []string{fmt.Sprintf(`%s.%d.%d - - [%s] "GET / HTTP/1.1" 200 1532 "-" "-" %d "web@kubernetescrd" "http://10.244.1.17:8080" 3ms`,
			pick(r, nets), r.IntN(255), r.IntN(254)+1, t.Format("02/Jan/2006:15:04:05 -0700"), k)}
	}
	var out []string
	// Gelegentlich Traefik-Systemlogs: Pod 0 als JSON, die übrigen als Text (wird ignoriert).
	if r.IntN(900) == 0 {
		msgs := []struct{ lv, msg, err string }{
			{"error", "Error while dialing backend", "dial tcp 10.244.3.5:8080: connect: connection refused"},
			{"warn", "Health check failed, removing server from pool", ""},
			{"error", "Unable to obtain ACME certificate for domains", "acme: error: 429 :: too many requests"},
			{"info", "Configuration loaded from Kubernetes CRD", ""},
			{"warn", "Circuit breaker tripped", ""},
		}
		m := pick(r, msgs)
		if pod == 0 {
			rec := map[string]any{"level": m.lv, "time": t.Format(time.RFC3339), "message": m.msg}
			if m.err != "" {
				rec["error"] = m.err
				rec["providerName"] = "kubernetescrd"
			}
			b, _ := json.Marshal(rec)
			out = append(out, string(b))
		} else {
			out = append(out, fmt.Sprintf("%s %s %s", t.Format(time.RFC3339), strings.ToUpper(m.lv[:3]), m.msg))
		}
	}

	var weights float64
	for _, st := range sites {
		weights += st.weight
	}
	x := r.Float64() * weights
	st := sites[0]
	for _, c := range sites {
		if x -= c.weight; x < 0 {
			st = c
			break
		}
	}
	host, router, service, addr := st.host, st.router, st.service, st.addr
	path, method, status := pick(r, st.paths), "GET", 200
	if strings.HasSuffix(path, ".mp3") && r.IntN(4) > 0 { // Streams/Downloads sind selten, laufen aber lange
		path = st.paths[0]
	}
	ua := pick(r, agents)
	if strings.Contains(ua, "bot") {
		path = pick(r, []string{"/robots.txt", "/sitemap.xml", "/", st.paths[1]})
	}
	// Wenige Clients erzeugen viele Requests (wie in echten Logs): Auswahl aus einem festen Pool.
	ci := uint64(min(r.ExpFloat64()*600, 5999))
	client := fmt.Sprintf("%s.%d.%d", nets[ci%uint64(len(nets))], ci*7919%255, ci*104729%254+1)
	durMs := 2 + r.ExpFloat64()*18
	size := int64(300 + r.IntN(40000))
	proto := pick(r, []string{"HTTP/2.0", "HTTP/2.0", "HTTP/2.0", "HTTP/3.0", "HTTP/1.1"})

	switch p := r.Float64(); {
	case p < 0.05: // Scanner ohne passenden Router
		host, router, service, addr = fmt.Sprintf("%d.%d.%d.%d", 100+r.IntN(90), r.IntN(255), r.IntN(255), r.IntN(255)), "", "", ""
		path, status, ua, proto = pick(r, scanPaths), 404, pick(r, []string{"curl/8.11.1", "python-requests/2.32.3", "zgrab/0.x", "-"}), "HTTP/1.1"
		durMs, size = 0.05+r.Float64()*0.3, 19
	case p < 0.07: // Traefik-Dashboard / interne Router
		host, router, service, addr = "10.244.0.12:8080", "api@internal", "api@internal", ""
		path, client, ua, proto = pick(r, []string{"/api/overview", "/api/http/routers", "/ping"}), "10.114.0.3", "kube-probe/1.34", "HTTP/1.1"
		durMs, size = 0.2+r.Float64(), 512
	case p < 0.10:
		status, size = 304, 0
	case p < 0.13:
		status, path, size = 404, path+pick(r, []string{".map", "/old", ".bak", "/index.php"}), 153
	case p < 0.15:
		status, size = pick(r, []int{301, 302}), 0
	case p < 0.158:
		status, durMs, size = pick(r, []int{500, 502, 502, 503, 504}), 30+r.ExpFloat64()*900, 21
	case p < 0.17:
		status, size = pick(r, []int{401, 403, 429}), 17
	case p < 0.21:
		method = pick(r, []string{"POST", "POST", "HEAD", "OPTIONS", "PUT", "DELETE"})
		if method == "POST" {
			status = pick(r, []int{200, 201, 204})
		}
	}
	if strings.HasPrefix(path, "/api/radios/search") {
		path += "?q=" + pick(r, []string{"jazz", "rock", "techno", "news", "klassik", "80s"})
		durMs += r.ExpFloat64() * 120
	}
	if strings.HasSuffix(path, ".mp3") && status == 200 { // lange laufende Streams/Downloads
		durMs = 5000 + r.ExpFloat64()*240000
		size = int64(durMs * 16)
	}
	dur := time.Duration(durMs * float64(time.Millisecond))
	start := t.Add(-dur)
	origin := time.Duration(float64(dur) * 0.93)
	rec := map[string]any{
		"ClientAddr": client + ":" + strconv.Itoa(1024+r.IntN(60000)), "ClientHost": client, "ClientPort": "443", "ClientUsername": "-",
		"DownstreamContentSize": size, "DownstreamStatus": status, "Duration": dur.Nanoseconds(),
		"OriginContentSize": size, "OriginDuration": origin.Nanoseconds(), "OriginStatus": status, "Overhead": (dur - origin).Nanoseconds(),
		"RequestAddr": host, "RequestContentSize": 0, "RequestCount": k, "RequestHost": host, "RequestMethod": method,
		"RequestPath": path, "RequestPort": "-", "RequestProtocol": proto, "RequestScheme": "https", "RetryAttempts": 0,
		"StartLocal": start.Format(time.RFC3339Nano), "StartUTC": start.Format(time.RFC3339Nano),
		"TLSCipher": "TLS_AES_128_GCM_SHA256", "TLSVersion": "1.3", "entryPointName": "websecure",
		"level": "info", "msg": "", "time": t.Format(time.RFC3339),
		"request_User-Agent": ua,
	}
	if router != "" {
		rec["RouterName"], rec["ServiceName"], rec["ServiceAddr"], rec["ServiceURL"] = router, service, addr, "http://"+addr
	}
	// Dynamische Felder: je nach Traefik-Konfiguration zusätzliche Header.
	if r.IntN(3) > 0 && router != "" {
		rec["request_Referer"] = pick(r, referers)
	}
	if router != "" {
		rec["request_Accept-Language"] = pick(r, langs)
		rec["downstream_Content-Type"] = contentType(path)
		if pod%2 == 0 {
			rec["downstream_X-Cache"] = pick(r, []string{"HIT", "HIT", "MISS", "BYPASS"})
		}
	}
	b, _ := json.Marshal(rec)
	return append(out, string(b))
}

func contentType(path string) string {
	switch {
	case strings.HasSuffix(path, ".js"):
		return "text/javascript"
	case strings.HasSuffix(path, ".css"):
		return "text/css"
	case strings.HasSuffix(path, ".mp3"):
		return "audio/mpeg"
	case strings.HasSuffix(path, ".xml"):
		return "application/xml"
	case strings.HasPrefix(path, "/api/"):
		return "application/json"
	}
	return "text/html; charset=utf-8"
}
