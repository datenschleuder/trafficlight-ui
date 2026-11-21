// fakekube startet eine simulierte Kubernetes-API mit Traefik-Pods, die Beispiel-Logs
// liefern. Nur für lokale Entwicklung und Screenshots:
//
//	go run ./cmd/fakekube -addr 127.0.0.1:8001 -pods 2 -text-pods 1 -history 72h -rate 2
//	KUBE_API_URL=http://127.0.0.1:8001 go run ./cmd/traefikgui
package main

import (
	"flag"
	"log"
	"net/http"
	"time"

	"traefikgui/internal/fakekube"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:8001", "Listen-Adresse")
	pods := flag.Int("pods", 2, "Pods mit JSON-Access-Log")
	text := flag.Int("text-pods", 1, "Pods mit Text-Access-Log (werden von traefikgui ignoriert)")
	history := flag.Duration("history", 24*time.Hour, "Zeitraum vorhandener Logs")
	rate := flag.Float64("rate", 2, "Requests pro Sekunde und Pod")
	flag.Parse()
	srv := fakekube.New(fakekube.Options{Pods: *pods, TextPods: *text, History: *history, Rate: *rate})
	log.Printf("fakekube auf http://%s (%d JSON-Pods, %d Text-Pods, %s Historie, %.1f req/s je Pod)", *addr, *pods, *text, *history, *rate)
	log.Fatal(http.ListenAndServe(*addr, srv))
}
