// traefikgui liest die JSON-Access-Logs aller Traefik-Pods, speichert sie in Redis
// (max. 90 Tage) und stellt eine Web-GUI mit Live-Aktualisierung per SSE bereit.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"traefikgui/internal/collector"
	"traefikgui/internal/geo"
	"traefikgui/internal/kube"
	"traefikgui/internal/store"
	"traefikgui/internal/web"
)

var version = "dev"

func env(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	n, err := strconv.Atoi(env(key, ""))
	if err != nil {
		return def
	}
	return n
}

func main() {
	if len(os.Args) > 1 && os.Args[1] == "-version" {
		fmt.Println(version)
		return
	}
	level := slog.LevelInfo
	_ = level.UnmarshalText([]byte(env("LOG_LEVEL", "info")))
	var handler slog.Handler = slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})
	if env("LOG_FORMAT", "json") == "text" {
		handler = slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level})
	}
	log := slog.New(handler)
	slog.SetDefault(log)
	if err := run(log); err != nil {
		log.Error("beendet mit Fehler", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	days := min(max(envInt("RETENTION_DAYS", store.MaxDays), 1), store.MaxDays)
	rawDays := min(max(envInt("RAW_RETENTION_DAYS", days), 1), days)
	namespaces := strings.FieldsFunc(env("TRAEFIK_NAMESPACES", "default"), func(r rune) bool { return r == ',' || r == ' ' })
	selector := env("TRAEFIK_SELECTOR", "app=traefik")

	redis.SetLogger(redisLog{log}) // interne Meldungen des Redis-Clients ins eigene Log-Format
	rdb := redis.NewClient(&redis.Options{
		Addr: env("REDIS_ADDR", "localhost:6379"), Password: os.Getenv("REDIS_PASSWORD"), DB: envInt("REDIS_DB", 0),
		DialTimeout: 5 * time.Second, ReadTimeout: 30 * time.Second, WriteTimeout: 30 * time.Second,
		PoolSize: 16, MaxRetries: 2,
	})
	defer rdb.Close()
	st := store.New(rdb, store.Config{
		Prefix:       env("REDIS_PREFIX", "tg:"),
		Retention:    time.Duration(days) * 24 * time.Hour,
		RawRetention: time.Duration(rawDays) * 24 * time.Hour,
		DetailMax:    envInt("DETAIL_MAX_ENTRIES", 1_000_000),
		MemHighPct:   envInt("REDIS_MEMORY_HIGH_PCT", 85),
		Logger:       log,
	})

	var db *geo.DB
	if p := env("GEOIP_DB", ""); p != "" {
		var err error
		if db, err = geo.Load(p); err != nil {
			log.Warn("GeoIP-Datenbank nicht ladbar, Länderzuordnung aus", "pfad", p, "err", err)
		} else {
			log.Info("GeoIP-Datenbank geladen", "bereiche", db.Ranges())
		}
	}

	api, err := kube.New(kube.Config{URL: env("KUBE_API_URL", ""), TokenFile: env("KUBE_TOKEN_FILE", ""), CAFile: env("KUBE_CA_FILE", "")})
	if err != nil {
		return fmt.Errorf("Kubernetes-API: %w", err)
	}
	col := collector.New(collector.Config{
		Namespaces: namespaces, Selector: selector, Container: env("TRAEFIK_CONTAINER", ""),
		Backfill: env("BACKFILL", "true") != "false",
		Refresh:  time.Duration(envInt("POD_REFRESH_SECONDS", 10)) * time.Second, Logger: log,
	}, api, st, db)

	srv := &http.Server{
		Addr:              env("LISTEN_ADDR", ":8080"),
		Handler:           web.New(st, col, web.Info{Version: version, Namespaces: namespaces, Selector: selector, Geo: db != nil}, log),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		IdleTimeout:       120 * time.Second,
		BaseContext:       func(net.Listener) context.Context { return ctx },
	}
	// Optionaler Profiling-Endpunkt (nur zur Fehlersuche, z. B. PPROF_ADDR=127.0.0.1:6060).
	if addr := env("PPROF_ADDR", ""); addr != "" {
		mux := http.NewServeMux()
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
		go func() { log.Warn("pprof beendet", "err", http.ListenAndServe(addr, mux)) }()
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		// Redis darf später starten als die Anwendung (z. B. lädt noch das AOF).
		for delay := time.Second; ; delay = min(delay*2, 15*time.Second) {
			err := st.Init(ctx)
			if err == nil {
				break
			}
			if ctx.Err() != nil {
				return
			}
			log.Warn("Redis noch nicht erreichbar", "err", err, "erneut_in", delay.String())
			select {
			case <-ctx.Done():
				return
			case <-time.After(delay):
			}
		}
		log.Info("Redis verbunden", "aufbewahrung_tage", days, "detail_tage", rawDays)
		wg.Add(2)
		go func() { defer wg.Done(); st.Run(ctx) }()
		go func() { defer wg.Done(); col.Run(ctx) }()
	}()

	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()
	log.Info("traefikgui gestartet", "version", version, "addr", srv.Addr, "namespaces", namespaces, "selector", selector)
	select {
	case err := <-errc:
		stop()
		wg.Wait()
		return err
	case <-ctx.Done():
	}
	log.Info("fahre herunter")
	sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(sctx); err != nil && !errors.Is(err, context.DeadlineExceeded) {
		log.Warn("HTTP-Shutdown", "err", err)
	}
	srv.Close()
	wg.Wait()
	return nil
}

// redisLog leitet die Meldungen von go-redis (Verbindungsfehler usw.) an slog weiter.
type redisLog struct{ log *slog.Logger }

func (l redisLog) Printf(_ context.Context, format string, v ...any) {
	l.log.Debug("redis-client: " + fmt.Sprintf(format, v...))
}
