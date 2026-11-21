package store

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// Einzel-Requests liegen je Stunde in raw:<stunde> (LIST, ein JSON je Eintrag). Ist
// eine Stunde abgeschlossen, wird sie "versiegelt": die ersten N Einträge wandern in
// gzip-Blöcken zu je blockSize Einträgen nach rawz:<stunde> (ca. 3x kleiner als die
// Liste), N steht im Hash rawn. Der Listenindex eines Eintrags ändert sich dabei nicht:
// idx < N liegt in Block idx/blockSize, sonst in raw:<stunde> an Position idx-N
// (Nachzügler).
const (
	blockSize   = 512
	sealMinAge  = 2  // Stunden, bevor versiegelt wird
	sealMinSize = 16 // kleinere Stunden lohnen nicht
)

func (s *Store) rawzKey(hour int64) string { return s.key("rawz", strconv.FormatInt(hour, 10)) }

func gzipLines(vals []string) []byte {
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	for i, v := range vals {
		if i > 0 {
			zw.Write([]byte{'\n'})
		}
		io.WriteString(zw, v)
	}
	zw.Close()
	return buf.Bytes()
}

func gunzipLines(data string) ([]string, error) {
	zr, err := gzip.NewReader(strings.NewReader(data))
	if err != nil {
		return nil, err
	}
	raw, err := io.ReadAll(zr)
	if err != nil {
		return nil, err
	}
	return strings.Split(string(raw), "\n"), nil
}

func (s *Store) sealedN(hour int64) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sealed[hour]
}

// readHour ruft fn für die Rohdaten einer Stunde in Listenreihenfolge auf und liefert
// den Index des ersten gelieferten Eintrags. Bei mehr als limit Einträgen wird nur
// das Ende der Stunde gelesen. Nicht lesbare Einträge werden als "" geliefert.
func (s *Store) readHour(ctx context.Context, hour, limit int64, fn func(v string)) (base int64, err error) {
	s.sealMu.RLock()
	defer s.sealMu.RUnlock()
	n := s.sealedN(hour)
	tail, err := s.rdb.LLen(ctx, s.rawKey(hour)).Result()
	if err != nil {
		return 0, err
	}
	start := max(n+tail-limit, 0)
	if start < n {
		start -= start % blockSize
	}
	for b := start / blockSize; b*blockSize < n; b += 16 {
		blocks, err := s.rdb.LRange(ctx, s.rawzKey(hour), b, b+15).Result()
		if err != nil {
			return 0, err
		}
		for j := int64(0); j < 16 && (b+j)*blockSize < n; j++ {
			want := int(min(blockSize, n-(b+j)*blockSize))
			var lines []string
			if int(j) < len(blocks) {
				if lines, err = gunzipLines(blocks[j]); err != nil {
					s.log.Error("Rohdaten-Block nicht lesbar", "stunde", hour, "block", b+j, "err", err)
				}
			}
			for i := range want { // Indizes müssen stimmen: fehlende Zeilen auffüllen
				if i < len(lines) {
					fn(lines[i])
				} else {
					fn("")
				}
			}
		}
	}
	for from := max(start-n, 0); from < tail; from += loadPage {
		to := min(from+loadPage, tail)
		vals, err := s.rdb.LRange(ctx, s.rawKey(hour), from, to-1).Result()
		if err != nil {
			return 0, err
		}
		for _, v := range vals {
			fn(v)
		}
		if int64(len(vals)) < to-from {
			break // Schlüssel ist währenddessen abgelaufen
		}
	}
	return start, nil
}

// RawEntry liefert den vollständigen Original-Eintrag als JSON (nil = nicht vorhanden).
func (s *Store) RawEntry(ctx context.Context, id string) ([]byte, error) {
	hs, is, ok := strings.Cut(id, "-")
	hour, e1 := strconv.ParseInt(hs, 10, 64)
	idx, e2 := strconv.ParseInt(is, 10, 64)
	if !ok || e1 != nil || e2 != nil || idx < 0 {
		return nil, fmt.Errorf("ungültige id")
	}
	s.sealMu.RLock()
	defer s.sealMu.RUnlock()
	if n := s.sealedN(hour); idx < n {
		blk, err := s.rdb.LIndex(ctx, s.rawzKey(hour), idx/blockSize).Result()
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		lines, err := gunzipLines(blk)
		if err != nil || int(idx%blockSize) >= len(lines) {
			return nil, err
		}
		return []byte(lines[idx%blockSize]), nil
	} else {
		v, err := s.rdb.LIndex(ctx, s.rawKey(hour), idx-n).Bytes()
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		return v, err
	}
}

// rawHours liefert alle Stunden, für die Einzel-Requests vorliegen (aufsteigend).
func (s *Store) rawHours(ctx context.Context) ([]int64, error) {
	set := map[int64]struct{}{}
	for _, name := range []string{"raw", "rawz"} {
		prefix := s.key(name) + ":"
		if err := s.scanKeys(ctx, prefix+"*", func(k string) {
			if h, err := strconv.ParseInt(strings.TrimPrefix(k, prefix), 10, 64); err == nil {
				set[h] = struct{}{}
			}
		}); err != nil {
			return nil, err
		}
	}
	hours := make([]int64, 0, len(set))
	for h := range set {
		hours = append(hours, h)
	}
	sort.Slice(hours, func(i, j int) bool { return hours[i] < hours[j] })
	return hours, nil
}

// sealer versiegelt abgeschlossene Stunden im Hintergrund.
func (s *Store) sealer(ctx context.Context) {
	t := time.NewTicker(30 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		case <-s.sealKick:
		}
		s.sealBusy.Store(true)
		for s.sealOnce(ctx) > 0 && ctx.Err() == nil {
		}
		s.sealBusy.Store(false)
	}
}

// sealOnce versiegelt alle abgeschlossenen Stunden und liefert deren Anzahl.
func (s *Store) sealOnce(ctx context.Context) (sealed int) {
	prefix := s.key("raw") + ":"
	var hours []int64
	cur := s.nowMs() / hourMs
	if err := s.scanKeys(ctx, prefix+"*", func(k string) {
		if h, err := strconv.ParseInt(strings.TrimPrefix(k, prefix), 10, 64); err == nil && h <= cur-sealMinAge {
			hours = append(hours, h)
		}
	}); err != nil {
		return 0
	}
	sort.Slice(hours, func(i, j int) bool { return hours[i] < hours[j] })
	for _, h := range hours {
		if ctx.Err() != nil {
			break
		}
		ok, err := s.sealHour(ctx, h)
		if err != nil {
			s.log.Warn("Verdichten der Detail-Stunde fehlgeschlagen", "stunde", h, "err", err)
		}
		if ok {
			sealed++
		}
	}
	if sealed > 0 {
		s.log.Debug("Detail-Stunden verdichtet", "stunden", sealed)
	}
	return sealed
}

// sealHour verschiebt die Einträge der Liste raw:<h> blockweise nach rawz:<h>. Ein
// angebrochener letzter Block wird dabei aufgefüllt, sodass alle Blöcke bis auf den
// letzten genau blockSize Einträge enthalten.
func (s *Store) sealHour(ctx context.Context, h int64) (bool, error) {
	const maxPerRound = 64 * blockSize
	did := false
	for ctx.Err() == nil {
		nOld := s.sealedN(h)
		tail, err := s.rdb.LLen(ctx, s.rawKey(h)).Result()
		if err != nil {
			return did, err
		}
		// Nachzügler einer bereits versiegelten Stunde erst ab einer lohnenden Menge packen.
		if tail < sealMinSize || (nOld > 0 && tail < blockSize/4) {
			return did, nil
		}
		take := min(tail, maxPerRound)
		var lines []string
		partial := nOld%blockSize != 0
		if partial {
			blk, err := s.rdb.LIndex(ctx, s.rawzKey(h), -1).Result()
			if err != nil {
				return did, err
			}
			if lines, err = gunzipLines(blk); err != nil || int64(len(lines)) != nOld%blockSize {
				return did, fmt.Errorf("letzter Block passt nicht zu rawn (%d Zeilen, erwartet %d): %v", len(lines), nOld%blockSize, err)
			}
		}
		for from := int64(0); from < take; from += loadPage {
			to := min(from+loadPage, take)
			vals, err := s.rdb.LRange(ctx, s.rawKey(h), from, to-1).Result()
			if err != nil {
				return did, err
			}
			if int64(len(vals)) != to-from {
				return did, nil // Liste hat sich verändert (abgelaufen/verworfen)
			}
			lines = append(lines, vals...)
		}
		blocks := make([]any, 0, len(lines)/blockSize+1)
		for i := 0; i < len(lines); i += blockSize {
			blocks = append(blocks, gzipLines(lines[i:min(i+blockSize, len(lines))]))
		}
		exp := time.UnixMilli((h + 1) * hourMs).Add(s.cfg.RawRetention)
		s.sealMu.Lock()
		if s.sealedN(h) != nOld { // inzwischen verworfen (Speicherdruck)
			s.sealMu.Unlock()
			return did, nil
		}
		_, err = s.rdb.TxPipelined(ctx, func(p redis.Pipeliner) error {
			if partial {
				p.LTrim(ctx, s.rawzKey(h), 0, -2)
			}
			p.RPush(ctx, s.rawzKey(h), blocks...)
			p.PExpireAt(ctx, s.rawzKey(h), exp)
			p.LTrim(ctx, s.rawKey(h), take, -1)
			p.HSet(ctx, s.key("rawn"), strconv.FormatInt(h, 10), nOld+take)
			return nil
		})
		if err == nil {
			s.mu.Lock()
			s.sealed[h] = nOld + take
			s.mu.Unlock()
		}
		s.sealMu.Unlock()
		if err != nil {
			return did, err
		}
		did = true
	}
	return did, nil
}

// dropRawHour entfernt die Einzel-Requests einer Stunde (läuft im Writer).
func (s *Store) dropRawHour(ctx context.Context, h int64) error {
	s.sealMu.Lock()
	defer s.sealMu.Unlock()
	if err := s.rdb.Del(ctx, s.rawKey(h), s.rawzKey(h)).Err(); err != nil {
		return err
	}
	s.rdb.HDel(ctx, s.key("rawn"), strconv.FormatInt(h, 10))
	delete(s.lens, h)
	s.mu.Lock()
	delete(s.sealed, h)
	if c := s.chunks[h]; c != nil {
		delete(s.chunks, h)
		s.cachedEntries.Add(-c.n.Load())
	}
	s.mu.Unlock()
	return nil
}

// relieve hält Redis unter der Speichergrenze (läuft im Writer). Reihenfolge:
// 1. abgeschlossene Stunden verdichten lassen (verlustfrei), 2. die ältesten
// Detail-Stunden verwerfen (die Voraggregate bleiben), 3. als letztes Mittel die
// ältesten Voraggregate verwerfen. force = Redis hat Schreibzugriffe bereits abgelehnt.
func (s *Store) relieve(ctx context.Context, force bool) {
	used, max := s.refreshRedisInfo(ctx)
	high := max * int64(s.cfg.MemHighPct) / 100
	if max <= 0 || (used <= high && !force) {
		return
	}
	select {
	case s.sealKick <- struct{}{}:
	default:
	}
	s.compactCubes(ctx)
	// Solange noch verdichtet wird, den Writer kurz anhalten statt Daten zu verwerfen.
	critical := (high + max) / 2
	for wait := 0; wait < 80 && s.sealBusy.Load() && ctx.Err() == nil; wait++ {
		time.Sleep(250 * time.Millisecond)
		if used, _ = s.refreshRedisInfo(ctx); used <= high {
			return
		}
	}
	if used <= critical && s.sealBusy.Load() && !force {
		return
	}
	target := max * int64(s.cfg.MemHighPct-10) / 100
	cur := s.nowMs() / hourMs
	hours, err := s.rawHours(ctx)
	if err != nil {
		return
	}
	deleted := 0
	for _, h := range hours {
		if h >= cur || used <= target || deleted >= 240 {
			break
		}
		if s.dropRawHour(ctx, h) != nil {
			break
		}
		deleted++
		used, _ = s.refreshRedisInfo(ctx)
	}
	if deleted > 0 {
		s.stPruned.Add(int64(deleted))
		s.log.Warn("Redis-Speicher knapp: älteste Detail-Stunden verworfen (Voraggregate bleiben erhalten)",
			"stunden", deleted, "used_bytes", used, "maxmemory", max)
		_ = s.refreshRawFrom(ctx)
	}
	if used <= high && !(force && deleted == 0) {
		return
	}
	// Letztes Mittel: älteste Stunden-Voraggregate verwerfen.
	var idx []int64
	for _, name := range []string{"cube", "cubez"} {
		prefix := s.key(name, s.Hour.Name) + ":"
		_ = s.scanKeys(ctx, prefix+"*", func(k string) {
			if i, err := strconv.ParseInt(strings.TrimPrefix(k, prefix), 10, 64); err == nil && i < cur-1 {
				idx = append(idx, i)
			}
		})
	}
	sort.Slice(idx, func(i, j int) bool { return idx[i] < idx[j] })
	dropped := 0
	for _, i := range idx {
		if dropped >= 48 || used <= target {
			break
		}
		b := &Bucket{res: s.Hour, index: i}
		if s.rdb.Del(ctx, s.cubeKey(b), s.blobKey(b)).Err() != nil {
			break
		}
		s.rdb.HDel(ctx, s.key("tot"), strconv.FormatInt(i, 10))
		s.mu.Lock()
		delete(s.totals, i)
		delete(s.buckets, bkKey{s.Hour, i})
		s.mu.Unlock()
		dropped++
		used, _ = s.refreshRedisInfo(ctx)
	}
	if dropped > 0 {
		s.log.Error("Redis-Speicher reicht nicht für die Aufbewahrung: älteste Voraggregate verworfen – maxmemory erhöhen oder RETENTION_DAYS senken",
			"stunden", dropped, "used_bytes", used, "maxmemory", max)
	}
}
