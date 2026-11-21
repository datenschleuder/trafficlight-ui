// Package geo ordnet IPs offline einem Land zu (DB-IP Lite, CC BY 4.0, https://db-ip.com).
package geo

import (
	"bufio"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"net/netip"
	"os"
	"sort"
	"strings"
	"sync"
)

type v6range struct {
	startHi, startLo, endHi, endLo uint64
	cc                             [2]byte
}

// DB ist die geladene Länder-Datenbank. Ein nil-*DB liefert stets "".
type DB struct {
	v4start, v4end []uint32
	v4cc           [][2]byte
	v6             []v6range

	mu    sync.Mutex
	cache map[string]string
}

// Load liest die DB-IP-Lite-CSV (start,end,cc), optional gzip-komprimiert.
func Load(path string) (*DB, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	br := bufio.NewReaderSize(f, 1<<16)
	var sc *bufio.Scanner
	if magic, _ := br.Peek(2); len(magic) == 2 && magic[0] == 0x1f && magic[1] == 0x8b {
		zr, err := gzip.NewReader(br)
		if err != nil {
			return nil, err
		}
		defer zr.Close()
		sc = bufio.NewScanner(zr)
	} else {
		sc = bufio.NewScanner(br)
	}
	db := &DB{cache: map[string]string{}}
	for sc.Scan() {
		parts := strings.Split(strings.TrimSpace(sc.Text()), ",")
		if len(parts) < 3 || len(parts[2]) != 2 {
			continue
		}
		a, err1 := netip.ParseAddr(parts[0])
		b, err2 := netip.ParseAddr(parts[1])
		if err1 != nil || err2 != nil {
			continue
		}
		cc := [2]byte{parts[2][0], parts[2][1]}
		if a.Is4() {
			a4, b4 := a.As4(), b.As4()
			db.v4start = append(db.v4start, binary.BigEndian.Uint32(a4[:]))
			db.v4end = append(db.v4end, binary.BigEndian.Uint32(b4[:]))
			db.v4cc = append(db.v4cc, cc)
		} else {
			a16, b16 := a.As16(), b.As16()
			db.v6 = append(db.v6, v6range{binary.BigEndian.Uint64(a16[:8]), binary.BigEndian.Uint64(a16[8:]),
				binary.BigEndian.Uint64(b16[:8]), binary.BigEndian.Uint64(b16[8:]), cc})
		}
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	if len(db.v4start) == 0 && len(db.v6) == 0 {
		return nil, fmt.Errorf("%s enthält keine Einträge", path)
	}
	return db, nil
}

// Ranges liefert die Anzahl geladener Bereiche.
func (db *DB) Ranges() int {
	if db == nil {
		return 0
	}
	return len(db.v4start) + len(db.v6)
}

// Lookup liefert den ISO-Ländercode, "LAN" für private Adressen, "??" wenn unbekannt.
func (db *DB) Lookup(ip string) string {
	if db == nil || ip == "" || ip == "-" {
		return ""
	}
	db.mu.Lock()
	if cc, ok := db.cache[ip]; ok {
		db.mu.Unlock()
		return cc
	}
	db.mu.Unlock()
	cc := db.lookup(ip)
	db.mu.Lock()
	if len(db.cache) > 200_000 {
		db.cache = map[string]string{}
	}
	db.cache[strings.Clone(ip)] = cc // ip verweist ggf. in eine ganze Logzeile
	db.mu.Unlock()
	return cc
}

func (db *DB) lookup(ip string) string {
	addr, err := netip.ParseAddr(strings.Trim(ip, "[]"))
	if err != nil {
		return "??"
	}
	addr = addr.Unmap().WithZone("")
	if addr.IsPrivate() || addr.IsLoopback() || addr.IsLinkLocalUnicast() {
		return "LAN"
	}
	if addr.Is4() {
		a4 := addr.As4()
		n := binary.BigEndian.Uint32(a4[:])
		i := sort.Search(len(db.v4start), func(i int) bool { return db.v4start[i] > n }) - 1
		if i >= 0 && n <= db.v4end[i] {
			return ccOf(db.v4cc[i])
		}
		return "??"
	}
	a16 := addr.As16()
	hi, lo := binary.BigEndian.Uint64(a16[:8]), binary.BigEndian.Uint64(a16[8:])
	i := sort.Search(len(db.v6), func(i int) bool {
		r := &db.v6[i]
		return r.startHi > hi || (r.startHi == hi && r.startLo > lo)
	}) - 1
	if i >= 0 {
		if r := &db.v6[i]; hi < r.endHi || (hi == r.endHi && lo <= r.endLo) {
			return ccOf(r.cc)
		}
	}
	return "??"
}

func ccOf(cc [2]byte) string {
	if cc == [2]byte{'Z', 'Z'} {
		return "??"
	}
	return string(cc[:])
}
