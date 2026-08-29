package store

import (
	"bytes"
	"fmt"
	"strconv"
	"sync"
	"testing"
)

func TestGetSet(t *testing.T) {
	s := New()

	if _, ok := s.Get("missing"); ok {
		t.Error("Get on an absent key reported it exists")
	}

	s.Set("k", []byte("v"))
	got, ok := s.Get("k")
	if !ok {
		t.Fatal("Get after Set: key not found")
	}
	if string(got) != "v" {
		t.Errorf("Get = %q, want %q", got, "v")
	}
}

func TestSetOverwrites(t *testing.T) {
	s := New()
	s.Set("k", []byte("first"))
	s.Set("k", []byte("second"))

	if got, _ := s.Get("k"); string(got) != "second" {
		t.Errorf("Get after overwrite = %q, want %q", got, "second")
	}
	if s.Len() != 1 {
		t.Errorf("Len = %d after overwriting one key, want 1", s.Len())
	}
}

// The empty value is a real value: a key holding "" exists, and that has to be
// distinguishable from a missing key, because GET replies differently for the
// two ($0 vs $-1).
func TestEmptyAndNilValues(t *testing.T) {
	s := New()
	s.Set("empty", []byte(""))
	s.Set("nil", nil)

	for _, k := range []string{"empty", "nil"} {
		got, ok := s.Get(k)
		if !ok {
			t.Errorf("key %q: not found", k)
			continue
		}
		if len(got) != 0 {
			t.Errorf("key %q: got %q, want an empty value", k, got)
		}
		if got == nil {
			t.Errorf("key %q: stored value is nil; Set should normalise it to an empty slice", k)
		}
	}
}

// Keys and values are arbitrary byte strings, not text: RESP is length-prefixed
// all the way down, so a key containing CRLF or NUL has to work.
func TestBinaryKeysAndValues(t *testing.T) {
	s := New()
	key := "a\r\nb\x00c"
	val := []byte("v\r\n\x00\xff")
	s.Set(key, val)

	got, ok := s.Get(key)
	if !ok {
		t.Fatal("binary key not found")
	}
	if !bytes.Equal(got, val) {
		t.Errorf("got %q, want %q", got, val)
	}
	if _, ok := s.Get("a"); ok {
		t.Error("a prefix of the binary key matched — keys are being truncated somewhere")
	}
}

func TestDel(t *testing.T) {
	tests := []struct {
		name    string
		present []string
		del     []string
		want    int
		wantLen int
	}{
		{"one present", []string{"a"}, []string{"a"}, 1, 0},
		{"one absent", []string{"a"}, []string{"b"}, 0, 1},
		{"some of each", []string{"a", "b"}, []string{"a", "x", "b"}, 2, 0},
		// A repeated key can only be deleted once, so DEL k k on an existing
		// key returns 1 — unlike EXISTS, which counts duplicates.
		{"duplicate keys", []string{"a"}, []string{"a", "a"}, 1, 0},
		{"nothing", nil, nil, 0, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := New()
			for _, k := range tt.present {
				s.Set(k, []byte("v"))
			}
			if got := s.Del(tt.del...); got != tt.want {
				t.Errorf("Del(%q) = %d, want %d", tt.del, got, tt.want)
			}
			if got := s.Len(); got != tt.wantLen {
				t.Errorf("Len after Del = %d, want %d", got, tt.wantLen)
			}
		})
	}
}

func TestExists(t *testing.T) {
	s := New()
	s.Set("a", []byte("1"))
	s.Set("b", []byte("2"))

	tests := []struct {
		keys []string
		want int
	}{
		{[]string{"a"}, 1},
		{[]string{"missing"}, 0},
		{[]string{"a", "b"}, 2},
		{[]string{"a", "missing", "b"}, 2},
		// Duplicates count, as in real Redis.
		{[]string{"a", "a", "a"}, 3},
		{nil, 0},
	}

	for _, tt := range tests {
		if got := s.Exists(tt.keys...); got != tt.want {
			t.Errorf("Exists(%q) = %d, want %d", tt.keys, got, tt.want)
		}
	}
}

// A value read out of the store stays valid and unchanged even if another
// goroutine overwrites or deletes the key underneath us. That is the invariant
// the whole no-copy design rests on: writes replace the map entry, they never
// write over the bytes a reader is holding.
func TestReadValueSurvivesOverwriteAndDelete(t *testing.T) {
	s := New()
	s.Set("k", []byte("original"))

	held, _ := s.Get("k")
	s.Set("k", []byte("replaced"))
	s.Del("k")

	if string(held) != "original" {
		t.Errorf("value held across an overwrite became %q, want %q", held, "original")
	}
}

// Run with -race: this is the test that would have caught an unlocked map.
// Concurrent writers to distinct keys, concurrent writers to one shared key,
// and concurrent readers of everything, all at once.
func TestConcurrentAccess(t *testing.T) {
	const (
		goroutines = 16
		iterations = 500
	)
	s := New()
	var wg sync.WaitGroup

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				own := "own:" + strconv.Itoa(g) + ":" + strconv.Itoa(i)
				s.Set(own, []byte(own))

				// Every goroutine fights over the same key. Whoever wins, the
				// value read back must be one somebody actually wrote — never
				// a torn mixture.
				s.Set("shared", []byte("writer-"+strconv.Itoa(g)))
				if v, ok := s.Get("shared"); ok && !bytes.HasPrefix(v, []byte("writer-")) {
					t.Errorf("shared key holds a torn value: %q", v)
					return
				}

				// Read back our own key, then delete it. Nobody else touches
				// it, so both results are deterministic even under contention.
				if v, ok := s.Get(own); !ok || string(v) != own {
					t.Errorf("own key %q read back as %q (found=%v)", own, v, ok)
					return
				}
				if n := s.Del(own); n != 1 {
					t.Errorf("Del(%q) = %d, want 1", own, n)
					return
				}
				s.Exists("shared", own, "missing")
			}
		}(g)
	}
	wg.Wait()

	// Every "own" key was deleted by its owner; only "shared" is left.
	if got := s.Len(); got != 1 {
		t.Errorf("Len = %d after all goroutines cleaned up, want 1 (just \"shared\")", got)
	}
}

// --- benchmarks ----------------------------------------------------------
//
// Run: go test ./internal/store -bench . -benchmem -cpu 1,4,8
//
// These exist to answer the question DECISIONS.md raises and cannot answer
// from first principles: does one RWMutex over one map actually hold up under
// parallel load, and where does it stop holding up? The read-heavy and
// write-heavy numbers should diverge sharply as -cpu goes up — the read side
// scales until the RWMutex's own cache line becomes the bottleneck, the write
// side serialises immediately.

const benchKeys = 4096

func benchStore() (*Store, []string) {
	s := New()
	ks := make([]string, benchKeys)
	for i := range ks {
		ks[i] = fmt.Sprintf("key:%06d", i)
		s.Set(ks[i], []byte("value"))
	}
	return s, ks
}

func BenchmarkGetParallel(b *testing.B) {
	s, ks := benchStore()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			s.Get(ks[i%benchKeys])
			i++
		}
	})
}

func BenchmarkSetParallel(b *testing.B) {
	s, ks := benchStore()
	val := []byte("value")
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			s.Set(ks[i%benchKeys], val)
			i++
		}
	})
}

// Roughly a real workload: 9 reads per write.
func BenchmarkMixedParallel(b *testing.B) {
	s, ks := benchStore()
	val := []byte("value")
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%10 == 0 {
				s.Set(ks[i%benchKeys], val)
			} else {
				s.Get(ks[i%benchKeys])
			}
			i++
		}
	})
}
