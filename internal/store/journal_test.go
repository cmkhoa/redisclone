package store

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// recorder is a Journal that keeps everything in memory.
type recorder struct {
	mu   sync.Mutex
	recs [][]string
}

func (r *recorder) Append(args ...[]byte) error {
	rec := make([]string, len(args))
	for i, a := range args {
		rec[i] = string(a)
	}
	r.mu.Lock()
	r.recs = append(r.recs, rec)
	r.mu.Unlock()
	return nil
}

func (r *recorder) all() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.recs))
	for i, rec := range r.recs {
		out[i] = strings.Join(rec, " ")
	}
	return out
}

func journalled(t *testing.T) (*Store, *recorder) {
	t.Helper()
	s, r := New(), &recorder{}
	s.SetJournal(r)
	return s, r
}

// What goes in the journal is the *effect*, not the command the client sent.
func TestJournalRecordsEffects(t *testing.T) {
	t.Run("plain set", func(t *testing.T) {
		s, r := journalled(t)
		s.Set("k", []byte("v"))
		want := []string{"SET k v"}
		if got := r.all(); !equalRecs(got, want) {
			t.Errorf("journal = %q, want %q", got, want)
		}
	})

	t.Run("del records only what existed", func(t *testing.T) {
		s, r := journalled(t)
		s.Set("a", []byte("1"))
		s.Set("b", []byte("2"))
		s.Del("a", "missing", "b")

		want := []string{"SET a 1", "SET b 2", "DEL a b"}
		if got := r.all(); !equalRecs(got, want) {
			t.Errorf("journal = %q, want %q", got, want)
		}
	})

	t.Run("a del that removed nothing is not recorded", func(t *testing.T) {
		s, r := journalled(t)
		s.Del("nope")
		if got := r.all(); len(got) != 0 {
			t.Errorf("journal = %q, want nothing", got)
		}
	})

	t.Run("expire in the past is recorded as a delete", func(t *testing.T) {
		s, r := journalled(t)
		s.Set("k", []byte("v"))
		s.Expire("k", -time.Second)

		want := []string{"SET k v", "DEL k"}
		if got := r.all(); !equalRecs(got, want) {
			t.Errorf("journal = %q, want %q", got, want)
		}
	})
}

// A relative deadline is not a fact: "expires in 60 seconds" replayed an hour
// later would resurrect a key that should have died. Everything that reaches
// the journal carries an absolute time.
func TestJournalConvertsDeadlinesToAbsolute(t *testing.T) {
	s, r := journalled(t)
	before := time.Now()
	s.SetWithTTL("k", []byte("v"), time.Hour)
	s.Expire("k", 2*time.Hour)
	after := time.Now()

	recs := r.all()
	if len(recs) != 2 {
		t.Fatalf("journal = %q, want 2 records", recs)
	}

	// SET carries its deadline inline rather than as a second record: a torn
	// tail between the two would replay as a key that never expires.
	fields := strings.Fields(recs[0])
	if len(fields) != 5 || fields[0] != "SET" || fields[3] != "PXAT" {
		t.Fatalf("record = %q, want SET k v PXAT <ms>", recs[0])
	}
	assertMillisWithin(t, fields[4], before.Add(time.Hour), after.Add(time.Hour))

	fields = strings.Fields(recs[1])
	if len(fields) != 3 || fields[0] != "PEXPIREAT" {
		t.Fatalf("record = %q, want PEXPIREAT k <ms>", recs[1])
	}
	assertMillisWithin(t, fields[2], before.Add(2*time.Hour), after.Add(2*time.Hour))
}

func assertMillisWithin(t *testing.T, field string, lo, hi time.Time) {
	t.Helper()
	ms, err := strconv.ParseInt(field, 10, 64)
	if err != nil {
		t.Fatalf("deadline %q is not an integer: %v", field, err)
	}
	got := time.UnixMilli(ms)
	if got.Before(lo.Add(-time.Millisecond)) || got.After(hi.Add(time.Millisecond)) {
		t.Errorf("deadline %v is outside [%v, %v]", got, lo, hi)
	}
}

// Expired keys collected by the lazy path or the sampler are deliberately not
// journalled: the log already says when they expire, so replay reaches the same
// conclusion on its own. Logging the collection would double the log's write
// volume for keys that are already accounted for.
func TestExpiryCollectionIsNotJournalled(t *testing.T) {
	s, r := journalled(t)
	plantExpired(s, "k")
	before := len(r.all())

	s.Get("k")            // lazy collection
	s.activeExpireCycle() // and the sampler
	if got := r.all(); len(got) != before {
		t.Errorf("journal grew to %q; collecting an expired key should record nothing", got)
	}
}

// The ordering property the whole design rests on: the journal's order is the
// order mutations actually happened in.
//
// Many goroutines race to SET the same key. The store's lock picks a winner —
// and because the journal append happens under that same lock, the *last*
// record for the key must name the value the store ended up holding. Appending
// after releasing the lock would let the loser's record land last, and replay
// would rebuild a keyspace that never existed.
func TestJournalOrderMatchesMutationOrder(t *testing.T) {
	for attempt := 0; attempt < 50; attempt++ {
		s, r := journalled(t)

		var wg sync.WaitGroup
		for g := 0; g < 8; g++ {
			wg.Add(1)
			go func(g int) {
				defer wg.Done()
				for i := 0; i < 100; i++ {
					s.Set("contended", []byte(fmt.Sprintf("writer-%d-%d", g, i)))
				}
			}(g)
		}
		wg.Wait()

		recs := r.all()
		last := strings.Fields(recs[len(recs)-1])
		if len(last) != 3 || last[0] != "SET" {
			t.Fatalf("last record = %q, want a SET", recs[len(recs)-1])
		}
		held, _ := s.Get("contended")
		if last[2] != string(held) {
			t.Fatalf("attempt %d: journal ends with %q but the store holds %q — "+
				"the log is out of order with the keyspace", attempt, last[2], held)
		}
	}
}

func equalRecs(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
