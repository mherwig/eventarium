package filter

import (
	"sync"
	"testing"
)

// TestCompileAttributeCaches pins the memoisation that took the fleet's idle
// load from loadavg 22 to 3. registry.Reload recompiles every subscription's
// filters on every poll in every process, so without this each tick paid for
// cel.NewEnv + Compile + Program on expressions that had not changed.
//
// What matters here is that caching changes *speed* and nothing else: the
// same source yields the equivalent program, a source that does not compile
// still errors on the second attempt (a cached failure must not become a
// silent success), and concurrent callers do not race.
func TestCompileAttributeCaches(t *testing.T) {
	const src = `attributes["shard"] == "3" && id != ""`

	first, err := CompileAttribute(src)
	if err != nil {
		t.Fatalf("first compile: %v", err)
	}
	second, err := CompileAttribute(src)
	if err != nil {
		t.Fatalf("second compile: %v", err)
	}
	if first != second {
		t.Error("same source returned different programs: the cache missed or copied")
	}
	if first.Raw != src {
		t.Errorf("Raw = %q, want %q", first.Raw, src)
	}

	// A different source must not collide with the first.
	other, err := CompileAttribute(`attributes["region"] == "eu"`)
	if err != nil {
		t.Fatalf("other compile: %v", err)
	}
	if other == first {
		t.Error("distinct sources shared a cached program")
	}
}

// TestCompileAttributeCachesFailures: a filter that does not compile must keep
// failing. Memoising only successes would leave an invalid subscription
// recompiling on every tick - the exact cost this cache exists to remove - and
// memoising wrongly would hide a real error from the registry.
func TestCompileAttributeCachesFailures(t *testing.T) {
	const bad = "this is not a CEL expression at all"
	_, err1 := CompileAttribute(bad)
	if err1 == nil {
		t.Fatal("first call accepted an invalid filter")
	}
	_, err2 := CompileAttribute(bad)
	if err2 == nil {
		t.Fatal("cached failure became a success")
	}
	if err1.Error() != err2.Error() {
		t.Errorf("error changed between calls:\n  %v\n  %v", err1, err2)
	}
	// Empty is rejected before the cache and must stay that way.
	if _, err := CompileAttribute("   "); err == nil {
		t.Error("empty expression accepted")
	}
}

// TestCompileDataCaches covers the data-filter path, which keeps its own map.
func TestCompileDataCaches(t *testing.T) {
	const src = `int(data.seq) % 2 == 0`
	first, err := CompileData(src)
	if err != nil {
		t.Fatalf("first compile: %v", err)
	}
	second, err := CompileData(src)
	if err != nil {
		t.Fatalf("second compile: %v", err)
	}
	if first != second {
		t.Error("same data source returned different programs")
	}
	if _, err := CompileData("data."); err == nil {
		t.Error("invalid data filter accepted")
	}
}

// TestCompileConcurrent exercises the sync.Map path under the race detector.
// Reload holds a mutex, but registry-api validates filters from HTTP handlers
// while a reload is in flight, so the two really do overlap.
func TestCompileConcurrent(t *testing.T) {
	const src = `attributes["shard"] == "7"`
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				if _, err := CompileAttribute(src); err != nil {
					t.Errorf("concurrent compile: %v", err)
					return
				}
				if _, err := CompileData(`int(data.seq) > 0`); err != nil {
					t.Errorf("concurrent data compile: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
}
