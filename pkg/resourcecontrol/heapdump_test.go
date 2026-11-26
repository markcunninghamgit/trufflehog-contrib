package resourcecontrol

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func dumpedFiles(t *testing.T, dir, pattern string) []string {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(dir, pattern))
	if err != nil {
		t.Fatal(err)
	}
	return matches
}

func TestHeapProfileDumper_SkipsHealthyAndUnconfigured(t *testing.T) {
	dir := t.TempDir()
	d := &HeapProfileDumper{Dir: dir}
	if d.MaybeDump(PressureHealthy, "ok") {
		t.Fatal("healthy pressure must not dump")
	}
	if (&HeapProfileDumper{}).MaybeDump(PressureCritical, "no dir") {
		t.Fatal("empty Dir must not dump")
	}
	var nilDumper *HeapProfileDumper
	if nilDumper.MaybeDump(PressureCritical, "nil") {
		t.Fatal("nil dumper must not dump")
	}
	if files := dumpedFiles(t, dir, "*"); len(files) != 0 {
		t.Fatalf("expected no files, got %v", files)
	}
}

func TestHeapProfileDumper_WritesHeapAndGoroutineProfiles(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "profiles")
	d := &HeapProfileDumper{Dir: dir}
	if !d.MaybeDump(PressureElevated, "test") {
		t.Fatal("elevated pressure should dump")
	}
	d.Wait()
	for _, pattern := range []string{"heap-*.pb.gz", "goroutine-*.txt"} {
		files := dumpedFiles(t, dir, pattern)
		if len(files) != 1 {
			t.Fatalf("%s: expected 1 file, got %v", pattern, files)
		}
		if info, err := os.Stat(files[0]); err != nil || info.Size() == 0 {
			t.Fatalf("%s: empty or unreadable: %v", files[0], err)
		}
	}
}

func TestHeapProfileDumper_RateLimitsAndEscalates(t *testing.T) {
	d := &HeapProfileDumper{Dir: t.TempDir(), MinInterval: time.Hour}
	if !d.MaybeDump(PressureElevated, "first") {
		t.Fatal("first elevated dump should run")
	}
	d.Wait()
	if d.MaybeDump(PressureElevated, "repeat") {
		t.Fatal("same level inside the interval must be skipped")
	}
	if !d.MaybeDump(PressureCritical, "escalated") {
		t.Fatal("escalation to critical must bypass the interval")
	}
	d.Wait()
	if d.MaybeDump(PressureCritical, "repeat") {
		t.Fatal("repeat critical inside the interval must be skipped")
	}
	d.MaybeDump(PressureHealthy, "recovered")
	if !d.MaybeDump(PressureElevated, "again") {
		t.Fatal("elevated after recovery should dump again")
	}
	d.Wait()
}

func TestHeapProfileDumper_HonorsMaxDumps(t *testing.T) {
	d := &HeapProfileDumper{Dir: t.TempDir(), MinInterval: time.Nanosecond, MaxDumps: 2}
	for i := 0; i < 2; i++ {
		if !d.MaybeDump(PressureElevated, "dump") {
			t.Fatalf("dump %d should run", i)
		}
		d.Wait()
		time.Sleep(time.Millisecond)
	}
	if d.MaybeDump(PressureCritical, "over cap") {
		t.Fatal("cap must hold even on escalation")
	}
}
