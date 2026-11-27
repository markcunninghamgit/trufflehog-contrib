package resourcecontrol

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime/pprof"
	"sync"
	"time"
)

const defaultHeapDumpInterval = 2 * time.Minute

// HeapProfileDumper writes heap and goroutine profiles under elevated or critical pressure so the
// allocation sites behind a growing heap are on disk even if the process is later SIGKILLed.
// A heap profile reflects the state as of the last completed GC.
type HeapProfileDumper struct {
	Dir         string
	MinInterval time.Duration
	// MaxDumps caps dumps per process; zero means unlimited.
	MaxDumps int
	Logf     func(msg string, keysAndValues ...any)

	mu        sync.Mutex
	last      time.Time
	lastLevel PressureLevel
	count     int
	inFlight  bool
	wg        sync.WaitGroup
}

// MaybeDump starts a background dump and reports whether it did. A dump is skipped when the level
// is below elevated, one is already running, the cap is reached, or the interval has not elapsed;
// a higher level than the last dump bypasses the interval.
func (d *HeapProfileDumper) MaybeDump(level PressureLevel, reason string) bool {
	if d == nil || d.Dir == "" {
		return false
	}
	d.mu.Lock()
	if level < PressureElevated {
		d.lastLevel = PressureUnknown
		d.mu.Unlock()
		return false
	}
	interval := d.MinInterval
	if interval <= 0 {
		interval = defaultHeapDumpInterval
	}
	escalated := level > d.lastLevel
	tooSoon := !d.last.IsZero() && time.Since(d.last) < interval
	capped := d.MaxDumps > 0 && d.count >= d.MaxDumps
	if d.inFlight || capped || (tooSoon && !escalated) {
		d.mu.Unlock()
		return false
	}
	d.inFlight = true
	d.last = time.Now()
	d.lastLevel = level
	d.count++
	seq := d.count
	d.wg.Add(1)
	d.mu.Unlock()

	go func() {
		defer d.wg.Done()
		d.write(seq, level, reason)
		d.mu.Lock()
		d.inFlight = false
		d.mu.Unlock()
	}()
	return true
}

// Wait blocks until any running dump finishes.
func (d *HeapProfileDumper) Wait() { d.wg.Wait() }

func (d *HeapProfileDumper) write(seq int, level PressureLevel, reason string) {
	if err := os.MkdirAll(d.Dir, 0o700); err != nil {
		d.logf("heap profile dump failed", "dir", d.Dir, "error", err.Error())
		return
	}
	base := fmt.Sprintf("%s-%03d-pressure%d", time.Now().Format("20060102-150405"), seq, level)
	profiles := []struct {
		name  string
		ext   string
		debug int
	}{
		{"heap", ".pb.gz", 0},
		{"goroutine", ".txt", 1},
	}
	for _, p := range profiles {
		path := filepath.Join(d.Dir, p.name+"-"+base+p.ext)
		size, err := writeProfile(p.name, path, p.debug)
		if err != nil {
			d.logf("heap profile dump failed", "profile", p.name, "path", path, "error", err.Error())
			continue
		}
		d.logf("profile written", "role", "resource-controller", "event", "profile_written", "profile", p.name, "path", path, "bytes", size, "pressure", level, "reason", reason)
	}
}

func (d *HeapProfileDumper) logf(msg string, keysAndValues ...any) {
	if d.Logf != nil {
		d.Logf(msg, keysAndValues...)
	}
}

func writeProfile(name, path string, debug int) (int64, error) {
	profile := pprof.Lookup(name)
	if profile == nil {
		return 0, fmt.Errorf("unknown profile %q", name)
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return 0, err
	}
	if err := profile.WriteTo(f, debug); err != nil {
		_ = f.Close()
		_ = os.Remove(path)
		return 0, err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(path)
		return 0, err
	}
	info, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}
