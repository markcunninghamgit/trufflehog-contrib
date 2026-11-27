package resourcecontrol

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"
)

// PressureLevel describes the amount of resource pressure observed by a provider.
type PressureLevel int

const (
	PressureUnknown PressureLevel = iota
	PressureHealthy
	PressureElevated
	PressureCritical
)

// Snapshot is one observation from a platform-specific resource provider.
type Snapshot struct {
	Level  PressureLevel
	Reason string
}

// Provider supplies resource observations. Platform-specific implementations can
// be added without changing the concurrency controller.
type Provider interface {
	Snapshot(context.Context) Snapshot
}

// UnknownProvider deliberately reports unknown pressure until a platform-specific
// provider is selected. Unknown pressure never causes an automatic scale-up.
type UnknownProvider struct{}

func (UnknownProvider) Snapshot(context.Context) Snapshot {
	return Snapshot{Level: PressureUnknown, Reason: "resource pressure provider unavailable"}
}

// RuntimeProvider reports conservative pressure from the Go runtime only.
// It deliberately does not claim to know OS memory pressure, swap, or Jetsam state.
type RuntimeProvider struct {
	ElevatedHeapBytes  uint64
	CriticalHeapBytes  uint64
	ElevatedGoroutines int
	CriticalGoroutines int
}

func (p RuntimeProvider) Snapshot(context.Context) Snapshot {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	goroutines := runtime.NumGoroutine()

	criticalHeap := p.CriticalHeapBytes > 0 && mem.HeapAlloc >= p.CriticalHeapBytes
	criticalGoroutines := p.CriticalGoroutines > 0 && goroutines >= p.CriticalGoroutines
	if criticalHeap || criticalGoroutines {
		return Snapshot{Level: PressureCritical, Reason: runtimePressureReason(mem.HeapAlloc, goroutines, p)}
	}

	elevatedHeap := p.ElevatedHeapBytes > 0 && mem.HeapAlloc >= p.ElevatedHeapBytes
	elevatedGoroutines := p.ElevatedGoroutines > 0 && goroutines >= p.ElevatedGoroutines
	if elevatedHeap || elevatedGoroutines {
		return Snapshot{Level: PressureElevated, Reason: runtimePressureReason(mem.HeapAlloc, goroutines, p)}
	}

	if p.ElevatedHeapBytes > 0 || p.ElevatedGoroutines > 0 {
		return Snapshot{Level: PressureHealthy, Reason: runtimePressureReason(mem.HeapAlloc, goroutines, p)}
	}
	return Snapshot{Level: PressureUnknown, Reason: "runtime pressure thresholds unavailable"}
}

func runtimePressureReason(heapBytes uint64, goroutines int, p RuntimeProvider) string {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	return fmt.Sprintf("heap_alloc=%d heap_sys=%d heap_inuse=%d heap_idle=%d stack_inuse=%d mallocs=%d frees=%d gc_cycles=%d goroutines=%d elevated_heap=%d critical_heap=%d elevated_goroutines=%d critical_goroutines=%d", heapBytes, mem.HeapSys, mem.HeapInuse, mem.HeapIdle, mem.StackInuse, mem.Mallocs, mem.Frees, mem.NumGC, goroutines, p.ElevatedHeapBytes, p.CriticalHeapBytes, p.ElevatedGoroutines, p.CriticalGoroutines)
}

// Decision records one concurrency change or observation.
type Decision struct {
	Previous          int
	Current           int
	Maximum           int
	Snapshot          Snapshot
	ScaleAction       string
	CooldownRemaining time.Duration
	ScaleEligible     bool
	// CPUPercent is the latest system-wide utilisation; CPUState is disabled, unavailable, cool, warm, or hot.
	CPUPercent float64
	CPUState   string
}

// Controller adjusts an admission limit without interrupting work already in flight.
// The caller owns the limit through SetLimit and may use Decision for structured logs.
type Controller struct {
	provider   Provider
	setLimit   func(int)
	onDecision func(Decision)

	minimum  int
	maximum  int
	current  int
	interval time.Duration
	cooldown time.Duration

	cpu        CPUSampler
	cpuHigh    float64
	cpuLow     float64
	cpuHotNeed int

	mu         sync.Mutex
	lastChange time.Time
	cpuHotRun  int
}

// Config configures an adaptive controller. Maximum defaults to runtime.NumCPU().
type Config struct {
	Provider   Provider
	SetLimit   func(int)
	OnDecision func(Decision)
	Minimum    int
	Maximum    int
	Initial    int
	Interval   time.Duration
	Cooldown   time.Duration

	// CPU gates scaling on system-wide utilisation. It is disabled when CPU is nil or CPUHighPercent is 0.
	// CPU pressure only scales down and blocks scaling up; it never feeds shedding or heap dumps.
	CPU CPUSampler
	// CPUHighPercent is the utilisation at or above which a sample counts as hot.
	CPUHighPercent float64
	// CPULowPercent is the utilisation at or below which scaling up is allowed again. Between low and high
	// the limit holds. Defaults to CPUHighPercent-20, or half of it when that is not positive.
	CPULowPercent float64
	// CPUHotSamples is how many consecutive hot samples scale down. Defaults to 3.
	CPUHotSamples int
}

func NewController(cfg Config) *Controller {
	maximum := cfg.Maximum
	if maximum <= 0 {
		maximum = runtime.NumCPU()
	}
	if maximum < 1 {
		maximum = 1
	}
	minimum := cfg.Minimum
	if minimum < 1 {
		minimum = 1
	}
	if minimum > maximum {
		minimum = maximum
	}
	initial := cfg.Initial
	if initial < minimum {
		initial = minimum
	}
	if initial > maximum {
		initial = maximum
	}
	provider := cfg.Provider
	if provider == nil {
		provider = UnknownProvider{}
	}
	interval := cfg.Interval
	if interval <= 0 {
		interval = 5 * time.Second
	}
	cooldown := cfg.Cooldown
	if cooldown <= 0 {
		cooldown = 30 * time.Second
	}
	cpuHigh := cfg.CPUHighPercent
	cpuLow := cfg.CPULowPercent
	if cpuLow <= 0 || cpuLow >= cpuHigh {
		cpuLow = cpuHigh - 20
		if cpuLow <= 0 {
			cpuLow = cpuHigh / 2
		}
	}
	cpuHotNeed := cfg.CPUHotSamples
	if cpuHotNeed < 1 {
		cpuHotNeed = 3
	}
	return &Controller{
		provider:   provider,
		setLimit:   cfg.SetLimit,
		onDecision: cfg.OnDecision,
		minimum:    minimum,
		maximum:    maximum,
		current:    initial,
		interval:   interval,
		cooldown:   cooldown,
		cpu:        cfg.CPU,
		cpuHigh:    cpuHigh,
		cpuLow:     cpuLow,
		cpuHotNeed: cpuHotNeed,
	}
}

func (c *Controller) Current() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.current
}

func (c *Controller) Maximum() int { return c.maximum }

// Start applies the initial limit and adjusts it until ctx is cancelled.
func (c *Controller) Start(ctx context.Context) {
	c.apply(c.current, Snapshot{Level: PressureUnknown, Reason: "initial concurrency"})
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.adjust(ctx)
		}
	}
}

func (c *Controller) adjust(ctx context.Context) {
	snapshot := c.provider.Snapshot(ctx)
	// Sampled every tick, outside the lock, so each reading covers exactly one interval.
	var busy float64
	var busyOK bool
	if c.cpuEnabled() {
		busy, busyOK = c.cpu.SampleCPU(ctx)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	cpuState := c.cpuStateLocked(busy, busyOK)
	cooldownRemaining := c.cooldown - time.Since(c.lastChange)
	if cooldownRemaining < 0 {
		cooldownRemaining = 0
	}
	eligible := cooldownRemaining == 0
	if snapshot.Level == PressureCritical || snapshot.Level == PressureElevated {
		if c.current > c.minimum {
			// Pressure response must not wait for the recovery cooldown. Upscaling
			// below still observes the cooldown after the last limit change.
			c.applyLocked(c.current-1, snapshot, "scale_down", cooldownRemaining, eligible, busy, cpuState)
			return
		}
	}
	if cpuState == cpuStateHot && c.current > c.minimum && eligible {
		c.applyLocked(c.current-1, snapshot, "scale_down_cpu", 0, true, busy, cpuState)
		return
	}
	// Unavailable CPU data does not block scale-up: a broken sampler must not freeze the limit forever.
	cpuAllowsUp := cpuState == cpuStateDisabled || cpuState == cpuStateUnavailable || cpuState == cpuStateCool
	if snapshot.Level == PressureHealthy && cpuAllowsUp && c.current < c.maximum && eligible {
		c.applyLocked(c.current+1, snapshot, "scale_up", 0, true, busy, cpuState)
		return
	}
	if c.onDecision != nil {
		c.onDecision(Decision{Previous: c.current, Current: c.current, Maximum: c.maximum, Snapshot: snapshot, ScaleAction: "hold", CooldownRemaining: cooldownRemaining, ScaleEligible: eligible, CPUPercent: busy, CPUState: cpuState})
	}
}

const (
	cpuStateDisabled    = "disabled"
	cpuStateUnavailable = "unavailable"
	cpuStateCool        = "cool"
	cpuStateWarm        = "warm"
	cpuStateHot         = "hot"
)

func (c *Controller) cpuEnabled() bool {
	return c.cpu != nil && c.cpuHigh > 0
}

// cpuStateLocked classifies one sample. Hot needs cpuHotNeed consecutive samples at or above the high
// mark, warm is anything above the low mark that is not hot, and cool is at or below the low mark.
func (c *Controller) cpuStateLocked(busy float64, ok bool) string {
	if !c.cpuEnabled() {
		return cpuStateDisabled
	}
	if !ok {
		c.cpuHotRun = 0
		return cpuStateUnavailable
	}
	if busy >= c.cpuHigh {
		c.cpuHotRun++
	} else {
		c.cpuHotRun = 0
	}
	switch {
	case c.cpuHotRun >= c.cpuHotNeed:
		return cpuStateHot
	case busy <= c.cpuLow:
		return cpuStateCool
	default:
		return cpuStateWarm
	}
}

func (c *Controller) apply(limit int, snapshot Snapshot) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.applyLocked(limit, snapshot, "initial", 0, true, 0, cpuStateDisabled)
}

func (c *Controller) applyLocked(limit int, snapshot Snapshot, action string, cooldownRemaining time.Duration, eligible bool, cpuPercent float64, cpuState string) {
	previous := c.current
	c.current = limit
	c.lastChange = time.Now()
	if c.setLimit != nil {
		c.setLimit(limit)
	}
	if c.onDecision != nil {
		c.onDecision(Decision{Previous: previous, Current: limit, Maximum: c.maximum, Snapshot: snapshot, ScaleAction: action, CooldownRemaining: cooldownRemaining, ScaleEligible: eligible, CPUPercent: cpuPercent, CPUState: cpuState})
	}
}
