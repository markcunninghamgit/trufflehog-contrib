package resourcecontrol

import (
	"context"
	"testing"
	"time"
)

type scriptedCPU struct {
	samples []float64
	// unavailable marks the sample at the same index as having no reading.
	unavailable map[int]bool
	calls       int
}

func (s *scriptedCPU) SampleCPU(context.Context) (float64, bool) {
	i := s.calls
	s.calls++
	if s.unavailable[i] {
		return 0, false
	}
	if i >= len(s.samples) {
		return s.samples[len(s.samples)-1], true
	}
	return s.samples[i], true
}

func newCPUTestController(level PressureLevel, initial int, cpu CPUSampler, decisions *[]Decision) *Controller {
	return NewController(Config{
		Provider:       fixedProvider{snapshot: Snapshot{Level: level}},
		Maximum:        8,
		Initial:        initial,
		Cooldown:       time.Nanosecond,
		CPU:            cpu,
		CPUHighPercent: 80,
		CPULowPercent:  60,
		CPUHotSamples:  3,
		OnDecision:     func(d Decision) { *decisions = append(*decisions, d) },
	})
}

func lastAction(t *testing.T, decisions []Decision) string {
	t.Helper()
	if len(decisions) == 0 {
		t.Fatal("no decisions recorded")
	}
	return decisions[len(decisions)-1].ScaleAction
}

func TestCPUHotNeedsSustainedSamplesToScaleDown(t *testing.T) {
	var decisions []Decision
	c := newCPUTestController(PressureHealthy, 4, &scriptedCPU{samples: []float64{95}}, &decisions)

	c.adjust(context.Background())
	c.adjust(context.Background())
	if got := c.Current(); got != 4 {
		t.Fatalf("after 2 hot samples Current() = %d, want 4", got)
	}
	if got := lastAction(t, decisions); got != "hold" {
		t.Fatalf("action = %q, want hold", got)
	}

	c.adjust(context.Background())
	if got := c.Current(); got != 3 {
		t.Fatalf("after 3 hot samples Current() = %d, want 3", got)
	}
	if got := lastAction(t, decisions); got != "scale_down_cpu" {
		t.Fatalf("action = %q, want scale_down_cpu", got)
	}
}

func TestCPUSpikeBelowStreakIsIgnored(t *testing.T) {
	var decisions []Decision
	cpu := &scriptedCPU{samples: []float64{95, 95, 50, 95, 95, 50}}
	c := newCPUTestController(PressureUnknown, 4, cpu, &decisions)

	for range cpu.samples {
		c.adjust(context.Background())
	}
	if got := c.Current(); got != 4 {
		t.Fatalf("Current() = %d, want 4: a streak broken by a cool sample must not scale down", got)
	}
}

func TestCPUWarmBandBlocksScaleUpButDoesNotScaleDown(t *testing.T) {
	var decisions []Decision
	// 70 is above the low mark and below the high mark, so the limit must hold.
	c := newCPUTestController(PressureHealthy, 4, &scriptedCPU{samples: []float64{70}}, &decisions)

	for range 6 {
		c.adjust(context.Background())
	}
	if got := c.Current(); got != 4 {
		t.Fatalf("Current() = %d, want 4 while CPU is in the warm band", got)
	}
	if got := decisions[len(decisions)-1].CPUState; got != cpuStateWarm {
		t.Fatalf("CPUState = %q, want %q", got, cpuStateWarm)
	}
}

func TestCPUOscillationAcrossHighMarkDoesNotThrash(t *testing.T) {
	var decisions []Decision
	// Alternating 85 and 70 never reaches 3 consecutive hot samples and never drops to the low mark.
	cpu := &scriptedCPU{samples: []float64{85, 70, 85, 70, 85, 70, 85, 70, 85, 70}}
	c := newCPUTestController(PressureHealthy, 4, cpu, &decisions)

	for range cpu.samples {
		c.adjust(context.Background())
	}
	if got := c.Current(); got != 4 {
		t.Fatalf("Current() = %d, want 4", got)
	}
	for _, d := range decisions {
		if d.Previous != d.Current {
			t.Fatalf("limit changed %d -> %d (%s) during oscillation", d.Previous, d.Current, d.ScaleAction)
		}
	}
}

func TestCPUCoolAllowsScaleUp(t *testing.T) {
	var decisions []Decision
	c := newCPUTestController(PressureHealthy, 4, &scriptedCPU{samples: []float64{30}}, &decisions)

	c.adjust(context.Background())
	if got := c.Current(); got != 5 {
		t.Fatalf("Current() = %d, want 5", got)
	}
	if got := lastAction(t, decisions); got != "scale_up" {
		t.Fatalf("action = %q, want scale_up", got)
	}
}

func TestCPUHotBlocksScaleUpAtMinimum(t *testing.T) {
	var decisions []Decision
	c := newCPUTestController(PressureHealthy, 1, &scriptedCPU{samples: []float64{99}}, &decisions)

	for range 5 {
		c.adjust(context.Background())
	}
	if got := c.Current(); got != 1 {
		t.Fatalf("Current() = %d, want 1: hot CPU must not scale up and cannot go below the minimum", got)
	}
}

func TestCPUUnavailableDoesNotFreezeScaleUp(t *testing.T) {
	var decisions []Decision
	cpu := &scriptedCPU{samples: []float64{0}, unavailable: map[int]bool{0: true}}
	c := newCPUTestController(PressureHealthy, 4, cpu, &decisions)

	c.adjust(context.Background())
	if got := c.Current(); got != 5 {
		t.Fatalf("Current() = %d, want 5 when the CPU sample is unavailable", got)
	}
	if got := decisions[len(decisions)-1].CPUState; got != cpuStateUnavailable {
		t.Fatalf("CPUState = %q, want %q", got, cpuStateUnavailable)
	}
}

func TestCPUUnavailableResetsHotStreak(t *testing.T) {
	var decisions []Decision
	cpu := &scriptedCPU{samples: []float64{95, 95, 0, 95, 95}, unavailable: map[int]bool{2: true}}
	c := newCPUTestController(PressureUnknown, 4, cpu, &decisions)

	for range cpu.samples {
		c.adjust(context.Background())
	}
	if got := c.Current(); got != 4 {
		t.Fatalf("Current() = %d, want 4: a gap in samples must restart the streak", got)
	}
}

func TestCPUDisabledKeepsMemoryOnlyBehaviour(t *testing.T) {
	var decisions []Decision
	c := NewController(Config{
		Provider:   fixedProvider{snapshot: Snapshot{Level: PressureHealthy}},
		Maximum:    8,
		Initial:    4,
		Cooldown:   time.Nanosecond,
		CPU:        &scriptedCPU{samples: []float64{99}},
		OnDecision: func(d Decision) { decisions = append(decisions, d) },
	})

	c.adjust(context.Background())
	if got := c.Current(); got != 5 {
		t.Fatalf("Current() = %d, want 5: CPUHighPercent=0 disables the CPU gate", got)
	}
	if got := decisions[len(decisions)-1].CPUState; got != cpuStateDisabled {
		t.Fatalf("CPUState = %q, want %q", got, cpuStateDisabled)
	}
}

func TestCPUHotDoesNotRaiseMemoryPressureLevel(t *testing.T) {
	var decisions []Decision
	c := newCPUTestController(PressureHealthy, 4, &scriptedCPU{samples: []float64{99}}, &decisions)

	for range 3 {
		c.adjust(context.Background())
	}
	for _, d := range decisions {
		if d.Snapshot.Level != PressureHealthy {
			t.Fatalf("snapshot level = %v, want healthy: CPU must not drive shedding or heap dumps", d.Snapshot.Level)
		}
	}
}

func TestCPUMemoryPressureStillScalesDownFirst(t *testing.T) {
	var decisions []Decision
	c := newCPUTestController(PressureCritical, 4, &scriptedCPU{samples: []float64{20}}, &decisions)

	c.adjust(context.Background())
	if got := lastAction(t, decisions); got != "scale_down" {
		t.Fatalf("action = %q, want scale_down", got)
	}
}
