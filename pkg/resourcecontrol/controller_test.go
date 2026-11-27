package resourcecontrol

import (
	"context"
	"testing"
	"time"
)

type fixedProvider struct{ snapshot Snapshot }

func (p fixedProvider) Snapshot(context.Context) Snapshot { return p.snapshot }

type mutableProvider struct{ snapshot Snapshot }

func (p *mutableProvider) Snapshot(context.Context) Snapshot { return p.snapshot }

func TestControllerCapsAtMaximum(t *testing.T) {
	limits := []int{}
	controller := NewController(Config{
		Provider: fixedProvider{snapshot: Snapshot{Level: PressureHealthy}},
		Maximum:  4,
		Initial:  99,
		SetLimit: func(limit int) { limits = append(limits, limit) },
	})

	if got := controller.Current(); got != 4 {
		t.Fatalf("Current() = %d, want 4", got)
	}
	controller.apply(4, Snapshot{Level: PressureHealthy})
	if limits[len(limits)-1] != 4 {
		t.Fatalf("last limit = %d, want 4", limits[len(limits)-1])
	}
}

func TestControllerUnknownPressureDoesNotScaleUp(t *testing.T) {
	limits := []int{}
	controller := NewController(Config{
		Provider: fixedProvider{snapshot: Snapshot{Level: PressureUnknown}},
		Maximum:  4,
		Initial:  2,
		Cooldown: time.Nanosecond,
		SetLimit: func(limit int) { limits = append(limits, limit) },
	})

	controller.adjust(context.Background())
	if got := controller.Current(); got != 2 {
		t.Fatalf("Current() = %d, want 2", got)
	}
	if len(limits) != 0 {
		t.Fatalf("unknown pressure changed the limit: %#v", limits)
	}
}

func TestControllerScalesDownOnElevatedPressure(t *testing.T) {
	limits := []int{}
	controller := NewController(Config{
		Provider: fixedProvider{snapshot: Snapshot{Level: PressureElevated, Reason: "test pressure"}},
		Maximum:  4,
		Initial:  3,
		Cooldown: time.Nanosecond,
		SetLimit: func(limit int) { limits = append(limits, limit) },
	})
	controller.lastChange = time.Now().Add(-time.Second)

	controller.adjust(context.Background())
	if got := controller.Current(); got != 2 {
		t.Fatalf("Current() = %d, want 2", got)
	}
	if len(limits) != 1 || limits[0] != 2 {
		t.Fatalf("limits = %#v, want [2]", limits)
	}
}

func TestControllerScalesDownOnElevatedPressureDuringCooldown(t *testing.T) {
	var limits []int
	controller := NewController(Config{
		Provider: fixedProvider{snapshot: Snapshot{Level: PressureElevated, Reason: "test pressure"}},
		Maximum:  4,
		Initial:  4,
		Cooldown: time.Hour,
		SetLimit: func(limit int) { limits = append(limits, limit) },
	})
	controller.lastChange = time.Now()

	controller.adjust(context.Background())

	if got := controller.Current(); got != 3 {
		t.Fatalf("Current() = %d, want 3", got)
	}
	if len(limits) != 1 || limits[0] != 3 {
		t.Fatalf("limits = %#v, want [3]", limits)
	}
}

func TestControllerScalesDownRepeatedlyOnCriticalPressureDuringCooldown(t *testing.T) {
	var limits []int
	controller := NewController(Config{
		Provider: fixedProvider{snapshot: Snapshot{Level: PressureCritical, Reason: "test pressure"}},
		Maximum:  4,
		Minimum:  1,
		Initial:  4,
		Cooldown: time.Hour,
		SetLimit: func(limit int) { limits = append(limits, limit) },
	})
	controller.lastChange = time.Now()

	for want := 3; want >= 1; want-- {
		controller.adjust(context.Background())
		if got := controller.Current(); got != want {
			t.Fatalf("Current() = %d after critical sample, want %d", got, want)
		}
	}
	if len(limits) != 3 {
		t.Fatalf("got %d limit changes, want 3: %#v", len(limits), limits)
	}
}

func TestControllerDoesNotScaleUpDuringCooldownAfterPressureClears(t *testing.T) {
	provider := &mutableProvider{snapshot: Snapshot{Level: PressureCritical, Reason: "test pressure"}}
	var limits []int
	controller := NewController(Config{
		Provider: provider,
		Maximum:  4,
		Initial:  4,
		Cooldown: time.Hour,
		SetLimit: func(limit int) { limits = append(limits, limit) },
	})
	controller.lastChange = time.Now()

	controller.adjust(context.Background())
	if got := controller.Current(); got != 3 {
		t.Fatalf("Current() = %d after pressure, want 3", got)
	}

	provider.snapshot = Snapshot{Level: PressureHealthy, Reason: "pressure cleared"}
	controller.adjust(context.Background())
	if got := controller.Current(); got != 3 {
		t.Fatalf("Current() = %d during recovery cooldown, want 3", got)
	}
	if len(limits) != 1 || limits[0] != 3 {
		t.Fatalf("limits = %#v, want [3]", limits)
	}
}
