package resourcecontrol

import (
	"context"
	"fmt"

	"github.com/shirou/gopsutil/v4/mem"
)

// ProviderConfig contains the existing runtime thresholds plus optional
// platform pressure thresholds. Platform providers remain opt-in through the
// adaptive-concurrency flag.
type ProviderConfig struct {
	Runtime RuntimeProvider
}

// CombineSnapshots returns the most severe known pressure observation.
func CombineSnapshots(snapshots ...Snapshot) Snapshot {
	combined := Snapshot{Level: PressureUnknown, Reason: "resource pressure unavailable"}
	for _, snapshot := range snapshots {
		if snapshot.Level > combined.Level {
			combined = snapshot
		}
	}
	return combined
}

// NewProvider combines the Go runtime provider with the system memory
// provider. The memory provider reads through gopsutil, so it works on every
// platform trufflehog builds for (linux, darwin, windows and the BSDs); on a
// platform gopsutil does not support it reports an unknown pressure level and
// the runtime provider still drives the controller.
func NewProvider(config ProviderConfig) Provider {
	return compositeProvider{providers: []Provider{config.Runtime, systemMemoryProvider{}}}
}

type compositeProvider struct {
	providers []Provider
}

func (p compositeProvider) Snapshot(ctx context.Context) Snapshot {
	snapshots := make([]Snapshot, 0, len(p.providers))
	for _, provider := range p.providers {
		snapshots = append(snapshots, provider.Snapshot(ctx))
	}
	return CombineSnapshots(snapshots...)
}

// systemMemoryProvider reports system-wide memory pressure from the
// perspective of what is still available to the OS. Free memory below 15% is
// elevated pressure and below 5% is critical, matching the runtime provider's
// heap thresholds.
type systemMemoryProvider struct{}

func (systemMemoryProvider) Snapshot(ctx context.Context) Snapshot {
	stats, err := mem.VirtualMemoryWithContext(ctx)
	if err != nil || stats.Total == 0 {
		return Snapshot{Level: PressureUnknown, Reason: "system memory stats unavailable"}
	}

	freePercent := 100 - stats.UsedPercent
	level := PressureHealthy
	if freePercent <= 5 {
		level = PressureCritical
	} else if freePercent <= 15 {
		level = PressureElevated
	}
	return Snapshot{Level: level, Reason: fmt.Sprintf("system_free_percent=%.1f", freePercent)}
}
