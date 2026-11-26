package resourcecontrol

import "context"

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

// NewProvider selects the platform provider while preserving a runtime-only
// fallback on platforms without an implementation.
func NewProvider(config ProviderConfig) Provider {
	return newPlatformProvider(config)
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
