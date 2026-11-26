package resourcecontrol

import (
	"context"

	"github.com/shirou/gopsutil/v4/cpu"
)

// CPUSampler reports system-wide CPU utilisation since its previous call, 0-100. ok is false when
// no sample is available, which includes the first call because it has nothing to compare against.
type CPUSampler interface {
	SampleCPU(context.Context) (busyPercent float64, ok bool)
}

// SystemCPUSampler reads system-wide utilisation through gopsutil.
type SystemCPUSampler struct{}

func (SystemCPUSampler) SampleCPU(ctx context.Context) (float64, bool) {
	busy, err := cpu.PercentWithContext(ctx, 0, false)
	if err != nil || len(busy) != 1 {
		return 0, false
	}
	return busy[0], true
}
