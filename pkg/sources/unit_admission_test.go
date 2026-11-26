package sources

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/trufflesecurity/trufflehog/v3/pkg/context"
)

// gatedUnitChunker holds every unit inside ChunkUnit until release is closed.
type gatedUnitChunker struct {
	counterChunker
	active  atomic.Int32
	peak    atomic.Int32
	release chan struct{}
}

func (c *gatedUnitChunker) ChunkUnit(ctx context.Context, _ SourceUnit, _ ChunkReporter) error {
	now := c.active.Add(1)
	for {
		peak := c.peak.Load()
		if now <= peak || c.peak.CompareAndSwap(peak, now) {
			break
		}
	}
	defer c.active.Add(-1)
	select {
	case <-c.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func waitForActive(t *testing.T, c *gatedUnitChunker, want int32) {
	t.Helper()
	require.Eventually(t, func() bool { return c.active.Load() == want }, 5*time.Second, 5*time.Millisecond,
		"active units: got %d, want %d", c.active.Load(), want)
}

func TestSetMaxConcurrentUnitsResizesRunningSource(t *testing.T) {
	chunker := &gatedUnitChunker{counterChunker: counterChunker{count: 6}, release: make(chan struct{})}
	mgr := NewManager(WithBufferedOutput(8), WithSourceUnits(), WithConcurrentUnits(1))
	source, err := buildDummy(chunker)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_, err = mgr.EnumerateAndScan(ctx, "dummy", source)
	require.NoError(t, err)

	waitForActive(t, chunker, 1)
	require.Never(t, func() bool { return chunker.active.Load() > 1 }, 100*time.Millisecond, 10*time.Millisecond)

	mgr.SetMaxConcurrentUnits(3)
	waitForActive(t, chunker, 3)
	require.Never(t, func() bool { return chunker.active.Load() > 3 }, 100*time.Millisecond, 10*time.Millisecond)

	// Lowering the limit must not interrupt running units or admit new ones.
	mgr.SetMaxConcurrentUnits(1)
	require.Never(t, func() bool { return chunker.active.Load() != 3 }, 100*time.Millisecond, 10*time.Millisecond)

	close(chunker.release)
	require.NoError(t, mgr.Wait())
	require.EqualValues(t, 3, chunker.peak.Load())
	require.Zero(t, chunker.active.Load())
}

func TestUnitAdmissionCancelledWhileWaiting(t *testing.T) {
	chunker := &gatedUnitChunker{counterChunker: counterChunker{count: 4}, release: make(chan struct{})}
	mgr := NewManager(WithBufferedOutput(8), WithSourceUnits(), WithConcurrentUnits(1))
	source, err := buildDummy(chunker)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_, err = mgr.EnumerateAndScan(ctx, "dummy", source)
	require.NoError(t, err)
	waitForActive(t, chunker, 1)

	cancel()
	done := make(chan error, 1)
	go func() { done <- mgr.Wait() }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Wait did not return after cancellation while units were waiting for admission")
	}
	require.Zero(t, chunker.active.Load())
	require.EqualValues(t, 1, chunker.peak.Load())
}
