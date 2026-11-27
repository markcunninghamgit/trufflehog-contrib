package sources

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/trufflesecurity/trufflehog/v3/pkg/context"
	"github.com/trufflesecurity/trufflehog/v3/pkg/taskrecovery"
)

// shedChunker blocks each unit's first run until release is closed or its context is cancelled.
// Later runs of the same unit complete immediately so a replay is distinguishable from a first run.
type shedChunker struct {
	counterChunker
	mu        sync.Mutex
	runs      map[byte]int
	completed map[byte]int
	release   chan struct{}
	// swallowCancel makes a cancelled first run return nil, like the git parser ending a scan early.
	swallowCancel bool
}

func newShedChunker(count int) *shedChunker {
	return &shedChunker{
		counterChunker: counterChunker{count: count},
		runs:           map[byte]int{},
		completed:      map[byte]int{},
		release:        make(chan struct{}),
	}
}

func (c *shedChunker) ChunkUnit(ctx context.Context, unit SourceUnit, _ ChunkReporter) error {
	id := byte(unit.(countChunk))
	c.mu.Lock()
	c.runs[id]++
	first := c.runs[id] == 1
	c.mu.Unlock()
	if first {
		select {
		case <-c.release:
		case <-ctx.Done():
			if c.swallowCancel {
				return nil
			}
			return ctx.Err()
		}
	}
	c.mu.Lock()
	c.completed[id]++
	c.mu.Unlock()
	return nil
}

func (c *shedChunker) snapshot() (runs, completed map[byte]int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	runs, completed = map[byte]int{}, map[byte]int{}
	for k, v := range c.runs {
		runs[k] = v
	}
	for k, v := range c.completed {
		completed[k] = v
	}
	return runs, completed
}

func startShedRun(t *testing.T, chunker *shedChunker, opts ...func(*SourceManager)) (*SourceManager, func()) {
	t.Helper()
	opts = append([]func(*SourceManager){WithBufferedOutput(8), WithSourceUnits(), WithConcurrentUnits(chunker.count)}, opts...)
	mgr := NewManager(opts...)
	go func() {
		for range mgr.Chunks() {
		}
	}()
	source, err := buildDummy(chunker)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	_, err = mgr.EnumerateAndScan(ctx, "dummy", source)
	require.NoError(t, err)
	return mgr, cancel
}

func TestShedExcessUnitsRequeuesAndReplays(t *testing.T) {
	chunker := newShedChunker(3)
	registry := taskrecovery.NewRegistry(1)
	mgr, cancel := startShedRun(t, chunker, WithTaskRegistry(registry))
	defer cancel()

	require.Eventually(t, func() bool { return runsTotal(chunker) == 3 }, 5*time.Second, 5*time.Millisecond)

	require.Equal(t, 2, mgr.ShedExcessUnits(1))
	require.Eventually(t, func() bool { return mgr.Stats().Shed == 2 }, 5*time.Second, 5*time.Millisecond)
	// Shedding again with nothing excess must not cancel the survivor.
	require.Zero(t, mgr.ShedExcessUnits(1))

	close(chunker.release)
	require.NoError(t, mgr.Wait(), "shed units are requeued, not failed")

	runs, completed := chunker.snapshot()
	total := 0
	for id := byte(0); id < 3; id++ {
		require.Equal(t, 1, completed[id], "unit %d must complete exactly once", id)
		total += runs[id]
	}
	require.Equal(t, 5, total, "three first runs plus two replays")
	stats := mgr.Stats()
	require.EqualValues(t, 2, stats.Shed)
	require.EqualValues(t, 3, stats.UnitDone)
	require.Zero(t, stats.Failed)
	for _, attempt := range registry.Snapshot() {
		require.Equal(t, taskrecovery.Completed, attempt.State)
		require.Equal(t, 1, attempt.Attempt, "a shed must not consume a retry attempt")
	}
}

// The git parser can end a cancelled scan without an error; a shed unit must still be replayed.
func TestShedExcessUnitsReplaysWhenCancellationReturnsNil(t *testing.T) {
	chunker := newShedChunker(2)
	chunker.swallowCancel = true
	mgr, cancel := startShedRun(t, chunker)
	defer cancel()
	require.Eventually(t, func() bool { return runsTotal(chunker) == 2 }, 5*time.Second, 5*time.Millisecond)

	require.Equal(t, 1, mgr.ShedExcessUnits(1))
	require.Eventually(t, func() bool { return mgr.Stats().Shed == 1 }, 5*time.Second, 5*time.Millisecond)

	close(chunker.release)
	require.NoError(t, mgr.Wait())

	_, completed := chunker.snapshot()
	require.Equal(t, 1, completed[0], "unit 0 must complete exactly once")
	require.Equal(t, 1, completed[1], "unit 1 must complete exactly once")
	require.EqualValues(t, 2, mgr.Stats().UnitDone)
}

func TestShedExcessUnitsStopsAfterRequeueBudget(t *testing.T) {
	chunker := newShedChunker(1)
	mgr, cancel := startShedRun(t, chunker)
	defer cancel()
	require.Eventually(t, func() bool { return runsTotal(chunker) == 1 }, 5*time.Second, 5*time.Millisecond)

	// The shed budget is per unit; once spent the unit is protected and must keep running.
	mgr.shedCounts = map[string]int{}
	mgr.runningMu.Lock()
	for u := range mgr.running {
		mgr.shedCounts[u.key] = maxUnitShedRequeues
	}
	mgr.runningMu.Unlock()
	require.Zero(t, mgr.ShedExcessUnits(0))

	close(chunker.release)
	require.NoError(t, mgr.Wait())
	require.EqualValues(t, 0, mgr.Stats().Shed)
}

func runsTotal(c *shedChunker) int {
	runs, _ := c.snapshot()
	total := 0
	for _, n := range runs {
		total += n
	}
	return total
}

func TestRegistryRequeueDoesNotConsumeAttempt(t *testing.T) {
	r := taskrecovery.NewRegistry(1)
	first, ok := r.Start("a", "a")
	require.True(t, ok)
	require.Equal(t, 1, first.Attempt)
	r.Requeue("a", "shed")
	again, ok := r.Start("a", "a")
	require.True(t, ok)
	require.Equal(t, 1, again.Attempt)
	require.True(t, r.Incomplete("a", "boom"), "one retry remains after a shed")
}
