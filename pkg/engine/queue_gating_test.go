package engine

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/trufflesecurity/trufflehog/v3/pkg/context"
	"github.com/trufflesecurity/trufflehog/v3/pkg/sources"
	"github.com/trufflesecurity/trufflehog/v3/pkg/workqueue"
)

func TestSetMaxConcurrentStagesUpdatesScannerQueue(t *testing.T) {
	q, err := workqueue.New[*sources.Chunk](8)
	require.NoError(t, err)
	e := &Engine{scannerQueue: q}

	e.SetMaxConcurrentStages(3)
	require.Equal(t, 3, q.Limit())
	e.SetMaxConcurrentStages(0)
	require.Equal(t, 1, q.Limit())
}

func TestRunQueuedNilQueueRunsDirectly(t *testing.T) {
	var ran, skipped atomic.Int32
	runQueued[int](context.Background(), nil, 1, func() { skipped.Add(1) }, func() { ran.Add(1) })
	require.EqualValues(t, 1, ran.Load())
	require.Zero(t, skipped.Load())
}

// A cancelled admission must run onNotAdmitted exactly once, never process, and leave no queue counts behind.
func TestRunQueuedCancelledAdmissionAcknowledgesOnce(t *testing.T) {
	q, err := workqueue.New[int](1)
	require.NoError(t, err)

	// Hold the only slot so the next item waits for admission.
	holder := q.Enqueue(workqueue.Item[int]{ID: "holder", Value: 0})
	require.NoError(t, holder.Admit(context.Background()))
	require.True(t, holder.Start())

	ctx, cancel := context.WithCancel(context.Background())
	var ran, notAdmitted atomic.Int32
	done := make(chan struct{})
	go func() {
		defer close(done)
		runQueued(ctx, q, 1, func() { notAdmitted.Add(1) }, func() { ran.Add(1) })
	}()

	require.Eventually(t, func() bool { return q.Counts().Queued == 1 }, 5*time.Second, 5*time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("runQueued did not return after cancellation")
	}

	require.EqualValues(t, 1, notAdmitted.Load())
	require.Zero(t, ran.Load())
	require.Zero(t, q.Counts().Queued)
	require.Equal(t, 1, q.Counts().Active)

	holder.Complete()
	holder.Acknowledge()
	holder.Release()
	require.Zero(t, q.Counts().Active)
}

// Lowering the limit must hold new work while in-flight work finishes, then admit it once capacity returns.
func TestRunQueuedLoweredLimitHoldsNewWork(t *testing.T) {
	q, err := workqueue.New[int](2)
	require.NoError(t, err)

	release := make(chan struct{})
	var active, peak atomic.Int32
	process := func() {
		n := active.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		<-release
		active.Add(-1)
	}

	finished := make(chan struct{}, 4)
	for i := 0; i < 4; i++ {
		go func(v int) {
			runQueued(context.Background(), q, v, func() {}, process)
			finished <- struct{}{}
		}(i)
	}
	require.Eventually(t, func() bool { return active.Load() == 2 }, 5*time.Second, 5*time.Millisecond)

	require.NoError(t, q.SetLimit(1))
	close(release)
	for i := 0; i < 4; i++ {
		select {
		case <-finished:
		case <-time.After(5 * time.Second):
			t.Fatal("work did not drain after limit change")
		}
	}
	require.EqualValues(t, 2, peak.Load())
	require.Zero(t, q.Counts().Active)
	require.Zero(t, q.Counts().Queued)
}
