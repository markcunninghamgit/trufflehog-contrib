package workqueue

import (
	"context"
	"runtime"
	"sync"
	"testing"
	"time"
)

func failAfter(t *testing.T, timeout time.Duration) func() {
	t.Helper()
	done := make(chan struct{})
	go func() {
		timer := time.NewTimer(timeout)
		defer timer.Stop()
		select {
		case <-timer.C:
			buf := make([]byte, 1<<20)
			n := runtime.Stack(buf, true)
			t.Errorf("test exceeded %s; goroutine dump:\n%s", timeout, string(buf[:n]))
		case <-done:
		}
	}()
	return func() { close(done) }
}

func TestQueueLifecycleIsExactlyOnce(t *testing.T) {
	stopWatchdog := failAfter(t, 10*time.Second)
	defer stopWatchdog()
	queue, err := New[int](1)
	if err != nil {
		t.Fatal(err)
	}
	handle := queue.Enqueue(Item[int]{ID: "one", Value: 1})
	if counts := queue.Counts(); counts.Queued != 1 || counts.Active != 0 {
		t.Fatalf("queued counts = %+v", counts)
	}
	if err := handle.Admit(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !handle.Start() || handle.Start() {
		t.Fatal("unexpected start transition")
	}
	if !handle.Complete() || handle.Complete() {
		t.Fatal("unexpected completion transition")
	}
	if !handle.Acknowledge() || handle.Acknowledge() {
		t.Fatal("unexpected acknowledgement transition")
	}
	if !handle.Release() || handle.Release() {
		t.Fatal("unexpected release transition")
	}
	counts := queue.Counts()
	if counts.Queued != 0 || counts.Admitted != 0 || counts.Active != 0 || counts.Completed != 1 || counts.Acknowledged != 1 {
		t.Fatalf("final counts = %+v", counts)
	}
}

func TestQueueLimitDecreaseStopsNewAdmission(t *testing.T) {
	stopWatchdog := failAfter(t, 10*time.Second)
	defer stopWatchdog()
	queue, err := New[int](2)
	if err != nil {
		t.Fatal(err)
	}
	first := queue.Enqueue(Item[int]{ID: "first"})
	second := queue.Enqueue(Item[int]{ID: "second"})
	third := queue.Enqueue(Item[int]{ID: "third"})
	for _, handle := range []*Handle[int]{first, second} {
		if err := handle.Admit(context.Background()); err != nil {
			t.Fatal(err)
		}
		if !handle.Start() {
			t.Fatal("could not start admitted item")
		}
	}
	if err := queue.SetLimit(1); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := third.Admit(ctx); err == nil {
		t.Fatal("third item admitted while active work exceeded reduced limit")
	}
	if !first.Cancel() || !first.Acknowledge() || !first.Release() {
		t.Fatal("first item did not finish cleanly")
	}
	if !second.Cancel() || !second.Acknowledge() || !second.Release() {
		t.Fatal("second item did not finish cleanly")
	}
	third = queue.Enqueue(Item[int]{ID: "third-retry"})
	if err := third.Admit(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestQueueFIFO(t *testing.T) {
	stopWatchdog := failAfter(t, 10*time.Second)
	defer stopWatchdog()
	queue, err := New[int](1)
	if err != nil {
		t.Fatal(err)
	}
	first := queue.Enqueue(Item[int]{ID: "first"})
	second := queue.Enqueue(Item[int]{ID: "second"})
	if err := first.Admit(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !first.Start() {
		t.Fatal("first item did not start")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := second.Admit(ctx); err == nil {
		t.Fatal("second item bypassed active first item")
	}
	if !first.Complete() || !first.Acknowledge() || !first.Release() {
		t.Fatal("first item did not finish")
	}
	second = queue.Enqueue(Item[int]{ID: "second-retry"})
	if err := second.Admit(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestQueueConcurrentLifecycle(t *testing.T) {
	stopWatchdog := failAfter(t, 10*time.Second)
	defer stopWatchdog()
	queue, err := New[int](4)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		handle := queue.Enqueue(Item[int]{ID: "item"})
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := handle.Admit(context.Background()); err != nil {
				t.Error(err)
				return
			}
			if !handle.Start() {
				t.Error("item did not start")
				return
			}
			if !handle.Complete() || !handle.Acknowledge() || !handle.Release() {
				t.Error("item did not finish exactly once")
			}
		}()
	}
	wg.Wait()
	counts := queue.Counts()
	if counts.Queued != 0 || counts.Admitted != 0 || counts.Active != 0 || counts.Completed != 32 || counts.Acknowledged != 32 {
		t.Fatalf("final counts = %+v", counts)
	}
}

func TestQueueAdmissionCancellationCleansQueuedItem(t *testing.T) {
	stopWatchdog := failAfter(t, 10*time.Second)
	defer stopWatchdog()
	queue, err := New[int](1)
	if err != nil {
		t.Fatal(err)
	}
	active := queue.Enqueue(Item[int]{ID: "active"})
	if err := active.Admit(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !active.Start() {
		t.Fatal("active item did not start")
	}

	pending := queue.Enqueue(Item[int]{ID: "pending"})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := pending.Admit(ctx); err == nil {
		t.Fatal("pending item admitted despite cancellation")
	}
	if !pending.Acknowledge() {
		t.Fatal("cancelled item was not acknowledged")
	}
	if !pending.Release() {
		t.Fatal("cancelled item was not released")
	}
	if !active.Cancel() || !active.Acknowledge() || !active.Release() {
		t.Fatal("active item did not finish cleanly")
	}

	counts := queue.Counts()
	if counts.Queued != 0 || counts.Admitted != 0 || counts.Active != 0 || counts.Cancelled != 2 || counts.Acknowledged != 2 {
		t.Fatalf("final counts = %+v", counts)
	}
}
