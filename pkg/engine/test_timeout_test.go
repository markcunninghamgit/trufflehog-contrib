package engine

import (
	"fmt"
	"runtime"
	"testing"
	"time"
)

// failAfter dumps all goroutine stacks before failing a test that has stopped
// making progress. The Go test runner still provides the outer process timeout.
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
			t.Errorf("test exceeded %s; goroutine dump:\n%s", timeout, fmt.Sprintf("%s", buf[:n]))
		case <-done:
		}
	}()
	return func() { close(done) }
}
