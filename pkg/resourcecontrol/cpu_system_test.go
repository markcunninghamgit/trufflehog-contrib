package resourcecontrol

import (
	"context"
	"testing"
	"time"
)

func TestSystemCPUSamplerReturnsReadingAfterWarmup(t *testing.T) {
	sampler := SystemCPUSampler{}
	sampler.SampleCPU(context.Background())
	time.Sleep(200 * time.Millisecond)

	busy, ok := sampler.SampleCPU(context.Background())
	if !ok {
		t.Fatal("SampleCPU() ok = false on the second call; the CPU signal would silently never engage")
	}
	if busy < 0 || busy > 100 {
		t.Fatalf("SampleCPU() = %v, want a value in [0, 100]", busy)
	}
}
