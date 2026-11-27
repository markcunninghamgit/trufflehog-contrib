package engine

import (
	"testing"

	"github.com/trufflesecurity/trufflehog/v3/pkg/detectors"
	"github.com/trufflesecurity/trufflehog/v3/pkg/workqueue"
)

func TestSetMaxConcurrentStagesUpdatesAllQueues(t *testing.T) {
	engine := &Engine{
		detectorWorkerMultiplier:            2,
		verificationOverlapWorkerMultiplier: 3,
		notificationWorkerMultiplier:        4,
	}

	var err error
	engine.detectorQueue, err = workqueue.New[detectableChunk](8)
	if err != nil {
		t.Fatal(err)
	}
	engine.verificationQueue, err = workqueue.New[verificationOverlapChunk](8)
	if err != nil {
		t.Fatal(err)
	}
	engine.notifierQueue, err = workqueue.New[detectors.ResultWithMetadata](8)
	if err != nil {
		t.Fatal(err)
	}

	engine.SetMaxConcurrentStages(5)

	if got := engine.detectorQueue.Limit(); got != 10 {
		t.Fatalf("detector queue limit = %d, want 10", got)
	}
	if got := engine.verificationQueue.Limit(); got != 15 {
		t.Fatalf("verification queue limit = %d, want 15", got)
	}
	if got := engine.notifierQueue.Limit(); got != 20 {
		t.Fatalf("notifier queue limit = %d, want 20", got)
	}

	engine.SetMaxConcurrentStages(0)
	if got := engine.detectorQueue.Limit(); got != 2 {
		t.Fatalf("minimum detector queue limit = %d, want 2", got)
	}
}
