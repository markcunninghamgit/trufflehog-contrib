package taskrecovery

import "testing"

func TestRegistryBoundsAttempts(t *testing.T) {
	registry := NewRegistry(1)
	first, ok := registry.Start("repo-1", "github.com/org/repo")
	if !ok || first.Attempt != 1 || first.State != InProgress {
		t.Fatalf("first attempt = %#v, ok=%t", first, ok)
	}
	if !registry.Incomplete(first.TaskID, "signal: killed") {
		t.Fatal("first incomplete attempt should be retryable")
	}
	second, ok := registry.Start(first.TaskID, first.Repository)
	if !ok || second.Attempt != 2 {
		t.Fatalf("second attempt = %#v, ok=%t", second, ok)
	}
	if registry.Incomplete(second.TaskID, "signal: killed") {
		t.Fatal("second incomplete attempt should exhaust retries")
	}
	if attempt, ok := registry.Start(second.TaskID, second.Repository); ok || attempt.State != Failed {
		t.Fatalf("exhausted attempt = %#v, ok=%t", attempt, ok)
	}
}

func TestRegistryComplete(t *testing.T) {
	registry := NewRegistry(1)
	attempt, ok := registry.Start("repo-1", "github.com/org/repo")
	if !ok {
		t.Fatal("start failed")
	}
	registry.Complete(attempt.TaskID)
	got := registry.Snapshot()
	if len(got) != 1 || got[0].State != Completed {
		t.Fatalf("snapshot = %#v", got)
	}
}
