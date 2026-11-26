package taskrecovery

import (
	"context"
	"errors"
	"testing"
)

func TestClassifyAndRetryable(t *testing.T) {
	tests := []struct {
		name  string
		err   error
		class FailureClass
		retry bool
	}{
		{name: "cancelled", err: context.Canceled, class: FailureCancelled, retry: true},
		{name: "deadline", err: context.DeadlineExceeded, class: FailureCancelled, retry: true},
		{name: "killed child", err: errors.New("git failed: signal: killed"), class: FailureResource, retry: true},
		{name: "network", err: errors.New("connection reset by peer"), class: FailureTransient, retry: true},
		{name: "foreign origin", err: errors.New("foreign origin rejected"), class: FailureDeterministic, retry: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			class := Classify(test.err)
			if class != test.class {
				t.Fatalf("Classify() = %q, want %q", class, test.class)
			}
			if Retryable(class) != test.retry {
				t.Fatalf("Retryable(%q) = %t, want %t", class, Retryable(class), test.retry)
			}
		})
	}
}
