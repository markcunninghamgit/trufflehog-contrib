package taskrecovery

import (
	"context"
	"errors"
	"strings"
)

// TaskState describes the lifecycle of one repository-level attempt.
type TaskState string

const (
	Queued     TaskState = "queued"
	InProgress TaskState = "in_progress"
	Completed  TaskState = "completed"
	Incomplete TaskState = "incomplete"
	Failed     TaskState = "failed"
)

// Attempt identifies one repository task execution without exposing secret data.
type Attempt struct {
	TaskID     string
	Repository string
	Attempt    int
	State      TaskState
	Reason     string
}

// FailureClass controls whether a failed task is eligible for bounded retry.
type FailureClass string

const (
	FailureTransient     FailureClass = "transient"
	FailureResource      FailureClass = "resource"
	FailureCancelled     FailureClass = "cancelled"
	FailureDeterministic FailureClass = "deterministic"
)

// Classify returns a conservative retry classification for an error.
func Classify(err error) FailureClass {
	if err == nil {
		return FailureDeterministic
	}
	message := strings.ToLower(err.Error())
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || strings.Contains(message, "context canceled") || strings.Contains(message, "deadline exceeded") {
		return FailureCancelled
	}
	if strings.Contains(message, "signal: killed") || strings.Contains(message, "resource temporarily unavailable") || strings.Contains(message, "out of memory") {
		return FailureResource
	}
	if strings.Contains(message, "connection reset") || strings.Contains(message, "timeout") || strings.Contains(message, "rate limit") {
		return FailureTransient
	}
	return FailureDeterministic
}

// Retryable reports whether a failure should be requeued under a bounded policy.
func Retryable(class FailureClass) bool {
	return class == FailureTransient || class == FailureResource || class == FailureCancelled
}
