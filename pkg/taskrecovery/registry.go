package taskrecovery

import (
	"sync"
)

// Registry tracks repository-level attempts and incomplete work for one scan.
type Registry struct {
	mu       sync.Mutex
	maxRetry int
	tasks    map[string]Attempt
}

func NewRegistry(maxRetry int) *Registry {
	if maxRetry < 0 {
		maxRetry = 0
	}
	return &Registry{maxRetry: maxRetry, tasks: make(map[string]Attempt)}
}

func (r *Registry) Start(taskID, repository string) (Attempt, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	previous, ok := r.tasks[taskID]
	attempt := 1
	if ok {
		attempt = previous.Attempt + 1
		if attempt > r.maxRetry+1 {
			return Attempt{TaskID: taskID, Repository: repository, Attempt: previous.Attempt, State: Failed, Reason: "retry limit exhausted"}, false
		}
	}
	current := Attempt{TaskID: taskID, Repository: repository, Attempt: attempt, State: InProgress}
	r.tasks[taskID] = current
	return current, true
}

func (r *Registry) Complete(taskID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	current := r.tasks[taskID]
	current.State = Completed
	r.tasks[taskID] = current
}

func (r *Registry) Incomplete(taskID, reason string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	current := r.tasks[taskID]
	current.State = Incomplete
	current.Reason = reason
	r.tasks[taskID] = current
	return current.Attempt <= r.maxRetry
}

// Requeue returns a task to the queued state without consuming a retry attempt,
// so work shed under resource pressure can be replayed within the retry budget.
func (r *Registry) Requeue(taskID, reason string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	current := r.tasks[taskID]
	if current.Attempt > 0 {
		current.Attempt--
	}
	current.State = Queued
	current.Reason = reason
	r.tasks[taskID] = current
}

func (r *Registry) Snapshot() []Attempt {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Attempt, 0, len(r.tasks))
	for _, task := range r.tasks {
		out = append(out, task)
	}
	return out
}
