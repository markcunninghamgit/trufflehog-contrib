package engine

import (
	"errors"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/trufflesecurity/trufflehog/v3/pkg/context"
	"github.com/trufflesecurity/trufflehog/v3/pkg/sources"
)

const (
	abandonReasonShed   = "shed_under_pressure"
	abandonReasonBudget = "decoded_bytes_budget_exceeded"

	// shedDecodeMinBytes is the retained decoded size at which a critical sample
	// sheds a decode even when it is within the stage limit.
	shedDecodeMinBytes = 64 << 20
)

var errDecodeShed = errors.New("decode shed under resource pressure")

// decodeRun is one in-flight iterativeDecode call.
type decodeRun struct {
	started  time.Time
	retained atomic.Int64
	cancel   func(error)
	shed     bool // guarded by stageWork.mu
}

func (r *decodeRun) addRetained(n int64) {
	if r != nil {
		r.retained.Add(n)
	}
}

func (r *decodeRun) retainedBytes() int64 {
	if r == nil {
		return 0
	}
	return r.retained.Load()
}

// decodeOutcome reports whether decoding stopped early. Only shed and budget
// stops are marked abandoned; a cancelled scan is not accounted as abandoned.
type decodeOutcome struct {
	abandoned     bool
	reason        string
	depth         int
	decoder       string
	variants      int
	retainedBytes int64
}

// stageWork tracks in-flight decodes so the controller can cancel the largest.
// The zero value is ready to use.
type stageWork struct {
	mu   sync.Mutex
	runs map[*decodeRun]struct{}
}

// begin registers a decode. The returned func must be called when it finishes.
func (w *stageWork) begin(parent context.Context) (context.Context, *decodeRun, func()) {
	ctx, cancel := context.WithCancelCause(parent)
	run := &decodeRun{started: time.Now(), cancel: cancel}
	w.mu.Lock()
	if w.runs == nil {
		w.runs = make(map[*decodeRun]struct{})
	}
	w.runs[run] = struct{}{}
	w.mu.Unlock()
	return ctx, run, func() {
		w.mu.Lock()
		delete(w.runs, run)
		w.mu.Unlock()
		cancel(nil)
	}
}

// shed cancels the largest in-flight decodes beyond limit, plus any decode
// retaining at least minBytes. It returns how many it cancelled.
func (w *stageWork) shed(limit int, minBytes int64) int {
	if limit < 1 {
		limit = 1
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	var live []*decodeRun
	for r := range w.runs {
		if !r.shed {
			live = append(live, r)
		}
	}
	sort.Slice(live, func(i, j int) bool { return live[i].retained.Load() > live[j].retained.Load() })
	excess := len(live) - limit
	shed := 0
	for i, r := range live {
		if i < excess || (minBytes > 0 && r.retained.Load() >= minBytes) {
			r.shed = true
			r.cancel(errDecodeShed)
			shed++
		}
	}
	return shed
}

// ShedStageWork cancels in-flight decodes under critical pressure. Cancellation
// is cooperative: a decode stops before its next decoder call, drops its decoded
// variants, and still has its plain pass scanned. It returns the number shed.
func (e *Engine) ShedStageWork(limit int) int {
	shed := e.stageWork.shed(limit, shedDecodeMinBytes)
	if shed > 0 {
		context.Background().Logger().V(1).Info("stage work shed", "role", "scanner-worker", "event", "stage_work_shed", "shed", shed, "limit", limit)
	}
	return shed
}

// recordAbandonedDecode counts and logs a chunk whose decoded forms were dropped.
func (e *Engine) recordAbandonedDecode(ctx context.Context, chunk *sources.Chunk, outcome decodeOutcome) {
	atomic.AddUint64(&e.metrics.AbandonedDecodes, 1)
	atomic.AddUint64(&e.metrics.AbandonedDecodeBytes, uint64(len(chunk.OriginalData)))
	meta := ""
	if chunk.SourceMetadata != nil {
		meta = chunk.SourceMetadata.String()
	}
	ctx.Logger().Info("decode abandoned",
		"role", "scanner-worker",
		"event", "decode_abandoned",
		"reason", outcome.reason,
		"decoder", outcome.decoder,
		"depth", outcome.depth,
		"variants", outcome.variants,
		"retained_bytes", outcome.retainedBytes,
		"chunk_bytes", len(chunk.OriginalData),
		"chunk_source_metadata", meta,
	)
}
