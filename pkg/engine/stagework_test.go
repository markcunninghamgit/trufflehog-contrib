package engine

import (
	"bytes"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/trufflesecurity/trufflehog/v3/pkg/context"
	"github.com/trufflesecurity/trufflehog/v3/pkg/decoders"
	"github.com/trufflesecurity/trufflehog/v3/pkg/pb/detectorspb"
	"github.com/trufflesecurity/trufflehog/v3/pkg/sources"
)

// plainTestDecoder returns its input unchanged, like the real PLAIN pass.
type plainTestDecoder struct{}

func (plainTestDecoder) Type() detectorspb.DecoderType { return detectorspb.DecoderType_PLAIN }
func (plainTestDecoder) FromChunk(c *sources.Chunk) *decoders.DecodableChunk {
	return &decoders.DecodableChunk{Chunk: c, DecoderType: detectorspb.DecoderType_PLAIN}
}

// doublingTestDecoder returns a copy twice the size of its input, so every
// depth doubles the retained bytes. before runs ahead of each call.
type doublingTestDecoder struct {
	before func()
	calls  int
	mu     sync.Mutex
}

func (d *doublingTestDecoder) Type() detectorspb.DecoderType { return detectorspb.DecoderType_BASE64 }
func (d *doublingTestDecoder) FromChunk(c *sources.Chunk) *decoders.DecodableChunk {
	d.mu.Lock()
	d.calls++
	d.mu.Unlock()
	if d.before != nil {
		d.before()
	}
	out := *c
	out.Data = bytes.Repeat(c.Data, 2)
	return &decoders.DecodableChunk{Chunk: &out, DecoderType: detectorspb.DecoderType_BASE64}
}

func (d *doublingTestDecoder) callCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.calls
}

func plainCount(results []*decoders.DecodableChunk) int {
	n := 0
	for _, r := range results {
		if r.DecoderType == detectorspb.DecoderType_PLAIN {
			n++
		}
	}
	return n
}

// With no budget and a live context, every depth runs and nothing is abandoned.
func TestIterativeDecodeUnboundedRunsEveryDepth(t *testing.T) {
	defer failAfter(t, 10*time.Second)()
	chunk := &sources.Chunk{Data: []byte("0123456789")}
	dec := &doublingTestDecoder{}

	results, outcome := iterativeDecode(context.Background(), nil, 0, chunk, []decoders.Decoder{plainTestDecoder{}, dec}, 4)

	require.False(t, outcome.abandoned)
	require.Equal(t, 4, dec.callCount())
	require.Len(t, results, 5) // one PLAIN pass plus four doubling passes
}

// The budget must stop decoding before the oversized pass runs and keep the PLAIN pass.
func TestIterativeDecodeBudgetAbandonsAndKeepsPlainPass(t *testing.T) {
	defer failAfter(t, 10*time.Second)()
	chunk := &sources.Chunk{Data: bytes.Repeat([]byte("a"), 100)}
	dec := &doublingTestDecoder{}
	run := &decodeRun{}

	// Depth 0 retains 200 bytes; depth 1 would add 400 more on top of that.
	results, outcome := iterativeDecode(context.Background(), run, 350, chunk, []decoders.Decoder{plainTestDecoder{}, dec}, 10)

	require.True(t, outcome.abandoned)
	require.Equal(t, abandonReasonBudget, outcome.reason)
	require.Equal(t, 1, outcome.depth)
	require.Equal(t, 1, dec.callCount(), "the pass that would exceed the budget must not run")
	require.Equal(t, 1, plainCount(results))
	require.Len(t, results, 1, "decoded variants must be dropped when abandoned")
	require.EqualValues(t, 200, outcome.retainedBytes)
}

// Shedding mid-decode must stop at the next decoder call and report the shed reason.
func TestIterativeDecodeShedStopsBetweenDecoders(t *testing.T) {
	defer failAfter(t, 10*time.Second)()
	var w stageWork
	ctx, run, end := w.begin(context.Background())
	defer end()

	dec := &doublingTestDecoder{}
	// A min-bytes of 1 sheds any run that already retains data.
	dec.before = func() {
		if dec.callCount() == 2 {
			require.Equal(t, 1, w.shed(1, 1))
		}
	}

	results, outcome := iterativeDecode(ctx, run, 0, &sources.Chunk{Data: []byte("0123456789")}, []decoders.Decoder{plainTestDecoder{}, dec}, 10)

	require.True(t, outcome.abandoned)
	require.Equal(t, abandonReasonShed, outcome.reason)
	require.Equal(t, 2, dec.callCount(), "no decoder call may start after the shed")
	require.Equal(t, 1, plainCount(results))
	require.Len(t, results, 1)
}

// A cancelled scan stops decoding but is not accounted as abandoned work.
func TestIterativeDecodeScanCancelIsNotAbandoned(t *testing.T) {
	defer failAfter(t, 10*time.Second)()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	dec := &doublingTestDecoder{}

	_, outcome := iterativeDecode(ctx, nil, 0, &sources.Chunk{Data: []byte("abc")}, []decoders.Decoder{plainTestDecoder{}, dec}, 5)

	require.False(t, outcome.abandoned)
	require.Zero(t, dec.callCount())
}

// shed picks the largest runs first and never double-sheds a run.
func TestStageWorkShedCancelsLargestFirst(t *testing.T) {
	var w stageWork
	ctxSmall, small, endSmall := w.begin(context.Background())
	defer endSmall()
	ctxBig, big, endBig := w.begin(context.Background())
	defer endBig()
	small.addRetained(10)
	big.addRetained(1000)

	require.Equal(t, 1, w.shed(1, 0))
	require.Error(t, ctxBig.Err(), "the largest run must be shed")
	require.NoError(t, ctxSmall.Err())
	require.ErrorIs(t, context.Cause(ctxBig), errDecodeShed)

	require.Zero(t, w.shed(1, 0), "an already shed run is not shed again")
}

// A run is unregistered when it ends, so a later shed finds nothing.
func TestStageWorkEndedRunIsNotShed(t *testing.T) {
	var w stageWork
	_, run, end := w.begin(context.Background())
	run.addRetained(1 << 30)
	end()

	require.Zero(t, w.shed(1, 1))
}

// Abandoned decodes must show up in the engine metrics.
func TestRecordAbandonedDecodeUpdatesMetrics(t *testing.T) {
	e := &Engine{}
	chunk := &sources.Chunk{OriginalData: []byte("12345")}

	e.recordAbandonedDecode(context.Background(), chunk, decodeOutcome{abandoned: true, reason: abandonReasonBudget})

	require.EqualValues(t, 1, e.metrics.AbandonedDecodes)
	require.EqualValues(t, 5, e.metrics.AbandonedDecodeBytes)
}
