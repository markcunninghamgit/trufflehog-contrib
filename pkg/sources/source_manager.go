package sources

import (
	"errors"
	"fmt"
	"io"
	"math"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/marusama/semaphore/v2"

	"github.com/trufflesecurity/trufflehog/v3/pkg/common"
	"github.com/trufflesecurity/trufflehog/v3/pkg/context"
	"github.com/trufflesecurity/trufflehog/v3/pkg/pb/sourcespb"
	"github.com/trufflesecurity/trufflehog/v3/pkg/taskrecovery"
)

// SourceManager provides an interface for starting and managing running
// sources.
type SourceManager struct {
	api   apiClient
	hooks []JobProgressHook
	// Pool limiting the amount of concurrent sources running.
	sem         semaphore.Semaphore
	prioritySem semaphore.Semaphore
	wg          sync.WaitGroup
	// Max number of units to scan concurrently per source. Zero or negative means unlimited.
	concurrentUnits int
	// Unit semaphores of runs in progress, so SetMaxConcurrentUnits can resize them.
	unitSemsMu sync.Mutex
	unitSems   map[semaphore.Semaphore]struct{}
	// Running units that ShedExcessUnits may cancel, and how often each has been shed.
	runningMu  sync.Mutex
	running    map[*runningUnit]struct{}
	shedCounts map[string]int
	// Run the sources using source unit enumeration / chunking if available.
	// Checked at runtime to allow feature flagging.
	useSourceUnitsFunc func() bool
	// Downstream chunks channel to be scanned.
	outputChunks chan *Chunk
	// Set when Wait() returns.
	firstErr     chan error
	waitErr      error
	done         bool
	stats        sourceManagerStats
	statsHook    func(SourceManagerStats)
	unitRetries  int
	taskRegistry *taskrecovery.Registry
}

// WithUnitRetries enables bounded retries for units that fail before emitting
// any downstream chunks. A value below zero is treated as zero.
func WithUnitRetries(retries int) func(*SourceManager) {
	return func(mgr *SourceManager) {
		if retries > 0 {
			mgr.unitRetries = retries
		}
	}
}

// WithTaskRegistry adds repository/unit attempt tracking without changing retry behavior.
func WithTaskRegistry(registry *taskrecovery.Registry) func(*SourceManager) {
	return func(mgr *SourceManager) { mgr.taskRegistry = registry }
}

// WithStatsHook installs an optional callback for source-manager diagnostics.
func WithStatsHook(hook func(SourceManagerStats)) func(*SourceManager) {
	return func(mgr *SourceManager) { mgr.statsHook = hook }
}

type sourceManagerStats struct {
	started    atomic.Uint64
	completed  atomic.Uint64
	errors     atomic.Uint64
	retries    atomic.Uint64
	incomplete atomic.Uint64
	discovered atomic.Uint64
	assigned   atomic.Uint64
	unitDone   atomic.Uint64
	cancelled  atomic.Uint64
	skipped    atomic.Uint64
	failed     atomic.Uint64
	shed       atomic.Uint64
}

// SourceManagerStats is a snapshot of source admission and completion counts.
type SourceManagerStats struct {
	Started    uint64
	Completed  uint64
	Errors     uint64
	Retries    uint64
	Incomplete uint64
	Active     int
	Limit      int
	Discovered uint64
	Assigned   uint64
	UnitDone   uint64
	Cancelled  uint64
	Skipped    uint64
	Failed     uint64
	Shed       uint64
}

// apiClient is an interface for optionally communicating with an external API.
type apiClient interface {
	// GetIDs informs the API of the source that's about to run and returns
	// two identifiers used during source initialization.
	GetIDs(ctx context.Context, name string, kind sourcespb.SourceType) (SourceID, JobID, error)
}

// WithAPI adds an API client to the manager for tracking jobs and progress. If
// the API is also a JobProgressHook, it will be added to the list of event hooks.
func WithAPI(api apiClient) func(*SourceManager) {
	return func(mgr *SourceManager) { mgr.api = api }
}

// WithReportHook adds a hook to the SourceManager's reporting feature for
// customizing data aggregation.
func WithReportHook(hook JobProgressHook) func(*SourceManager) {
	return func(mgr *SourceManager) {
		mgr.hooks = append(mgr.hooks, hook)
	}
}

// WithConcurrentSources limits the concurrent number of sources a manager can run.
func WithConcurrentSources(concurrency int) func(*SourceManager) {
	return func(mgr *SourceManager) {
		mgr.sem.SetLimit(concurrency)
	}
}

// WithConcurrentTargets limits the concurrent number of targets a manager can run.
func WithConcurrentTargets(concurrency int) func(*SourceManager) {
	return func(mgr *SourceManager) {
		mgr.prioritySem.SetLimit(concurrency)
	}
}

// WithBufferedOutput sets the size of the buffer used for the Chunks() channel.
func WithBufferedOutput(size int) func(*SourceManager) {
	return func(mgr *SourceManager) { mgr.outputChunks = make(chan *Chunk, size) }
}

// WithSourceUnits enables using source unit enumeration and chunking if the
// source supports it.
func WithSourceUnits() func(*SourceManager) {
	return func(mgr *SourceManager) {
		mgr.useSourceUnitsFunc = func() bool { return true }
	}
}

// WithSourceUnitsFunc dynamically configures whether to use source unit
// enumeration and chunking if the source supports it. If the function returns
// true and the source supports it, then units will be used. Otherwise, the
// legacy scanning method will be used.
func WithSourceUnitsFunc(f func() bool) func(*SourceManager) {
	return func(mgr *SourceManager) { mgr.useSourceUnitsFunc = f }
}

// WithConcurrentUnits limits the number of units to be scanned concurrently.
// The default is unlimited.
func WithConcurrentUnits(n int) func(*SourceManager) {
	return func(mgr *SourceManager) { mgr.concurrentUnits = n }
}

// The default channel size for all the channels that are used to transport chunks.
const defaultChannelSize = 64

func effectiveUnitLimit(n int) int {
	if n <= 0 {
		return math.MaxInt32
	}
	return n
}

// newUnitSem returns a unit semaphore that follows SetMaxConcurrentUnits until release is called.
func (s *SourceManager) newUnitSem() (sem semaphore.Semaphore, release func()) {
	s.unitSemsMu.Lock()
	defer s.unitSemsMu.Unlock()
	sem = semaphore.New(effectiveUnitLimit(s.concurrentUnits))
	if s.unitSems == nil {
		s.unitSems = make(map[semaphore.Semaphore]struct{})
	}
	s.unitSems[sem] = struct{}{}
	return sem, func() {
		s.unitSemsMu.Lock()
		defer s.unitSemsMu.Unlock()
		delete(s.unitSems, sem)
	}
}

// SetMaxConcurrentUnits resizes unit admission for running and future sources.
// Units already running continue; only new admissions are held back.
func (s *SourceManager) SetMaxConcurrentUnits(n int) {
	s.unitSemsMu.Lock()
	defer s.unitSemsMu.Unlock()
	previous := s.concurrentUnits
	s.concurrentUnits = n
	for sem := range s.unitSems {
		sem.SetLimit(effectiveUnitLimit(n))
	}
	context.Background().Logger().V(3).Info("unit admission limit changed", "role", "source-manager", "event", "unit_admission_limit_changed", "previous_limit", previous, "limit", n)
}

var errUnitShed = errors.New("unit shed under resource pressure")

// maxUnitShedRequeues bounds how often one unit is shed; after that it is protected and runs to completion.
const maxUnitShedRequeues = 2

type runningUnit struct {
	key     string
	started time.Time
	emitted func() uint64
	cancel  context.CancelCauseFunc
	shed    bool
}

// unitRun holds units shed during one source run until the run relaunches them.
type unitRun struct {
	mu      sync.Mutex
	pending []SourceUnit
}

func (r *unitRun) add(unit SourceUnit) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pending = append(r.pending, unit)
}

func (r *unitRun) take() []SourceUnit {
	r.mu.Lock()
	defer r.mu.Unlock()
	pending := r.pending
	r.pending = nil
	return pending
}

func (s *SourceManager) registerRunning(u *runningUnit) {
	s.runningMu.Lock()
	defer s.runningMu.Unlock()
	if s.running == nil {
		s.running = make(map[*runningUnit]struct{})
	}
	s.running[u] = struct{}{}
}

// finishRunning removes u and reports whether it was shed while running. It is idempotent.
func (s *SourceManager) finishRunning(u *runningUnit) bool {
	s.runningMu.Lock()
	defer s.runningMu.Unlock()
	delete(s.running, u)
	return u.shed
}

// ShedExcessUnits cancels running units beyond limit so they can be requeued.
// Units with the fewest emitted chunks go first to limit replayed work, and a
// unit already shed maxUnitShedRequeues times is never shed again. It returns
// the number of units cancelled.
func (s *SourceManager) ShedExcessUnits(limit int) int {
	if limit < 1 {
		limit = 1
	}
	s.runningMu.Lock()
	defer s.runningMu.Unlock()
	active := 0
	var candidates []*runningUnit
	for u := range s.running {
		if u.shed {
			continue
		}
		active++
		if s.shedCounts[u.key] < maxUnitShedRequeues {
			candidates = append(candidates, u)
		}
	}
	excess := active - limit
	if excess <= 0 || len(candidates) == 0 {
		return 0
	}
	sort.Slice(candidates, func(i, j int) bool {
		ei, ej := candidates[i].emitted(), candidates[j].emitted()
		if ei != ej {
			return ei < ej
		}
		return candidates[i].started.After(candidates[j].started)
	})
	if excess > len(candidates) {
		excess = len(candidates)
	}
	if s.shedCounts == nil {
		s.shedCounts = make(map[string]int)
	}
	for _, u := range candidates[:excess] {
		u.shed = true
		s.shedCounts[u.key]++
		u.cancel(errUnitShed)
	}
	context.Background().Logger().V(1).Info("units shed", "role", "source-manager", "event", "units_shed", "shed", excess, "active", active, "limit", limit)
	return excess
}

// NewManager creates a new manager with the provided options.
func NewManager(opts ...func(*SourceManager)) *SourceManager {
	mgr := SourceManager{
		// Default to the headless API. Can be overwritten by the WithAPI option.
		api:          &headlessAPI{},
		sem:          semaphore.New(runtime.NumCPU()),
		prioritySem:  semaphore.New(runtime.NumCPU()),
		outputChunks: make(chan *Chunk, defaultChannelSize),
		firstErr:     make(chan error, 1),
	}
	for _, opt := range opts {
		opt(&mgr)
	}
	return &mgr
}

func (s *SourceManager) GetIDs(ctx context.Context, sourceName string, kind sourcespb.SourceType) (SourceID, JobID, error) {
	return s.api.GetIDs(ctx, sourceName, kind)
}

// EnumerateAndScan blocks until a resource is available to run the source, then
// asynchronously runs it. Error information is stored and accessible via the
// JobProgressRef as it becomes available.
func (s *SourceManager) EnumerateAndScan(ctx context.Context, sourceName string, source Source, targets ...ChunkingTarget) (JobProgressRef, error) {
	sourceID, jobID := source.SourceID(), source.JobID()
	// Do preflight checks before waiting on the pool.
	if err := s.preflightChecks(ctx); err != nil {
		return JobProgressRef{
			SourceName: sourceName,
			SourceID:   sourceID,
			JobID:      jobID,
		}, err
	}
	// Create a JobProgress object for tracking progress.
	sem := s.sem
	if len(targets) > 0 {
		sem = s.prioritySem
	}
	ctx, cancel := context.WithCancelCause(ctx)
	progress := NewJobProgress(jobID, sourceID, sourceName, WithHooks(s.hooks...), WithCancel(cancel))
	acquireStarted := time.Now()
	if err := sem.Acquire(ctx, 1); err != nil {
		// Context cancelled.
		ctx.Logger().V(3).Info("source admission rejected", "role", "source-manager", "event", "source_admission_rejected", "wait_duration", time.Since(acquireStarted).String(), "active", sem.GetCount(), "limit", sem.GetLimit())
		interruptErr := Interrupt{err}
		progress.ReportError(interruptErr)
		progress.finishWithCause(interruptErr)
		return progress.Ref(), interruptErr
	}
	ctx.Logger().V(3).Info("source admission granted", "role", "source-manager", "event", "source_admission_granted", "wait_duration", time.Since(acquireStarted).String(), "active", sem.GetCount(), "limit", sem.GetLimit())
	s.wg.Add(1)
	s.stats.started.Add(1)
	s.reportStats()
	go func() {
		// Call Finish after the semaphore has been released, and release
		// the WaitGroup only after Finish's hooks have run: Wait() closes
		// the hooks, so it must not return while a hook can still fire.
		defer s.wg.Done()
		defer s.stats.completed.Add(1)
		defer s.reportStats()
		defer progress.Finish()
		defer func() {
			sem.Release(1)
			ctx.Logger().V(3).Info("source admission released", "role", "source-manager", "event", "source_admission_released", "active", sem.GetCount(), "limit", sem.GetLimit())
		}()
		ctx := context.WithValues(ctx,
			"source_manager_worker_id", common.RandomID(5),
		)
		defer common.RecoverWithHandler(ctx, func(err error) {
			progress.ReportError(Fatal{err})
			select {
			case s.firstErr <- err:
			default:
			}
		})
		defer cancel(nil)
		if err := s.run(ctx, source, progress, targets...); err != nil {
			s.stats.errors.Add(1)
			select {
			case s.firstErr <- err:
			default:
			}
		}
	}()
	return progress.Ref(), nil
}

func (s *SourceManager) Enumerate(ctx context.Context, sourceName string, source Source, reporter UnitReporter) (JobProgressRef, error) {
	sourceID, jobID := source.SourceID(), source.JobID()
	// Do preflight checks before waiting on the pool.
	if err := s.preflightChecks(ctx); err != nil {
		return JobProgressRef{
			SourceName: sourceName,
			SourceID:   sourceID,
			JobID:      jobID,
		}, err
	}

	// Create a JobProgress object for tracking progress.
	ctx, cancel := context.WithCancelCause(ctx)
	progress := NewJobProgress(jobID, sourceID, sourceName, WithHooks(s.hooks...), WithCancel(cancel))

	// Wrap the passed in reporter so we update the progress information.
	reporter = baseUnitReporter{
		child:    reporter,
		progress: progress,
	}
	reporter = enumerationReporter{
		child:      reporter,
		ctx:        ctx,
		sourceName: sourceName,
		sourceID:   sourceID,
		jobID:      jobID,
		sourceType: fmt.Sprint(ctx.Value("source_type")),
		queueDepth: func() int { return 0 },
		discovered: new(atomic.Uint64),
		stats:      &s.stats,
	}

	s.wg.Add(1)
	s.stats.started.Add(1)
	s.reportStats()
	go func() {
		// Release the WaitGroup only after Finish's hooks have run: Wait()
		// closes the hooks, so it must not return while a hook can still
		// fire.
		defer s.wg.Done()
		defer s.stats.completed.Add(1)
		defer s.reportStats()
		defer progress.Finish()
		ctx := context.WithValues(ctx,
			"source_manager_worker_id", common.RandomID(5),
		)
		defer common.Recover(ctx)
		defer cancel(nil)
		if err := s.enumerate(ctx, source, progress, reporter); err != nil {
			s.stats.errors.Add(1)
			select {
			case s.firstErr <- err:
			default:
			}
			progress.ReportError(jobErr(ctx, err))
		}
	}()
	return progress.Ref(), nil
}

// Scan blocks until a resource is available to run the source against a single
// SourceUnit, then asynchronously runs it. Error information is stored and
// accessible via the JobProgressRef as it becomes available.
func (s *SourceManager) Scan(ctx context.Context, sourceName string, source Source, unit SourceUnit) (JobProgressRef, error) {
	sourceID, jobID := source.SourceID(), source.JobID()
	// Do preflight checks before waiting on the pool.
	if err := s.preflightChecks(ctx); err != nil {
		return JobProgressRef{
			SourceName: sourceName,
			SourceID:   sourceID,
			JobID:      jobID,
		}, err
	}
	// Create a JobProgress object for tracking progress.
	ctx, cancel := context.WithCancelCause(ctx)
	progress := NewJobProgress(jobID, sourceID, sourceName, WithHooks(s.hooks...), WithCancel(cancel))
	if err := s.sem.Acquire(ctx, 1); err != nil {
		// Context cancelled.
		interruptErr := Interrupt{err}
		progress.ReportError(interruptErr)
		progress.finishWithCause(interruptErr)
		return progress.Ref(), interruptErr
	}
	s.wg.Add(1)
	s.stats.started.Add(1)
	s.reportStats()
	go func() {
		// Call Finish after the semaphore has been released, and release
		// the WaitGroup only after Finish's hooks have run: Wait() closes
		// the hooks, so it must not return while a hook can still fire.
		defer s.wg.Done()
		defer s.stats.completed.Add(1)
		defer s.reportStats()
		defer progress.Finish()
		defer s.sem.Release(1)
		ctx := context.WithValues(ctx,
			"source_manager_worker_id", common.RandomID(5),
		)
		defer common.Recover(ctx)
		defer cancel(nil)
		if err := s.scan(ctx, source, progress, unit); err != nil {
			s.stats.errors.Add(1)
			select {
			case s.firstErr <- err:
			default:
			}
			progress.ReportError(jobErr(ctx, err))
		}
	}()
	return progress.Ref(), nil
}

// Chunks returns the read only channel of all the chunks produced by all of
// the sources managed by this manager.
func (s *SourceManager) Chunks() <-chan *Chunk {
	return s.outputChunks
}

// Wait blocks until all running sources are completed and closes the channel
// returned by Chunks(). The manager should not be reused after calling this
// method. This current implementation is not thread safe and should only be
// called by one thread.
func (s *SourceManager) Wait() error {
	// Check if the manager has been Waited.
	if s.done {
		return s.waitErr
	}
	s.done = true

	// Return the first error returned by run.
	s.wg.Wait()
	select {
	case s.waitErr = <-s.firstErr:
	default:
	}
	close(s.outputChunks)
	close(s.firstErr)
	for _, hook := range s.hooks {
		if hookCloser, ok := hook.(io.Closer); ok {
			_ = hookCloser.Close()
		}
	}
	return s.waitErr
}

// ScanChunk injects a chunk into the output stream of chunks to be scanned.
// This method should rarely be used. TODO(THOG-1577): Remove when dependencies
// no longer rely on this functionality.
func (s *SourceManager) ScanChunk(chunk *Chunk) {
	s.outputChunks <- chunk
}

// AvailableCapacity returns the number of concurrent jobs the manager can
// accommodate at this time.
func (s *SourceManager) AvailableCapacity() int {
	return s.sem.GetLimit() - s.sem.GetCount()
}

// Stats returns source lifecycle counters and the current admission state.
func (s *SourceManager) Stats() SourceManagerStats {
	return s.statsSnapshot()
}

func (s *SourceManager) statsSnapshot() SourceManagerStats {
	return SourceManagerStats{
		Started:    s.stats.started.Load(),
		Completed:  s.stats.completed.Load(),
		Errors:     s.stats.errors.Load(),
		Retries:    s.stats.retries.Load(),
		Incomplete: s.stats.incomplete.Load(),
		Active:     s.ConcurrentSources(),
		Limit:      s.MaxConcurrentSources(),
		Discovered: s.stats.discovered.Load(),
		Assigned:   s.stats.assigned.Load(),
		UnitDone:   s.stats.unitDone.Load(),
		Cancelled:  s.stats.cancelled.Load(),
		Skipped:    s.stats.skipped.Load(),
		Failed:     s.stats.failed.Load(),
		Shed:       s.stats.shed.Load(),
	}
}

func (s *SourceManager) reportStats() {
	if s.statsHook != nil {
		s.statsHook(s.statsSnapshot())
	}
}

// MaxConcurrentSources returns the maximum configured limit of concurrent
// sources the manager will run.
func (s *SourceManager) MaxConcurrentSources() int {
	return s.sem.GetLimit()
}

// ConcurrentSources returns the current number of concurrently running
// sources.
func (s *SourceManager) ConcurrentSources() int {
	return s.sem.GetCount()
}

// SetMaxConcurrentSources sets the maximum number of concurrently running
// sources. If the count is lower than the already existing number of
// concurrently running sources, no sources will be scheduled to run until the
// existing sources complete.
func (s *SourceManager) SetMaxConcurrentSources(maxRunCount int) {
	previous := s.sem.GetLimit()
	s.sem.SetLimit(maxRunCount)
	context.Background().Logger().V(3).Info("source admission limit changed", "role", "source-manager", "event", "source_admission_limit_changed", "previous_limit", previous, "limit", s.sem.GetLimit(), "active", s.sem.GetCount())
}

// preflightChecks is a helper method to check the Manager or the context isn't
// done.
func (s *SourceManager) preflightChecks(ctx context.Context) error {
	// Check if the manager has been Waited.
	if s.done {
		return fmt.Errorf("manager is done")
	}
	return ctx.Err()
}

// jobErr classifies an error that ended a job, or part of one. It is an
// Interrupt if the job's context is done and err came from that, for example a
// source returning ctx.Err() after CancelRun. Otherwise it is Fatal. An error
// that is already an Interrupt or Fatal is returned unchanged.
func jobErr(ctx context.Context, err error) error {
	if _, ok := errors.AsType[Interrupt](err); ok {
		return err
	}
	if _, ok := errors.AsType[Fatal](err); ok {
		return err
	}
	if ctxErr := ctx.Err(); ctxErr != nil && (errors.Is(err, ctxErr) || errors.Is(err, context.Cause(ctx))) {
		return Interrupt{err}
	}
	return Fatal{err}
}

// run is a helper method to synchronously run the source. It does not check for
// acquired resources. An error is returned if there was a fatal error or an
// interruption during the run. This information is also recorded in the
// JobProgress.
func (s *SourceManager) run(ctx context.Context, source Source, report *JobProgress, targets ...ChunkingTarget) error {
	report.Start(time.Now())
	defer func() { report.End(time.Now()) }()

	defer func() {
		if err := context.Cause(ctx); err != nil {
			report.ReportError(Interrupt{err})
		}
	}()

	report.TrackProgress(source.GetProgress())
	ctx = withSourceFields(ctx, source, report)

	// Check if source units are supported and configured.
	canUseSourceUnits := len(targets) == 0 && s.useSourceUnitsFunc != nil
	if enumChunker, ok := source.(SourceUnitEnumChunker); ok && canUseSourceUnits && s.useSourceUnitsFunc() {
		ctx.Logger().Info("running source",
			"with_units", true)
		return s.runWithUnits(ctx, enumChunker, report)
	}
	ctx.Logger().Info("running source",
		"with_units", false,
		"target_count", len(targets),
		"source_manager_units_configurable", s.useSourceUnitsFunc != nil)
	return s.runWithoutUnits(ctx, source, report, targets...)
}

// withSourceFields adds the job and source fields to the context, so every log
// line the source writes says which source and job it came from.
// A caller (like thog) may have added some of them already. We skip those so a
// field doesn't show up twice. A missing value is nil, not "", so check for nil.
func withSourceFields(ctx context.Context, source Source, report *JobProgress) context.Context {
	if ctx.Value("job_id") == nil {
		ctx = context.WithValue(ctx, "job_id", report.JobID)
	}
	if ctx.Value("source_id") == nil {
		ctx = context.WithValue(ctx, "source_id", report.SourceID)
	}
	if ctx.Value("source_name") == nil {
		ctx = context.WithValue(ctx, "source_name", report.SourceName)
	}
	if ctx.Value("source_type") == nil {
		ctx = context.WithValue(ctx, "source_type", source.Type().String())
	}
	return ctx
}

// enumerate is a helper method to enumerate a Source.
func (s *SourceManager) enumerate(ctx context.Context, source Source, report *JobProgress, reporter UnitReporter) error {
	report.Start(time.Now())
	defer func() { report.End(time.Now()) }()

	defer func() {
		if err := context.Cause(ctx); err != nil {
			report.ReportError(Interrupt{err})
		}
	}()

	report.TrackProgress(source.GetProgress())
	ctx = withSourceFields(ctx, source, report)

	// Check if source units are supported and configured.
	canUseSourceUnits := s.useSourceUnitsFunc != nil
	if enumChunker, ok := source.(SourceUnitEnumerator); ok && canUseSourceUnits && s.useSourceUnitsFunc() {
		ctx.Logger().Info("running source", "with_units", true)
		return s.enumerateWithUnits(ctx, enumChunker, report, reporter)
	}
	return fmt.Errorf("enumeration not supported or configured for source: %s", source.Type().String())
}

// scan runs a scan against a single SourceUnit as its only job. This method
// manages the lifecycle of the provided report.
func (s *SourceManager) scan(ctx context.Context, source Source, report *JobProgress, unit SourceUnit) error {
	report.Start(time.Now())
	defer func() { report.End(time.Now()) }()

	defer func() {
		if err := context.Cause(ctx); err != nil {
			report.ReportError(Interrupt{err})
		}
	}()

	report.ReportUnit(unit)
	report.TrackProgress(source.GetProgress())
	ctx = withSourceFields(ctx, source, report)

	// Check if source units are supported and configured.
	canUseSourceUnits := s.useSourceUnitsFunc != nil
	if unitChunker, ok := source.(SourceUnitChunker); ok && canUseSourceUnits && s.useSourceUnitsFunc() {
		ctx.Logger().Info("running source",
			"with_units", true)
		return s.scanWithUnit(ctx, unitChunker, report, unit)
	}
	return fmt.Errorf("source units not supported or configured for source: %s (%s)", report.SourceName, source.Type().String())
}

// enumerateWithUnits is a helper method to enumerate a Source that is also a
// SourceUnitEnumerator. This allows better introspection of what is getting
// enumerated and any errors encountered.
func (s *SourceManager) enumerateWithUnits(ctx context.Context, source SourceUnitEnumerator, report *JobProgress, reporter UnitReporter) error {
	// Create a function that will save the first error encountered (if
	// any) and discard the rest.
	firstErr := make(chan error, 1)
	catchFirstError := func(err error) {
		select {
		case firstErr <- err:
		default:
		}
	}

	// Produce units.
	func() {
		report.StartEnumerating(time.Now())
		defer func() { report.EndEnumerating(time.Now()) }()
		ctx.Logger().Info("source enumeration started", "role", "source-manager", "event", "source_enumeration_started", "source_name", report.SourceName, "source_id", report.SourceID, "job_id", report.JobID, "source_type", ctx.Value("source_type"))
		ctx.Logger().V(2).Info("enumerating source with units")
		defer common.RecoverWithHandler(ctx, func(err error) {
			report.ReportError(Fatal{err})
			catchFirstError(Fatal{err})
		})
		if err := source.Enumerate(ctx, reporter); err != nil {
			report.ReportError(jobErr(ctx, err))
			catchFirstError(jobErr(ctx, err))
		}
		ctx.Logger().Info("source enumeration finished", "role", "source-manager", "event", "source_enumeration_finished", "source_name", report.SourceName, "source_id", report.SourceID, "job_id", report.JobID, "source_type", ctx.Value("source_type"))
	}()

	select {
	case err := <-firstErr:
		return err
	default:
		return nil
	}
}

// runWithoutUnits is a helper method to run a Source. It has coarse-grained
// job reporting.
func (s *SourceManager) runWithoutUnits(ctx context.Context, source Source, report *JobProgress, targets ...ChunkingTarget) error {
	// Introspect on the chunks we get from the Chunks method.
	ch := make(chan *Chunk, defaultChannelSize)
	var wg sync.WaitGroup
	// Consume chunks and export chunks.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for chunk := range ch {
			chunk.JobID = source.JobID()
			report.ReportChunk(nil, chunk)
			s.outputChunks <- chunk
		}
	}()

	// Don't return from this function until the goroutine has finished
	// outputting chunks to the downstream channel. Closing the channel
	// will stop the goroutine, so that needs to happen first in the defer
	// stack.
	defer wg.Wait()
	defer close(ch)
	if err := source.Chunks(ctx, ch, targets...); err != nil {
		err = jobErr(ctx, err)
		report.ReportError(err)
		return err
	}
	return nil
}

// runWithUnits is a helper method to run a Source that is also a
// SourceUnitEnumChunker. This allows better introspection of what is getting
// scanned and any errors encountered.
func (s *SourceManager) runWithUnits(ctx context.Context, source SourceUnitEnumChunker, report *JobProgress) error {
	unitReporter := &mgrUnitReporter{
		unitCh: make(chan SourceUnit, 1),
		report: report,
	}
	unitReporter.enumerationReporter = enumerationReporter{
		child:      unitReporter,
		ctx:        ctx,
		sourceName: report.SourceName,
		sourceID:   report.SourceID,
		jobID:      report.JobID,
		sourceType: fmt.Sprint(ctx.Value("source_type")),
		queueDepth: func() int { return len(unitReporter.unitCh) },
		discovered: new(atomic.Uint64),
		stats:      &s.stats,
	}
	// Create a function that will save the first error encountered (if
	// any) and discard the rest.
	firstErr := make(chan error, 1)
	catchFirstError := func(err error) {
		select {
		case firstErr <- err:
		default:
		}
	}

	// Produce units.
	go func() {
		report.StartEnumerating(time.Now())
		defer func() { report.EndEnumerating(time.Now()) }()
		ctx.Logger().Info("source enumeration started", "role", "source-manager", "event", "source_enumeration_started", "source_name", report.SourceName, "source_id", report.SourceID, "job_id", report.JobID, "source_type", ctx.Value("source_type"))
		defer close(unitReporter.unitCh)
		ctx.Logger().V(2).Info("enumerating source")
		defer common.RecoverWithHandler(ctx, func(err error) {
			report.ReportError(Fatal{err})
			catchFirstError(Fatal{err})
		})
		if err := source.Enumerate(ctx, unitReporter.enumerationReporter); err != nil {
			report.ReportError(jobErr(ctx, err))
			catchFirstError(jobErr(ctx, err))
		}
		ctx.Logger().Info("source enumeration finished", "role", "source-manager", "event", "source_enumeration_finished", "source_name", report.SourceName, "source_id", report.SourceID, "job_id", report.JobID, "source_type", ctx.Value("source_type"), "discovered_count", unitReporter.enumerationReporter.discovered.Load(), "queue_depth", len(unitReporter.unitCh))
	}()
	var wg sync.WaitGroup
	unitSem, releaseUnitSem := s.newUnitSem()
	defer releaseUnitSem()
	run := &unitRun{}
	launch := func(unit SourceUnit) bool {
		id, kind := unit.SourceUnitID()
		if err := unitSem.Acquire(ctx, 1); err != nil {
			ctx.Logger().V(1).Info("unit admission cancelled", "role", "repository-worker", "event", "repository_cancelled", "task_id", id, "unit_kind", kind)
			s.stats.cancelled.Add(1)
			report.ReportError(Interrupt{err})
			catchFirstError(Interrupt{err})
			return false
		}
		s.stats.assigned.Add(1)
		ctx.Logger().V(2).Info("repository assigned", "role", "repository-worker", "event", "repository_assigned", "task_id", id, "unit_kind", kind, "task_state", taskrecovery.InProgress)
		chunkReporter := &mgrChunkReporter{
			unit:    unit,
			chunkCh: make(chan *Chunk, defaultChannelSize),
			report:  report,
		}
		// Consume units and produce chunks.
		var shedAttempt atomic.Bool
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer unitSem.Release(1)
			parent := ctx
			ctx, cancelUnit := context.WithCancelCause(parent)
			defer cancelUnit(nil)
			report.StartUnitChunking(unit, time.Now())
			defer close(chunkReporter.chunkCh)
			attempt := 1
			ctx.Logger().V(2).Info("repository scan started", "role", "repository-worker", "event", "repository_scan_started", "task_id", id, "unit_kind", kind)
			if s.taskRegistry != nil {
				record, admitted := s.taskRegistry.Start(id, id)
				if !admitted {
					s.stats.skipped.Add(1)
					ctx.Logger().V(1).Info("unit retry limit exhausted", "role", "repository-worker", "task_id", id, "task_state", taskrecovery.Failed)
					return
				}
				attempt = record.Attempt
			}
			ctx = context.WithValues(ctx, "unit_kind", kind, "unit", id, "task_attempt", attempt, "task_state", taskrecovery.InProgress)
			ctx.Logger().V(3).Info("chunking unit", "role", "repository-worker", "task_attempt", attempt, "task_state", taskrecovery.InProgress)
			defer common.RecoverWithHandler(ctx, func(err error) {
				report.ReportError(Fatal{ChunkError{Unit: unit, Err: err}})
				catchFirstError(Fatal{err})
			})
			tracked := &runningUnit{key: fmt.Sprintf("%d/%s", report.JobID, id), started: time.Now(), emitted: chunkReporter.Emitted, cancel: cancelUnit}
			s.registerRunning(tracked)
			defer s.finishRunning(tracked)
			err := s.chunkUnitWithRetries(ctx, source, unit, chunkReporter)
			if s.finishRunning(tracked) && parent.Err() == nil {
				// Replay even when err is nil: the git parser can end a cancelled scan without an error.
				// Shed under pressure, not failed: replay it later without spending a retry attempt.
				shedAttempt.Store(true)
				s.stats.shed.Add(1)
				if s.taskRegistry != nil {
					s.taskRegistry.Requeue(id, errUnitShed.Error())
				}
				run.add(unit)
				ctx.Logger().V(1).Info("repository shed", "role", "repository-worker", "event", "repository_shed", "task_id", id, "unit_kind", kind, "task_attempt", attempt, "task_state", taskrecovery.Queued, "emitted_chunks", chunkReporter.Emitted())
				return
			}
			if err != nil {
				failureClass := taskrecovery.Classify(err)
				if failureClass == taskrecovery.FailureCancelled {
					s.stats.cancelled.Add(1)
				} else {
					s.stats.failed.Add(1)
				}
				if s.taskRegistry != nil {
					canRetry := s.taskRegistry.Incomplete(id, err.Error())
					if canRetry && chunkReporter.Emitted() == 0 {
						ctx.Logger().V(2).Info("repository requeue requested", "role", "repository-worker", "event", "repository_requeue_requested", "task_id", id, "unit_kind", kind, "task_attempt", attempt, "failure_class", failureClass)
					}
				}
				if failureClass == taskrecovery.FailureCancelled {
					ctx.Logger().V(1).Info("repository cancelled", "role", "repository-worker", "event", "repository_cancelled", "task_id", id, "unit_kind", kind, "task_attempt", attempt)
				}
				ctx.Logger().Error(err, "unit failed", "role", "repository-worker", "task_attempt", attempt, "failure_class", failureClass, "retryable", taskrecovery.Retryable(failureClass), "emitted_chunks", chunkReporter.Emitted())
				ctx.Logger().V(1).Info("repository failed", "role", "repository-worker", "event", "repository_failed", "task_id", id, "unit_kind", kind, "task_attempt", attempt, "failure_class", failureClass, "emitted_chunks", chunkReporter.Emitted())
				report.ReportError(jobErr(ctx, ChunkError{Unit: unit, Err: err}))
				catchFirstError(jobErr(ctx, err))
			} else {
				s.stats.unitDone.Add(1)
				if s.taskRegistry != nil {
					s.taskRegistry.Complete(id)
					ctx.Logger().V(2).Info("repository acknowledged", "role", "repository-worker", "event", "repository_acknowledged", "task_id", id, "unit_kind", kind, "task_attempt", attempt, "task_state", taskrecovery.Completed)
				}
			}
			ctx.Logger().V(2).Info("repository scan finished", "role", "repository-worker", "event", "repository_scan_finished", "task_id", id, "unit_kind", kind, "task_attempt", attempt, "success", err == nil)
		}()
		// Consume chunks and export chunks.
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				if !shedAttempt.Load() {
					report.EndUnitChunking(unit, time.Now())
				}
			}()
			for chunk := range chunkReporter.chunkCh {
				if src, ok := source.(Source); ok {
					chunk.JobID = src.JobID()
				}
				queuedAt := time.Now()
				s.outputChunks <- chunk
				ctx.Logger().V(4).Info("chunk output dispatched", "role", "output-worker", "event", "chunk_output_dispatched", "queue_wait", time.Since(queuedAt).String(), "queue_depth", len(s.outputChunks))
			}
		}()
		return true
	}
	for unit := range unitReporter.unitCh {
		if !launch(unit) {
			break
		}
	}
	// Shed units are replayed once the running ones drain, so they run at the reduced limit.
	for round := 1; ; round++ {
		wg.Wait()
		pending := run.take()
		if len(pending) == 0 || ctx.Err() != nil {
			break
		}
		timer := time.NewTimer(time.Duration(round) * 250 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
		case <-timer.C:
		}
		for _, unit := range pending {
			if !launch(unit) {
				break
			}
		}
	}
	select {
	case err := <-firstErr:
		return err
	default:
		return nil
	}
}

func (s *SourceManager) chunkUnitWithRetries(ctx context.Context, source SourceUnitChunker, unit SourceUnit, reporter *mgrChunkReporter) error {
	maxAttempts := s.unitRetries + 1
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if attempt > 1 {
			s.stats.retries.Add(1)
			s.reportStats()
			delay := time.Duration(attempt-1) * 250 * time.Millisecond
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
		}
		if attempt > 1 {
			id, kind := unit.SourceUnitID()
			ctx.Logger().V(2).Info("retrying unit", "role", "repository-worker", "task_id", id, "unit", id, "unit_kind", kind, "task_attempt", attempt, "task_state", taskrecovery.InProgress)
		}
		lastErr = source.ChunkUnit(ctx, unit, reporter)
		if lastErr == nil {
			return nil
		}
		failureClass := taskrecovery.Classify(lastErr)
		if reporter.Emitted() != 0 {
			s.stats.incomplete.Add(1)
			s.reportStats()
			id, kind := unit.SourceUnitID()
			ctx.Logger().V(1).Info("unit incomplete", "role", "repository-worker", "task_id", id, "unit_kind", kind, "task_attempt", attempt, "task_state", taskrecovery.Incomplete, "emitted_chunks", reporter.Emitted())
			return lastErr
		}
		if !taskrecovery.Retryable(failureClass) || attempt == maxAttempts {
			s.stats.incomplete.Add(1)
			s.reportStats()
			id, kind := unit.SourceUnitID()
			ctx.Logger().V(1).Info("unit failed", "role", "repository-worker", "task_id", id, "unit_kind", kind, "task_attempt", attempt, "task_state", taskrecovery.Failed, "failure_class", failureClass)
			return lastErr
		}
	}
	return lastErr
}

// scanWithUnit produces chunks from a single SourceUnit.
func (s *SourceManager) scanWithUnit(ctx context.Context, source SourceUnitChunker, report *JobProgress, unit SourceUnit) error {
	// Create a function that will save the first error encountered (if
	// any) and discard the rest.
	chunkReporter := &mgrChunkReporter{
		unit:    unit,
		chunkCh: make(chan *Chunk, defaultChannelSize),
		report:  report,
	}
	// Produce chunks from the given unit.
	var chunkErr error
	go func() {
		report.StartUnitChunking(unit, time.Now())
		defer close(chunkReporter.chunkCh)
		id, kind := unit.SourceUnitID()
		ctx := context.WithValues(ctx, "unit_kind", kind, "unit", id, "task_attempt", 1, "task_state", taskrecovery.InProgress)
		ctx.Logger().V(3).Info("chunking unit", "role", "repository-worker", "task_attempt", 1, "task_state", taskrecovery.InProgress)
		defer common.RecoverWithHandler(ctx, func(err error) {
			report.ReportError(Fatal{ChunkError{Unit: unit, Err: err}})
			chunkErr = Fatal{err}
		})
		if err := s.chunkUnitWithRetries(ctx, source, unit, chunkReporter); err != nil {
			failureClass := taskrecovery.Classify(err)
			ctx.Logger().Error(err, "unit failed", "role", "repository-worker", "task_attempt", 1, "failure_class", failureClass, "retryable", taskrecovery.Retryable(failureClass), "emitted_chunks", chunkReporter.Emitted())
			report.ReportError(jobErr(ctx, ChunkError{Unit: unit, Err: err}))
			chunkErr = jobErr(ctx, err)
		}
	}()
	// Consume chunks and export chunks.
	// This anonymous function blocks until the chunkReporter.chunkCh is
	// closed in the above goroutine.
	func() {
		defer func() { report.EndUnitChunking(unit, time.Now()) }()
		for chunk := range chunkReporter.chunkCh {
			if src, ok := source.(Source); ok {
				chunk.JobID = src.JobID()
			}
			s.outputChunks <- chunk
		}
	}()
	return chunkErr
}

// headlessAPI implements the apiClient interface locally.
type headlessAPI struct {
	// Counters for assigning source and job IDs.
	sourceIDCounter int64
	jobIDCounter    int64
}

func (api *headlessAPI) GetIDs(context.Context, string, sourcespb.SourceType) (SourceID, JobID, error) {
	return SourceID(atomic.AddInt64(&api.sourceIDCounter, 1)), JobID(atomic.AddInt64(&api.jobIDCounter, 1)), nil
}

// mgrUnitReporter implements the UnitReporter interface.
var _ UnitReporter = (*mgrUnitReporter)(nil)

type mgrUnitReporter struct {
	unitCh chan SourceUnit
	report *JobProgress
	enumerationReporter
}

// UnitOk implements the UnitReporter interface by recording the unit in the
// report and sending it on the SourceUnit channel.
func (s *mgrUnitReporter) UnitOk(ctx context.Context, unit SourceUnit) error {
	s.report.ReportUnit(unit)
	return common.CancellableWrite(ctx, s.unitCh, unit)
}

// UnitErr implements the UnitReporter interface by recording the error in the
// report.
func (s *mgrUnitReporter) UnitErr(ctx context.Context, err error) error {
	s.report.ReportError(err)
	return nil
}

// EnumerationPageReporter is an optional reporter extension for paginated sources.
type EnumerationPageReporter interface {
	Page(context.Context, EnumerationPage)
}

// EnumerationPage describes one source enumeration response without exposing
// provider-specific continuation tokens or request URLs.
type EnumerationPage struct {
	Number       int
	RequestKind  string
	ItemsSeen    int
	Continuation bool
}

type enumerationReporter struct {
	child      UnitReporter
	ctx        context.Context
	sourceName string
	sourceID   SourceID
	jobID      JobID
	sourceType string
	queueDepth func() int
	discovered *atomic.Uint64
	stats      *sourceManagerStats
}

func (r enumerationReporter) UnitOk(ctx context.Context, unit SourceUnit) error {
	count := r.discovered.Add(1)
	if r.stats != nil {
		r.stats.discovered.Add(1)
	}
	id, kind := unit.SourceUnitID()
	r.ctx.Logger().V(2).Info("source unit discovered", "role", "source-manager", "event", "source_unit_discovered", "source_name", r.sourceName, "source_id", r.sourceID, "job_id", r.jobID, "source_type", r.sourceType, "unit_id", id, "unit_kind", kind, "discovered_count", count, "queue_depth", r.queueDepth())
	if err := r.child.UnitOk(ctx, unit); err != nil {
		return err
	}
	r.ctx.Logger().V(2).Info("repository queued", "role", "repository-worker", "event", "repository_queued", "task_id", id, "unit_kind", kind, "task_state", taskrecovery.Queued, "discovered_count", count, "queue_depth", r.queueDepth())
	return nil
}

func (r enumerationReporter) UnitErr(ctx context.Context, err error) error {
	r.ctx.Logger().Error(err, "source enumeration error", "role", "source-manager", "event", "source_enumeration_error", "source_name", r.sourceName, "source_id", r.sourceID, "job_id", r.jobID, "source_type", r.sourceType, "discovered_count", r.discovered.Load(), "queue_depth", r.queueDepth())
	return r.child.UnitErr(ctx, err)
}

func (r enumerationReporter) Page(_ context.Context, page EnumerationPage) {
	r.ctx.Logger().V(2).Info("source enumeration page", "role", "source-manager", "event", "source_enumeration_page", "source_name", r.sourceName, "source_id", r.sourceID, "job_id", r.jobID, "source_type", r.sourceType, "page_number", page.Number, "request_kind", page.RequestKind, "items_seen", page.ItemsSeen, "continuation", page.Continuation, "discovered_count", r.discovered.Load(), "queue_depth", r.queueDepth())
}

// mgrChunkReporter implements the ChunkReporter interface.
var _ ChunkReporter = (*mgrChunkReporter)(nil)

type mgrChunkReporter struct {
	unit    SourceUnit
	chunkCh chan *Chunk
	report  *JobProgress
	emitted atomic.Uint64
}

// ChunkOk implements the ChunkReporter interface by recording the chunk and
// its associated unit in the report and sending it on the Chunk channel.
func (s *mgrChunkReporter) ChunkOk(ctx context.Context, chunk Chunk) error {
	s.report.ReportChunk(s.unit, &chunk)
	queuedAt := time.Now()
	if err := common.CancellableWrite(ctx, s.chunkCh, &chunk); err != nil {
		ctx.Logger().Error(err, "chunk abandoned", "role", "repository-worker", "event", "chunk_abandoned", "queue_wait", time.Since(queuedAt).String(), "queue_depth", len(s.chunkCh))
		return err
	}
	s.emitted.Add(1)
	ctx.Logger().V(4).Info("chunk produced", "role", "repository-worker", "event", "chunk_produced", "queue_wait", time.Since(queuedAt).String(), "queue_depth", len(s.chunkCh), "emitted_chunks", s.emitted.Load())
	return nil
}

func (s *mgrChunkReporter) Emitted() uint64 {
	return s.emitted.Load()
}

// ChunkErr implements the ChunkReporter interface by recording the error and
// its associated unit in the report.
func (s *mgrChunkReporter) ChunkErr(ctx context.Context, err error) error {
	ctx.Logger().V(3).Info("chunk processing error", "role", "repository-worker", "event", "chunk_error")
	s.report.ReportError(ChunkError{s.unit, err})
	return nil
}
