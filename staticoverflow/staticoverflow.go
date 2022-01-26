// Package staticoverflow provides an azblob.TransferManager that is more adjustable to users needs.
// The TransferManager can be a:
// 	Static buffer manager - memory is preset to a limit of blockSize * staticBlocks
//	Dyanmaic buffer manager - uses a sync.Pool that is.args.DynamicLimited by concurrency and sync.Pool's deallocation scheme
//	Static buffer + limted overflow - same as static buffer, but when static allocation runs out, uses a sync.Pool that has.args.DynamicLimited growth
//	Static buffer + dynamic buffer - when static buffer runs out, will act as the dynamic buffer manager until static allocation is sufficient.
//
// In addition the TransferManager allows adjusting of the settings while the TransferManager is running using the Reset() method.
// Finally the TransferManager supports gathering statistics about its internal to allow better decision making.
//
// A note on allocations: When using static buffering and your static buffers are able to handle the load, no allocations will happen.
// However, if load exceeds buffers you will see allocations. This is due to channel blocking in select causing an allocation. A good rule
// to keep low allocations for static use is staticBlocks > concurrency. This is of course dependent on network blocking and other items.
package staticoverflow

import (
	"context"
	"fmt"
	"log"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// _1MiB represents 1 MiB size as an integer.
	_1MiB = 8 << 17
)

// statCalcs is used to calculate our runtime numbers.
type statCalcs struct {
	staticCount, dynamicCount, numRunning int64

	runtimeStats RuntimeStats

	// This block are required to calculate Run() latency times.
	latencyMu    sync.RWMutex // protects everything in this block
	latencyStore int64        // only use atomic to access
	latencyCount int64        // only use atomic to access
	latency      int64        // latencyStore/latencyCount
}

// TransferManager implements azblob.TransferManager. This TransferManager supports doing static buffer allocation,
// dynamic buffer allocation using a sync.Pool, a static buffer with limited overflow using a sync.Pool and a
// static buffer with unlimited overflow.
type TransferManager struct {
	// ctx is provided to detect calls to Close().
	ctx context.Context
	// cancel is used to cancel ctx on Close().
	cancel context.CancelFunc

	// work is incremented whenver a Get() or Run() call is made. It is decremented when Put() is called or
	// the function passed to Run() is finished.
	work sync.WaitGroup

	// runnerIn is used by Run() to send a function to be run by any available goroutine.
	runnerIn chan func()

	// statsMu protects statCalcs.
	statsMu sync.Mutex
	// statCalcs holds the values that are used to calculate our stats.
	statCalcs *statCalcs
	// stats are the currently calculated Stats.
	stats atomic.Value // Stats
	// statsCh hold a channel that will return Stats when they are changed.
	statsCh chan Stats

	// Mu protects everything below so we can adjust settings on a live TransferManager.
	mu sync.RWMutex
	// args are our current arguments.
	args Args
	// staticCh holds all statically allocated blocks.
	staticCh chan []byte
	// pool holds our overflow pool.
	pool sync.Pool
	// limiter limits how many blocks we can get dynamically from the pool.
	// When set to nil this is unlimited.
	limiter chan struct{}
	// runnerCancels holds a list of cancel functions that can be used to kill
	// off runners individually. All runners are killed if .cancel() from above is called.
	runnerCancels []context.CancelFunc

	// once is used to start collection().
	once sync.Once
}

// Args are arguments to the New() constructor.
type Args struct {
	// BlockSize is the size of each block that will be allocated. Must be >= 1MiB.
	BlockSize int
	// StaticBlocks is the amount of pre-allocated blocks that are always available.
	// If staticBlocks <= 0, all block allocation will be dynamic using a sync.Pool.
	StaticBlocks int
	// DynamicLimit sets the amount of blocks that can be pulled from a sync.Pool
	// when you have run out of static blocks. Unlike static blocks, when not in use
	// these blocks will be de-allocated. Setting this to -1 allows unlimited overflow
	// and 0 disables overflow.
	DynamicLimit int
	// Concurrency is the size of a goroutine pool that limits how many concurrent transfers
	// can happen at a time. Must be > 0.
	Concurrency int
}

func (a Args) validate() error {
	if a.BlockSize < _1MiB {
		return fmt.Errorf("BlockSize cannot be < 1MiB")
	}

	if a.Concurrency < 1 {
		return fmt.Errorf("Concurrency cannot be < 1")
	}
	if a.StaticBlocks < 1 && a.DynamicLimit == 0 {
		return fmt.Errorf("StaticBlocks cannot be < 1 if DynamicLimit is 0")
	}
	return nil
}

func (a Args) validateReset(n Args, s *TransferManager) error {
	if a.StaticBlocks != n.StaticBlocks {
		if n.StaticBlocks < 1 {
			return fmt.Errorf("cannot set static block size < 1")
		}

		if s.staticCh == nil {
			return fmt.Errorf("cannot set static block size if static blocks were disabled")
		}
	}
	if a.BlockSize != n.BlockSize {
		if n.BlockSize < _1MiB {
			return fmt.Errorf("cannot set block size < _1MiB")
		}
	}
	if a.DynamicLimit != n.DynamicLimit {
		if n.DynamicLimit == 0 {
			return fmt.Errorf("n cannot be set to 0")
		}
		if a.DynamicLimit == 0 {
			return fmt.Errorf("pool was disabled, cannot change limit")
		}
	}
	if a.Concurrency != n.Concurrency {
		if n.Concurrency < 1 {
			return fmt.Errorf("cannot set concurrency < 1")
		}
	}
	return nil
}

// New creates a new TransferManager. blockSize is the size of each block that will be allocated which must be >= 1MiB. staticBlocks is the
// amount of pre-allocated blocks that are always available. If staticBlocks <= 0, all block allocation will be dynamic using a sync.Pool.
// overflowLimit sets the amount of blocks that can be pulled from a sync.Pool when you have run out of static blocks. Unlike static blocks,
// when not in use these blocks will be de-allocated. Setting this to -1 allows unlimited overflow and 0 disables overflow. concurrency
// is the size of a goroutine pool that limits how many concurrent transfers can happen at a time.
func New(args Args) (*TransferManager, error) {
	if err := args.validate(); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	so := &TransferManager{
		ctx:       ctx,
		cancel:    cancel,
		args:      args,
		runnerIn:  make(chan func(), 1),
		statCalcs: &statCalcs{},
		statsCh:   make(chan Stats, 1),
	}

	for i := 0; i < args.Concurrency; i++ {
		ctx, cancel = context.WithCancel(ctx)
		so.runnerCancels = append(so.runnerCancels, cancel)
		go so.runner(ctx)
	}

	if args.StaticBlocks > 0 {
		so.staticCh = make(chan []byte, args.StaticBlocks)
		for i := 0; i < args.StaticBlocks; i++ {
			so.staticCh <- make([]byte, args.BlockSize)
		}
	}

	if args.DynamicLimit > 0 || args.DynamicLimit == -1 {
		so.pool = sync.Pool{
			New: func() interface{} {
				return make([]byte, args.BlockSize)
			},
		}
		if args.DynamicLimit > 0 {
			so.limiter = make(chan struct{}, args.DynamicLimit)
		}
	}

	go so.collections()

	return so, nil
}

// collection collects stats every 10 seconds until Close() or Reset() is called. This is actually a slightly complex piece
// of machinery, so make sure you understand what is going on before you make changes.
func (s *TransferManager) collections() {
	// last is used to inform us that the last push into statsCh has completed before attempting
	// the next one.
	last := make(chan struct{})
	// We close this that on the first loop it isn't blocked, because we'd be waiting for something
	// that hasn't happened.
	close(last)
	// We derive a new ctx and cancel from the main one so that we can cancel a push into statsCh
	// when the next collection occurs.
	ctx, cancel := context.WithCancel(s.ctx)
	parentCtx := s.ctx
	for {
		select {
		// Close() or Reset() was called.
		case <-parentCtx.Done():
			return
		// Do a collection after 10 seconds.
		case <-time.After(10 * time.Second):
			cancel()
			ctx, cancel = context.WithCancel(parentCtx)

			s.collect()

			// If no one has called SubStats(), no need to do the rest.
			if s.statsCh == nil {
				continue
			}
			// Wait for the last collection push to be done.
			<-last

			s.renderStats()

			last = s.pushStats(ctx, s.Stats())
		}
	}
}

// collect runs all of our stat collections concurrently.
func (s *TransferManager) collect() {
	wg := sync.WaitGroup{}

	collections := []func(){s.runtimeCollect, s.latencyCollect}

	wg.Add(len(collections))

	for _, f := range collections {
		f := f
		go func() {
			defer wg.Done()
			f()
		}()
	}

	wg.Wait()
}

// pushStats pushes Stats on to our internal statsCh. If the channel is blocked, it removes the previous entry.
// We return a channel that is closed when the push has succeeded.
func (s *TransferManager) pushStats(ctx context.Context, stats Stats) chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			// Either Close()/Reset() was called or we couldn't get this into the
			// channel by the next collection time.
			case <-ctx.Done():
				return
			// If it goes in, then the stats reader is reading fast enough.
			case s.statsCh <- stats:
				return
			// Well, the stats reader isn't fast enough, so we need to pull the old
			// entry out and put the new one in.
			default:
				select {
				// Yank the blocking entry.
				case <-s.statsCh:
				// It was pulled out before we could get it. So try another insert.
				default:
				}
			}
		}
	}()

	return done
}

// nower returns the current time. Exists to allow subing out during tests.
var nower = time.Now

func (s *TransferManager) runtimeCollect() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	rs := RuntimeStats{
		CurrentAllocs: m.Alloc,
		TotalAllocs:   m.TotalAlloc,
		SysMemory:     m.Sys,
		NumGC:         m.NumGC,
	}
	s.statCalcs.runtimeStats = rs
}

// latencyCollect collects the latency information and stores the new one in .latency.
// It then zeros the counters.
func (s *TransferManager) latencyCollect() {
	s.statCalcs.latencyMu.Lock()
	c := s.perCalc(&s.statCalcs.latencyStore, &s.statCalcs.latencyCount)
	s.statCalcs.latencyMu.Unlock()

	atomic.StoreInt64(&s.statCalcs.latency, c)
}

func (s *TransferManager) perCalc(sum, count *int64) int64 {
	suml := atomic.LoadInt64(sum)
	countl := atomic.LoadInt64(count)
	switch int64(0) {
	case suml, countl:
		return 0
	}
	atomic.StoreInt64(sum, 0)
	atomic.StoreInt64(count, 0)
	return suml / countl
}

// Stats give internal stats on our TransferManager.
type Stats struct {
	// Collection is when the Stats were collected.
	Collection time.Time
	// RuntimeStats are the runtime stats of the application.
	RuntimeStats RuntimeStats
	// UsedStaticBuffer is how many of the buffers are currently in use.
	UsedStaticBuffer int64
	// UsedDynamicBuffer is how many dynamic buffers are currently in use.
	UsedDynamicBuffer int64
	// NumRunning is the number of Run() calls currently executing.
	NumRunning int64
	// RunLatency gives latency information for Run() calls. This will be a 10 second
	// average.
	RunLatency time.Duration
	// TransferRate is the avg rate in bytes per second that we are transfering in.
	// This is calculated by looking at the Put() calls per second and averaging over 10 seconds.
	TransferRate int64
}

// RuntimeStats are runtime stats for the entire program.
type RuntimeStats struct {
	// CurrentAllocs is the current amount of heap objects that are allocated.
	CurrentAllocs uint64
	// TotalAllocs are the total amount of heap objects that have been allocated.
	TotalAllocs uint64
	// SysMemory is the current system memory use in bytes.
	SysMemory uint64
	// NumGC is the total number of garbage collections that have occured.
	NumGC uint32
}

// Args returns a copy of the current Args.
func (t *TransferManager) Args() Args {
	t.mu.Lock()
	defer t.mu.Unlock()

	return t.args
}

// Stats returns the current TransferManager stats. This can be used to guage if changing values would
// help impact performance.
func (t *TransferManager) Stats() Stats {
	s := t.stats.Load()
	if s == nil {
		return Stats{}
	}
	return s.(Stats)
}

func (t *TransferManager) renderStats() {
	t.statsMu.Lock()
	defer t.statsMu.Unlock()

	s := Stats{
		Collection:        time.Now(),
		RuntimeStats:      t.statCalcs.runtimeStats,
		UsedStaticBuffer:  atomic.LoadInt64(&t.statCalcs.staticCount),
		UsedDynamicBuffer: atomic.LoadInt64(&t.statCalcs.dynamicCount),
		NumRunning:        atomic.LoadInt64(&t.statCalcs.numRunning),
		RunLatency:        time.Duration(atomic.LoadInt64(&t.statCalcs.latency)),
	}
	t.stats.Store(s)
}

func (s *TransferManager) resetStats() {
	s.statsMu.Lock()
	defer s.statsMu.Unlock()

	s.statCalcs = &statCalcs{}
}

// SubStats subscribes to Stats as they are collected. The returned chan is closed when
// either Reset() or Close is called. SubStats will always return the same channel as lon
// as Reset() or Close has not been called, as there should be a single subscriber at most.
func (s *TransferManager) SubStats() chan Stats {
	return s.statsCh
}

// Get implements TransferManger.Get().
func (s *TransferManager) Get() []byte {
	s.work.Add(1)

	s.mu.RLock()
	defer s.mu.RUnlock()

	s.once.Do(func() {
		go s.collections()
	})

	// No static buffer.
	if s.staticCh == nil {
		switch {
		case s.args.DynamicLimit < 0:
			atomic.AddInt64(&s.statCalcs.dynamicCount, 1)
			b := s.pool.Get().([]byte)
			// This handles switching out for a new block size if SetBlockSize() was called.
			if len(b) != s.args.BlockSize {
				b = make([]byte, s.args.BlockSize)
			}
			return b
		case s.args.DynamicLimit > 0:
			select {
			case s.limiter <- struct{}{}:
				atomic.AddInt64(&s.statCalcs.dynamicCount, 1)
				b := s.pool.Get().([]byte)
				// This handles switching out for a new block size if SetBlockSize() was called.
				if len(b) != s.args.BlockSize {
					b = make([]byte, s.args.BlockSize)
				}
				return b
			}
		case s.args.DynamicLimit == 0:
			panic("well, you found a bug: no static buffer and a disabled dynamic buffer, should have been caught with New()")
		}
	}

	// We have static buffer.
	switch {
	case s.args.DynamicLimit == 0:
		b := <-s.staticCh
		// This handles switching out for a new block size if SetBlockSize() was called.
		if len(b) != s.args.BlockSize {
			b = make([]byte, s.args.BlockSize)
		}
		atomic.AddInt64(&s.statCalcs.staticCount, 1)
		return b
	case s.args.DynamicLimit < 0:
		select {
		case b := <-s.staticCh:
			// This handles switching out for a new block size if SetBlockSize() was called.
			if len(b) != s.args.BlockSize {
				b = make([]byte, s.args.BlockSize)
			}
			atomic.AddInt64(&s.statCalcs.staticCount, 1)
			return b
		default:
			atomic.AddInt64(&s.statCalcs.dynamicCount, 1)
			b := s.pool.Get().([]byte)
			// This handles switching out for a new block size if SetBlockSize() was called.
			if len(b) != s.args.BlockSize {
				b = make([]byte, s.args.BlockSize)
			}
			return b
		}
	case s.args.DynamicLimit > 0:
		select {
		case b := <-s.staticCh:
			// This handles switching out for a new block size if SetBlockSize() was called.
			if len(b) != s.args.BlockSize {
				b = make([]byte, s.args.BlockSize)
			}
			atomic.AddInt64(&s.statCalcs.staticCount, 1)
			return b
		case s.limiter <- struct{}{}:
			atomic.AddInt64(&s.statCalcs.dynamicCount, 1)
			b := s.pool.Get().([]byte)
			// This handles switching out for a new block size if SetBlockSize() was called.
			if len(b) != s.args.BlockSize {
				b = make([]byte, s.args.BlockSize)
			}
			return b
		}
	}
	panic("should not get here") // Required as to not have "return nil", which would be a bug
}

// Put implements TransferManger.Put().
func (t *TransferManager) Put(b []byte) {
	defer t.work.Done()

	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.staticCh == nil {
		switch {
		case t.args.DynamicLimit < 0:
			atomic.AddInt64(&t.statCalcs.dynamicCount, -1)
			t.pool.Put(b)
			return
		case t.args.DynamicLimit > 0:
			select {
			case <-t.limiter:
				atomic.AddInt64(&t.statCalcs.dynamicCount, -1)
				t.pool.Put(b)
				return
			default:
				return
			}
		case t.args.DynamicLimit == 0:
			panic("well, you found a bug: no static buffer and a disabled dynamic buffer, should have been caught with New()")
		}
	}
	// We have static buffer.
	switch {
	case t.args.DynamicLimit == 0:
		select {
		case t.staticCh <- b:
			atomic.AddInt64(&t.statCalcs.staticCount, -1)
		default:
			return
		}
	case t.args.DynamicLimit < 0:
		select {
		case t.staticCh <- b:
			atomic.AddInt64(&t.statCalcs.staticCount, -1)
			return
		default:
			atomic.AddInt64(&t.statCalcs.dynamicCount, -1)
			t.pool.Put(b)
			return
		}
	case t.args.DynamicLimit > 0:
		select {
		case t.staticCh <- b:
			atomic.AddInt64(&t.statCalcs.staticCount, -1)
			return
		case <-t.limiter:
			atomic.AddInt64(&t.statCalcs.dynamicCount, -1)
			t.pool.Put(b)
			return
		default:
			return
		}
	}
}

// Run implements TransferManger.Run().
func (s *TransferManager) Run(f func()) {
	s.work.Add(1)

	s.mu.RLock()
	s.runnerIn <- f
	s.mu.RUnlock()
}

// Wait can be used to wait for all Get() calls to have matching Put() calls and all
// Run() calls to have completed. This is generally only useful when you are doing benchmarking
// and want to have everything done before calling the Reset(). If there is a bug in the upstream
// using this where it is holding a buffer returned by Get(), this will block forever. If someone
// does a Put() without a Get() being called first, this is going to panic. We log ever 5 seconds
// of waiting on this call.
func (s *TransferManager) Wait() {
	tick := time.NewTicker(5 * time.Second)
	defer tick.Stop()

	done := make(chan struct{})
	go func() {
		defer close(done)
		s.work.Wait()
	}()

	for {
		select {
		case <-tick.C:
			log.Println("still waiting for all operations to finish (or upstream has a bug)")
		case <-done:
			return
		}
	}
}

// runner is used to run func() that are sent by Run() until Close() is called.
func (s *TransferManager) runner(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case f := <-s.runnerIn:
			atomic.AddInt64(&s.statCalcs.numRunning, 1)
			start := time.Now()
			f()
			s.work.Done()
			atomic.AddInt64(&s.statCalcs.numRunning, -1)
			s.recordLatency(start)
		}
	}
}

// sincer records the time since the passed time.Time. Exists to allow substitution during tests.
var sincer = time.Since

func (s *TransferManager) recordLatency(t time.Time) {
	s.statCalcs.latencyMu.RLock()
	defer s.statCalcs.latencyMu.RUnlock()

	atomic.AddInt64(&s.statCalcs.latencyStore, int64(sincer(t)))
	atomic.AddInt64(&s.statCalcs.latencyCount, 1)
}

// Close implements TransferManager.Close().
func (s *TransferManager) Close() {
	s.cancel()
}

type ResetOption func(o *resetOptions)

type resetOptions struct {
	statsReset bool
}

// StatsReset indicates that stat collection should be zero'd out and stopped. Stat
// collection will restart on the next Get() call.
func StatsReset() ResetOption {
	return func(o *resetOptions) {
		o.statsReset = true
	}
}

// Reset resets the internals based on new Args. This is thread-safe.
// There are limitations over the New() that should be noted:
// 	Setting .StaticBlocks < 0 cannot be done unless it is already < 0.
// 	Setting .StaticBlocks > 0 cannot be done if .StaticBlocks > 0 already.
// 	Setting .BlockSize < 1MiB cannot be done.
// 	Setting .DynamicLimit > 0 || -1 cannot be done if it is 0 and vice versus.
// 	Setting .Concurrency < 1 cannot be done.
// Finally, it should be noted that Reset() will cause all Put()/Get()/Run() calls to momentarily
// block while the reset occurs.
func (s *TransferManager) Reset(args Args, options ...ResetOption) error {
	opts := resetOptions{}
	for _, o := range options {
		o(&opts)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	old := s.args
	s.args = args

	if err := old.validateReset(args, s); err != nil {
		return err
	}

	if opts.statsReset {
		s.resetStats()
		s.once = sync.Once{}
	}

	if old.BlockSize != args.BlockSize {
		s.setBlockSize(args)
	}
	if old.StaticBlocks != args.StaticBlocks {
		s.setNumStatic(args)
	}
	if old.DynamicLimit != args.DynamicLimit {
		s.setLimit(args)
	}
	if old.Concurrency != args.Concurrency {
		s.setConcurrency(old, args)
	}

	return nil
}

func (s *TransferManager) setBlockSize(args Args) {
	if args.DynamicLimit != 0 {
		s.pool = sync.Pool{
			New: func() interface{} {
				return make([]byte, args.BlockSize)
			},
		}
	}
}

func (s *TransferManager) setNumStatic(args Args) {
	ch := make(chan []byte, args.StaticBlocks)

	close(s.staticCh)

	// This takes all current blocks stored in our channel and adds them to the
	// new channel until full.
	for b := range s.staticCh {
		select {
		case ch <- b:
			continue
		default:
		}
		break
	}
	// If we aren't full, make new blocks until full.
	for len(ch) != cap(ch) {
		ch <- make([]byte, args.BlockSize)
	}

	s.staticCh = ch
}

func (s *TransferManager) setLimit(args Args) {
	switch {
	case args.DynamicLimit < 0:
		s.limiter = nil
	default:
		s.limiter = make(chan struct{}, args.DynamicLimit)
	}
}

func (s *TransferManager) setConcurrency(old, n Args) {
	switch {
	case n.Concurrency == old.Concurrency:
	case n.Concurrency > old.Concurrency:
		for i := 0; i < (n.Concurrency - old.Concurrency); i++ {
			ctx, cancel := context.WithCancel(s.ctx)
			go s.runner(ctx)
			s.runnerCancels = append(s.runnerCancels, cancel)
		}
	default:
		for i := 0; i < (old.Concurrency - n.Concurrency); i++ {
			cancel := s.runnerCancels[len(s.runnerCancels)-1]
			cancel()
			s.runnerCancels = s.runnerCancels[0 : len(s.runnerCancels)-1]
		}
	}
}
