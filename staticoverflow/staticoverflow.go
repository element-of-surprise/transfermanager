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
	"sync"
	"sync/atomic"
	"time"
)

const (
	// _1MiB represents 1 MiB size as an integer.
	_1MiB = 8 << 17
)

// TransferManager implements azblob.TransferManager. This TransferManager supports doing static buffer allocation,
// dynamic buffer allocation using a sync.Pool, a static buffer with limited overflow using a sync.Pool and a
// static buffer with unlimited overflow.
type TransferManager struct {
	// ctx is provided to detect calls to Close().
	ctx context.Context
	// cancel is used to cancel ctx on Close().
	cancel context.CancelFunc

	// runnerIn is used by Run() to send a function to be run by any available goroutine.
	runnerIn chan func()

	// These hold statistics that only require atomic.
	staticCount, dynamicCount, numRunning int64 // only use atomic to access

	// This block are required to calculate Run() latency times.
	latencyMu    sync.RWMutex // protects everything in this block
	latencyStore int64        // only use atomic to access
	latencyCount int64        // only use atomic to access
	latency      atomic.Value // Holds RunLatency

	speedMu    sync.RWMutex // protects everything in this block
	speedStore int64        // only use atomic to access
	speedCount int64        // only use atomic to access
	speed      int64        // only use atomic to access

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
		ctx:      ctx,
		cancel:   cancel,
		args:     args,
		runnerIn: make(chan func(), 1),
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

	go so.latencyCollection()
	go so.speedCollection()

	return so, nil
}

// latencyCollection collects Run() latency information every 10 seconds until
// Close() is called.
func (s *TransferManager) latencyCollection() {
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-time.After(10 * time.Second):
			go s.latencyCollect()
		}
	}
}

// nower returns the current time. Exists to allow subing out during tests.
var nower = time.Now

// latencyCollect collects the latency information and stores the new one in .latency.
// It then zeros the counters.
func (s *TransferManager) latencyCollect() {
	t := nower()

	s.latencyMu.Lock()
	c, _, count := s.perCalc(&s.latencyStore, &s.latencyCount)
	s.latencyMu.Unlock()

	s.latency.Store(RunLatency{Avg: time.Duration(c), Collections: count, Time: t})
}

func (s *TransferManager) speedCollection() {
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-time.After(10 * time.Second):
			s.speedCollect()
		}
	}
}

func (s *TransferManager) speedCollect() {
	s.speedMu.Lock()
	c, _, _ := s.perCalc(&s.speedStore, &s.speedCount)
	s.speedMu.Unlock()

	atomic.StoreInt64(&s.speed, c)
}

func (s *TransferManager) perCalc(sum, count *int64) (int64, int64, int64) {
	suml := atomic.LoadInt64(sum)
	countl := atomic.LoadInt64(count)
	switch int64(0) {
	case suml, countl:
		return 0, suml, countl
	}
	atomic.StoreInt64(sum, 0)
	atomic.StoreInt64(count, 0)
	return suml / countl, suml, countl
}

// Stats give internal stats on our TransferManager.
type Stats struct {
	// UsedStaticBuffer is how many of the buffers are currently in use.
	UsedStaticBuffer int64
	// UsedDynamicBuffer is how many dynamic buffers are currently in use.
	UsedDynamicBuffer int64
	// NumRunning is the number of Run() calls currently executing.
	NumRunning int64
	// RunLatency gives latency information for Run() calls. This will be a 10 second
	// average.
	RunLatency RunLatency
	// TransferRate is the avg rate in bytes per second that we are transfering in.
	// This is calculated by looking at the Put() calls per second and averaging over 10 seconds.
	TransferRate int64
}

// RunLatency gives latency information for Run() calls.
type RunLatency struct {
	// Avg is the average latency averaged over 10 seconds.
	Avg time.Duration
	// Collections is how many collections during the window.
	Collections int64
	// Time was when the collection was done.
	Time time.Time
}

// Stats returns the current buffer stats. This can be used to guage if changing values would
// help impact performance.
func (s *TransferManager) Stats() Stats {
	var l RunLatency
	x := s.latency.Load()
	if x != nil {
		l = x.(RunLatency)
	}

	return Stats{
		UsedStaticBuffer:  atomic.LoadInt64(&s.staticCount),
		UsedDynamicBuffer: atomic.LoadInt64(&s.dynamicCount),
		NumRunning:        atomic.LoadInt64(&s.numRunning),
		RunLatency:        l,
		TransferRate:      atomic.LoadInt64(&s.speed),
	}
}

// Get implements TransferManger.Get().
func (s *TransferManager) Get() []byte {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// No static buffer.
	if s.staticCh == nil {
		switch {
		case s.args.DynamicLimit < 0:
			atomic.AddInt64(&s.dynamicCount, 1)
			b := s.pool.Get().([]byte)
			// This handles switching out for a new block size if SetBlockSize() was called.
			if len(b) != s.args.BlockSize {
				b = make([]byte, s.args.BlockSize)
			}
			return b
		case s.args.DynamicLimit > 0:
			select {
			case s.limiter <- struct{}{}:
				atomic.AddInt64(&s.dynamicCount, 1)
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
		atomic.AddInt64(&s.staticCount, 1)
		return b
	case s.args.DynamicLimit < 0:
		select {
		case b := <-s.staticCh:
			// This handles switching out for a new block size if SetBlockSize() was called.
			if len(b) != s.args.BlockSize {
				b = make([]byte, s.args.BlockSize)
			}
			atomic.AddInt64(&s.staticCount, 1)
			return b
		default:
			atomic.AddInt64(&s.dynamicCount, 1)
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
			atomic.AddInt64(&s.staticCount, 1)
			return b
		case s.limiter <- struct{}{}:
			atomic.AddInt64(&s.dynamicCount, 1)
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
func (s *TransferManager) Put(b []byte) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.staticCh == nil {
		switch {
		case s.args.DynamicLimit < 0:
			atomic.AddInt64(&s.dynamicCount, -1)
			s.pool.Put(b)
			return
		case s.args.DynamicLimit > 0:
			select {
			case <-s.limiter:
				atomic.AddInt64(&s.dynamicCount, -1)
				s.pool.Put(b)
				return
			default:
				return
			}
		case s.args.DynamicLimit == 0:
			panic("well, you found a bug: no static buffer and a disabled dynamic buffer, should have been caught with New()")
		}
	}
	// We have static buffer.
	switch {
	case s.args.DynamicLimit == 0:
		select {
		case s.staticCh <- b:
			atomic.AddInt64(&s.staticCount, -1)
		default:
			return
		}
	case s.args.DynamicLimit < 0:
		select {
		case s.staticCh <- b:
			atomic.AddInt64(&s.staticCount, -1)
			return
		default:
			atomic.AddInt64(&s.dynamicCount, -1)
			s.pool.Put(b)
			return
		}
	case s.args.DynamicLimit > 0:
		select {
		case s.staticCh <- b:
			atomic.AddInt64(&s.staticCount, -1)
			return
		case <-s.limiter:
			atomic.AddInt64(&s.dynamicCount, -1)
			s.pool.Put(b)
			return
		default:
			return
		}
	}
}

// Run implements TransferManger.Run().
func (s *TransferManager) Run(f func()) {
	s.mu.RLock()
	s.runnerIn <- f
	s.mu.RUnlock()
}

// runner is used to run func() that are sent by Run() until Close() is called.
func (s *TransferManager) runner(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case f := <-s.runnerIn:
			atomic.AddInt64(&s.numRunning, 1)
			start := time.Now()
			f()
			atomic.AddInt64(&s.numRunning, -1)
			s.recordLatency(start)
		}
	}
}

// sincer records the time since the passed time.Time. Exists to allow substitution during tests.
var sincer = time.Since

func (s *TransferManager) recordLatency(t time.Time) {
	s.latencyMu.RLock()
	defer s.latencyMu.RUnlock()

	atomic.AddInt64(&s.latencyStore, int64(sincer(t)))
	atomic.AddInt64(&s.latencyCount, 1)
}

// Close implements TransferManager.Close().
func (s *TransferManager) Close() {
	s.cancel()
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
func (s *TransferManager) Reset(args Args) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	old := s.args
	s.args = args

	if err := old.validateReset(args, s); err != nil {
		return err
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
