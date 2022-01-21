package staticoverflow

import (
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/kylelemons/godebug/pretty"
)

func TestStaticBuffer(t *testing.T) {
	tm, err := New(Args{BlockSize: _1MiB, StaticBlocks: 8, DynamicLimit: 0, Concurrency: 16})
	if err != nil {
		panic(err)
	}

	f := func() {
		b := tm.Get()
		for i := range b {
			b[i] = byte(255)
		}
		tm.Put(b)
	}

	time.Sleep(1 * time.Second)
	for i := 0; i < 10000; i++ {
		tm.Run(f)
	}
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	if m.Alloc > 10*_1MiB {
		PrintMemUsage()
		t.Fatalf("TestStatic: memory allocation for static buffer memory test allocated %dMiB, expect < 10MiB", m.Alloc/_1MiB)
	}
	time.Sleep(1 * time.Second)
}

func TestDynamicLimitedBuffer(t *testing.T) {
	var startAlloc uint64

	// Wrapped to allow the tm to go out of scope and be GC'd.
	func() {
		tm, err := New(Args{BlockSize: _1MiB, StaticBlocks: 0, DynamicLimit: 8, Concurrency: 16})
		if err != nil {
			panic(err)
		}
		defer tm.Close()

		f := func() {
			b := tm.Get()
			for i := range b {
				b[i] = byte(255)
			}
			tm.Put(b)
		}

		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		startAlloc = m.Alloc

		PrintMemUsage()
		for i := 0; i < 10000; i++ {
			tm.Run(f)
		}

		runtime.ReadMemStats(&m)
		endMiB := bToMb(m.Alloc)
		maxMiB := bToMb(startAlloc + (16 * _1MiB))
		if endMiB > maxMiB {
			PrintMemUsage()
			t.Fatalf("TestDynamicLimitedBuffer: ended by exceeding maximum memory(%d MiB), had %d MiB", maxMiB, endMiB)
		}
	}()

	// Get the pool to release memory.
	time.Sleep(1 * time.Second)
	runtime.GC()
	time.Sleep(1 * time.Second)
	runtime.GC()

	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	endMiB := bToMb(m.Alloc)
	maxMiB := bToMb(startAlloc + _1MiB)
	if endMiB > maxMiB {
		t.Errorf("TestDynamicLimitedBuffer: ended, after GC(), exceeding maximum memory(%d MiB), had %d MiB", maxMiB, endMiB)
	}

	PrintMemUsage()
}

// TestGet tests our limitation when we are doing Get() with various
// settings. We want to make sure that we cannot exceed any of the
// limits we setup.
func TestGet(t *testing.T) {
	tests := []struct {
		desc       string
		staticSize int
		limit      int
		gets       int
	}{
		{
			desc:       "get from static",
			staticSize: 1,
			gets:       2,
		},
		{
			desc:       "get from static with pool of 1",
			staticSize: 1,
			limit:      1,
			gets:       3,
		},
	}

	for _, test := range tests {
		so := &TransferManager{
			args: Args{DynamicLimit: test.limit},
			pool: sync.Pool{
				New: func() interface{} {
					return []byte{}
				},
			},
		}
		if test.staticSize > 0 {
			so.staticCh = make(chan []byte, test.staticSize)
		}

		for i := 0; i < test.staticSize; i++ {
			so.staticCh <- []byte{}
		}

		if test.limit > 0 {
			so.limiter = make(chan struct{}, test.limit)
		}

		for i := 0; i < test.gets-1; i++ {
			so.Get()
		}
		done := make(chan struct{})
		go func() {
			defer close(done)
			so.Get()
		}()

		select {
		case <-done:
			t.Fatalf("TestGet(%s): Get() exceeded buffer limits", test.desc)
		case <-time.After(1 * time.Second):
			so.Put([]byte{})
			<-done

			stats := so.Stats()
			// This is 1 because when we do the Put() above there is 1 outstanding buffer.
			if stats.UsedStaticBuffer != 1 {
				t.Errorf("TestGet(%s): UsedStaticBuffers: got %d, want %d", test.desc, stats.UsedStaticBuffer, 1)
			}
			if test.limit > 0 {
				if stats.UsedDynamicBuffer != int64(test.limit) {
					fmt.Errorf("TestGet(%s): UsedDynamicBuffers: got %d, want %d", test.desc, stats.UsedDynamicBuffer, test.limit)
				}
			}
		}
	}
}

func TestRun(t *testing.T) {
	so, err := New(Args{BlockSize: _1MiB, StaticBlocks: 1, Concurrency: 8})
	if err != nil {
		panic(err)
	}
	defer so.Close()

	mu := sync.Mutex{}
	sl := make([]int, 0, 100)
	wg := sync.WaitGroup{}

	for i := 0; i < 100; i++ {
		i := i
		wg.Add(1)
		so.Run(
			func() {
				defer wg.Done()
				mu.Lock()
				defer mu.Unlock()
				sl = append(sl, i)
			},
		)
	}
	wg.Wait()

	inOrder := true
	last := 0
	for _, i := range sl {
		if i < last {
			inOrder = false
			break
		}
		last = i
	}
	if inOrder {
		t.Fatalf("TestRun: output order is not random as expected")
	}

	stats := so.Stats()
	if stats.NumRunning != 0 {
		t.Errorf("TestRun: NumRunning: got %d, want 0", stats.NumRunning)
	}
	if so.latencyCount != 100 {
		t.Errorf("TestRun: .latencyCount: got %d, want 100", so.latencyCount)
	}
	if so.latencyStore == 0 {
		t.Errorf("TestRun: .latencyStore was never updated")
	}
}

func TestRecordLatency(t *testing.T) {
	now := time.Now()
	sincer = func(t time.Time) time.Duration {
		return t.Sub(now)
	}
	defer func() { sincer = time.Since }()

	ti := now.Add(1 * time.Second)

	so := &TransferManager{}
	so.recordLatency(ti)

	if so.latencyCount != 1 {
		t.Errorf("TestRecordLatency: .latencyCount: got %d, want 1", so.latencyCount)
	}
	if so.latencyStore != int64(1*time.Second) {
		t.Errorf("TestRecordLatency: .latencyStore: got %d, want 1 second", time.Duration(so.latencyStore))
	}
}

func TestLatencyCollect(t *testing.T) {
	now := time.Now()
	nower = func() time.Time {
		return now
	}
	defer func() { nower = time.Now }()

	so := TransferManager{
		latencyStore: int64(10 * time.Duration(10*time.Millisecond)),
		latencyCount: 10,
	}
	so.latencyCollect()
	got := so.latency.Load().(RunLatency)
	want := RunLatency{
		Avg:         10 * time.Millisecond,
		Collections: 10,
		Time:        now,
	}
	if diff := pretty.Compare(want, got); diff != "" {
		t.Errorf("TestLatencyCollect: -want/+got:\n%s", diff)
	}
	if so.latencyStore != 0 {
		t.Errorf("TestLatencyCollect: .latencyStore was not reset")
	}
	if so.latencyCount != 0 {
		t.Errorf("TestLatencyCollect: .latencyCount was not reset")
	}
}

func TestReset(t *testing.T) {
	tests := []struct {
		desc    string
		args    Args
		newArgs Args
		err     bool
	}{
		{
			desc: "Bad Blocksize",
			args: Args{
				BlockSize:    _1MiB,
				StaticBlocks: 1,
				DynamicLimit: 1,
				Concurrency:  1,
			},
			newArgs: Args{
				BlockSize:    _1MiB - 1,
				StaticBlocks: 1,
				DynamicLimit: 1,
				Concurrency:  1,
			},
			err: true,
		},
		{
			desc: "Bad StaticBlocks: it was off, turned it on",
			args: Args{
				BlockSize:    _1MiB,
				StaticBlocks: 0,
				DynamicLimit: 1,
				Concurrency:  1,
			},
			newArgs: Args{
				BlockSize:    _1MiB,
				StaticBlocks: 1,
				DynamicLimit: 1,
				Concurrency:  1,
			},
			err: true,
		},
		{
			desc: "Bad StaticBlocks: it was on, turned it off",
			args: Args{
				BlockSize:    _1MiB,
				StaticBlocks: 1,
				DynamicLimit: 1,
				Concurrency:  1,
			},
			newArgs: Args{
				BlockSize:    _1MiB,
				StaticBlocks: 0,
				DynamicLimit: 1,
				Concurrency:  1,
			},
			err: true,
		},
		{
			desc: "Bad DynamicLimt: it was off, turned it on",
			args: Args{
				BlockSize:    _1MiB,
				StaticBlocks: 1,
				DynamicLimit: 0,
				Concurrency:  1,
			},
			newArgs: Args{
				BlockSize:    _1MiB,
				StaticBlocks: 1,
				DynamicLimit: 1,
				Concurrency:  1,
			},
			err: true,
		},
		{
			desc: "Bad DynamicLimt: it was on, turned it off",
			args: Args{
				BlockSize:    _1MiB,
				StaticBlocks: 1,
				DynamicLimit: 1,
				Concurrency:  1,
			},
			newArgs: Args{
				BlockSize:    _1MiB,
				StaticBlocks: 1,
				DynamicLimit: 0,
				Concurrency:  1,
			},
			err: true,
		},
		{
			desc: "Bad Concurrency: set it to 0",
			args: Args{
				BlockSize:    _1MiB,
				StaticBlocks: 1,
				DynamicLimit: 1,
				Concurrency:  1,
			},
			newArgs: Args{
				BlockSize:    _1MiB,
				StaticBlocks: 1,
				DynamicLimit: 1,
				Concurrency:  0,
			},
			err: true,
		},
		{
			desc: "Success, increased all values",
			args: Args{
				BlockSize:    _1MiB,
				StaticBlocks: 1,
				DynamicLimit: 1,
				Concurrency:  1,
			},
			newArgs: Args{
				BlockSize:    2 * _1MiB,
				StaticBlocks: 2,
				DynamicLimit: 2,
				Concurrency:  2,
			},
		},
		{
			desc: "Success, decreased all values",
			args: Args{
				BlockSize:    2 * _1MiB,
				StaticBlocks: 2,
				DynamicLimit: 2,
				Concurrency:  2,
			},
			newArgs: Args{
				BlockSize:    _1MiB,
				StaticBlocks: 1,
				DynamicLimit: 1,
				Concurrency:  1,
			},
		},
		{
			desc: "Success, set DynamicLimit to unlimited",
			args: Args{
				BlockSize:    _1MiB,
				StaticBlocks: 1,
				DynamicLimit: 1,
				Concurrency:  1,
			},
			newArgs: Args{
				BlockSize:    _1MiB,
				StaticBlocks: 1,
				DynamicLimit: -1,
				Concurrency:  1,
			},
		},
		{
			desc: "Success, set DynamicLimit from unlimited to limit",
			args: Args{
				BlockSize:    _1MiB,
				StaticBlocks: 1,
				DynamicLimit: -1,
				Concurrency:  1,
			},
			newArgs: Args{
				BlockSize:    _1MiB,
				StaticBlocks: 1,
				DynamicLimit: 1,
				Concurrency:  1,
			},
		},
	}

	for _, test := range tests {
		func() {
			so, err := New(test.args)
			if err != nil {
				panic(err)
			}
			defer so.Close()

			err = so.Reset(test.newArgs)
			switch {
			case test.err && err == nil:
				t.Errorf("TestReset(%s): got err == nil, want err != nil", test.desc)
				return
			case !test.err && err != nil:
				t.Errorf("TestReset(%s): got err == %s, want err == nil", test.desc, err)
				return
			case err != nil:
				return
			}

			if so.args.BlockSize != test.newArgs.BlockSize {
				t.Errorf("TestReset(%s): got args.BlockSize == %d, want args.BlockSize == %d", test.desc, so.args.BlockSize, test.newArgs.BlockSize)
			}
			b := so.pool.Get().([]byte)
			if len(b) != test.newArgs.BlockSize {
				t.Errorf("TestReset(%s): pool is ommitting blocks of size %d, want %d", test.desc, len(b), test.newArgs.BlockSize)
			}
			if len(so.staticCh) != test.newArgs.StaticBlocks {
				t.Errorf("TestReset(%s): .staticCh has size %d, want %d", test.desc, len(so.staticCh), test.newArgs.StaticBlocks)
			}
			if test.newArgs.DynamicLimit < 0 {
				if so.limiter != nil {
					t.Errorf("TestReset(%s): .DynamicLimiter < 0, but so.limiter != nil", test.desc)
				}
			}
			if test.newArgs.DynamicLimit > 0 {
				if cap(so.limiter) != test.newArgs.DynamicLimit {
					t.Errorf("TestReset(%s): .DynamicLimiter is %d, but len(so.limiter) is %d", test.desc, test.newArgs.DynamicLimit, len(so.limiter))
				}
			}
			if len(so.runnerCancels) != test.newArgs.Concurrency {
				t.Errorf("TestReset(%s): .Concurrency is %d, but len(so.runnerCancels) is %d", test.desc, test.newArgs.Concurrency, len(so.runnerCancels))
			}
		}()
	}
}

func PrintMemUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	// For info on each, see: https://golang.org/pkg/runtime/#MemStats
	fmt.Printf("Alloc = %v MiB", bToMb(m.Alloc))
	fmt.Printf("\tTotalAlloc = %v MiB", bToMb(m.TotalAlloc))
	fmt.Printf("\tSys = %v MiB", bToMb(m.Sys))
	fmt.Printf("\tNumGC = %v\n", m.NumGC)
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}
