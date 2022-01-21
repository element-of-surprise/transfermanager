package staticoverflow

import (
	"testing"
)

func BenchmarkStatic(b *testing.B) {
	b.ReportAllocs()

	tm, err := New(Args{BlockSize: _1MiB, StaticBlocks: 8, DynamicLimit: 0, Concurrency: 8})
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

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for i := 0; i < 10000; i++ {
			tm.Run(f)
		}
	}
}

func BenchmarkStaticOverflow(b *testing.B) {
	b.ReportAllocs()

	tm, err := New(Args{BlockSize: _1MiB, StaticBlocks: 8, DynamicLimit: 8, Concurrency: 16})
	if err != nil {
		panic(err)
	}

	f := func() {
		b := tm.Get()
		for i, _ := range b {
			b[i] = byte(255)
		}
		tm.Put(b)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for i := 0; i < 10000; i++ {
			tm.Run(f)
		}
	}
}
