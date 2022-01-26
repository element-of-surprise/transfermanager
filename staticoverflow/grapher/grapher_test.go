package grapher

import (
	"os"
	"testing"
	"time"

	"github.com/element-of-surprise/transfermanager/staticoverflow"
)

var collection = time.Unix(1643209507, 0)

// TestCreateGolden simply creates a golden output file. Unfortunately, e-charts creates random identifiers
// in the HTML, so its hard to do comparisons without doing a lot of string replacement. I'm lazy at the
// moment, so you just ned to go and verify the graphs look right given the data.
func TestCreateGolden(t *testing.T) {
	g := &Grapher{
		args: staticoverflow.Args{
			StaticBlocks: 8,
			DynamicLimit: 50,
			Concurrency:  100,
		},
		stats: []staticoverflow.Stats{
			{
				Collection:        collection.Add(-10 * time.Second),
				UsedStaticBuffer:  3,
				UsedDynamicBuffer: 0,
				NumRunning:        22,
				RuntimeStats: staticoverflow.RuntimeStats{
					CurrentAllocs: 5000,
					SysMemory:     104857600,
				},
				RunLatency: 102 * time.Millisecond,
			},
			{
				Collection:        collection,
				UsedStaticBuffer:  7,
				UsedDynamicBuffer: 30,
				NumRunning:        67,
				RuntimeStats: staticoverflow.RuntimeStats{
					CurrentAllocs: 10000,
					SysMemory:     209715200,
				},
				RunLatency: 110 * time.Millisecond,
			},
		},
	}

	g.render()

	rec, err := g.Graphs()
	if err != nil {
		panic(err)
	}

	got, err := RenderPage(rec)
	if err != nil {
		panic(err)
	}

	f, _ := os.Create("golden/page.html")
	if _, err := f.Write(got); err != nil {
		panic(err)
	}
}
