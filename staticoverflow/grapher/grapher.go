// Package grapher holds a azblob.TransferManager that wraps the staticoverflow.TransferManager in order to render
// graphs that indicate what changes in settings does to a program's memory, allocations and transfer rate to
// allow a user to hone in optimal settings for their unique constraints.
package grapher

import (
	"bytes"
	"context"
	"fmt"
	"html/template"
	"sync"
	"sync/atomic"
	"time"

	"github.com/element-of-surprise/transfermanager/staticoverflow"

	"github.com/go-echarts/go-echarts/v2/charts"
	"github.com/go-echarts/go-echarts/v2/opts"
	"github.com/go-echarts/go-echarts/v2/types"
)

// _1MiB represents 1 MiB size as an integer.
const _1MiB = 8 << 17

// Record holds records related to use of our Grapher.
type Record struct {
	// Collection is a collection of Graphs, each representing
	// a run using our Grapher. A run is completed once Reset() or Close()
	// is called.
	Collection Collection
}

// Collection is a set of Graphs that were collected from various runs.
type Collection []Graphs

// Graphs is a collection of graphs that were collected for transfers done
// with a set of Args.
type Graphs struct {
	// Args are the args used when the graph was rendered.
	Args staticoverflow.Args
	// Graphs are the graphs that were generated.
	Graphs []Graph
}

// GraphType is the type of graph.
type GraphType string

const (
	UnknownGraphType GraphType = ""
	SysMemory        GraphType = "System Memory"
	Allocations      GraphType = "Memory Allocations"
	StaticUse        GraphType = "Static Block Use"
	DynamicUse       GraphType = "Dynamic Block Use"
	GoroutineUse     GraphType = "Goroutine Use"
	RunLatency       GraphType = "Run Latency"
)

// Graph represents a data graph.
type Graph struct {
	// Type is the type of graph.
	Type GraphType
	// Data is the HTML for the graph.
	Data template.HTML
}

// Grapher implements azblob.TransferManager by wrapping staticoverflow.TransferManager
// and writing collection graphs whenever .Reset() or .Close() is called. This is not
// meant to be used as a general TransferManager, only to get an idea of the effect of
// settings on your system and transfer rates. To be useful on checking settings the
// Grapher needs to be used for more than 10 seconds of sustained transfers before Reset() or Close()
// is called.
type Grapher struct {
	*staticoverflow.TransferManager

	mu     sync.Mutex
	args   staticoverflow.Args
	ctx    context.Context
	cancel context.CancelFunc

	statsCh chan staticoverflow.Stats
	stats   []staticoverflow.Stats

	total int64 // only use atomic to access
	once  sync.Once
	start time.Time

	collection Collection
}

// New creates a new Grapher.
func New(args staticoverflow.Args) (*Grapher, error) {
	so, err := staticoverflow.New(args)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	g := &Grapher{
		TransferManager: so,
		args:            args,
		ctx:             ctx,
		cancel:          cancel,
		statsCh:         so.SubStats(),
		collection:      make(Collection, 1),
	}
	go g.captureStats()
	return g, nil
}

// Put implememnts staticcoverflow.TransferManager.Put().
func (g *Grapher) Put(b []byte) {
	atomic.AddInt64(&g.total, int64(len(b)))

	g.TransferManager.Put(b)
}

// Get implememnts staticcoverflow.TransferManager.Get().
func (g *Grapher) Get() []byte {
	g.once.Do(func() {
		g.start = time.Now()
	})

	return g.Get()
}

// Reset waits for all Get() calls to have a Put() and
// Run() calls to complete, renders a graph, then resets the stat collection and TransferManager.
// If the call to Reset() fails, Grapher is useless.
func (g *Grapher) Reset(args staticoverflow.Args) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.once = sync.Once{}
	g.cancel()
	g.ctx, g.cancel = context.WithCancel(context.Background())

	g.TransferManager.Wait()

	g.render()
	err := g.TransferManager.Reset(args, staticoverflow.StatsReset())
	if err != nil {
		return err
	}
	g.args = args

	go g.captureStats()

	return nil
}

// Close implements staticcoverflow.TransferManager.Close().
func (g *Grapher) Close() {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.cancel()

	g.render()
	g.TransferManager.Close()
}

// Graph returns all the graph data. This can only be called after Close() has been called.
func (g *Grapher) Graphs() (Record, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.collection == nil {
		return Record{}, fmt.Errorf("no collection has taken place")
	}

	return Record{Collection: g.collection}, nil
}

func (g *Grapher) captureStats() {
	ctx := g.ctx

	for {
		select {
		case <-ctx.Done():
			return
		case stats := <-g.statsCh:
			g.stats = append(g.stats, stats)
		}
	}
}

func (g *Grapher) render() {
	calls := []func() Graph{
		g.graphAllocs,
		g.graphSysMemory,
		g.graphStaticUse,
		g.graphDynamicUse,
		g.graphGoroutineUse,
		g.graphRunLatency,
	}

	graph := Graphs{Args: g.args, Graphs: make([]Graph, len(calls))}

	wg := sync.WaitGroup{}

	wg.Add(len(calls))
	for i, c := range calls {
		i := i
		c := c
		go func() {
			defer wg.Done()
			graph.Graphs[i] = c()
		}()
	}
	wg.Wait()

	g.collection = append(g.collection, graph)
}

func (g *Grapher) makeGraph(title, subTitle string, gType GraphType, xAxis []string, items []opts.LineData) Graph {
	// create a new line instance
	line := charts.NewLine()
	// set some global options like Title/Legend/ToolTip or anything else
	line.SetGlobalOptions(
		charts.WithInitializationOpts(
			opts.Initialization{
				Theme: types.ThemeWesteros,
			},
		),
		charts.WithTitleOpts(
			opts.Title{
				Title:    title,
				Subtitle: subTitle,
			},
		),
	)
	line.SetXAxis(xAxis)
	line.AddSeries("Utilization", items)
	line.SetSeriesOptions(
		charts.WithLineChartOpts(
			opts.LineChart{Smooth: true},
		),
	)
	b := bytes.Buffer{}
	r := &renderChart{line, []func(){line.Validate}}
	r.Render(&b)

	return Graph{Type: gType, Data: template.HTML(b.Bytes())}
}

func (g *Grapher) graphAllocs() Graph {
	title := "Current Heap Allocations"
	subTitle := "The number of oustanding Heap allocations"
	xAxis := make([]string, 0, len(g.stats))
	items := make([]opts.LineData, 0, len(g.stats))

	for _, s := range g.stats {
		xAxis = append(xAxis, s.Collection.String())
		items = append(items, opts.LineData{Value: s.RuntimeStats.CurrentAllocs})
	}

	return g.makeGraph(title, subTitle, Allocations, xAxis, items)
}

func (g *Grapher) graphSysMemory() Graph {
	title := "Current Heap Allocations in MiB"
	subTitle := "The number of oustanding Heap allocations"
	xAxis := make([]string, 0, len(g.stats))
	items := make([]opts.LineData, 0, len(g.stats))

	for _, s := range g.stats {
		xAxis = append(xAxis, s.Collection.String())
		items = append(items, opts.LineData{Value: s.RuntimeStats.SysMemory / _1MiB})
	}

	return g.makeGraph(title, subTitle, SysMemory, xAxis, items)
}

func (g *Grapher) graphStaticUse() Graph {
	title := fmt.Sprintf("Static buffer utilization(%d buffers allocated)", g.args.StaticBlocks)
	subTitle := "The usage of the allocated static buffers at 10 second intervals"
	xAxis := make([]string, 0, len(g.stats))
	items := make([]opts.LineData, 0, len(g.stats))
	for _, s := range g.stats {
		xAxis = append(xAxis, s.Collection.String())
		items = append(items, opts.LineData{Value: s.UsedStaticBuffer})
	}

	return g.makeGraph(title, subTitle, StaticUse, xAxis, items)
}

func (g *Grapher) graphDynamicUse() Graph {
	title := fmt.Sprintf("Dynamic buffer utilization with %d Limit", g.args.DynamicLimit)
	subTitle := "The number of dynamically allocated buffers at 10 second intervals"
	xAxis := make([]string, 0, len(g.stats))
	items := make([]opts.LineData, 0, len(g.stats))
	for _, s := range g.stats {
		xAxis = append(xAxis, s.Collection.String())
		items = append(items, opts.LineData{Value: s.UsedDynamicBuffer})
	}

	return g.makeGraph(title, subTitle, DynamicUse, xAxis, items)
}

func (g *Grapher) graphGoroutineUse() Graph {
	title := fmt.Sprintf("Goroutine utilization(out of %d possible)", g.args.Concurrency)
	subTitle := "The number of goroutines in use for transfers at 10 second intervals"
	xAxis := make([]string, 0, len(g.stats))
	items := make([]opts.LineData, 0, len(g.stats))
	for _, s := range g.stats {
		xAxis = append(xAxis, s.Collection.String())
		items = append(items, opts.LineData{Value: s.NumRunning})
	}

	return g.makeGraph(title, subTitle, GoroutineUse, xAxis, items)
}

func (g *Grapher) graphRunLatency() Graph {
	title := "Run() Latency in seconds"
	subTitle := "The avg latency of a Run() call averaged over 10 seconds"
	xAxis := make([]string, 0, len(g.stats))
	items := make([]opts.LineData, 0, len(g.stats))
	for _, s := range g.stats {
		xAxis = append(xAxis, s.Collection.String())
		items = append(items, opts.LineData{Value: float64(s.RunLatency) / float64((1 * time.Second))})
	}

	return g.makeGraph(title, subTitle, RunLatency, xAxis, items)
}
