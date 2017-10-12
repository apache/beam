package main

import (
	"context"
	"flag"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/log"
	"github.com/apache/beam/sdks/go/pkg/beam/transforms/stats"
	"github.com/apache/beam/sdks/go/pkg/beam/transforms/top"
	"github.com/apache/beam/sdks/go/pkg/beam/x/beamx"
	"github.com/apache/beam/sdks/go/pkg/beam/x/debug"
)

type Grade struct {
	Name string
	GPA  float32
}

func less(a, b Grade) bool {
	return a.GPA < b.GPA
}

func alphabetically(a, b Grade) bool {
	return a.Name < b.Name
}

func printTopFn(ctx context.Context, list []Grade) {
	log.Infof(ctx, "TOP %v student(s):", len(list))
	for i, student := range list {
		log.Infof(ctx, " %v:\t%v\t(GPA: %v)", i+1, student.Name, student.GPA)
	}
}

func main() {
	flag.Parse()
	beam.Init()

	ctx := context.Background()

	data := []Grade{
		{"Adam", 2.3},
		{"Alice", 3.8},
		{"Alex", 2.5},
		{"Bart", 3.2},
		{"Bob", 3.9},
		{"Brittney", 3.1},
		{"Brenda", 3.5},
		{"Chad", 1.1},
	}

	log.Info(ctx, "Running grades")

	p := beam.NewPipeline()
	students := beam.CreateList(p, data)

	// (1) Print top 3 students overall by GPA

	best := top.Largest(p, students, 3, less)
	beam.ParDo0(p, printTopFn, best)

	// (2) Print top student per initial (then ordered by name)

	keyed := beam.ParDo(p, func(g Grade) (string, Grade) {
		return g.Name[:1], g
	}, students)
	keyedBest := top.Largest(p, keyed, 1, less)
	unkeyed := beam.Flatten(p, beam.DropKey(p, keyedBest))

	altBest := top.Smallest(p, unkeyed, 30, alphabetically)
	beam.ParDo0(p, printTopFn, altBest)

	// (3) Print Chad

	chad := top.Smallest(p, students, 1, less)
	beam.ParDo0(p, printTopFn, chad)

	// (4) Print min/max/sum/mean grades

	grades := beam.ParDo(p, func(g Grade) float32 { return g.GPA }, students)
	debug.Printf(p, "Min: %v", stats.Min(p, grades))
	debug.Printf(p, "Max: %v", stats.Max(p, grades))
	debug.Printf(p, "Sum: %v", stats.Sum(p, grades))
	debug.Printf(p, "Mean: %v", stats.Mean(p, grades))

	if err := beamx.Run(ctx, p); err != nil {
		log.Exitf(ctx, "Failed to execute job: %v", err)
	}
}
