package main

import (
	"context"
	"flag"
	"log"

	"github.com/apache/beam/sdks/go/pkg/beam"
	_ "github.com/apache/beam/sdks/go/pkg/beam/runners/dataflow"
	_ "github.com/apache/beam/sdks/go/pkg/beam/runners/dot"
	_ "github.com/apache/beam/sdks/go/pkg/beam/runners/local"
	"github.com/apache/beam/sdks/go/pkg/beam/transforms/stats"
	"github.com/apache/beam/sdks/go/pkg/beam/transforms/top"
	"github.com/apache/beam/sdks/go/pkg/beam/x/debug"
)

var (
	runner = flag.String("runner", "local", "Pipeline runner.")
)

type Grade struct {
	Name string
	GPA  float32
}

func less(a, b Grade) bool {
	return a.GPA < b.GPA
}

func greater(a, b Grade) bool {
	return a.GPA > b.GPA
}

func alphabetically(a, b Grade) bool {
	return a.Name > b.Name
}

func printTopFn(list []Grade) {
	log.Printf("TOP %v student(s):", len(list))
	for i, student := range list {
		log.Printf(" %v:\t%v\t(GPA: %v)", i+1, student.Name, student.GPA)
	}
}

func main() {
	flag.Parse()
	beam.Init()

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

	log.Print("Running grades")

	p := beam.NewPipeline()
	students := beam.CreateList(p, data)

	// (1) Print top 3 students overall by GPA

	best := top.Globally(p, students, 3, less)
	beam.ParDo0(p, printTopFn, best)

	// (2) Print top student per initial (then ordered by name)

	keyed := beam.ParDo(p, func(g Grade) (string, Grade) {
		return g.Name[:1], g
	}, students)
	keyedBest := top.PerKey(p, keyed, 1, less)
	unkeyed := beam.FlattenCol(p, beam.DropKey(p, keyedBest))

	altBest := top.Globally(p, unkeyed, 30, alphabetically)
	beam.ParDo0(p, printTopFn, altBest)

	// (3) Print Chad

	chad := top.Globally(p, students, 1, greater)
	beam.ParDo0(p, printTopFn, chad)

	// (4) Print min/max/sum/mean grades

	grades := beam.ParDo(p, func(g Grade) float32 { return g.GPA }, students)
	debug.Printf(p, "Min: %v", stats.Min(p, grades))
	debug.Printf(p, "Max: %v", stats.Max(p, grades))
	debug.Printf(p, "Sum: %v", stats.Sum(p, grades))
	debug.Printf(p, "Mean: %v", stats.Mean(p, grades))

	if err := beam.Run(context.Background(), *runner, p); err != nil {
		log.Fatalf("Failed to execute job: %v", err)
	}
}
