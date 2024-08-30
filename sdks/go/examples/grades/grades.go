// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

// beam-playground:
//   name: Grades
//   description: An example that combines grades data.
//   multifile: false
//   context_line: 81
//   categories:
//     - Debugging
//     - Combiners
//     - Filtering
//   complexity: MEDIUM
//   tags:
//     - combine
//     - map
//     - strings
//     - numbers

import (
	"context"
	"flag"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/stats"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/top"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/debug"
)

func init() {
	register.Function2x0(printTopFn)
	register.Function2x1(less)
	register.Function2x1(alphabetically)
	register.Function1x2(keyGrade)
	register.Function1x1(getGPA)
}

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

func keyGrade(g Grade) (string, Grade) {
	return g.Name[:1], g
}

func getGPA(g Grade) float32 { return g.GPA }

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
	s := p.Root()
	students := beam.CreateList(s, data)

	// (1) Print top 3 students overall by GPA

	best := top.Largest(s, students, 3, less)
	beam.ParDo0(s, printTopFn, best)

	// (2) Print top student per initial (then ordered by name)

	keyed := beam.ParDo(s, keyGrade, students)
	keyedBest := top.LargestPerKey(s, keyed, 1, less)
	unkeyed := beam.Explode(s, beam.DropKey(s, keyedBest))

	altBest := top.Smallest(s, unkeyed, 30, alphabetically)
	beam.ParDo0(s, printTopFn, altBest)

	// (3) Print Chad

	chad := top.Smallest(s, students, 1, less)
	beam.ParDo0(s, printTopFn, chad)

	// (4) Print min/max/sum/mean grades

	grades := beam.ParDo(s, getGPA, students)
	debug.Printf(s, "Min: %v", stats.Min(s, grades))
	debug.Printf(s, "Max: %v", stats.Max(s, grades))
	debug.Printf(s, "Sum: %v", stats.Sum(s, grades))
	debug.Printf(s, "Mean: %v", stats.Mean(s, grades))

	if err := beamx.Run(ctx, p); err != nil {
		log.Exitf(ctx, "Failed to execute job: %v", err)
	}
}
