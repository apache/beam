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

// yatzy is an implementation of https://en.wikipedia.org/wiki/Yatzy that shows
// that pipeline construction is normal Go code. It can even be
// non-deterministic and produce different pipelines on each invocation.
package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"time"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/log"
	"github.com/apache/beam/sdks/go/pkg/beam/x/beamx"
)

var (
	real = flag.Int("real_dice", 20, "Actual dice to use (cropped to formal).")
	dice = flag.Int("dice", 6, "Formal dice to use.")
)

func init() {
	beam.RegisterType(reflect.TypeOf((*minFn)(nil)).Elem())
}

// roll is a composite PTransform for a construction-time dice roll. The value
// is encoded in the shape of the pipeline, which will produce a single
// element of that value. The shape is as follows:
//
//     0 -> \x.x+1 -> \x.x+1 -> (N times) -> \x.min(x, 6)
//
// The single output will be a number between 1 and 6.
func roll(ctx context.Context, s beam.Scope) beam.PCollection {
	num := rand.Intn(*real) + 1
	log.Debugf(ctx, "Lucky number %v!", num)

	s = s.Scope(fmt.Sprintf("roll[%v]", num))

	col := beam.Create(s, 0)
	for i := 0; i < num; i++ {
		col = beam.ParDo(s, incFn, col)
	}
	return beam.ParDo(s, minFn{Num: *dice}, col)
}

// minFn is a DoFn that computes outputs the minimum of a fixed value
// and each incoming value.
type minFn struct {
	Num int `json:"num"`
}

func (m minFn) ProcessElement(num int) int {
	if m.Num < num {
		return m.Num
	}
	return num
}

// incFn is a DoFn that increments each value by 1.
func incFn(num int) int {
	return num + 1
}

func eq(n int, other ...int) bool {
	for _, num := range other {
		if num != n {
			return false
		}
	}
	return true
}

// evalFn is a DoFn that takes 5 dice rolls as singleton side inputs and
// evaluates them. It is triggered by an impulse, whose value is ignored.
// It does not output any value, but simply logs the result.
func evalFn(ctx context.Context, _ []byte, a, b, c, d, e int) {
	r := []int{a, b, c, d, e}
	sort.Ints(r)

	log.Infof(ctx, "Roll: %v", r)

	switch {
	case eq(r[0], r[1], r[2], r[3], r[4]):
		log.Info(ctx, "Yatzy!")
	case eq(r[0], r[1], r[2], r[3]) || eq(r[1], r[2], r[3], r[4]):
		log.Info(ctx, "Four of a kind!")
	case eq(r[0], r[1], r[2]) && r[3] == r[4], r[0] == r[1] && eq(r[2], r[3], r[4]):
		log.Info(ctx, "Full house!")
	case r[0] == 1 && r[1] == 2 && r[2] == 3 && r[3] == 4 && r[4] == 5:
		log.Info(ctx, "Small straight!")
	case r[0] == 2 && r[1] == 3 && r[2] == 4 && r[3] == 5 && r[4] == 6:
		log.Info(ctx, "Big straight!")
	default:
		log.Info(ctx, "Sorry, try again.")
	}
}

func main() {
	flag.Parse()
	beam.Init()

	rand.Seed(time.Now().UnixNano())
	ctx := context.Background()

	log.Info(ctx, "Running yatzy")

	// Construct a construction-time-randomized pipeline.
	p := beam.NewPipeline()
	s := p.Root()
	beam.ParDo0(s, evalFn, beam.Impulse(s),
		beam.SideInput{Input: roll(ctx, s)},
		beam.SideInput{Input: roll(ctx, s)},
		beam.SideInput{Input: roll(ctx, s)},
		beam.SideInput{Input: roll(ctx, s)},
		beam.SideInput{Input: roll(ctx, s)},
	)

	if err := beamx.Run(context.Background(), p); err != nil {
		log.Exitf(ctx, "Failed to execute job: %v", err)
	}
}
