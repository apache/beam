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

// timer_wordcap is a toy streaming pipeline that demonstrates the use of State and Timers.
// Periodic Impulse is used as a streaming source that produces sequence of elements upto 5 minutes
// from the start of the pipeline every 5 seconds. These elements are keyed and fed to the Stateful DoFn
// where state and timers are set and cleared. Since this pipeline uses a Periodic Impulse,
// the pipeline is terminated automatically after it is done producing elements for 5 minutes.
package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/state"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/timers"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/periodic"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/debug"
)

type Stateful struct {
	ElementBag state.Bag[string]
	TimerTime  state.Value[int64]
	MinTime    state.Combining[int64, int64, int64]

	OutputState timers.ProcessingTime
}

func NewStateful() *Stateful {
	return &Stateful{
		ElementBag: state.MakeBagState[string]("elementBag"),
		TimerTime:  state.MakeValueState[int64]("timerTime"),
		MinTime: state.MakeCombiningState[int64, int64, int64]("minTiInBag", func(a, b int64) int64 {
			if a < b {
				return a
			}
			return b
		}),

		OutputState: timers.InProcessingTime("outputState"),
	}
}

func (s *Stateful) ProcessElement(ctx context.Context, ts beam.EventTime, sp state.Provider, tp timers.Provider, key, word string, _ func(beam.EventTime, string, string)) error {
	s.ElementBag.Add(sp, word)
	s.MinTime.Add(sp, int64(ts))

	toFire, ok, err := s.TimerTime.Read(sp)
	if err != nil {
		return err
	}
	if !ok {
		toFire = int64(time.Now().Add(30 * time.Second).UnixMilli())
	}
	minTime, _, err := s.MinTime.Read(sp)
	if err != nil {
		return err
	}

	s.OutputState.Set(tp, time.UnixMilli(toFire), timers.WithOutputTimestamp(time.UnixMilli(minTime)))
	// A timer can be set with independent to fire with independant string tags.
	s.OutputState.Set(tp, time.UnixMilli(toFire), timers.WithTag(word), timers.WithOutputTimestamp(time.UnixMilli(minTime)))
	s.TimerTime.Write(sp, toFire)
	return nil
}

func (s *Stateful) OnTimer(ctx context.Context, ts beam.EventTime, sp state.Provider, tp timers.Provider, key string, timer timers.Context, emit func(beam.EventTime, string, string)) {
	log.Infof(ctx, "Timer fired for key %q, for family %q and tag %q", key, timer.Family, timer.Tag)

	const tag = "emit" // Tags can be arbitrary strings, but we're associating behavior with this tag in this method.

	// Check which timer has fired.
	switch timer.Family {
	case s.OutputState.Family:
		switch timer.Tag {
		case "":
			// Timers can be set within the OnTimer method.
			// In this case the emit tag timer to fire in 5 seconds.
			s.OutputState.Set(tp, ts.ToTime().Add(5*time.Second), timers.WithTag(tag))
		case tag:
			// When the emit tag fires, read the batched data.
			es, ok, err := s.ElementBag.Read(sp)
			if err != nil {
				log.Errorf(ctx, "error reading ElementBag: %v", err)
				return
			}
			if !ok {
				log.Infof(ctx, "No elements in bag.")
				return
			}
			minTime, _, err := s.MinTime.Read(sp)
			if err != nil {
				log.Errorf(ctx, "error reading ElementBag: %v", err)
				return
			}
			log.Infof(ctx, "Emitting %d elements", len(es))
			for _, word := range es {
				emit(beam.EventTime(minTime), key, word)
			}
			// Clean up the state that has been evicted.
			s.ElementBag.Clear(sp)
			s.MinTime.Clear(sp)
		}
	}
}

func init() {
	register.DoFn7x1[context.Context, beam.EventTime, state.Provider, timers.Provider, string, string, func(beam.EventTime, string, string), error](&Stateful{})
	register.Emitter3[beam.EventTime, string, string]()
	register.Emitter2[beam.EventTime, int64]()
	register.Function1x2(toKeyedString)
}

func generateSequence(s beam.Scope, now time.Time, duration, interval time.Duration) beam.PCollection {
	s = s.Scope("generateSequence")
	def := beam.Create(s, periodic.NewSequenceDefinition(now, now.Add(duration), interval))
	seq := periodic.Sequence(s, def)
	return seq
}

func toKeyedString(b int64) (string, string) {
	return "test", fmt.Sprintf("%03d", b)
}

func main() {
	flag.Parse()
	beam.Init()

	ctx := context.Background()

	p, s := beam.NewPipelineWithRoot()

	out := generateSequence(s, time.Now(), 1*time.Minute, 5*time.Second)

	keyed := beam.ParDo(s, toKeyedString, out)
	timed := beam.ParDo(s, NewStateful(), keyed)
	debug.Printf(s, "post stateful: %v", timed)

	if err := beamx.Run(context.Background(), p); err != nil {
		log.Exitf(ctx, "Failed to execute job: %v", err)
	}
}
