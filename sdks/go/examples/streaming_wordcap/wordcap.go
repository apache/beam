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

// streaming_wordcap is a toy streaming pipeline that uses PubSub. It
// does the following:
//
//	(1) create a topic and publish a few messages to it
//	(2) start a streaming pipeline that converts the messages to
//	    upper case and logs the result.
//
// NOTE: it only runs on Dataflow and must be manually cancelled.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/mtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/sdf"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/state"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/timers"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/rtrackers/offsetrange"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/debug"
	"golang.org/x/exp/slog"
)

var (
	input = flag.String("input", os.ExpandEnv("$USER-wordcap"), "Pubsub input topic.")
)

var (
	data = []string{
		"foo",
		"bar",
		"baz",
	}
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

func (s *Stateful) ProcessElement(ctx context.Context, ts beam.EventTime, sp state.Provider, tp timers.Provider, key, word string, emit func(string, string)) error {
	log.Infof(ctx, "stateful dofn invoked key: %v word: %v", key, word)

	s.ElementBag.Add(sp, word)
	s.MinTime.Add(sp, int64(ts))

	toFire, ok, err := s.TimerTime.Read(sp)
	if err != nil {
		return err
	}
	if !ok {
		toFire = int64(mtime.Now().Add(1 * time.Minute))
	}
	minTime, _, err := s.MinTime.Read(sp)
	if err != nil {
		return err
	}

	s.OutputState.SetWithOpts(tp, mtime.Time(toFire).ToTime(), timers.Opts{Hold: mtime.Time(minTime).ToTime()})
	s.TimerTime.Write(sp, toFire)
	log.Infof(ctx, "stateful dofn key: %v word: %v, timer: %v, minTime: %v", key, word, toFire, minTime)

	// // Get the Value stored in our state
	// val, ok, err := s.Val.Read(p)
	// if err != nil {
	// 	return err
	// }
	// log.Infof(ctx, "stateful dofn state read key: %v word: %v val: %v", key, word, val)
	// if !ok {
	// 	s.Val.Write(p, 1)
	// } else {
	// 	s.Val.Write(p, val+1)
	// }

	// if val > 5 {
	// 	log.Infof(ctx, "stateful dofn clearing key: %v word: %v val: %v", key, word, val)
	// 	// Example of clearing and starting again with an empty bag
	// 	s.Val.Clear(p)
	// }
	// fire := time.Now().Add(10 * time.Second)

	// log.Infof(ctx, "stateful dofn timer family: %v fire: %v now: %v key: %v word: %v", s.Fire.Family, fire, time.Now(), key, word)
	// s.Fire.Set(tp, fire)

	// emit(key, word)

	return nil
}

type eventtimeSDFStream struct {
	RestSize, Mod, Fixed int64
	Sleep                time.Duration
}

func (fn *eventtimeSDFStream) Setup() error {
	return nil
}

func (fn *eventtimeSDFStream) CreateInitialRestriction(v beam.T) offsetrange.Restriction {
	return offsetrange.Restriction{Start: 0, End: fn.RestSize}
}

func (fn *eventtimeSDFStream) SplitRestriction(v beam.T, r offsetrange.Restriction) []offsetrange.Restriction {
	// No split
	return []offsetrange.Restriction{r}
}

func (fn *eventtimeSDFStream) RestrictionSize(v beam.T, r offsetrange.Restriction) float64 {
	return r.Size()
}

func (fn *eventtimeSDFStream) CreateTracker(r offsetrange.Restriction) *sdf.LockRTracker {
	return sdf.NewLockRTracker(offsetrange.NewTracker(r))
}

func (fn *eventtimeSDFStream) ProcessElement(ctx context.Context, _ *CWE, rt *sdf.LockRTracker, v beam.T, emit func(beam.EventTime, int64)) sdf.ProcessContinuation {
	r := rt.GetRestriction().(offsetrange.Restriction)
	i := r.Start
	if r.Size() < 1 {
		log.Debugf(ctx, "size 0 restriction, stoping to process sentinel", slog.Any("value", v))
		return sdf.StopProcessing()
	}
	slog.Debug("emitting element to restriction", slog.Any("value", v), slog.Group("restriction",
		slog.Any("value", v),
		slog.Float64("size", r.Size()),
		slog.Int64("pos", i),
	))
	if rt.TryClaim(i) {
		v := (i % fn.Mod) + fn.Fixed
		emit(mtime.Now(), v)
	}
	return sdf.ResumeProcessingIn(fn.Sleep)
}

func (fn *eventtimeSDFStream) InitialWatermarkEstimatorState(_ beam.EventTime, _ offsetrange.Restriction, _ beam.T) int64 {
	return int64(mtime.MinTimestamp)
}

func (fn *eventtimeSDFStream) CreateWatermarkEstimator(initialState int64) *CWE {
	return &CWE{Watermark: initialState}
}

func (fn *eventtimeSDFStream) WatermarkEstimatorState(e *CWE) int64 {
	return e.Watermark
}

type CWE struct {
	Watermark int64 // uses int64, since the SDK prevent mtime.Time from serialization.
}

func (e *CWE) CurrentWatermark() time.Time {
	return mtime.Time(e.Watermark).ToTime()
}

func (e *CWE) ObserveTimestamp(ts time.Time) {
	// We add 10 milliseconds to allow window boundaries to
	// progress after emitting
	e.Watermark = int64(mtime.FromTime(ts.Add(-90 * time.Millisecond)))
}

func init() {
	register.DoFn7x1[context.Context, beam.EventTime, state.Provider, timers.Provider, string, string, func(string, string), error](&Stateful{})
	register.Emitter2[string, string]()
	register.DoFn5x1[context.Context, *CWE, *sdf.LockRTracker, beam.T, func(beam.EventTime, int64), sdf.ProcessContinuation]((*eventtimeSDFStream)(nil))
	register.Emitter2[beam.EventTime, int64]()
}

func main() {
	flag.Parse()
	beam.Init()

	ctx := context.Background()
	//project := gcpopts.GetProject(ctx)

	log.Infof(ctx, "Publishing %v messages to: %v", len(data), *input)

	// defer pubsubx.CleanupTopic(ctx, project, *input)
	// sub, err := pubsubx.Publish(ctx, project, *input, data...)
	// if err != nil {
	// 	log.Fatal(ctx, err)
	// }

	//log.Infof(ctx, "Running streaming wordcap with subscription: %v", sub.ID())

	p := beam.NewPipeline()
	s := p.Root()

	//col := pubsubio.Read(s, project, *input, &pubsubio.ReadOptions{Subscription: sub.ID()})
	// col = beam.WindowInto(s, window.NewFixedWindows(60*time.Second), col)

	// str := beam.ParDo(s, func(b []byte) string {
	// 	return (string)(b)
	// }, col)

	imp := beam.Impulse(s)
	elms := 100
	out := beam.ParDo(s, &eventtimeSDFStream{
		Sleep:    time.Second,
		RestSize: int64(elms),
		Mod:      int64(elms),
		Fixed:    1,
	}, imp)
	// out = beam.WindowInto(s, window.NewFixedWindows(10*time.Second), out)
	str := beam.ParDo(s, func(b int64) string {
		return fmt.Sprintf("element%03d", b)
	}, out)

	keyed := beam.ParDo(s, func(ctx context.Context, ts beam.EventTime, s string) (string, string) {
		log.Infof(ctx, "adding key ts: %v now: %v  word: %v", ts.ToTime(), time.Now(), s)
		return "test", s
	}, str)
	debug.Printf(s, "pre stateful: %v", keyed)

	timed := beam.ParDo(s, NewStateful(), keyed)
	debug.Printf(s, "post stateful: %v", timed)

	if err := beamx.Run(context.Background(), p); err != nil {
		log.Exitf(ctx, "Failed to execute job: %v", err)
	}
}
