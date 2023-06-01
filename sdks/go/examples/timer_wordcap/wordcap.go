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
	"bytes"
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/mtime"
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

func (s *Stateful) OnTimer(ctx context.Context, ts beam.EventTime, tp timers.Provider, key, timerKey, timerTag string, emit func(string, string)) {
	switch timerKey {
	case "outputState":
		log.Infof(ctx, "Timer outputState fired on stateful for element: %v.", key)
		s.OutputState.Set(tp, ts.ToTime().Add(5*time.Second), timers.WithTag("1"))
		switch timerTag {
		case "1":
			s.OutputState.Clear(tp)
			log.Infof(ctx, "Timer with tag 1 fired on outputState stateful DoFn.")
			emit(timerKey, timerTag)
		}
	}
}

func (s *Stateful) ProcessElement(ctx context.Context, ts beam.EventTime, sp state.Provider, tp timers.Provider, key, word string, emit func(string, string)) error {
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

	s.OutputState.Set(tp, time.UnixMilli(toFire), timers.WithOutputTimestamp(time.UnixMilli(minTime)), timers.WithTag(word))
	s.TimerTime.Write(sp, toFire)

	return nil
}

func init() {
	register.DoFn7x1[context.Context, beam.EventTime, state.Provider, timers.Provider, string, string, func(string, string), error](&Stateful{})
	register.Emitter2[string, string]()
	register.Emitter2[beam.EventTime, int64]()
}

func main() {
	flag.Parse()
	beam.Init()

	ctx := context.Background()

	p := beam.NewPipeline()
	s := p.Root()

	out := periodic.Impulse(s, time.Now(), time.Now().Add(5*time.Minute), 5*time.Second, true)

	intOut := beam.ParDo(s, func(b []byte) int64 {
		var val int64
		buf := bytes.NewReader(b)
		binary.Read(buf, binary.BigEndian, &val)
		return val
	}, out)

	str := beam.ParDo(s, func(b int64) string {
		return fmt.Sprintf("%03d", b)
	}, intOut)

	keyed := beam.ParDo(s, func(ctx context.Context, ts beam.EventTime, s string) (string, string) {
		return "test", s
	}, str)

	timed := beam.ParDo(s, NewStateful(), keyed)
	debug.Printf(s, "post stateful: %v", timed)

	if err := beamx.Run(context.Background(), p); err != nil {
		log.Exitf(ctx, "Failed to execute job: %v", err)
	}
}
