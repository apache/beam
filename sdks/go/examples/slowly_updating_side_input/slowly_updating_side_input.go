// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// slowly_updating_side_input is an example pipeline demonstrating the pattern described
// at https://beam.apache.org/documentation/patterns/side-inputs/.
package main

import (
	"context"
	"flag"
	"strings"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window/trigger"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/pubsubio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/options/gcpopts"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/dataflow"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/periodic"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/util/pubsubx"
)

func init() {
	register.Function4x0(update)
	register.Function4x0(process)
	register.Emitter2[int, string]()
	register.Iter1[string]()
}

// update simulates an external call to get data for the side input.
func update(ctx context.Context, t beam.EventTime, _ []byte, emit func(int, string)) {
	log.Infof(ctx, "Making external call at event time %s", t.ToTime().Format(time.RFC3339))

	// zero is the key used in beam.AddFixedKey which will be applied on the main input.
	id, externalData := 0, "some fake data that changed at "+time.Now().Format(time.RFC3339)

	emit(id, externalData)
}

// process simulates processing of main input. It reads side input by key
func process(ctx context.Context, k int, v []byte, side func(int) func(*string) bool) {
	log.Infof(ctx, "Processing (key:%d,value:%q)", k, v)

	iter := side(k)

	var externalData []string
	var externalDatum string
	for iter(&externalDatum) {
		externalData = append(externalData, externalDatum)
	}

	log.Infof(ctx, "Processing (key:%d,value:%q) with external data %q", k, v, strings.Join(externalData, ","))
}

func fatalf(err error, format string, args ...interface{}) {
	if err != nil {
		log.Fatalf(context.TODO(), format, args...)
	}
}

func main() {
	var inputTopic, periodicSequenceStart, periodicSequenceEnd string
	var periodicSequenceInterval time.Duration

	now := time.Now()

	flag.StringVar(&periodicSequenceStart, "periodic_sequence_start", now.Add(-1*time.Hour).Format(time.RFC3339),
		"The time at which to start the periodic sequence.")

	flag.StringVar(&periodicSequenceEnd, "periodic_sequence_end", now.Add(100*time.Hour).Format(time.RFC3339),
		"The time at which to end the periodic sequence.")

	flag.DurationVar(&periodicSequenceInterval, "periodic_sequence_interval", 1*time.Minute,
		"The interval between periodic sequence output.")

	flag.StringVar(&inputTopic, "input_topic", "input",
		"The PubSub topic from which to read the main input data.")

	flag.Parse()
	beam.Init()
	ctx := context.Background()
	p, s := beam.NewPipelineWithRoot()

	project := gcpopts.GetProject(ctx)
	client, err := pubsub.NewClient(ctx, project)
	fatalf(err, "Failed to create client: %v", err)
	_, err = pubsubx.EnsureTopic(ctx, client, inputTopic)
	fatalf(err, "Failed to ensure topic: %v", err)

	source := pubsubio.Read(s, project, inputTopic, nil)
	keyedSource := beam.AddFixedKey(s, source) // simulate keyed data by adding a fixed key
	mainInput := beam.WindowInto(
		s,
		window.NewFixedWindows(periodicSequenceInterval),
		keyedSource,
		beam.Trigger(trigger.Repeat(trigger.Always())),
		beam.PanesDiscard(),
	)

	startTime, _ := time.Parse(time.RFC3339, periodicSequenceStart)
	endTime, _ := time.Parse(time.RFC3339, periodicSequenceEnd)

	// Generate an impulse every period.
	periodicImp := periodic.Impulse(s, startTime, endTime, periodicSequenceInterval, false)

	// Use the impulse to trigger some other ordinary transform.
	updatedImp := beam.ParDo(s, update, periodicImp)

	// Window for use as a side input, to allow the input to change with windows.
	sideInput := beam.WindowInto(s, window.NewFixedWindows(periodicSequenceInterval),
		updatedImp,
		beam.Trigger(trigger.Repeat(trigger.Always())),
		beam.PanesDiscard(),
	)

	beam.ParDo0(s, process, mainInput,
		beam.SideInput{
			Input: sideInput,
		},
	)

	if _, err := beam.Run(context.Background(), "dataflow", p); err != nil {
		log.Exitf(ctx, "Failed to run job: %v", err)
	}
}
