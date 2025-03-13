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
	"os"
	"strings"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/pubsubio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/options/gcpopts"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/util/pubsubx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/debug"
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

func main() {
	flag.Parse()
	beam.Init()

	ctx := context.Background()
	project := gcpopts.GetProject(ctx)

	log.Infof(ctx, "Publishing %v messages to: %v", len(data), *input)

	defer pubsubx.CleanupTopic(ctx, project, *input)
	sub, err := pubsubx.Publish(ctx, project, *input, data...)
	if err != nil {
		log.Fatal(ctx, err)
	}

	log.Infof(ctx, "Running streaming wordcap with subscription: %v", sub.ID())

	p := beam.NewPipeline()
	s := p.Root()

	col := pubsubio.Read(s, project, *input, &pubsubio.ReadOptions{Subscription: sub.ID()})
	str := beam.ParDo(s, func(b []byte) string {
		return (string)(b)
	}, col)
	cap := beam.ParDo(s, strings.ToUpper, str)
	debug.Print(s, cap)

	if err := beamx.Run(context.Background(), p); err != nil {
		log.Exitf(ctx, "Failed to execute job: %v", err)
	}
}
