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

// The integration driver provides a suite of tests to run against a registered runner.
package main

import (
	"context"
	"flag"
	"regexp"
	"sync"
	"sync/atomic"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/io/filesystem/memfs"
	"github.com/apache/beam/sdks/go/pkg/beam/log"
	"github.com/apache/beam/sdks/go/pkg/beam/x/beamx"
	"github.com/apache/beam/sdks/go/test/integration/primitives"
	"github.com/apache/beam/sdks/go/test/integration/synthetic"
	"github.com/apache/beam/sdks/go/test/integration/wordcount"
)

var (
	parallel = flag.Int("parallel", 10, "Number of tests to run in parallel")
	filter   = flag.String("filter", ".*", "Test filer to run a subset of tests")
)

const old_pond = "memfs://old_pond"

func init() {
	memfs.Write(old_pond, []byte("old pond \na frog leaps in\nwater's sound\n"))
}

type namedPipeline struct {
	name string
	p    *beam.Pipeline
}

func main() {
	flag.Parse()
	beam.Init()

	if *parallel < 1 {
		*parallel = 1
	}

	pipelines := []namedPipeline{
		{"wordcount:memfs", wordcount.WordCount(old_pond, "+Qj8iAnV5BI2A4sbzUbb6Q==", 8)},
		{"wordcount:kinglear", wordcount.WordCount("gs://apache-beam-samples/shakespeare/kinglear.txt", "7ZCh5ih9m8IW1w+iS8sRKg==", 4749)},
		{"synthetic:simple", synthetic.SimplePipeline()},
		{"synthetic:splittable", synthetic.SplittablePipeline()},
		{"pardo:multioutput", primitives.ParDoMultiOutput()},
		{"pardo:sideinput", primitives.ParDoSideInput()},
		{"pardo:kvsideinput", primitives.ParDoKVSideInput()},
		{"cogbk:cogbk", primitives.CoGBK()},
		{"flatten:flatten", primitives.Flatten()},
		// {"flatten:dup", primitives.FlattenDup()},
		{"reshuffle:reshuffle", primitives.Reshuffle()},
		{"reshuffle:reshufflekv", primitives.ReshuffleKV()},
	}

	re := regexp.MustCompile(*filter)

	ctx := context.Background()

	ch := make(chan namedPipeline, len(pipelines))
	for _, np := range pipelines {
		if re.MatchString(np.name) {
			ch <- np
		}
	}
	close(ch)

	var failures int32
	var wg sync.WaitGroup
	for i := 0; i < *parallel; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for np := range ch {
				log.Infof(ctx, "Running %v test ..", np.name)

				if err := beamx.Run(ctx, np.p); err != nil {
					atomic.AddInt32(&failures, 1)
					log.Errorf(ctx, "Test %v failed: %v", np.name, err)
				} else {
					log.Infof(ctx, "Test %v completed", np.name)
				}
			}
		}()
	}
	wg.Wait()

	if failures > 0 {
		log.Exitf(ctx, "Result: %v tests failed", failures)
	}
	log.Infof(ctx, "Result: all tests passed!")
}
