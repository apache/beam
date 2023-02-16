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

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"regexp"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
)

var (
	input  = flag.String("input", os.ExpandEnv("$GOPATH/src/github.com/apache/beam/sdks/go/data/haiku/old_pond.txt"), "Files to read.")
	output = flag.String("output", "/tmp/pingpong/out.", "Prefix of output.")
)

func init() {
	register.Function4x1(multiFn)
	register.Function3x1(subsetFn)
	register.Function2x0(extractFn)

	register.Emitter1[string]()
	register.Iter1[string]()
}

// stitch constructs two composite PTransforms that provide input to each other. It
// is a (deliberately) complex DAG to show what kind of structures are possible.
func stitch(s beam.Scope, words beam.PCollection) (beam.PCollection, beam.PCollection) {
	ping := s.Scope("ping")
	pong := ping // s.Scope("pong")

	// NOTE(herohde) 2/23/2017: Dataflow does not allow cyclic composite structures.

	small1, big1 := beam.ParDo2(ping, multiFn, words, beam.SideInput{Input: words}) // self-sample (ping)
	small2, big2 := beam.ParDo2(pong, multiFn, words, beam.SideInput{Input: big1})  // big-sample  (pong). More words are small.
	_, big3 := beam.ParDo2(ping, multiFn, big2, beam.SideInput{Input: small1})      // small-sample big (ping). All words are big.
	small4, _ := beam.ParDo2(pong, multiFn, small2, beam.SideInput{Input: big3})    // big-sample small (pong). All words are small.

	return small4, big3
}

// Slice side input.

func multiFn(word string, sample []string, small, big func(string)) error {
	// TODO: side input processing into start bundle, once supported.

	count := 0
	size := 0
	for _, w := range sample {
		count++
		size += len(w)
	}
	if count == 0 {
		return errors.New("empty sample")
	}
	avg := size / count

	if len(word) < avg {
		small(word)
	} else {
		big(word)
	}
	return nil
}

func subset(s beam.Scope, a, b beam.PCollection) {
	beam.ParDo0(s, subsetFn, beam.Impulse(s), beam.SideInput{Input: a}, beam.SideInput{Input: b})
}

func subsetFn(_ []byte, a, b func(*string) bool) error {
	larger := make(map[string]bool)
	var elm string
	for b(&elm) {
		larger[elm] = true
	}
	for a(&elm) {
		if !larger[elm] {
			return fmt.Errorf("extra element: %v", elm)
		}
	}
	return nil
}

var wordRE = regexp.MustCompile(`[a-zA-Z]+('[a-z])?`)

func extractFn(line string, emit func(string)) {
	for _, word := range wordRE.FindAllString(line, -1) {
		emit(word)
	}
}

func main() {
	flag.Parse()
	beam.Init()

	ctx := context.Background()

	log.Info(ctx, "Running pingpong")

	// PingPong constructs a convoluted pipeline with two "cyclic" composites.
	p := beam.NewPipeline()
	s := p.Root()

	lines := textio.Read(s, *input)
	words := beam.ParDo(s, extractFn, lines)

	// Run baseline and stitch; then compare them.
	small, big := beam.ParDo2(s, multiFn, words, beam.SideInput{Input: words})
	small2, big2 := stitch(s, words)

	subset(s, small, small2)
	subset(s, big2, big)

	textio.Write(s, *output+"small.txt", small2)
	textio.Write(s, *output+"big.txt", big2)

	if err := beamx.Run(ctx, p); err != nil {
		log.Exitf(ctx, "Failed to execute job: %v", err)
	}
}
