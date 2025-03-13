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

// beam-playground:
//   name: combine-per-key
//   description: CombinePerKey
//   multifile: false
//   context_line: 44
//   categories:
//     - Quickstart
//   complexity: MEDIUM
//   tags:
//     - hellobeam

package main

import (
	"context"
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/debug"
)

const (
	Player1 = "Player 1"
	Player2 = "Player 2"
	Player3 = "Player 3"
)

func main() {
	ctx := context.Background()

	p, s := beam.NewPipelineWithRoot()

	input := beam.ParDo(s, func(_ []byte, emit func(string, int)) {
		emit(Player1, 15)
		emit(Player2, 10)
		emit(Player1, 100)
		emit(Player3, 25)
		emit(Player2, 75)
	}, beam.Impulse(s))

	output := applyTransform(s, input)

	debug.Print(s, output)

	err := beamx.Run(ctx, p)

	if err != nil {
		log.Exitf(context.Background(), "Failed to execute job: %v", err)
	}
}

func applyTransform(s beam.Scope, input beam.PCollection) beam.PCollection {
	return beam.CombinePerKey(s, func(score1, score2 int) int {
		return score1 + score2
	}, input)
}
