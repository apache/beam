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
	"beam.apache.org/learning/katas/core_transforms/side_input/side_input/pkg/task"
	"context"
	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/log"
	"github.com/apache/beam/sdks/go/pkg/beam/x/beamx"
	"github.com/apache/beam/sdks/go/pkg/beam/x/debug"
)

func main() {
	ctx := context.Background()

	p, s := beam.NewPipelineWithRoot()

	citiesToCountriesKV := beam.ParDo(s, func(_ []byte, emit func(string, string)){
		emit("Beijing", "China")
		emit("London", "United Kingdom")
		emit("San Francisco", "United States")
		emit("Singapore", "Singapore")
		emit("Sydney", "Australia")
	}, beam.Impulse(s))

	persons := beam.Create(s,
		task.Person{Name: "Henry", City: "Singapore"},
		task.Person{Name: "Jane", City: "San Francisco"},
		task.Person{Name: "Lee", City: "Beijing"},
		task.Person{Name: "John", City: "Sydney"},
		task.Person{Name: "Alfred", City: "London"},
	)

	output := task.ApplyTransform(s, persons, citiesToCountriesKV)

	debug.Print(s, output)

	err := beamx.Run(ctx, p)

	if err != nil {
		log.Exitf(context.Background(), "Failed to execute job: %v", err)
	}
}
