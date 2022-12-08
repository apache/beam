/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
/*
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// beam-playground:
//   name: adding-timestamp
//   description: Adding timestamp example.
//   multifile: false
//   context_line: 36
//   categories:
//     - Quickstart
//   complexity: MEDIUM
//   tags:
//     - hellobeam

import (
    "context"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/mtime"
	"time"
)

func main() {
	ctx := context.Background()

	p, s := beam.NewPipelineWithRoot()

	input := common.CreateCommits(s)

	result := task.ApplyTransform(s, input)

	beam.ParDo0(s, func(et beam.EventTime, commit task.Commit){
		t := time.Unix(0, int64(et.Milliseconds()) * 1e6)
		log.Infof(ctx, "time: %s, message: %s", t.Format("03:04"), commit)
	}, result)

	err := beamx.Run(ctx, p)

	if err != nil {
		log.Exitf(context.Background(), "Failed to execute job: %v", err)
	}
}

// Commit represents data about a git commit message.
type Commit struct {
	Datetime time.Time
	Message string
}

// ApplyTransform applies a beam.EventTime timestamp to PCollection<Commit> elements.
func ApplyTransform(s beam.Scope, input beam.PCollection) beam.PCollection {
	return beam.ParDo(s, func(element Commit) (beam.EventTime, Commit) {
		return mtime.FromTime(element.Datetime), element
	}, input)
}