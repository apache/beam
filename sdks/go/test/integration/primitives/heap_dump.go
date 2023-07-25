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

package primitives

import (
	"context"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
)

func oomFn(ctx context.Context, elm int, emit func(int)) {
	size := 1 << 25
	// Simulate a slow memory leak
	for {
		abc := make([]int64, size)
		log.Debugf(ctx, "abc %v", abc)
		time.Sleep(5 * time.Second)
		log.Debugf(ctx, "abc %v", abc)
		if size > 1<<40 {
			break
		}

		size = int(float64(size) * 1.2)
	}
	emit(elm)
}

// OomParDo tests a DoFn that OOMs.
func OomParDo() *beam.Pipeline {
	p, s := beam.NewPipelineWithRoot()

	in := beam.Create(s, 1)
	beam.ParDo(s, oomFn, in)

	return p
}
