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

package internal

import (
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/mtime"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/runners/prism/internal/worker"
)

// stage represents a fused subgraph.
// temporary implementation to break up PRs.
type stage struct {
	transforms []string
}

type transformExecuter interface {
	ExecuteUrns() []string
	ExecuteWith(t *pipepb.PTransform) string
	ExecuteTransform(tid string, t *pipepb.PTransform, comps *pipepb.Components, watermark mtime.Time, data [][]byte) *worker.B
}
