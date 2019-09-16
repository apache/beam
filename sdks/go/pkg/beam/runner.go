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

package beam

import (
	"context"
	"fmt"

	"github.com/apache/beam/sdks/go/pkg/beam/log"
)

// TODO(herohde) 7/6/2017: do we want to make the selected runner visible to
// transformations? That would allow runner-dependent operations or
// verification, but require that it is stored in Init and used for Run.

var (
	runners = make(map[string]func(ctx context.Context, p *Pipeline) error)
)

// RegisterRunner associates the name with the supplied runner, making it available
// to execute a pipeline via Run.
func RegisterRunner(name string, fn func(ctx context.Context, p *Pipeline) error) {
	if _, ok := runners[name]; ok {
		panic(fmt.Sprintf("runner %v already defined", name))
	}
	runners[name] = fn
}

// Run executes the pipeline using the selected registred runner. It is customary
// to define a "runner" with no default as a flag to let users control runner
// selection.
func Run(ctx context.Context, runner string, p *Pipeline) error {
	fn, ok := runners[runner]
	if !ok {
		log.Exitf(ctx, "Runner %v not registered. Forgot to _ import it?", runner)
	}
	return fn(ctx, p)
}
