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

package ptest

import (
	"context"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/runners/direct"
)

// TODO(herohde) 7/10/2017: add hooks to verify counters, logs, etc.

// Create creates a pipeline and a PCollection with the given values.
func Create(values []interface{}) (*beam.Pipeline, beam.PCollection) {
	p := beam.NewPipeline()
	return p, beam.Create(p, values...)
}

// CreateList creates a pipeline and a PCollection with the given values.
func CreateList(values interface{}) (*beam.Pipeline, beam.PCollection) {
	p := beam.NewPipeline()
	return p, beam.CreateList(p, values)
}

// Create2 creates a pipeline and 2 PCollections with the given values.
func Create2(a, b []interface{}) (*beam.Pipeline, beam.PCollection, beam.PCollection) {
	p := beam.NewPipeline()
	return p, beam.Create(p, a...), beam.Create(p, b...)
}

// CreateList2 creates a pipeline and 2 PCollections with the given values.
func CreateList2(a, b interface{}) (*beam.Pipeline, beam.PCollection, beam.PCollection) {
	p := beam.NewPipeline()
	return p, beam.CreateList(p, a), beam.CreateList(p, b)
}

// Run runs a pipeline for testing. The semantics of the pipeline is expected
// to be verified through passert.
func Run(p *beam.Pipeline) error {
	return direct.Execute(context.Background(), p)
}
