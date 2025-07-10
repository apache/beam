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

// Package schema has the cross language implementation for calling schema transforms in other language SDKs.
package schema

import (
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/xlangx"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	"google.golang.org/protobuf/proto"
)

const schemaTransformURN = "beam:expansion:payload:schematransform:v1"

type options struct {
	inputs        map[string]beam.PCollection
	outputTypes   map[string]beam.FullType
	expansionAddr string
}

// Option is the base type of all the schema transform options.
type Option func(*options)

// Input adds a named PCollection input to the transform.
func Input(name string, in beam.PCollection) Option {
	return func(opts *options) {
		if opts.inputs == nil {
			opts.inputs = map[string]beam.PCollection{}
		}
		opts.inputs[name] = in
	}
}

// OutputType specifies an output PCollection type of the transform.
// It must match the external transform's output schema.
func OutputType(name string, tpe beam.FullType) Option {
	return func(opts *options) {
		if opts.outputTypes == nil {
			opts.outputTypes = map[string]beam.FullType{}
		}
		opts.outputTypes[name] = tpe
	}
}

// UnnamedOutputType specifies an output PCollection type of the transform.
// It must match the external transform's output schema.  This is simply
// syntactic sugar for OutputType(beam.UnnamedOutputTag(), tpe).
func UnnamedOutputType(tpe beam.FullType) Option {
	return OutputType(beam.UnnamedOutputTag(), tpe)
}

// ExpansionAddr is the URL of the expansion service to use.
func ExpansionAddr(addr string) Option {
	return func(opts *options) {
		opts.expansionAddr = addr
	}
}

// Transform configures a new cross language transform to call a "schema transform" in an external SDK.
func Transform(scope beam.Scope, config any, transformIdentifier string, opts ...Option) map[string]beam.PCollection {
	ecp, err := xlangx.CreateExternalConfigurationPayload(config)
	if err != nil {
		panic(err)
	}

	pl, err := proto.Marshal(&pipepb.SchemaTransformPayload{
		Identifier:          transformIdentifier,
		ConfigurationSchema: ecp.GetSchema(),
		ConfigurationRow:    ecp.GetPayload(),
	})
	if err != nil {
		panic(err)
	}

	allOpts := &options{}
	for _, o := range opts {
		o(allOpts)
	}

	return beam.CrossLanguage(scope, schemaTransformURN, pl, allOpts.expansionAddr, allOpts.inputs, allOpts.outputTypes)
}
