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

package artifact

import (
	"context"

	structpb "google.golang.org/protobuf/types/known/structpb"
)

type contextKey string

const pipelineOptionsKey contextKey = "pipeline_options"

// WithPipelineOptions returns a new context carrying the full pipeline options struct.
func WithPipelineOptions(ctx context.Context, options *structpb.Struct) context.Context {
	return context.WithValue(ctx, pipelineOptionsKey, options)
}

// PipelineOptions returns the pipeline options from the context if present.
func PipelineOptions(ctx context.Context) *structpb.Struct {
	options, _ := ctx.Value(pipelineOptionsKey).(*structpb.Struct)
	return options
}

// GetExperiments extracts a list of experiments from the pipeline options.
func GetExperiments(options *structpb.Struct) []string {
	if options == nil {
		return nil
	}

	// Try legacy style first
	var exps []string
	for _, v := range options.GetFields()["options"].GetStructValue().GetFields()["experiments"].GetListValue().GetValues() {
		exps = append(exps, v.GetStringValue())
	}
	if len(exps) > 0 {
		return exps
	}

	// Try URN style
	for _, v := range options.GetFields()["beam:option:experiments:v1"].GetListValue().GetValues() {
		exps = append(exps, v.GetStringValue())
	}
	return exps
}

// HasExperiment checks if a specific experiment is enabled in the pipeline options.
func HasExperiment(options *structpb.Struct, experiment string) bool {
	for _, exp := range GetExperiments(options) {
		if exp == experiment {
			return true
		}
	}
	return false
}
