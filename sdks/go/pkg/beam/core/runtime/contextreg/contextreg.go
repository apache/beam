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

// Package contextreg contains the global registrations of functions for extracting
// ptransform annotations or environment resource hints from context.Context attached to
// scopes.
//
// For beam internal use only. API subject to change.
package contextreg

import (
	"context"
	"sync"

	"golang.org/x/exp/maps"
)

var defaultReg = &Registry{}

// Default is the default registry for context extractors.
func Default() *Registry {
	return defaultReg
}

// Registry contains a set of registrations for extracting annotations and hints from a context.Context.
//
// This type is exported to allow simpler testing of new extractors, and their interaction with the registry.
type Registry struct {
	mu         sync.Mutex
	transforms []func(context.Context) TransformMetadata
	envs       []func(context.Context) EnvironmentMetadata
}

// TransformMetadata represents additional information on transforms to be added to the Pipeline proto graph.
type TransformMetadata struct {
	Annotations map[string][]byte
	// DisplayData []*pipepb.DisplayData
}

// EnvironmentMetadata represent additional information on environmental requirements to be added to the Pipeline
// proto graph.
type EnvironmentMetadata struct {
	ResourceHints map[string][]byte
	// DisplayData   []*pipepb.DisplayData
	// Dependencies  []*pipepb.ArtifactInformation
}

// TransformExtractor registers a transform metadata extractor to this registry.
// These will be set on the current composite transform scope.
// They are accessible to runners via the transform hypergraph.
func (r *Registry) TransformExtractor(ext func(context.Context) TransformMetadata) {
	r.mu.Lock()
	r.transforms = append(r.transforms, ext)
	r.mu.Unlock()
}

// EnvExtrator registers an environment metadata extractor to this registry.
// When non-empty extraction occurs, a new environment will be derived from the parent scopes environment.
func (r *Registry) EnvExtrator(ext func(context.Context) EnvironmentMetadata) {
	r.mu.Lock()
	r.envs = append(r.envs, ext)
	r.mu.Unlock()
}

// ExtractTransformMetadata runs all registered transform extractors on the provided context,
// and returns the resulting metadata.
//
// A metadata field will be nil if there's no data. A nil context bypasses extractor execution.
func (r *Registry) ExtractTransformMetadata(ctx context.Context) TransformMetadata {
	r.mu.Lock()
	defer r.mu.Unlock()
	if ctx == nil {
		return TransformMetadata{}
	}
	ret := TransformMetadata{
		Annotations: map[string][]byte{},
	}
	for _, ext := range r.transforms {
		k := ext(ctx)
		maps.Copy(ret.Annotations, k.Annotations)
	}
	if len(ret.Annotations) == 0 {
		ret.Annotations = nil
	}
	return ret
}

// ExtractEnvironmentMetadata runs all registered environment extractors on the provided context,
// and returns the resulting metadata.
//
// A metadata field will be nil if there's no data. A nil context bypasses extractor execution.
func (r *Registry) ExtractEnvironmentMetadata(ctx context.Context) EnvironmentMetadata {
	r.mu.Lock()
	defer r.mu.Unlock()
	if ctx == nil {
		return EnvironmentMetadata{}
	}
	ret := EnvironmentMetadata{
		ResourceHints: map[string][]byte{},
	}
	for _, ext := range r.envs {
		k := ext(ctx)
		maps.Copy(ret.ResourceHints, k.ResourceHints)
	}
	if len(ret.ResourceHints) == 0 {
		ret.ResourceHints = nil
	}
	return ret
}
