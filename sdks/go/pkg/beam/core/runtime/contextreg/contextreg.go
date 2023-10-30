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
// Intended for beam internal use only.
package contextreg

import (
	"context"
	"maps"
	"sync"
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
	mu                    sync.Mutex
	annotations, envHints []func(context.Context) map[string][]byte
}

// AnnotationExtractor registers an annotation extractor.
func (r *Registry) AnnotationExtractor(ext func(context.Context) map[string][]byte) {
	r.mu.Lock()
	r.annotations = append(r.annotations, ext)
	r.mu.Unlock()
}

// HintExtractor registers a resource hint extractor.
func (r *Registry) HintExtractor(ext func(context.Context) map[string][]byte) {
	r.mu.Lock()
	r.envHints = append(r.envHints, ext)
	r.mu.Unlock()
}

func (r *Registry) extract(ctx context.Context, exts []func(context.Context) map[string][]byte) map[string][]byte {
	r.mu.Lock()
	defer r.mu.Unlock()
	ret := map[string][]byte{}
	for _, ext := range exts {
		k := ext(ctx)
		maps.Copy(ret, k)
	}
	return ret
}

// ExtractAnnotations runs all registered annotation extractors on the provided context,
// and returns the resulting map.
//
// Callers must check the length of the returned map. If 0, then no values exist.
func (r *Registry) ExtractAnnotations(ctx context.Context) map[string][]byte {
	return r.extract(ctx, r.annotations)
}

// ExtractHints runs all registered resource hint extractors on the provided context,
// and returns the resulting map.
//
// Callers must check the length of the returned map. If 0, then no values exist, and no
// additional environment should be created.
func (r *Registry) ExtractHints(ctx context.Context) map[string][]byte {
	return r.extract(ctx, r.envHints)
}
