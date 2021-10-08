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

package xlangx

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"sync"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/graphx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/pipelinex"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
	jobpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/jobmanagement_v1"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
)

var defaultReg = newRegistry()

// RegisterHandler associates a namespace with a HandlerFunc which can be used to
// replace calls to a Beam ExpansionService.
//
// Then, expansion addresses of the forms
//   "<namespace>" or
//   "<namespace>:<configuration>"
// can be used with beam.CrossLanguage. Any configuration after the separator is
// provided to the HandlerFunc on call for the handler func to use at it's leisure.
func RegisterHandler(namespace string, handler HandlerFunc) {
	if err := defaultReg.RegisterHandler(namespace, handler); err != nil {
		panic(err)
	}
}

// RegisterExpansionForUrn overrides which expansion address is used to
// expand a specific transform URN. The expansion address must be a URL
// or be a namespaced handler registered with RegisterHandler.
//
// When the expansion address is for a handler, it may take the forms
//  "<namespace>" or
//  "<namespace>:<configuration>"
func RegisterOverrideForUrn(urn, expansionAddr string) {
	if err := defaultReg.RegisterOverrideForUrn(urn, expansionAddr); err != nil {
		panic(err)
	}
}

// HandlerParams is the parameter to an expansion service handler.
type HandlerParams struct {
	// Additional parameterization string, if any.
	Config string

	Req *jobpb.ExpansionRequest

	// Additional pipeline graph information for custom handling
	// Not exported to avoid mutation.
	edge *graph.MultiEdge
	ext  *graph.ExternalTransform
}

// CoderMarshaller returns a coder marshaller initialized with the request's namespace.
func (p *HandlerParams) CoderMarshaller() *graphx.CoderMarshaller {
	cm := graphx.NewCoderMarshaller()
	cm.Namespace = p.Req.Namespace
	return cm
}

// OutputPCollections returns the local identifiers for expected outputs
// for this expansion service request.
//
// If no collections are returned, none are currently expected.
func (p *HandlerParams) OutputPCollections() []string {
	var out []string
	for local := range p.ext.OutputsMap {
		out = append(out, local)
	}
	return out
}

// InputPCollections returns the local identifiers for expected outputs
// for this expansion service request.
//
// If no collections are returned, none are currently expected.
func (p *HandlerParams) InputPCollections() []string {
	var out []string
	for local := range p.ext.InputsMap {
		out = append(out, local)
	}
	return out
}

func (p *HandlerParams) panicIfMissing(m map[string]int, local string) int {
	i, ok := m[local]
	if !ok {
		panic(fmt.Errorf("unknown local output identifier provided: %v", local))
	}
	return i
}

// OutputCoder returns the coder for the associated output PCollection.
// Panics if local is not returned by OutputPCollections.
func (p *HandlerParams) OutputCoder(local string) *coder.Coder {
	i := p.panicIfMissing(p.ext.OutputsMap, local)
	return p.edge.Output[i].To.Coder
}

// OutputType returns the full type for the associated output PCollection.
// Panics if local is not returned by OutputPCollections.
func (p *HandlerParams) OutputType(local string) typex.FullType {
	i := p.panicIfMissing(p.ext.OutputsMap, local)
	return p.edge.Output[i].Type
}

// OutputBounded returns whether the associated output PCollection is bounded.
// Panics if local is not returned by OutputPCollections.
func (p *HandlerParams) OutputBounded(local string) pipepb.IsBounded_Enum {
	i := p.panicIfMissing(p.ext.OutputsMap, local)
	return pipelinex.BoolToBounded(p.edge.Output[i].To.Bounded())
}

// OutputWindowingStrategy returns the windowing strategy for the associated output PCollection.
// Panics if local is not returned by OutputPCollections.
func (p *HandlerParams) OutputWindowingStrategy(local string, cm *graphx.CoderMarshaller) *pipepb.WindowingStrategy {
	i := p.panicIfMissing(p.ext.OutputsMap, local)
	wspb, err := graphx.MarshalWindowingStrategy(cm, p.edge.Output[i].To.WindowingStrategy())
	if err != nil {
		panic(fmt.Errorf("unable to marshal windowing strategy for output %v: %w", local, err))
	}
	return wspb
}

// OutputCoder returns the coder for the associated output PCollection.
// Panics if local is not returned by InputPCollections.
func (p *HandlerParams) InputCoder(local string) *coder.Coder {
	i := p.panicIfMissing(p.ext.InputsMap, local)
	return p.edge.Input[i].From.Coder
}

// InputType returns the full type for the associated output PCollection.
// Panics if local is not returned by InputPCollections.
func (p *HandlerParams) InputType(local string) typex.FullType {
	i := p.panicIfMissing(p.ext.OutputsMap, local)
	return p.edge.Output[i].Type
}

// InputBounded returns whether the associated output PCollection is bounded.
// Panics if local is not returned by InputPCollections.
func (p *HandlerParams) InputBounded(local string) pipepb.IsBounded_Enum {
	i := p.panicIfMissing(p.ext.InputsMap, local)
	return pipelinex.BoolToBounded(p.edge.Input[i].From.Bounded())
}

// OutputWindowingStrategy returns the windowing strategy for the associated output PCollection.
// Panics if given a string not returned by OutputPCollections.
func (p *HandlerParams) InputWindowingStrategy(local string, cm *graphx.CoderMarshaller) *pipepb.WindowingStrategy {
	i := p.panicIfMissing(p.ext.InputsMap, local)
	wspb, err := graphx.MarshalWindowingStrategy(cm, p.edge.Input[i].From.WindowingStrategy())
	if err != nil {
		panic(fmt.Errorf("unable to marshal windowing strategy for output %v: %w", local, err))
	}
	return wspb
}

// PColID produces a standard format globally namespaced id for a PCollection from the local identifier.
func (p *HandlerParams) PColID(local string) string {
	return fmt.Sprintf("n%v@%v", local, p.Req.Namespace)
}

// WSID produces a standard format globally namespaced id for a WindowingStrategy from the local identifier.
func (p *HandlerParams) WSID(local string) string {
	return fmt.Sprintf("ws%v@%v", local, p.Req.Namespace)
}

// HandlerFunc abstracts making an ExpansionService request.
type HandlerFunc func(context.Context, *HandlerParams) (*jobpb.ExpansionResponse, error)

type registry struct {
	mu           sync.Mutex
	handlers     map[string]HandlerFunc // namespace -> handlerfuncs
	urnOverrides map[string]string      // URNs -> expansionAddrs
}

func newRegistry() *registry {
	return &registry{
		handlers:     map[string]HandlerFunc{},
		urnOverrides: map[string]string{},
	}
}

// RegisterHandler associates a namespace, with a handler.
//
// Namespaces may not have the configuration separator ":" in them,
// nor may they be a restricted namespace, like "localhost" or "http".
func (r *registry) RegisterHandler(namespace string, handler HandlerFunc) error {
	if err := validateNamespace(namespace); err != nil {
		return fmt.Errorf("xlangx.RegisterHandler: %v", err)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.handlers[namespace] = handler
	return nil
}

func validateNamespace(namespace string) error {
	if strings.Contains(namespace, Separator) {
		return fmt.Errorf("invalid namespace, provide a different one: %q contains the separator %q", namespace, Separator)
	}
	if _, ok := restricted[namespace]; ok {
		return fmt.Errorf("invalide namespace, provide a different one: %q is a restricted namespace", namespace)
	}
	return nil
}

// RegisterOverrideForUrn instructs using expansionAddr for CrossLanguage
// transforms with urn. expansionAddr should either be registered with an
// Expansion handler, or an Expansion service address.
func (r *registry) RegisterOverrideForUrn(urn, expansionAddr string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.validateAddr(expansionAddr); err != nil {
		return fmt.Errorf("xlangx.RegisterExpansionForUrn(%q,%q) error: %v", urn, expansionAddr, err)
	}
	r.urnOverrides[urn] = expansionAddr
	return nil
}

func (r *registry) validateAddr(expansionAddr string) error {
	u, err := url.Parse(expansionAddr)
	if err == nil && u.Scheme != "" && u.Host != "" {
		// This is likely a URL, so allow it.
		return nil
	}
	// Otherwise, let's check that we have a handler registered.
	ns, _ := parseAddr(expansionAddr)
	if _, ok := r.handlers[ns]; !ok {
		return fmt.Errorf("expansionAddr %q trying to use unregistered namespace: %q", expansionAddr, ns)
	}
	return nil
}

// getHandlerFunc returns HandlerFunc and the config string to put into the params when called.
func (r *registry) getHandlerFunc(urn, expansionAddr string) (HandlerFunc, string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	// By the time this is called, we want *some* kind of HandlerFunc at all,
	// So first we check for the hard override.
	ns, config := parseAddr(expansionAddr)
	if ns == hardOverrideNamespace {
		// We have the override namespace and config we must use, so skip the urn step.
		expansionAddr = config // The expansionAddr becomes the full config, in case of service.
		ns, config = parseAddr(config)

	} else if addr, ok := r.urnOverrides[urn]; ok {
		// If there is no hard override, check the urn overrides.
		expansionAddr = addr
		ns, config = parseAddr(addr)
	}

	// Now that overrides have been handled, we can look up if there's a handler, and return that.
	if h, ok := r.handlers[ns]; ok {
		return h, config
	}

	// Otherwise, we query the expansion service address, passing it to the func as a config.
	return QueryExpansionService, expansionAddr
}

const (
	Separator             = ":"
	hardOverrideNamespace = "hardoverride"
)

// Require takes an expansionAddr and requires cross language expansion
// to use it and it's associated handler. If the transform's urn has a
// specific override, it will be ignored.
//
// Intended for use by cross language wrappers to permit
// per-call overrides of the expansion address within a
// single pipeline, such as for testing purposes.
func Require(expansionAddr string) string {
	return hardOverrideNamespace + Separator + expansionAddr
}

// restricted namespaces to prevent some awkward edge cases.
var restricted = map[string]struct{}{
	hardOverrideNamespace: {}, // Special handler for overriding.
	"localhost":           {},
	"http":                {},
	"https":               {},
	"tcp":                 {},
	"udp":                 {},
}

// parseAddr takes an expansion address, and separates it into the namespace,
// and config string if any.
func parseAddr(expansionAddr string) (ns, config string) {
	split := strings.SplitN(expansionAddr, Separator, 2)
	if len(split) == 1 {
		return expansionAddr, ""
	}
	return split[0], split[1]
}
