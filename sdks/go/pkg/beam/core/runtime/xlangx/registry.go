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
	jobpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/jobmanagement_v1"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
)

var defaultReg = newRegistry()

// RegisterHandler associates a namespace with a HandlerFunc which can be used to
// replace calls to a Beam ExpansionService.
//
// Then, expansion addresses of the forms
//
//	"<namespace>" or
//	"<namespace>:<configuration>"
//
// can be used with beam.CrossLanguage. Any configuration after the separator is
// provided to the HandlerFunc on call for the handler func to use at it's leisure.
func RegisterHandler(namespace string, handler HandlerFunc) {
	if err := defaultReg.RegisterHandler(namespace, handler); err != nil {
		panic(err)
	}
}

// RegisterOverrideForUrn overrides which expansion address is used to
// expand a specific transform URN. The expansion address must be a URL
// or be a namespaced handler registered with RegisterHandler.
//
// When the expansion address is for a handler, it may take the forms
//
//	"<namespace>" or
//	"<namespace>:<configuration>"
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

// PCol represents input or output pcollections to the cross language transform being expanded.
type PCol struct {
	Index   int          // Positional index of this input or output
	Local   string       // Local name of the PCollection (may be used in the cross language PTransform)
	Coder   *coder.Coder // Contains the full type and other coder information.
	Bounded pipepb.IsBounded_Enum

	namespace string
	node      *graph.Node
}

// ID produces a standard format globally namespaced id for a PCollection from the local identifier.
func (p *PCol) ID() string {
	return fmt.Sprintf("n%v@%v", p.Local, p.namespace)
}

// WSID produces a standard format globally namespaced id for a WindowingStrategy from the local identifier.
func (p *PCol) WSID() string {
	return fmt.Sprintf("ws%v@%v", p.Local, p.namespace)
}

// WindowingStrategy returns the id to this PCollection's windowing strategy, and the associated proto.
//
// TODO: intern windowing strategies.
func (p *PCol) WindowingStrategy(cm *graphx.CoderMarshaller) (string, *pipepb.WindowingStrategy) {
	wspb, err := graphx.MarshalWindowingStrategy(cm, p.node.WindowingStrategy())
	if err != nil {
		panic(fmt.Errorf("unable to marshal windowing strategy for PCol %v: %w", p.Local, err))
	}
	return p.WSID(), wspb
}

func makePCol(node *graph.Node, index int, local, namespace string) PCol {
	return PCol{
		Index:   index,
		Local:   local,
		Coder:   node.Coder,
		Bounded: pipelinex.BoolToBounded(node.Bounded()),

		namespace: namespace,
		node:      node,
	}
}

// Outputs returns the provided output PCollections, if any, for expected outputs
// for this expansion service request.
//
// If no collections are returned, none are currently expected, but may be provided
// by the expansion.
func (p *HandlerParams) Outputs() []PCol {
	out := make([]PCol, 0, len(p.ext.OutputsMap))
	for local, i := range p.ext.OutputsMap {
		out = append(out, makePCol(p.edge.Output[i].To, i, local, p.Req.Namespace))
	}
	return out
}

// Inputs returns the provided input PCollections, if any, for the PTransform to expand
// in this expansion service request.
func (p *HandlerParams) Inputs() []PCol {
	out := make([]PCol, 0, len(p.ext.InputsMap))
	for local, i := range p.ext.InputsMap {
		out = append(out, makePCol(p.edge.Input[i].From, i, local, p.Req.Namespace))
	}
	return out
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
	// Check this after hardoverrides and URN overrides so those can point to automated expansion
	// themselves.
	switch ns {
	case autoJavaNamespace:
		// Leave expansionAddr unmodified so the autoNamespace keyword sticks.
		// We strip it manually in the HandlerFunc.
		return QueryAutomatedExpansionService, expansionAddr
	case autoPythonNamespace:
		return QueryPythonExpansionService, expansionAddr
	}

	// Now that overrides have been handled, we can look up if there's a handler, and return that.
	if h, ok := r.handlers[ns]; ok {
		return h, config
	}

	// Otherwise, we query the expansion service address, passing it to the func as a config.
	return QueryExpansionService, expansionAddr
}

const (
	// Separator is the canonical separator between a namespace and optional configuration.
	Separator = ":"
	// ClasspathSeparator is the canonical separator between a classpath namespace config string from other namespace-configuration string.
	ClasspathSeparator    = ";"
	hardOverrideNamespace = "hardoverride"
	autoJavaNamespace     = "autojava"
	autoPythonNamespace   = "autopython"
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

// ExpansionServiceOption provides an option for xlangx.UseAutomatedJavaExpansionService()
type ExpansionServiceOption func(*string)

// AddClasspaths is an expansion service option for xlangx.UseAutomatedExpansionService
// that accepts a classpaths slice and creates a tagged  expansion address string
// suffixed with classpath separator and classpaths provided.
func AddClasspaths(classpaths []string) ExpansionServiceOption {
	return func(expansionAddress *string) {
		*expansionAddress += ClasspathSeparator + strings.Join(classpaths, " ")
	}
}

// AddExtraPackages is an expansion service option for xlangx.UseAutomatedPythonExpansionService
// that accepts a extra packages slice and creates a tagged  expansion address string
// suffixed with classpath separator and service module provided.
func AddExtraPackages(packages []string) ExpansionServiceOption {
	return func(expansionAddress *string) {
		*expansionAddress += ClasspathSeparator + strings.Join(packages, " ")
	}
}

// UseAutomatedJavaExpansionService takes a gradle target and creates a
// tagged string to indicate that it should be used to start up an
// automated expansion service for a cross-language expansion.
//
// Intended for use by cross language wrappers to permit spinning
// up an expansion service for a user if no expansion service address
// is provided.
func UseAutomatedJavaExpansionService(gradleTarget string, opts ...ExpansionServiceOption) string {
	expansionAddress := autoJavaNamespace + Separator + gradleTarget

	for _, opt := range opts {
		opt(&expansionAddress)
	}
	return expansionAddress
}

// UseAutomatedPythonExpansionService takes a expansion service module name and creates a
// tagged string to indicate that it should be used to start up an
// automated expansion service for a cross-language expansion.
//
// Intended for use by cross language wrappers to permit spinning
// up an expansion service for a user if no expansion service address
// is provided.
func UseAutomatedPythonExpansionService(service string, opts ...ExpansionServiceOption) string {
	expansionAddress := autoPythonNamespace + Separator + service

	for _, opt := range opts {
		opt(&expansionAddress)
	}
	return expansionAddress
}

// restricted namespaces to prevent some awkward edge cases.
var restricted = map[string]struct{}{
	hardOverrideNamespace: {}, // Special handler for overriding.
	autoJavaNamespace:     {}, // Special handler for automated Java expansion services.
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

func parseClasspath(expansionAddr string) (string, string) {
	split := strings.SplitN(expansionAddr, ClasspathSeparator, 2)
	if len(split) == 1 {
		return expansionAddr, ""
	}
	return split[0], split[1]
}

func parseExtraPackages(expansionAddr string) (string, string) {
	return parseClasspath(expansionAddr)
}
