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

// Package config defines and handles the parsing and provision of configurations
// for the runner. This package should be refered to, and should not take dependencies
// on other parts of this runner.
//
// 1. A given configuation file has one or more variations configured.
// 2. Each variation has a name, and one or more handlers configured.
// 3. Each handler maps to a specific struct.
//
//	 <variation1 name>:
//		  <handler1 name>:
//		    <handler1 characteristics>
//		  <handler2 name>:
//		    <handler2 characteristics>
//
//	 <variation2 name>:
//		  <handler1 name>:
//		    <handler1 characteristics>
//		  <handler2 name>:
//		    <handler2 characteristics>
//
// Handler has it's own name, and an associated characterisitc type.
package config

import (
	"bytes"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"golang.org/x/exp/maps"
	"gopkg.in/yaml.v3"
)

// configFile is the struct configs are decoded into by YAML.
// This represents the whole configuration file.
type configFile struct {
	Version      int
	HandlerOrder []string
	Default      string                 // reserved for laer
	Variants     map[string]*rawVariant `yaml:",inline"`
}

// rawVariant holds an individual Variant's handlers,
// and any common fields as decoded by YAML.
type rawVariant struct {
	HandlerOrder []string
	Handlers     map[string]yaml.Node `yaml:",inline"`
}

// HandlerMetadata is required information about handler configurations.
// Handlers have an URN, which key for how configurations refer to them,
// and a Characteristic type, which is it's own individual configuration.
//
// Characteristic types must have useful zero values, representing the
// default configuration for the handler.
type HandlerMetadata interface {
	// ConfigURN represents the urn for the handle.
	ConfigURN() string

	// ConfigCharacteristic returns the type of the detailed configuration for the handler.
	// A characteristic type must have a useful zero value that defines the default behavior.
	ConfigCharacteristic() reflect.Type
}

type unknownHandlersErr struct {
	handlersToVariants map[string][]string
}

func (e *unknownHandlersErr) valid() bool {
	return e.handlersToVariants != nil
}

func (e *unknownHandlersErr) add(handler, variant string) {
	if e.handlersToVariants == nil {
		e.handlersToVariants = map[string][]string{}
	}
	vs := e.handlersToVariants[handler]
	vs = append(vs, variant)
	e.handlersToVariants[handler] = vs
}

func (e *unknownHandlersErr) Error() string {
	var sb strings.Builder
	sb.WriteString("yaml config contained unknown handlers")
	for h, vs := range e.handlersToVariants {
		sort.Strings(vs)
		sb.WriteString("\n\t")
		sb.WriteString(h)
		sb.WriteString(" present in variants ")
		sb.WriteString(strings.Join(vs, ","))
	}
	return sb.String()
}

// Variant represents a single complete configuration of all handlers in the registry.
type Variant struct {
	parent *HandlerRegistry

	name     string
	handlers map[string]yaml.Node
}

// GetCharacteristics returns the characteristics of this handler within this variant.
//
// If the variant doesn't configure this handler, the zero value of the handler characteristic
// type will be returned. If the handler is unknown to the registry this variant came from,
// a nil will be returned.
func (v *Variant) GetCharacteristics(handler string) any {
	if v == nil {
		return nil
	}
	md, ok := v.parent.metadata[handler]
	if !ok {
		return nil
	}
	rt := md.ConfigCharacteristic()

	// Get a pointer to the concrete value.
	rtv := reflect.New(rt)

	// look up the handler urn in the variant.
	yn := v.handlers[handler]
	//
	if err := yn.Decode(rtv.Interface()); err != nil {
		// We prevalidated the config, so this shouldn't happen.
		panic(fmt.Sprintf("couldn't decode characteristic for variant %v handler %v: %v", v.name, handler, err))
	}

	// Return the value pointed to by the pointer.
	return rtv.Elem().Interface()
}

// HandlerRegistry stores known handlers and their associated metadata needed to parse
// the YAML configuration.
type HandlerRegistry struct {
	variations map[string]*rawVariant
	metadata   map[string]HandlerMetadata

	// cached names
	variantIDs, handerIDs []string
}

// NewHandlerRegistry creates an initialized HandlerRegistry.
func NewHandlerRegistry() *HandlerRegistry {
	return &HandlerRegistry{
		variations: map[string]*rawVariant{},
		metadata:   map[string]HandlerMetadata{},
	}
}

// RegisterHandlers is about registering the metadata for handler configurations.
func (r *HandlerRegistry) RegisterHandlers(mds ...HandlerMetadata) {
	for _, md := range mds {
		r.metadata[md.ConfigURN()] = md
	}
}

// LoadFromYaml takes in a yaml formatted configuration and eagerly processes it for errors.
//
// All handlers are validated against their registered characteristic, and it is an error
// to have configurations for unknown handlers
func (r *HandlerRegistry) LoadFromYaml(in []byte) error {
	vs := configFile{Variants: r.variations}
	buf := bytes.NewBuffer(in)
	d := yaml.NewDecoder(buf)
	if err := d.Decode(&vs); err != nil {
		return err
	}

	err := &unknownHandlersErr{}
	handlers := map[string]struct{}{}
	for v, hs := range r.variations {
		for hk, hyn := range hs.Handlers {
			handlers[hk] = struct{}{}

			md, ok := r.metadata[hk]
			if !ok {
				err.add(hk, v)
				continue
			}

			// Validate that handler config so we can give a good error message now.
			// We re-encode, then decode, since then we don't need to re-implement
			// the existing Known fields. Sadly, this doens't persist through
			// yaml.Node fields.
			hb, err := yaml.Marshal(hyn)
			if err != nil {
				panic(fmt.Sprintf("error re-encoding characteristic for variant %v handler %v: %v", v, hk, err))
			}
			buf := bytes.NewBuffer(hb)
			dec := yaml.NewDecoder(buf)
			dec.KnownFields(true)
			rt := md.ConfigCharacteristic()
			rtv := reflect.New(rt)
			if err := dec.Decode(rtv.Interface()); err != nil {
				return fmt.Errorf("error decoding characteristic strictly for variant %v handler %v: %v", v, hk, err)
			}

		}
	}

	if err.valid() {
		return err
	}

	r.variantIDs = maps.Keys(r.variations)
	sort.Strings(r.variantIDs)
	r.handerIDs = maps.Keys(handlers)
	sort.Strings(r.handerIDs)
	return nil
}

// Variants returns the IDs of all variations loaded into this registry.
func (r *HandlerRegistry) Variants() []string {
	return r.variantIDs
}

// Handlers returns the IDs of all handlers used in variations.
func (r *HandlerRegistry) UsedHandlers() []string {
	return r.handerIDs
}

// GetVariant returns the Variant with the given name.
// If none exist, GetVariant returns nil.
func (r *HandlerRegistry) GetVariant(name string) *Variant {
	vs, ok := r.variations[name]
	if !ok {
		return nil
	}
	return &Variant{parent: r, name: name, handlers: vs.Handlers}
}
