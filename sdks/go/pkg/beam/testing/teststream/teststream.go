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

// Package teststream contains code configuring the TestStream primitive for
// use in testing code that is meant to be run on streaming data sources.
//
// See https://beam.apache.org/blog/test-stream/ for more information.
//
// TestStream is supported on the Flink, and Prism runners.
// Use on Flink currently supports int64, float64, and boolean types, while
// Prism supports arbitrary types.
package teststream

import (
	"bytes"
	"fmt"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/mtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/protox"

	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
)

const urn = "beam:transform:teststream:v1"

// Config holds information used to create a TestStreamPayload object.
type Config struct {
	elmType   beam.FullType
	events    []*pipepb.TestStreamPayload_Event
	endpoint  *pipepb.ApiServiceDescriptor
	watermark int64
}

// NewConfig returns a Config to build a sequence of a test stream's events.
// Requires that users provide the coder for the elements they are trying to emit.
func NewConfig() Config {
	return Config{elmType: nil,
		events:    []*pipepb.TestStreamPayload_Event{},
		endpoint:  &pipepb.ApiServiceDescriptor{},
		watermark: mtime.MinTimestamp.Milliseconds(),
	}
}

// SetEndpoint sets a URL for a TestStreamService that will emit events instead of having them
// defined manually. Currently does not support authentication, so the TestStreamService should
// be accessed in a trusted context.
func (c *Config) setEndpoint(url string) {
	c.endpoint.Url = url
}

// createPayload converts the Config object into a TestStreamPayload to be sent to the runner.
func (c *Config) createPayload() *pipepb.TestStreamPayload {
	// c0 is always the first coder in the pipeline, and inserting the TestStream as the first
	// element in the pipeline guarantees that the c0 coder corresponds to the type it outputs.
	return &pipepb.TestStreamPayload{CoderId: "c0", Events: c.events, Endpoint: c.endpoint}
}

// AdvanceWatermark adds an event to the Config Events struct advancing the watermark for the PCollection
// to the given timestamp. Timestamp is in milliseconds
func (c *Config) AdvanceWatermark(timestamp int64) error {
	if c.watermark >= timestamp {
		return fmt.Errorf("watermark must be monotonally increasing, is at %v, got %v", c.watermark, timestamp)
	}
	watermarkAdvance := &pipepb.TestStreamPayload_Event_AdvanceWatermark{NewWatermark: timestamp}
	watermarkEvent := &pipepb.TestStreamPayload_Event_WatermarkEvent{WatermarkEvent: watermarkAdvance}
	c.events = append(c.events, &pipepb.TestStreamPayload_Event{Event: watermarkEvent})
	c.watermark = timestamp
	return nil
}

// AdvanceWatermarkToInfinity advances the watermark to the maximum timestamp.
func (c *Config) AdvanceWatermarkToInfinity() error {
	return c.AdvanceWatermark(mtime.MaxTimestamp.Milliseconds())
}

// AdvanceProcessingTime adds an event advancing the processing time by a given duration.
// This advancement is applied to all of the PCollections output by the TestStream.
func (c *Config) AdvanceProcessingTime(duration int64) {
	processingAdvance := &pipepb.TestStreamPayload_Event_AdvanceProcessingTime{AdvanceDuration: duration}
	processingEvent := &pipepb.TestStreamPayload_Event_ProcessingTimeEvent{ProcessingTimeEvent: processingAdvance}
	c.events = append(c.events, &pipepb.TestStreamPayload_Event{Event: processingEvent})
}

// AdvanceProcessingTimeToInfinity moves the TestStream processing time to the largest possible
// timestamp.
func (c *Config) AdvanceProcessingTimeToInfinity() {
	c.AdvanceProcessingTime(mtime.MaxTimestamp.Milliseconds())
}

// AddElements adds a number of elements to the stream at the specified event timestamp. Must be called with
// at least one element.
//
// On the first call, a type will be inferred from the passed in elements, which must be of all the same type.
// Type mismatches on this or subsequent calls will cause AddElements to return an error.
//
// Element types must have built-in coders in Beam.
func (c *Config) AddElements(timestamp int64, elements ...any) error {
	t := reflect.TypeOf(elements[0])
	if c.elmType == nil {
		c.elmType = typex.New(t)
	} else if c.elmType.Type() != t {
		return fmt.Errorf("element type mismatch, previous additions were of type %v, tried to add type %v", c.elmType, t)
	}
	for i, ele := range elements {
		if reflect.TypeOf(ele) != c.elmType.Type() {
			return fmt.Errorf("element %d was type %T, previous additions were of type %v", i, ele, c.elmType)
		}
	}
	newElements := []*pipepb.TestStreamPayload_TimestampedElement{}
	enc := beam.NewElementEncoder(t)
	for _, e := range elements {
		var buf bytes.Buffer
		if err := enc.Encode(e, &buf); err != nil {
			return fmt.Errorf("encoding value %v failed, got %v", e, err)
		}
		newElements = append(newElements, &pipepb.TestStreamPayload_TimestampedElement{EncodedElement: buf.Bytes(), Timestamp: timestamp})
	}
	addElementsEvent := &pipepb.TestStreamPayload_Event_AddElements{Elements: newElements}
	elementEvent := &pipepb.TestStreamPayload_Event_ElementEvent{ElementEvent: addElementsEvent}
	c.events = append(c.events, &pipepb.TestStreamPayload_Event{Event: elementEvent})
	return nil
}

// AddElementList inserts a slice of elements into the stream at the specified event timestamp. Must be called with
// at least one element.
//
// Calls into AddElements, which panics if an inserted type does not match a previously inserted element type.
//
// Element types must have built-in coders in Beam.
func (c *Config) AddElementList(timestamp int64, elements any) error {
	val := reflect.ValueOf(elements)
	if val.Kind() != reflect.Slice && val.Kind() != reflect.Array {
		return fmt.Errorf("input %v must be a slice or array", elements)
	}

	var inputs []any
	for i := 0; i < val.Len(); i++ {
		inputs = append(inputs, val.Index(i).Interface())
	}
	return c.AddElements(timestamp, inputs...)
}

// Create inserts a TestStream primitive into a pipeline, taking a scope and a Config object and
// producing an output PCollection. The TestStream must be the first PTransform in the
// pipeline.
func Create(s beam.Scope, c Config) beam.PCollection {
	pyld := protox.MustEncode(c.createPayload())
	outputs := []beam.FullType{c.elmType}

	output := beam.External(s, urn, pyld, []beam.PCollection{}, outputs, false)

	// This should only ever contain one PCollection.
	return output[0]
}

// CreateWithEndpoint inserts a TestStream primitive into a pipeline, taking a scope, a url to a
// TestStreamService, and a FullType object describing the elements that will be returned by the
// TestStreamService. Authentication is currently not supported, so the service the URL points to
// should be accessed in a trusted context.
func CreateWithEndpoint(s beam.Scope, url string, elementType beam.FullType) beam.PCollection {
	c := NewConfig()
	c.setEndpoint(url)
	c.elmType = elementType
	return Create(s, c)
}
