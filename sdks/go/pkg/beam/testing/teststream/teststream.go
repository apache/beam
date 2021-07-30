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
// TestStream is not supported on the Go direct runner.
package teststream

import (
	"bytes"
	"fmt"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/mtime"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/protox"

	pipepb "github.com/apache/beam/sdks/go/pkg/beam/model/pipeline_v1"
)

const urn = "beam:transform:teststream:v1"

// Config holds information used to create a TestStreamPayload object.
type Config struct {
	ElmCoder  *coder.Coder
	Events    []*pipepb.TestStreamPayload_Event
	Endpoint  *pipepb.ApiServiceDescriptor
	Watermark int64
}

// MakeConfig initializes a Config struct to begin inserting TestStream events/endpoints into.
// Requires that users provide the coder for the elements they are trying to emit.
func MakeConfig(c *coder.Coder) Config {
	return Config{ElmCoder: c,
		Events:    []*pipepb.TestStreamPayload_Event{},
		Endpoint:  &pipepb.ApiServiceDescriptor{},
		Watermark: 0,
	}
}

// SetEndpoint sets a URL for a TestStreamService that will emit events instead of having them
// defined manually. Currently does not support authentication, so the TestStreamService should
// be accessed in a trusted context.
func (c *Config) SetEndpoint(url string) {
	c.Endpoint.Url = url
}

// CreatePayload converts the Config object into a TestStreamPayload to be sent to the runner.
func (c *Config) CreatePayload() *pipepb.TestStreamPayload {
	return &pipepb.TestStreamPayload{CoderId: "c0", Events: c.Events, Endpoint: c.Endpoint}
}

// AdvanceWatermark adds an event to the Config Events struct advancing the watermark for a PCollection
// to the given timestamp. if the tag is empty, this is applied to the default PCollection. Timestamp is
// in milliseconds
func (c *Config) AdvanceWatermark(timestamp int64) error {
	if c.Watermark >= timestamp {
		return fmt.Errorf("watermark must be monotonally increasing, is at %v, got %v", c.Watermark, timestamp)
	}
	watermarkAdvance := &pipepb.TestStreamPayload_Event_AdvanceWatermark{NewWatermark: timestamp}
	watermarkEvent := &pipepb.TestStreamPayload_Event_WatermarkEvent{WatermarkEvent: watermarkAdvance}
	c.Events = append(c.Events, &pipepb.TestStreamPayload_Event{Event: watermarkEvent})
	c.Watermark = timestamp
	return nil
}

// AdvanceWatermarkToInfinity advances the watermark for the PCollection corresponding to the tag
// to the maximum timestamp.
func (c *Config) AdvanceWatermarkToInfinity() error {
	return c.AdvanceWatermark(mtime.MaxTimestamp.Milliseconds())
}

// AdvanceProcessingTime adds an event into the Config Events struct advancing the processing time by a given
// duration. This advancement is applied to all of the PCollections output by the TestStream.
func (c *Config) AdvanceProcessingTime(duration int64) {
	processingAdvance := &pipepb.TestStreamPayload_Event_AdvanceProcessingTime{AdvanceDuration: duration}
	processingEvent := &pipepb.TestStreamPayload_Event_ProcessingTimeEvent{ProcessingTimeEvent: processingAdvance}
	c.Events = append(c.Events, &pipepb.TestStreamPayload_Event{Event: processingEvent})
}

// AdvanceProcessingTimeToInfinity moves the TestStream processing time to the largest possible
// timestamp.
func (c *Config) AdvanceProcessingTimeToInfinity() {
	c.AdvanceProcessingTime(mtime.MaxTimestamp.Milliseconds())
}

// AddElements adds a number of elements to the Config object at the specified timestamp.
// The encoder will panic if there is a type mismatch between the provided coder and the
// elements.
func (c *Config) AddElements(timestamp int64, elements ...interface{}) error {
	newElements := []*pipepb.TestStreamPayload_TimestampedElement{}
	enc := beam.NewElementEncoder(c.ElmCoder.T.Type())
	for _, e := range elements {
		var buf bytes.Buffer
		if err := enc.Encode(e, &buf); err != nil {
			return fmt.Errorf("encoding value %v failed, got %v", e, err)
		}
		newElements = append(newElements, &pipepb.TestStreamPayload_TimestampedElement{EncodedElement: buf.Bytes(), Timestamp: timestamp})
	}
	addElementsEvent := &pipepb.TestStreamPayload_Event_AddElements{Elements: newElements}
	elementEvent := &pipepb.TestStreamPayload_Event_ElementEvent{ElementEvent: addElementsEvent}
	c.Events = append(c.Events, &pipepb.TestStreamPayload_Event{Event: elementEvent})
	return nil
}

// TestStream inserts a TestStream primitive into a pipeline, taking a scope and a Config object and
// producing an array of output PCollections.
func TestStream(s beam.Scope, c Config) []beam.PCollection {
	pyld := protox.MustEncode(c.CreatePayload())
	outputs := []beam.FullType{c.ElmCoder.T}

	outputMap := beam.External(s, urn, pyld, []beam.PCollection{}, outputs, false)

	var ret []beam.PCollection
	for _, val := range outputMap {
		ret = append(ret, val)
	}
	return ret
}
