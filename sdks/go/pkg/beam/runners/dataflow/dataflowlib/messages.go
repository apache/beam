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

package dataflowlib

import (
	"encoding/json"
	"fmt"

	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime"
	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/graphx"
	"google.golang.org/api/googleapi"
)

// newMsg creates a json-encoded RawMessage. Panics if encoding fails.
func newMsg(msg interface{}) googleapi.RawMessage {
	data, err := json.Marshal(msg)
	if err != nil {
		panic(err)
	}
	return googleapi.RawMessage(data)
}

// pipelineOptions models Job/Environment/SdkPipelineOptions
type pipelineOptions struct {
	DisplayData []*displayData     `json:"display_data,omitempty"`
	Options     interface{}        `json:"options,omitempty"`
	GoOptions   runtime.RawOptions `json:"beam:option:go_options:v1,omitempty"`
}

// NOTE(herohde) 2/9/2017: most of the v1b3 messages are weakly-typed json
// blobs. We manually add them here for convenient and safer use.

// userAgent models Job/Environment/UserAgent. Example value:
//    "userAgent": {
//        "name": "Apache Beam SDK for Python",
//        "version": "0.6.0.dev"
//    },
type userAgent struct {
	Name    string `json:"name,omitempty"`
	Version string `json:"version,omitempty"`
}

// version models Job/Environment/Version. Example value:
//    "version": {
//       "job_type": "PYTHON_BATCH",
//       "major": "5"
//    },
type version struct {
	JobType string `json:"job_type,omitempty"`
	Major   string `json:"major,omitempty"`
}

// properties models Step/Properties. Note that the valid subset of fields
// depend on the step kind.
type properties struct {
	UserName    string        `json:"user_name,omitempty"`
	DisplayData []displayData `json:"display_data,omitempty"`

	// UserFn string  `json:"user_fn,omitempty"`

	CustomSourceInputStep   *customSourceInputStep      `json:"custom_source_step_input,omitempty"`  // Source
	DisallowCombinerLifting bool                        `json:"disallow_combiner_lifting,omitempty"` // GBK.
	Element                 []string                    `json:"element,omitempty"`                   // Impulse
	Encoding                *graphx.CoderRef            `json:"encoding,omitempty"`                  // Combine (accumulator coder).
	Format                  string                      `json:"format,omitempty"`                    // Source
	Inputs                  []*outputReference          `json:"inputs,omitempty"`                    // Flatten.
	NonParallelInputs       map[string]*outputReference `json:"non_parallel_inputs,omitempty"`       // ParDo
	OutputInfo              []output                    `json:"output_info,omitempty"`               // Source, ParDo, GBK, Flatten, Combine, WindowInto
	ParallelInput           *outputReference            `json:"parallel_input,omitempty"`            // ParDo, GBK, Flatten, Combine, WindowInto
	RestrictionEncoder      *graphx.CoderRef            `json:"restriction_encoding,omitempty"`      // ParDo (Splittable DoFn)
	SerializedFn            string                      `json:"serialized_fn,omitempty"`             // ParDo, GBK, Combine, WindowInto

	PubSubTopic          string `json:"pubsub_topic,omitempty"`           // Read,Write
	PubSubSubscription   string `json:"pubsub_subscription,omitempty"`    // Read,Write
	PubSubIDLabel        string `json:"pubsub_id_label,omitempty"`        // Read,Write
	PubSubTimestampLabel string `json:"pubsub_timestamp_label,omitempty"` // Read,Write

	// This special property triggers whether the below struct should be used instead.
	PubSubWithAttributes bool `json:"pubsub_with_attributes,omitempty"`
}

type propertiesWithPubSubMessage struct {
	properties
	PubSubSerializedAttributesFn string `json:"pubsub_serialized_attributes_fn"` // Read,Write
}

type output struct {
	UserName         string           `json:"user_name,omitempty"`
	OutputName       string           `json:"output_name,omitempty"`
	Encoding         *graphx.CoderRef `json:"encoding,omitempty"`
	UseIndexedFormat bool             `json:"use_indexed_format,omitempty"`
}

type integer struct {
	Type  string `json:"@type,omitempty"` // "http://schema.org/Integer"
	Value int    `json:"value,omitempty"`
}

func newInteger(value int) *integer {
	return &integer{
		Type:  "http://schema.org/Integer",
		Value: value,
	}
}

type customSourceInputStep struct {
	Spec     customSourceInputStepSpec      `json:"spec"`
	Metadata *customSourceInputStepMetadata `json:"metadata,omitempty"`
}

type customSourceInputStepSpec struct {
	Type             string `json:"@type,omitempty"` // "CustomSourcesType"
	SerializedSource string `json:"serialized_source,omitempty"`
}

type customSourceInputStepMetadata struct {
	EstimatedSizeBytes *integer `json:"estimated_size_bytes,omitempty"`
}

func newCustomSourceInputStep(serializedSource string) *customSourceInputStep {
	return &customSourceInputStep{
		Spec: customSourceInputStepSpec{
			Type:             "CustomSourcesType",
			SerializedSource: serializedSource,
		},
		Metadata: &customSourceInputStepMetadata{
			EstimatedSizeBytes: newInteger(5 << 20), // 5 MB
		},
	}
}

type outputReference struct {
	Type       string `json:"@type,omitempty"` // "OutputReference"
	StepName   string `json:"step_name,omitempty"`
	OutputName string `json:"output_name,omitempty"`
}

func newOutputReference(step, output string) *outputReference {
	return &outputReference{
		Type:       "OutputReference",
		StepName:   step,
		OutputName: output,
	}
}

type displayData struct {
	Key        string      `json:"key,omitempty"`
	Label      string      `json:"label,omitempty"`
	Namespace  string      `json:"namespace,omitempty"`
	ShortValue string      `json:"shortValue,omitempty"`
	Type       string      `json:"type,omitempty"`
	Value      interface{} `json:"value,omitempty"`
}

func findDisplayDataType(value interface{}) (string, interface{}) {
	switch value.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return "INTEGER", value
	case bool:
		return "BOOLEAN", value
	case string:
		return "STRING", value
	default:
		return "STRING", fmt.Sprintf("%v", value)
	}
}

func newDisplayData(key, label, namespace string, value interface{}) *displayData {
	t, v := findDisplayDataType(value)

	return &displayData{
		Key:       key,
		Label:     label,
		Namespace: namespace,
		Type:      t,
		Value:     v,
	}
}
