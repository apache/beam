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

package cache

import (
	"context"
	"github.com/google/uuid"
	"time"
)

// SubKey is used to keep value with Cache using nested structure like pipelineId:subKey:value
type SubKey string

// All possible subKeys to process with Cache
const (
	// Status is used to keep playground.Status value
	Status SubKey = "STATUS"

	// RunOutput is used to keep run code output value
	RunOutput SubKey = "RUN_OUTPUT"

	// RunError is used to keep run code error value
	RunError SubKey = "RUN_ERROR"

	// CompileOutput is used to keep compilation output value
	CompileOutput SubKey = "COMPILE_OUTPUT"

	// Canceled is used to keep the canceled status
	Canceled SubKey = "CANCELED"

	// RunOutputIndex is the index of the start of the run step's output
	RunOutputIndex SubKey = "RUN_OUTPUT_INDEX"

	// Logs is used to keep logs value
	Logs SubKey = "LOGS"

	// LogsIndex is the index of the start of the log
	LogsIndex SubKey = "LOGS_INDEX"
)

// Cache is used to store states and outputs for Apache Beam pipelines that running in Playground
// Cache allows keep and read any value by pipelineId and subKey:
// pipelineId_1:
//				subKey_1: value_1
//				subKey_2: value_2
// pipelineId_2:
//				subKey_1: value_3
//				subKey_3: value_4
// pipelineId is uuid that calculates in the controller when the server takes new request to run code
type Cache interface {
	// GetValue returns value from cache by pipelineId and subKey.
	GetValue(ctx context.Context, pipelineId uuid.UUID, subKey SubKey) (interface{}, error)

	// SetValue adds value to cache by pipelineId and subKey.
	SetValue(ctx context.Context, pipelineId uuid.UUID, subKey SubKey, value interface{}) error

	// SetExpTime adds expiration time of the pipeline to cache by pipelineId.
	SetExpTime(ctx context.Context, pipelineId uuid.UUID, expTime time.Duration) error
}
