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
	"time"

	"github.com/google/uuid"

	pb "beam.apache.org/playground/backend/internal/api/v1"
	"beam.apache.org/playground/backend/internal/db/entity"
)

// SubKey is used to keep value with Cache using nested structure like pipelineId:subKey:value
type SubKey string

// All possible subKeys and string keys to process with Cache
const (
	// Status is used to keep playground.Status value
	Status SubKey = "STATUS"

	// RunOutput is used to keep run code output value
	RunOutput SubKey = "RUN_OUTPUT"

	// RunError is used to keep run code error value
	RunError SubKey = "RUN_ERROR"

	// ValidationOutput is used to keep validation output value
	ValidationOutput SubKey = "VALIDATION_OUTPUT"

	// PreparationOutput is used to keep prepare step output value
	PreparationOutput SubKey = "PREPARATION_OUTPUT"

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

	// Graph is used to keep graph of the execution
	Graph SubKey = "GRAPH"

	// ExamplesCatalog is catalog of examples available in Playground
	ExamplesCatalog string = "EXAMPLES_CATALOG"

	// SdksCatalog is the catalog of SDKs in Playground
	SdksCatalog string = "SDKS_CATALOG"

	// DefaultPrecompiledExamples is used to keep default examples
	DefaultPrecompiledExamples string = "DEFAULT_PRECOMPILED_OBJECTS"
)

// Cache is used to store states and outputs for Apache Beam pipelines that running in Playground
// Cache allows keep and read any value by pipelineId and subKey:
// pipelineId_1:
//
//	subKey_1: value_1
//	subKey_2: value_2
//
// pipelineId_2:
//
//	subKey_1: value_3
//	subKey_3: value_4
//
// pipelineId is uuid that calculates in the controller when the server takes new request to run code
type Cache interface {
	// GetValue returns value from cache by pipelineId and subKey.
	GetValue(ctx context.Context, pipelineId uuid.UUID, subKey SubKey) (interface{}, error)

	// SetValue adds value to cache by pipelineId and subKey.
	SetValue(ctx context.Context, pipelineId uuid.UUID, subKey SubKey, value interface{}) error

	// SetExpTime adds expiration time of the pipeline to cache by pipelineId.
	SetExpTime(ctx context.Context, pipelineId uuid.UUID, expTime time.Duration) error

	// SetCatalog adds the given catalog to cache by ExamplesCatalog key.
	SetCatalog(ctx context.Context, catalog []*pb.Categories) error

	// GetCatalog returns catalog from cache by ExamplesCatalog key.
	GetCatalog(ctx context.Context) ([]*pb.Categories, error)

	// SetDefaultPrecompiledObject adds default precompiled object for SDK into cache.
	SetDefaultPrecompiledObject(ctx context.Context, sdk pb.Sdk, precompiledObject *pb.PrecompiledObject) error

	// GetDefaultPrecompiledObject returns default precompiled object for SDK from cache.
	GetDefaultPrecompiledObject(ctx context.Context, sdk pb.Sdk) (*pb.PrecompiledObject, error)

	// SetSdkCatalog adds the given sdk catalog to the cache.
	SetSdkCatalog(ctx context.Context, sdks []*entity.SDKEntity) error

	// GetSdkCatalog returns sdk catalog from the cache.
	GetSdkCatalog(ctx context.Context) ([]*entity.SDKEntity, error)
}
