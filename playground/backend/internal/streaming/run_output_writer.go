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

package streaming

import (
	"beam.apache.org/playground/backend/internal/cache"
	"context"
	"fmt"
	"github.com/google/uuid"
)

// RunOutputWriter is used to write the run step's output to cache as a stream.
type RunOutputWriter struct {
	Ctx          context.Context
	CacheService cache.Cache
	PipelineId   uuid.UUID
}

// Write writes len(p) bytes from p to cache with cache.RunOutput subKey.
// In case some error occurs - returns (0, error).
// In case finished with no error - returns (len(p), nil).
//
// As a result new bytes will be added to cache with old run output value.
// Example:
//
//	p = []byte(" with new run output")
//	before Write(p): {pipelineId}:cache.RunOutput = "old run output"
//	after Write(p): {pipelineId}:cache.RunOutput = "old run output with new run output"
func (row *RunOutputWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}

	prevOutput, err := row.CacheService.GetValue(row.Ctx, row.PipelineId, cache.RunOutput)
	if err != nil {
		customErr := fmt.Errorf("error during saving output: %s", err)
		return 0, customErr
	}

	// concat prevValue and new value
	str := fmt.Sprintf("%s%s", prevOutput.(string), string(p))

	// set new cache value
	err = row.CacheService.SetValue(row.Ctx, row.PipelineId, cache.RunOutput, str)
	if err != nil {
		customErr := fmt.Errorf("error during saving output: %s", err)
		return 0, customErr
	}
	return len(p), nil
}
