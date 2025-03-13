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

package utils

import (
	"beam.apache.org/playground/backend/internal/cache"
	"beam.apache.org/playground/backend/internal/logger"
	"context"
	"github.com/google/uuid"
)

// SetToCache puts value to cache by key and subKey.
// If error occurs during the function - logs and returns error.
func SetToCache(cacheService cache.Cache, key uuid.UUID, subKey cache.SubKey, value interface{}) error {
	// Background context is used in cache operations so the operation would not be interrupted
	// by the timeout or cancellation of the pipeline context. Cache timeouts are handled by
	// the cache client itself.
	err := cacheService.SetValue(context.Background(), key, subKey, value)
	if err != nil {
		logger.Errorf("%s: cache.SetValue: %s\n", key, err.Error())
		// TODO send email to fix error with writing to cache
	}
	return err
}
