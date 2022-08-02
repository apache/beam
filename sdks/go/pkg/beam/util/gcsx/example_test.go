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

package gcsx_test

import (
	"context"
	"time"

	"cloud.google.com/go/storage"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/util/gcsx"
)

func Example() {
	ctx := context.Background()
	c, err := gcsx.NewClient(ctx, storage.ScopeReadOnly)
	if err != nil {
		// do something
	}

	buckets, object, err := gcsx.ParseObject("gs://some-bucket/some-object")
	if err != nil {
		// do something
	}

	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	bytes, err := gcsx.ReadObject(ctx, c, buckets, object)
	if err != nil {
		// do something
	}

	_ = bytes
}
