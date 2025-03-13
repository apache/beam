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

package spannerio

import (
	"cloud.google.com/go/spanner"
	"errors"
	"fmt"
)

const (
	defaultBatching = true
)

// QueryOptionFn is a function that can be passed to Read or Query to configure options for reading or querying spanner.
type QueryOptionFn func(*queryOptions) error

// queryOptions represents additional options for executing a query.
type queryOptions struct {
	Batching       bool                   `json:"batching"`       // Batched reading, default is true.
	MaxPartitions  int64                  `json:"maxPartitions"`  // Maximum partitions
	TimestampBound spanner.TimestampBound `json:"timestampBound"` // The TimestampBound to use for batched reading
}

func newQueryOptions(options ...QueryOptionFn) queryOptions {
	opts := queryOptions{
		Batching: defaultBatching,
	}

	for _, opt := range options {
		if err := opt(&opts); err != nil {
			panic(fmt.Sprintf("spannerio.Query: invalid option: %v", err))
		}
	}

	return opts
}

// WithBatching sets whether we will use a batched reader. Batching is set to true by default, disable it when the
// underlying query is not root-partitionable.
func WithBatching(batching bool) QueryOptionFn {
	return func(opts *queryOptions) error {
		opts.Batching = batching
		return nil
	}
}

// WithMaxPartitions sets the maximum number of Partitions to split the query into when batched reading.
func WithMaxPartitions(maxPartitions int64) QueryOptionFn {
	return func(opts *queryOptions) error {
		if maxPartitions <= 0 {
			return errors.New("max partitions must be greater than 0")
		}

		opts.MaxPartitions = maxPartitions
		return nil
	}
}

// WithTimestampBound sets the TimestampBound to use when doing batched reads.
func WithTimestampBound(timestampBound spanner.TimestampBound) QueryOptionFn {
	return func(opts *queryOptions) error {
		opts.TimestampBound = timestampBound
		return nil
	}
}
