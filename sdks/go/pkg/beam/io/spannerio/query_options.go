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

// queryOptions represents additional options for executing a query.
type queryOptions struct {
	Batching       bool                   `json:"batching""`      // Batched reading, default is true.
	MaxPartitions  int64                  `json:"maxPartitions"`  // Maximum partitions
	TimestampBound spanner.TimestampBound `json:"timestampBound"` // The TimestampBound to use for batched reading
}

func newQueryOptions(options ...func(*queryOptions) error) queryOptions {
	opts := queryOptions{
		Batching: true,
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
func WithBatching(batching bool) func(opts *queryOptions) error {
	return func(opts *queryOptions) error {
		opts.Batching = batching
		return nil
	}
}

// WithMaxPartitions sets the maximum number of Partitions to split the query into when batched reading.
func WithMaxPartitions(maxPartitions int64) func(opts *queryOptions) error {
	return func(opts *queryOptions) error {
		if maxPartitions <= 0 {
			return errors.New("max partitions must be greater than 0")
		}

		opts.MaxPartitions = maxPartitions
		return nil
	}
}

// WithTimestampBound sets the TimestampBound to use when doing batched reads.
func WithTimestampBound(timestampBound spanner.TimestampBound) func(opts *queryOptions) error {
	return func(opts *queryOptions) error {
		opts.TimestampBound = timestampBound
		return nil
	}
}
