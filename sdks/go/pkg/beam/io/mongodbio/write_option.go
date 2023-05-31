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

package mongodbio

import (
	"errors"
)

// WriteOption represents options for writing to MongoDB.
type WriteOption struct {
	BatchSize int64
	Ordered   bool
}

// WriteOptionFn is a function that configures a WriteOption.
type WriteOptionFn func(option *WriteOption) error

// WithWriteBatchSize configures the WriteOption to use the provided batch size when writing
// documents.
func WithWriteBatchSize(batchSize int64) WriteOptionFn {
	return func(o *WriteOption) error {
		if batchSize <= 0 {
			return errors.New("batch size must be greater than 0")
		}

		o.BatchSize = batchSize
		return nil
	}
}

// WithWriteOrdered configures the WriteOption whether to apply an ordered bulk write.
func WithWriteOrdered(ordered bool) WriteOptionFn {
	return func(o *WriteOption) error {
		o.Ordered = ordered
		return nil
	}
}
