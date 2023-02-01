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

	"go.mongodb.org/mongo-driver/bson"
)

// ReadOption represents options for reading from MongoDB.
type ReadOption struct {
	BucketAuto bool
	Filter     bson.M
	BundleSize int64
}

// ReadOptionFn is a function that configures a ReadOption.
type ReadOptionFn func(option *ReadOption) error

// WithReadBucketAuto configures the ReadOption whether to use the bucketAuto aggregation stage.
func WithReadBucketAuto(bucketAuto bool) ReadOptionFn {
	return func(o *ReadOption) error {
		o.BucketAuto = bucketAuto
		return nil
	}
}

// WithReadFilter configures the ReadOption to use the provided filter.
func WithReadFilter(filter bson.M) ReadOptionFn {
	return func(o *ReadOption) error {
		o.Filter = filter
		return nil
	}
}

// WithReadBundleSize configures the ReadOption to use the provided bundle size in bytes.
func WithReadBundleSize(bundleSize int64) ReadOptionFn {
	return func(o *ReadOption) error {
		if bundleSize <= 0 {
			return errors.New("bundle size must be greater than 0")
		}

		o.BundleSize = bundleSize
		return nil
	}
}
