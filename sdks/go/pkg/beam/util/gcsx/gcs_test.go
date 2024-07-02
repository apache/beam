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

package gcsx

import (
	"strings"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
)

func TestMakeObject(t *testing.T) {
	if got, want := MakeObject("some-bucket", "some/path"), "gs://some-bucket/some/path"; got != want {
		t.Fatalf("MakeObject() Got: %v Want: %v", got, want)
	}
}

func TestParseObject(t *testing.T) {
	tests := []struct {
		object string
		bucket string
		path   string
		err    error
	}{
		{
			object: "gs://some-bucket/some-object",
			bucket: "some-bucket",
			path:   "some-object",
			err:    nil,
		},
		{
			object: "gs://some-bucket",
			bucket: "some-bucket",
			path:   "",
			err:    nil,
		},
		{
			object: "gs://",
			bucket: "",
			path:   "",
			err:    errors.Errorf("object gs:// must have bucket"),
		},
		{
			object: "other://some-bucket/some-object",
			bucket: "",
			path:   "",
			err:    errors.Errorf("object other://some-bucket/some-object must have 'gs' scheme"),
		},
	}

	for _, test := range tests {
		if bucket, path, err := ParseObject(test.object); bucket != test.bucket || path != test.path || (err != nil && test.err == nil) || (err == nil && test.err != nil) {
			t.Errorf("ParseObject(%v) Got: %v, %v, %v Want: %v, %v, %v", test.object, bucket, path, err, test.bucket, test.path, test.err)
		}
	}
}

func TestJoin(t *testing.T) {
	tests := []struct {
		object string
		elms   []string
		result string
	}{
		{
			object: "gs://some-bucket/some-object",
			elms:   []string{"some/path", "more/pathing"},
			result: "gs://some-bucket/some-object/some/path/more/pathing",
		},
		{
			object: "gs://some-bucket/some-object",
			elms:   []string{"some/path"},
			result: "gs://some-bucket/some-object/some/path",
		},
		{
			object: "gs://some-bucket/some-object",
			elms:   []string{},
			result: "gs://some-bucket/some-object",
		},
	}
	for _, test := range tests {
		if got, want := Join(test.object, test.elms...), test.result; got != want {
			t.Errorf("Join(%v, %v) Got: %v Want: %v", test.object, strings.Join(test.elms, ", "), got, want)
		}
	}
}

func TestGetDisableSoftDeletePolicyBucketAttrs(t *testing.T) {
	attrs := getDisableSoftDeletePolicyBucketAttrs()
	if attrs == nil {
		t.Errorf("Fail to getDisableSoftDeletePolicyBucketAttrs.")
	}
	if attrs != nil && attrs.SoftDeletePolicy.RetentionDuration != 0 {
		t.Errorf("attrs has RetentionDuration %v which is not correct", attrs.SoftDeletePolicy.RetentionDuration)
	}
}
