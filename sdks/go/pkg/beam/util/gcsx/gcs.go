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

// Package gcsx contains utilities for working with Google Cloud Storage (GCS).
package gcsx

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"

	"golang.org/x/oauth2/google"
	"google.golang.org/api/storage/v1"
)

// NewClient creates a new GCS client with default application credentials.
func NewClient(ctx context.Context, scope string) (*storage.Service, error) {
	cl, err := google.DefaultClient(ctx, scope)
	if err != nil {
		return nil, err
	}
	return storage.New(cl)
}

// WriteObject writes the given content to the specified object. If the object
// already exist, it is overwritten.
func WriteObject(client *storage.Service, bucket, object string, r io.Reader) error {
	obj := &storage.Object{
		Name:   object,
		Bucket: bucket,
	}
	_, err := client.Objects.Insert(bucket, obj).Media(r).Do()
	return err
}

// ReadObject reads the content of the given object in full.
func ReadObject(client *storage.Service, bucket, object string) ([]byte, error) {
	resp, err := client.Objects.Get(bucket, object).Download()
	if err != nil {
		return nil, err
	}
	return ioutil.ReadAll(resp.Body)
}

// MakeObject creates a object location from bucket and path. For example,
// MakeObject("foo", "bar/baz") returns "gs://foo/bar/baz". The bucket
// must be non-empty.
func MakeObject(bucket, path string) string {
	if bucket == "" {
		panic("bucket must be non-empty")
	}
	return fmt.Sprintf("gs://%v/%v", bucket, path)
}

// ParseObject deconstructs a GCS object name into (bucket, name).
func ParseObject(object string) (bucket, path string, err error) {
	parsed, err := url.Parse(object)
	if err != nil {
		return "", "", err
	}

	if parsed.Scheme != "gs" {
		return "", "", fmt.Errorf("object %s must have 'gs' scheme", object)
	}
	if parsed.Host == "" {
		return "", "", fmt.Errorf("object %s must have bucket", object)
	}
	if parsed.Path == "" {
		return parsed.Host, "", nil
	}

	// remove leading "/" in URL path
	return parsed.Host, parsed.Path[1:], nil
}
