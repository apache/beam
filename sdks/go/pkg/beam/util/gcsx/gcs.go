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

	"net/http"

	"golang.org/x/oauth2/google"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
	"google.golang.org/api/storage/v1"
	ghttp "google.golang.org/api/transport/http"
)

// NewClient creates a new GCS client with default application credentials.
func NewClient(ctx context.Context, scope string) (*storage.Service, error) {
	cl, err := google.DefaultClient(ctx, scope)
	if err != nil {
		return nil, err
	}
	return storage.New(cl)
}

// NewUnauthenticatedClient creates a new GCS client without authentication.
func NewUnauthenticatedClient(ctx context.Context) (*storage.Service, error) {
	cl, _, err := ghttp.NewClient(ctx, option.WithoutAuthentication())
	if err != nil {
		return nil, fmt.Errorf("dialing: %v", err)
	}
	return storage.New(cl)
}

// Upload writes the given content to GCS. If the specified bucket does not
// exist, it is created first. Returns the full path of the object.
func Upload(client *storage.Service, project, bucket, object string, r io.Reader) (string, error) {
	exists, err := BucketExists(client, bucket)
	if err != nil {
		return "", err
	}
	if !exists {
		if err = CreateBucket(client, project, bucket); err != nil {
			return "", err
		}
	}

	if err := WriteObject(client, bucket, object, r); err != nil {
		return "", err
	}
	return fmt.Sprintf("gs://%s/%s", bucket, object), nil
}

// CreateBucket creates a bucket in GCS.
func CreateBucket(client *storage.Service, project, bucket string) error {
	b := &storage.Bucket{
		Name: bucket,
	}
	_, err := client.Buckets.Insert(project, b).Do()
	return err
}

// BucketExists returns true iff the given bucket exists.
func BucketExists(client *storage.Service, bucket string) (bool, error) {
	_, err := client.Buckets.Get(bucket).Do()
	if e, ok := err.(*googleapi.Error); ok && e.Code == http.StatusNotFound {
		return false, nil
	}
	return err == nil, err
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
