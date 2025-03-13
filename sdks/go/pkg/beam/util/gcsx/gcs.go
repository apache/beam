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
	"net/url"
	"path"

	"cloud.google.com/go/storage"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
	"google.golang.org/api/option"
)

var userAgent = option.WithUserAgent("(GPN:Beam)")

// NewClient creates a new GCS client with default application credentials, and supplied
// OAuth scope. The OAuth scopes are defined in https://pkg.go.dev/cloud.google.com/go/storage#pkg-constants.
// Sets the user agent to Beam.
func NewClient(ctx context.Context, scope string) (*storage.Client, error) {
	return storage.NewClient(ctx, option.WithScopes(scope), userAgent)
}

// NewUnauthenticatedClient creates a new GCS client without authentication.
// Sets the user agent to Beam.
func NewUnauthenticatedClient(ctx context.Context) (*storage.Client, error) {
	return storage.NewClient(ctx, option.WithoutAuthentication(), userAgent)
}

// Upload writes the given content to GCS. If the specified bucket does not
// exist, it is created first. Returns the full path of the object.
func Upload(ctx context.Context, client *storage.Client, project, bucket, object string, r io.Reader) (string, error) {
	exists, err := BucketExists(ctx, client, bucket)
	if err != nil {
		return "", err
	}
	if !exists {
		if err = CreateBucket(ctx, client, project, bucket); err != nil {
			return "", err
		}
	}

	if err := WriteObject(ctx, client, bucket, object, r); err != nil {
		return "", err
	}
	return fmt.Sprintf("gs://%s/%s", bucket, object), nil

}

// Get BucketAttrs with RetentionDuration of SoftDeletePolicy set to zero for disabling SoftDeletePolicy.
func getDisableSoftDeletePolicyBucketAttrs() *storage.BucketAttrs {
	attrs := &storage.BucketAttrs{
		SoftDeletePolicy: &storage.SoftDeletePolicy{
			RetentionDuration: 0,
		},
	}
	return attrs
}

// CreateBucket creates a bucket in GCS with RetentionDuration of zero to disable SoftDeletePolicy.
func CreateBucket(ctx context.Context, client *storage.Client, project, bucket string) error {
	disableSoftDeletePolicyBucketAttrs := getDisableSoftDeletePolicyBucketAttrs()
	return client.Bucket(bucket).Create(ctx, project, disableSoftDeletePolicyBucketAttrs)
}

// BucketExists returns true iff the given bucket exists.
func BucketExists(ctx context.Context, client *storage.Client, bucket string) (bool, error) {
	_, err := client.Bucket(bucket).Attrs(ctx)
	if err == storage.ErrBucketNotExist {
		return false, nil
	}
	return err == nil, err
}

// WriteObject writes the given content to the specified object. If the object
// already exist, it is overwritten.
func WriteObject(ctx context.Context, client *storage.Client, bucket, object string, r io.Reader) error {
	w := client.Bucket(bucket).Object(object).NewWriter(ctx)
	_, err := io.Copy(w, r)
	if err != nil {
		return err
	}
	return w.Close()
}

// ReadObject reads the content of the given object in full.
func ReadObject(ctx context.Context, client *storage.Client, bucket, object string) ([]byte, error) {
	r, err := client.Bucket(bucket).Object(object).NewReader(ctx)
	if err != nil {
		return nil, err
	}
	return io.ReadAll(r)
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
		return "", "", errors.Errorf("object %s must have 'gs' scheme", object)
	}
	if parsed.Host == "" {
		return "", "", errors.Errorf("object %s must have bucket", object)
	}
	if parsed.Path == "" {
		return parsed.Host, "", nil
	}

	// remove leading "/" in URL path
	return parsed.Host, parsed.Path[1:], nil
}

// Join joins a GCS path with an element. Preserves
// the gs:// prefix.
func Join(object string, elms ...string) string {
	bucket, prefix, err := ParseObject(object)
	if err != nil {
		panic(err)
	}
	return MakeObject(bucket, path.Join(prefix, path.Join(elms...)))
}
