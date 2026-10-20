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
	"os"
	"path"
	"sync"

	"cloud.google.com/go/storage"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/options/gcpopts"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
)

var userAgent = option.WithUserAgent("(GPN:Beam)")

const (
	defaultUniverseDomain = "googleapis.com"
	universeDomainEnvVar  = "GOOGLE_CLOUD_UNIVERSE_DOMAIN"
)

// findDefaultCredentials is a variable to allow overriding in tests.
var findDefaultCredentials = google.FindDefaultCredentials

// cachedUniverseDomain caches the universe domain resolved from the default
// credentials, since resolving it may require a metadata server round trip
// and GCS clients are created frequently.
var cachedUniverseDomain struct {
	mu       sync.Mutex
	resolved bool
	value    string
}

// NewClient creates a new GCS client with default application credentials, and supplied
// OAuth scope. The OAuth scopes are defined in https://pkg.go.dev/cloud.google.com/go/storage#pkg-constants.
// Sets the user agent to Beam.
//
// The endpoint can be overridden with the --gcs_endpoint flag. In non-default
// universes (e.g. Trusted Partner Cloud), the universe domain is taken from
// GOOGLE_CLOUD_UNIVERSE_DOMAIN or, if unset, from the default credentials.
func NewClient(ctx context.Context, scope string) (*storage.Client, error) {
	opts := []option.ClientOption{option.WithScopes(scope), userAgent}
	opts = append(opts, endpointOptions()...)
	if ud := universeDomain(ctx, scope); ud != "" && ud != defaultUniverseDomain {
		opts = append(opts, option.WithUniverseDomain(ud))
	}
	return storage.NewClient(ctx, opts...)
}

// NewUnauthenticatedClient creates a new GCS client without authentication.
// Sets the user agent to Beam.
func NewUnauthenticatedClient(ctx context.Context) (*storage.Client, error) {
	opts := []option.ClientOption{option.WithoutAuthentication(), userAgent}
	opts = append(opts, endpointOptions()...)
	return storage.NewClient(ctx, opts...)
}

// endpointOptions returns the client options for the --gcs_endpoint override,
// if any.
func endpointOptions() []option.ClientOption {
	if ep := gcpopts.GetGcsEndpoint(); ep != "" {
		return []option.ClientOption{option.WithEndpoint(ep)}
	}
	return nil
}

// universeDomain returns the universe domain to use for GCS clients.
//
// storage.NewClient only reads the universe domain from
// option.WithUniverseDomain or GOOGLE_CLOUD_UNIVERSE_DOMAIN; it ignores the
// credentials' universe domain, and requests fail if the latter differs from
// the configured one (googleapis.com by default). Resolve it explicitly to
// support non-default universes such as Trusted Partner Cloud (TPC).
//
// Returns an empty string if it cannot be determined, leaving the decision to
// the client library.
func universeDomain(ctx context.Context, scope string) string {
	if ud := os.Getenv(universeDomainEnvVar); ud != "" {
		return ud
	}

	cachedUniverseDomain.mu.Lock()
	defer cachedUniverseDomain.mu.Unlock()
	if cachedUniverseDomain.resolved {
		return cachedUniverseDomain.value
	}
	creds, err := findDefaultCredentials(ctx, scope)
	if err != nil {
		// Don't cache: credentials may become available later, and
		// storage.NewClient surfaces the credential error itself.
		return ""
	}
	ud, err := creds.GetUniverseDomain()
	if err != nil {
		return ""
	}
	cachedUniverseDomain.resolved = true
	cachedUniverseDomain.value = ud
	return ud
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

var getBucketAttrs = func(ctx context.Context, client *storage.Client, bucketName string) (*storage.BucketAttrs, error) {
	return client.Bucket(bucketName).Attrs(ctx)
}

// SoftDeletePolicyEnabled returns true if SoftDeletePolicy is enabled on bucket
func SoftDeletePolicyEnabled(ctx context.Context, client *storage.Client, bucketName string) (bool, error) {
	attrs, err := getBucketAttrs(ctx, client, bucketName)
	if err != nil {
		return false, err
	}
	return attrs.SoftDeletePolicy != nil && attrs.SoftDeletePolicy.RetentionDuration > 0, nil
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
