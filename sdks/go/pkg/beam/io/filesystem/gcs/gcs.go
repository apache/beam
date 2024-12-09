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

// Package gcs contains a Google Cloud Storage (GCS) implementation of the
// Beam file system.
package gcs

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"cloud.google.com/go/storage"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/util/fsx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/util/gcsx"
	"google.golang.org/api/iterator"
)

func init() {
	filesystem.Register("gs", New)
}

type fs struct {
	client           *storage.Client
	billingProjectID string
}

// New creates a new Google Cloud Storage filesystem using application
// default credentials. If it fails, it falls back to unauthenticated
// access.
// It will use the environment variable named `BILLING_PROJECT_ID` as requester payer bucket attribute.
func New(ctx context.Context) filesystem.Interface {
	client, err := gcsx.NewClient(ctx, storage.ScopeReadWrite)
	if err != nil {
		log.Warnf(ctx, "Warning: falling back to unauthenticated GCS access: %v", err)

		client, err = gcsx.NewUnauthenticatedClient(ctx)
		if err != nil {
			panic(errors.Wrapf(err, "failed to create GCS client"))
		}
	}
	billingProjectIDEnvVarName := "BILLING_PROJECT_ID"
	billingProjectID := os.Getenv(billingProjectIDEnvVarName)
	return &fs{
		client:           client,
		billingProjectID: billingProjectID,
	}
}

func (f *fs) Close() error {
	return f.client.Close()
}

func (f *fs) List(ctx context.Context, glob string) ([]string, error) {
	bucket, object, err := gcsx.ParseObject(glob)
	if err != nil {
		return nil, err
	}

	var candidates []string

	// We handle globs by list all candidates and matching them here.
	// For now, we assume * is the first matching character to make a
	// prefix listing and not list the entire bucket.
	prefix := fsx.GetPrefix(object)
	it := f.client.Bucket(bucket).UserProject(f.billingProjectID).Objects(ctx, &storage.Query{
		Prefix: prefix,
	})
	for {
		obj, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}

		match, err := filepath.Match(object, obj.Name)
		if err != nil {
			return nil, err
		}
		if match {
			candidates = append(candidates, obj.Name)
		}
	}

	var ret []string
	for _, obj := range candidates {
		ret = append(ret, fmt.Sprintf("gs://%v/%v", bucket, obj))
	}
	return ret, nil
}

func (f *fs) OpenRead(ctx context.Context, filename string) (io.ReadCloser, error) {
	bucket, object, err := gcsx.ParseObject(filename)
	if err != nil {
		return nil, err
	}

	return f.client.Bucket(bucket).UserProject(f.billingProjectID).Object(object).NewReader(ctx)
}

// TODO(herohde) 7/12/2017: should we create the bucket in OpenWrite? For now, "no".

func (f *fs) OpenWrite(ctx context.Context, filename string) (io.WriteCloser, error) {
	bucket, object, err := gcsx.ParseObject(filename)
	if err != nil {
		return nil, err
	}

	return f.client.Bucket(bucket).UserProject(f.billingProjectID).Object(object).NewWriter(ctx), nil
}

func (f *fs) Size(ctx context.Context, filename string) (int64, error) {
	bucket, object, err := gcsx.ParseObject(filename)
	if err != nil {
		return -1, err
	}

	obj := f.client.Bucket(bucket).UserProject(f.billingProjectID).Object(object)
	attrs, err := obj.Attrs(ctx)
	if err != nil {
		return -1, err
	}

	return attrs.Size, nil
}

// LastModified returns the time at which the file was last modified.
func (f *fs) LastModified(ctx context.Context, filename string) (time.Time, error) {
	bucket, object, err := gcsx.ParseObject(filename)
	if err != nil {
		return time.Time{}, err
	}

	obj := f.client.Bucket(bucket).UserProject(f.billingProjectID).Object(object)
	attrs, err := obj.Attrs(ctx)
	if err != nil {
		return time.Time{}, err
	}

	return attrs.Updated, nil
}

// Remove the named file from the filesystem.
func (f *fs) Remove(ctx context.Context, filename string) error {
	bucket, object, err := gcsx.ParseObject(filename)
	if err != nil {
		return err
	}

	obj := f.client.Bucket(bucket).UserProject(f.billingProjectID).Object(object)
	return obj.Delete(ctx)
}

// Copy copies from srcpath to the dstpath.
func (f *fs) Copy(ctx context.Context, srcpath, dstpath string) error {
	bucket, src, err := gcsx.ParseObject(srcpath)
	if err != nil {
		return err
	}
	srcobj := f.client.Bucket(bucket).UserProject(f.billingProjectID).Object(src)

	bucket, dst, err := gcsx.ParseObject(dstpath)
	if err != nil {
		return err
	}
	dstobj := f.client.Bucket(bucket).UserProject(f.billingProjectID).Object(dst)

	cp := dstobj.CopierFrom(srcobj)
	_, err = cp.Run(ctx)
	return err
}

// Rename the old path to the new path.
func (f *fs) Rename(ctx context.Context, srcpath, dstpath string) error {
	bucket, src, err := gcsx.ParseObject(srcpath)
	if err != nil {
		return err
	}
	srcobj := f.client.Bucket(bucket).UserProject(f.billingProjectID).Object(src)

	bucket, dst, err := gcsx.ParseObject(dstpath)
	if err != nil {
		return err
	}
	dstobj := f.client.Bucket(bucket).UserProject(f.billingProjectID).Object(dst)

	cp := dstobj.CopierFrom(srcobj)
	_, err = cp.Run(ctx)
	if err != nil {
		return err
	}
	return srcobj.Delete(ctx)
}

// Compile time check for interface implementations.
var (
	_ filesystem.LastModifiedGetter = ((*fs)(nil))
	_ filesystem.Remover            = ((*fs)(nil))
	_ filesystem.Copier             = ((*fs)(nil))
	_ filesystem.Renamer            = ((*fs)(nil))
)
