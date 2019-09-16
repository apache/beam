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
	"path/filepath"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/apache/beam/sdks/go/pkg/beam/internal/errors"
	"github.com/apache/beam/sdks/go/pkg/beam/io/filesystem"
	"github.com/apache/beam/sdks/go/pkg/beam/log"
	"github.com/apache/beam/sdks/go/pkg/beam/util/gcsx"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

func init() {
	filesystem.Register("gs", New)
}

type fs struct {
	client *storage.Client
}

// New creates a new Google Cloud Storage filesystem using application
// default credentials. If it fails, it falls back to unauthenticated
// access.
func New(ctx context.Context) filesystem.Interface {
	client, err := storage.NewClient(ctx, option.WithScopes(storage.ScopeReadWrite))
	if err != nil {
		log.Warnf(ctx, "Warning: falling back to unauthenticated GCS access: %v", err)

		client, err = storage.NewClient(ctx, option.WithoutAuthentication())
		if err != nil {
			panic(errors.Wrapf(err, "failed to create GCS client"))
		}
	}
	return &fs{client: client}
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
	if index := strings.Index(object, "*"); index > 0 {
		// We handle globs by list all candidates and matching them here.
		// For now, we assume * is the first matching character to make a
		// prefix listing and not list the entire bucket.

		it := f.client.Bucket(bucket).Objects(ctx, &storage.Query{
			Prefix: object[:index],
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
	} else {
		// Single object.
		candidates = []string{object}
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

	return f.client.Bucket(bucket).Object(object).NewReader(ctx)
}

// TODO(herohde) 7/12/2017: should we create the bucket in OpenWrite? For now, "no".

func (f *fs) OpenWrite(ctx context.Context, filename string) (io.WriteCloser, error) {
	bucket, object, err := gcsx.ParseObject(filename)
	if err != nil {
		return nil, err
	}

	return f.client.Bucket(bucket).Object(object).NewWriter(ctx), nil
}
