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

package dataflowlib

import (
	"bytes"
	"context"
	"io"
	"os"

	"cloud.google.com/go/storage"
	"github.com/apache/beam/sdks/go/pkg/beam/internal/errors"
	"github.com/apache/beam/sdks/go/pkg/beam/util/gcsx"
)

// StageModel uploads the pipeline model to GCS as a unique object.
func StageModel(ctx context.Context, project, modelURL string, model []byte) error {
	return upload(ctx, project, modelURL, bytes.NewReader(model))
}

// StageFile uploads a file to GCS.
func StageFile(ctx context.Context, project, url, filename string) error {
	fd, err := os.Open(filename)
	if err != nil {
		return errors.Wrapf(err, "failed to open file %s", filename)
	}
	defer fd.Close()

	return upload(ctx, project, url, fd)
}

func upload(ctx context.Context, project, object string, r io.Reader) error {
	bucket, obj, err := gcsx.ParseObject(object)
	if err != nil {
		return errors.Wrapf(err, "invalid staging location %v", object)
	}
	client, err := gcsx.NewClient(ctx, storage.ScopeReadWrite)
	if err != nil {
		return err
	}
	_, err = gcsx.Upload(ctx, client, project, bucket, obj, r)
	return err
}
