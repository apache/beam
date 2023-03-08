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

package s3

import (
	"context"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type writer struct {
	ctx      context.Context
	client   *s3.Client
	bucket   string
	key      string
	done     chan struct{}
	isOpened bool
	pw       *io.PipeWriter
	err      error
}

// newWriter returns a writer that creates and writes to an S3 object. If an object with the same
// bucket and key already exists, it will be overwritten. The caller must call Close on the writer
// when done writing for the object to become available.
func newWriter(
	ctx context.Context,
	client *s3.Client,
	bucket string,
	key string,
) *writer {
	return &writer{
		ctx:    ctx,
		client: client,
		bucket: bucket,
		key:    key,
		done:   make(chan struct{}),
	}
}

// Write writes data to a pipe.
func (w *writer) Write(p []byte) (int, error) {
	if !w.isOpened {
		w.open()
	}

	return w.pw.Write(p)
}

// Close completes the write operation.
func (w *writer) Close() error {
	if !w.isOpened {
		w.open()
	}

	if err := w.pw.Close(); err != nil {
		return err
	}

	<-w.done
	return w.err
}

// open creates a pipe for writing to the S3 object.
func (w *writer) open() {
	pr, pw := io.Pipe()
	w.pw = pw

	go func() {
		defer close(w.done)

		params := &s3.PutObjectInput{
			Bucket: aws.String(w.bucket),
			Key:    aws.String(w.key),
			Body:   io.Reader(pr),
		}
		uploader := manager.NewUploader(w.client)
		if _, err := uploader.Upload(w.ctx, params); err != nil {
			w.err = err
			pr.CloseWithError(err)
		}
	}()

	w.isOpened = true
}
