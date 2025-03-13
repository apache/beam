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

// Package s3 contains an AWS S3 implementation of the Beam file system.
package s3

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/util/fsx"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func init() {
	filesystem.Register("s3", New)
}

type fs struct {
	client *s3.Client
}

// New creates a new S3 filesystem using AWS default configuration sources.
func New(ctx context.Context) filesystem.Interface {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		panic(fmt.Sprintf("error loading AWS config: %v", err))
	}

	client := s3.NewFromConfig(cfg)
	return &fs{client: client}
}

// Close closes the filesystem.
func (f *fs) Close() error {
	return nil
}

// List returns a slice of the files in the filesystem that match the glob pattern.
func (f *fs) List(ctx context.Context, glob string) ([]string, error) {
	bucket, keyPattern, err := parseURI(glob)
	if err != nil {
		return nil, fmt.Errorf("error parsing S3 uri: %v", err)
	}

	keys, err := f.listObjectKeys(ctx, bucket, keyPattern)
	if err != nil {
		return nil, fmt.Errorf("error listing object keys: %v", err)
	}

	if len(keys) == 0 {
		return nil, nil
	}

	uris := make([]string, len(keys))
	for i, key := range keys {
		uris[i] = makeURI(bucket, key)
	}

	return uris, nil
}

// listObjectKeys returns a slice of the keys in the bucket that match the key pattern.
func (f *fs) listObjectKeys(
	ctx context.Context,
	bucket string,
	keyPattern string,
) ([]string, error) {
	prefix := fsx.GetPrefix(keyPattern)
	params := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	}
	paginator := s3.NewListObjectsV2Paginator(f.client, params)

	var objects []string
	for paginator.HasMorePages() {
		output, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("error retrieving page: %v", err)
		}

		for _, object := range output.Contents {
			key := aws.ToString(object.Key)
			match, err := filepath.Match(keyPattern, key)
			if err != nil {
				return nil, fmt.Errorf("invalid key pattern: %s", keyPattern)
			}

			if match {
				objects = append(objects, key)
			}
		}
	}

	return objects, nil
}

// OpenRead returns a new io.ReadCloser to read contents from the file. The caller must call Close
// on the returned io.ReadCloser when done reading.
func (f *fs) OpenRead(ctx context.Context, filename string) (io.ReadCloser, error) {
	bucket, key, err := parseURI(filename)
	if err != nil {
		return nil, fmt.Errorf("error parsing S3 uri %s: %v", filename, err)
	}

	params := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}
	output, err := f.client.GetObject(ctx, params)
	if err != nil {
		return nil, fmt.Errorf("error getting object %s: %v", filename, err)
	}

	return output.Body, nil
}

// OpenWrite returns a new io.WriteCloser to write contents to the file. The caller must call Close
// on the returned io.WriteCloser when done writing.
func (f *fs) OpenWrite(ctx context.Context, filename string) (io.WriteCloser, error) {
	bucket, key, err := parseURI(filename)
	if err != nil {
		return nil, fmt.Errorf("error parsing S3 uri %s: %v", filename, err)
	}

	return newWriter(ctx, f.client, bucket, key), nil
}

// Size returns the size of the file.
func (f *fs) Size(ctx context.Context, filename string) (int64, error) {
	bucket, key, err := parseURI(filename)
	if err != nil {
		return -1, fmt.Errorf("error parsing S3 uri %s: %w", filename, err)
	}

	params := &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}
	output, err := f.client.HeadObject(ctx, params)
	if err != nil {
		return -1, fmt.Errorf("error getting metadata for object %s: %w", filename, err)
	}

	if output.ContentLength != nil {
		return *output.ContentLength, nil
	}

	return -1, fmt.Errorf("content length for object %s was nil", filename)
}

// LastModified returns the time at which the file was last modified.
func (f *fs) LastModified(ctx context.Context, filename string) (time.Time, error) {
	bucket, key, err := parseURI(filename)
	if err != nil {
		return time.Time{}, fmt.Errorf("error parsing S3 uri %s: %v", filename, err)
	}

	params := &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}
	output, err := f.client.HeadObject(ctx, params)
	if err != nil {
		return time.Time{}, fmt.Errorf("error getting metadata for object %s: %v", filename, err)
	}

	return aws.ToTime(output.LastModified), err
}

// Remove removes the file from the filesystem.
func (f *fs) Remove(ctx context.Context, filename string) error {
	bucket, key, err := parseURI(filename)
	if err != nil {
		return fmt.Errorf("error parsing S3 uri %s: %v", filename, err)
	}

	params := &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}
	if _, err = f.client.DeleteObject(ctx, params); err != nil {
		return fmt.Errorf("error deleting object %s: %v", filename, err)
	}

	return nil
}

// Copy copies the file from the old path to the new path.
func (f *fs) Copy(ctx context.Context, oldpath, newpath string) error {
	sourceBucket, sourceKey, err := parseURI(oldpath)
	if err != nil {
		return fmt.Errorf("error parsing S3 source uri %s: %v", oldpath, err)
	}

	copySource := fmt.Sprintf("%s/%s", sourceBucket, sourceKey)
	destBucket, destKey, err := parseURI(newpath)
	if err != nil {
		return fmt.Errorf("error parsing S3 destination uri %s: %v", newpath, err)
	}

	params := &s3.CopyObjectInput{
		Bucket:     aws.String(destBucket),
		CopySource: aws.String(copySource),
		Key:        aws.String(destKey),
	}
	if _, err = f.client.CopyObject(ctx, params); err != nil {
		return fmt.Errorf("error copying object %s: %v", oldpath, err)
	}

	return nil
}

// Compile time check for interface implementations.
var (
	_ filesystem.LastModifiedGetter = (*fs)(nil)
	_ filesystem.Remover            = (*fs)(nil)
	_ filesystem.Copier             = (*fs)(nil)
)
