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
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
)

func newServer(t *testing.T) *httptest.Server {
	t.Helper()
	backend := s3mem.New()
	faker := gofakes3.New(backend)
	handler := faker.Server()

	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return server
}

func newClient(ctx context.Context, t *testing.T, url string) *s3.Client {
	t.Helper()
	credentialsProvider := credentials.NewStaticCredentialsProvider("key", "secret", "session")
	httpClient := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}
	endpointResolverFn := aws.EndpointResolverWithOptionsFunc(
		func(service string, region string, options ...any) (aws.Endpoint, error) {
			return aws.Endpoint{URL: url}, nil
		},
	)

	cfg, err := config.LoadDefaultConfig(
		ctx,
		config.WithCredentialsProvider(credentialsProvider),
		config.WithHTTPClient(httpClient),
		config.WithEndpointResolverWithOptions(endpointResolverFn),
	)
	if err != nil {
		t.Fatalf("error loading config: %v", err)
	}

	return s3.NewFromConfig(cfg, func(options *s3.Options) {
		options.UsePathStyle = true
	})
}

func createBucket(ctx context.Context, t *testing.T, client *s3.Client, bucket string) {
	t.Helper()
	params := &s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	}
	if _, err := client.CreateBucket(ctx, params); err != nil {
		t.Fatalf("error creating bucket: %v", err)
	}
}

func createObject(
	ctx context.Context,
	t *testing.T,
	client *s3.Client,
	bucket string,
	key string,
	content []byte,
) {
	t.Helper()
	params := &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(content),
	}
	if _, err := client.PutObject(ctx, params); err != nil {
		t.Fatalf("error creating object: %v", err)
	}
}

func objectExists(
	ctx context.Context,
	t *testing.T,
	client *s3.Client,
	bucket string,
	key string,
) bool {
	t.Helper()
	params := &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}
	if _, err := client.HeadObject(ctx, params); err != nil {
		var apiError smithy.APIError
		if errors.As(err, &apiError) && apiError.ErrorCode() == "NotFound" {
			return false
		}

		t.Fatalf("error getting object metadata: %v", err)
	}

	return true
}

func getObject(
	ctx context.Context,
	t *testing.T,
	client *s3.Client,
	bucket string,
	key string,
) []byte {
	t.Helper()
	params := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}
	output, err := client.GetObject(ctx, params)
	if err != nil {
		t.Fatalf("error getting object: %v", err)
	}

	defer output.Body.Close()
	content, err := io.ReadAll(output.Body)
	if err != nil {
		t.Fatalf("error reading object: %v", err)
	}

	return content
}
