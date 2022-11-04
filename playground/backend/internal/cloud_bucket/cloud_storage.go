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

package cloud_bucket

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"

	pb "beam.apache.org/playground/backend/internal/api/v1"
)

const (
	timeout       = 5 * time.Second
	gcsPrefix     = "gs://"
	gcsFullPrefix = "https://storage.googleapis.com/"
)

type CloudStorage struct {
	client *storage.Client
}

func NewCloudStorage(ctx context.Context) (*CloudStorage, error) {
	client, err := storage.NewClient(ctx, option.WithoutAuthentication())
	if err != nil {
		return nil, err
	}
	return &CloudStorage{client: client}, nil
}

func (cs *CloudStorage) GetDatasets(ctx context.Context, bucketName string, datasets []*pb.Dataset) ([]*DatasetDTO, error) {
	if cs.client == nil {
		return nil, errors.New("storage client not set")
	}

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	bucket := cs.client.Bucket(bucketName)

	result := make([]*DatasetDTO, 0)
	for _, dataset := range datasets {
		if len(dataset.DatasetPath) == 0 {
			return nil, errors.New("dataset path not found")
		}
		_, obj, err := parseGCSUri(dataset.DatasetPath)
		if err != nil {
			return nil, err
		}
		rc, err := bucket.Object(obj).NewReader(ctx)
		if err != nil {
			return nil, fmt.Errorf("error while getting a dataset. path: %s. error: %v", dataset.DatasetPath, err)
		}
		data, err := ioutil.ReadAll(rc)
		if err != nil {
			return nil, fmt.Errorf("error while reading a dataset. path: %s. error: %v", dataset.DatasetPath, err)
		}
		result = append(result, &DatasetDTO{
			Dataset: dataset,
			Data:    data,
		})
	}

	return result, nil
}

func parseGCSUri(uri string) (bucket, obj string, err error) {
	shortPrefix := strings.HasPrefix(uri, gcsPrefix)
	fullPrefix := strings.HasPrefix(uri, gcsFullPrefix)
	if !shortPrefix && !fullPrefix {
		return "", "", errors.New("gcs uri has an invalid prefix")
	}
	if shortPrefix {
		uri = uri[len(gcsPrefix):]
		i := strings.IndexByte(uri, '/')
		if i == -1 {
			return "", "", errors.New("gcs: no object name")
		}
		bucket, obj = uri[:i], uri[i+1:]
		return bucket, obj, err
	}
	if fullPrefix {
		uri = uri[len(gcsFullPrefix):]
		i := strings.IndexByte(uri, '/')
		if i == -1 {
			return "", "", errors.New("gcs: no object name")
		}
		bucket, obj = uri[:i], uri[i+1:]
		return bucket, obj, err
	}
	return "", "", errors.New("gcs: no object name")
}

type DatasetDTO struct {
	Dataset *pb.Dataset
	Data    []byte
}
