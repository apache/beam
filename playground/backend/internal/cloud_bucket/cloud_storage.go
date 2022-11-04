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
	"beam.apache.org/playground/backend/internal/constants"
	"beam.apache.org/playground/backend/internal/logger"
)

const (
	timeout = 5 * time.Second
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

	var rc storage.Reader
	defer func(rc *storage.Reader) {
		err := rc.Close()
		if err != nil {
			logger.Error(err.Error())
		}
	}(&rc)
	result := make([]*DatasetDTO, 0, len(datasets))
	for _, dataset := range datasets {
		if len(dataset.DatasetPath) == 0 {
			return nil, errors.New("dataset path not found")
		}
		elements := strings.Split(dataset.DatasetPath, constants.CloudPathDelimiter)
		if len(elements) < 3 {
			return nil, errors.New("wrong the cloud storage uri")
		}
		obj := fmt.Sprintf("%s%s%s", elements[len(elements)-2], constants.CloudPathDelimiter, elements[len(elements)-1])
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

type DatasetDTO struct {
	Dataset *pb.Dataset
	Data    []byte
}
