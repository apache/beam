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

package emulators

import (
	"errors"
	"io/ioutil"

	pb "beam.apache.org/playground/backend/internal/api/v1"
	"beam.apache.org/playground/backend/internal/constants"
	"beam.apache.org/playground/backend/internal/logger"
)

const DATASETS_PATH = "/opt/playground/backend/datasets"

type EmulatorMockCluster interface {
	Stop()
	GetAddress() string
}

type EmulatorProducer interface {
	ProduceDatasets(datasets []*DatasetDTO) error
}

func PrepareMockClustersAndGetPrepareParams(request *pb.RunCodeRequest) ([]EmulatorMockCluster, map[string]string, error) {
	datasetsByEmulatorTypeMap := map[pb.EmulatorType][]*pb.Dataset{}
	for _, dataset := range request.Datasets {
		datasets, ok := datasetsByEmulatorTypeMap[dataset.Type]
		if !ok {
			datasets = make([]*pb.Dataset, 0)
			datasets = append(datasets, dataset)
			datasetsByEmulatorTypeMap[dataset.Type] = datasets
			continue
		}
		datasets = append(datasets, dataset)
		datasetsByEmulatorTypeMap[dataset.Type] = datasets
	}

	var prepareParams = make(map[string]string)
	var mockClusters = make([]EmulatorMockCluster, 0)
	for emulatorType, datasets := range datasetsByEmulatorTypeMap {
		switch emulatorType {
		case pb.EmulatorType_EMULATOR_TYPE_KAFKA:
			kafkaMockCluster, err := NewKafkaMockCluster()
			if err != nil {
				logger.Errorf("failed to run a kafka mock cluster, %v", err)
				return nil, nil, err
			}
			mockClusters = append(mockClusters, kafkaMockCluster)
			datasetDTOs, err := toDatasetDTOs(datasets)
			if err != nil {
				logger.Errorf("failed to get datasets from the repository, %v", err)
				return nil, nil, err
			}
			producer, err := NewKafkaProducer(kafkaMockCluster)
			if err != nil {
				logger.Errorf("failed to create a producer, %v", err)
				return nil, nil, err
			}
			if err = producer.ProduceDatasets(datasetDTOs); err != nil {
				logger.Errorf("failed to produce a dataset, %v", err)
				return nil, nil, err
			}
			for _, dataset := range datasets {
				for k, v := range dataset.Options {
					prepareParams[k] = v
				}
			}
			prepareParams[constants.BootstrapServerKey] = kafkaMockCluster.GetAddress()
		default:
			return nil, nil, errors.New("unsupported emulator type")
		}
	}
	return mockClusters, prepareParams, nil
}

func toDatasetDTOs(datasets []*pb.Dataset) ([]*DatasetDTO, error) {
	result := make([]*DatasetDTO, 0, len(datasets))
	for _, dataset := range datasets {
		path := DATASETS_PATH + "/" + dataset.DatasetPath
		data, err := ioutil.ReadFile(path)
		if err != nil {
			return nil, err
		}
		result = append(result, &DatasetDTO{Dataset: dataset, Data: data})
	}
	return result, nil
}

type DatasetDTO struct {
	Dataset *pb.Dataset
	Data    []byte
}
