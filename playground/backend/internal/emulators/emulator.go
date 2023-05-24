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
	"beam.apache.org/playground/backend/internal/constants"
	"errors"
	"os"
	"path"

	pb "beam.apache.org/playground/backend/internal/api/v1"
	"beam.apache.org/playground/backend/internal/logger"
)

type EmulatorMockCluster interface {
	Stop()
	GetAddress() string
	GetPreparerParameters() map[string]string
}

type EmulatorProducer interface {
	ProduceDatasets(datasets []*DatasetDTO) error
}

type EmulatorConfiguration struct {
	KafkaEmulatorExecutablePath string
	DatasetsPath                string
	Datasets                    []*pb.Dataset
}

func PrepareMockClusters(configuration EmulatorConfiguration) ([]EmulatorMockCluster, error) {
	datasetsByEmulatorTypeMap := map[pb.EmulatorType][]*pb.Dataset{}
	for _, dataset := range configuration.Datasets {
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

	var mockClusters = make([]EmulatorMockCluster, 0)

	kafkaMockCluster, err := NewKafkaMockCluster(configuration.KafkaEmulatorExecutablePath)
	if err != nil {
		logger.Errorf("failed to run a kafka mock cluster, %v", err)
		return nil, err
	}
	kafkaMockCluster.preparerParameters[constants.BootstrapServerKey] = kafkaMockCluster.GetAddress()
	mockClusters = append(mockClusters, kafkaMockCluster)

	for emulatorType, datasets := range datasetsByEmulatorTypeMap {
		datasetDTOs, err := toDatasetDTOs(configuration.DatasetsPath, datasets)
		if err != nil {
			logger.Errorf("failed to get datasets from the repository, %v", err)
			return nil, err
		}

		switch emulatorType {
		case pb.EmulatorType_EMULATOR_TYPE_KAFKA:
			producer, err := NewKafkaProducer(kafkaMockCluster)
			if err != nil {
				logger.Errorf("failed to create a producer, %v", err)
				kafkaMockCluster.Stop()
				return nil, err
			}
			if err = producer.ProduceDatasets(datasetDTOs); err != nil {
				logger.Errorf("failed to produce a dataset, %v", err)
				kafkaMockCluster.Stop()
				return nil, err
			}
			for _, dataset := range datasets {
				for k, v := range dataset.Options {
					kafkaMockCluster.preparerParameters[k] = v
				}
			}

		default:
			return nil, errors.New("unsupported emulator type")
		}
	}
	return mockClusters, nil
}

func toDatasetDTOs(datasetsPath string, datasets []*pb.Dataset) ([]*DatasetDTO, error) {
	result := make([]*DatasetDTO, 0, len(datasets))
	for _, dataset := range datasets {
		datasetPath := path.Join(datasetsPath, dataset.DatasetPath)
		data, err := os.ReadFile(datasetPath)
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
