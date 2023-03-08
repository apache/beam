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
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/linkedin/goavro"

	"beam.apache.org/playground/backend/internal/constants"
)

const (
	brokerCount        = 1
	addressSeperator   = ":"
	pauseDuration      = 100 * time.Millisecond
	globalDuration     = 2 * time.Second
	bootstrapServerKey = "bootstrap.servers"
	networkType        = "tcp"
	jsonExt            = ".json"
	avroExt            = ".avro"
)

type KafkaMockCluster struct {
	cluster *kafka.MockCluster
	host    string
	port    string
}

func NewKafkaMockCluster() (*KafkaMockCluster, error) {
	cluster, err := kafka.NewMockCluster(brokerCount)
	if err != nil {
		return nil, err
	}
	bootstrapServers := cluster.BootstrapServers()
	bootstrapServersArr := strings.Split(bootstrapServers, addressSeperator)
	if len(bootstrapServersArr) != 2 {
		return nil, errors.New("wrong bootstrap server value")
	}

	workTicker := time.NewTicker(pauseDuration)
	globalTicker := time.NewTicker(globalDuration)
	for {
		select {
		case <-workTicker.C:
			if _, err = net.DialTimeout(networkType, bootstrapServers, pauseDuration); err == nil {
				return &KafkaMockCluster{
					cluster: cluster,
					host:    bootstrapServersArr[0],
					port:    bootstrapServersArr[1]}, nil
			}
		case <-globalTicker.C:
			return nil, errors.New("timeout while a mock cluster is starting")
		}
	}
}

func (kmc *KafkaMockCluster) Stop() {
	if kmc.cluster != nil {
		kmc.cluster.Close()
	}
}

func (kmc *KafkaMockCluster) GetAddress() string {
	return fmt.Sprintf("%s%s%s", kmc.host, addressSeperator, kmc.port)
}

type KafkaProducer struct {
	cluster  *KafkaMockCluster
	producer *kafka.Producer
}

func NewKafkaProducer(cluster *KafkaMockCluster) (*KafkaProducer, error) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		bootstrapServerKey: cluster.GetAddress(),
		"acks":             "all",
	})
	if err != nil {
		return nil, err
	}
	return &KafkaProducer{cluster: cluster, producer: p}, nil
}

func (kp *KafkaProducer) ProduceDatasets(datasets []*DatasetDTO) error {
	if kp.producer == nil {
		return errors.New("producer not set")
	}
	defer kp.producer.Close()
	for _, dataset := range datasets {
		topic := getTopic(dataset)
		entries, err := unmarshallDatasets(dataset)
		if err != nil {
			return err
		}
		err = produce(entries, kp, topic)
		if err != nil {
			return err
		}
	}
	return nil
}

func produce(entries []map[string]interface{}, kp *KafkaProducer, topic *string) error {
	for _, entry := range entries {
		entryBytes, err := json.Marshal(entry)
		if err != nil {
			return fmt.Errorf("failed to marshal data, err: %v", err)
		}
		if err := kp.producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: topic, Partition: 0},
			Value:          entryBytes},
			nil,
		); err != nil {
			return errors.New("failed to produce data to a topic")
		}
		e := <-kp.producer.Events()
		m := e.(*kafka.Message)

		if m.TopicPartition.Error != nil {
			return fmt.Errorf("delivery failed: %v", m.TopicPartition.Error)
		}
	}

	return nil
}

func getTopic(dataset *DatasetDTO) *string {
	topicName := dataset.Dataset.Options[constants.TopicNameKey]
	topic := new(string)
	*topic = topicName
	return topic
}

func unmarshallDatasets(dataset *DatasetDTO) ([]map[string]interface{}, error) {
	ext := filepath.Ext(dataset.Dataset.DatasetPath)
	var entries []map[string]interface{}
	switch ext {
	case jsonExt:
		err := json.Unmarshal(dataset.Data, &entries)
		if err != nil {
			return nil, fmt.Errorf("failed to parse a dataset in json format, err: %v", err)
		}
		break
	case avroExt:
		ocfReader, err := goavro.NewOCFReader(strings.NewReader(string(dataset.Data)))
		if err != nil {
			return nil, fmt.Errorf("failed to parse a dataset in avro format, err: %v", err)
		}
		for ocfReader.Scan() {
			record, err := ocfReader.Read()
			if err != nil {
				return nil, fmt.Errorf("failed to read a file in avro format, err: %v", err)
			}
			entries = append(entries, record.(map[string]interface{}))
		}
		break
	default:
		return nil, errors.New("wrong dataset extension")
	}
	return entries, nil
}
