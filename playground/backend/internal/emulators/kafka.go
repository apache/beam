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
	"beam.apache.org/playground/backend/internal/logger"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
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
	globalDuration     = 120 * time.Second
	bootstrapServerKey = "bootstrap.servers"
	networkType        = "tcp"
	jsonExt            = ".json"
	avroExt            = ".avro"
)

type KafkaMockCluster struct {
	cmd  *exec.Cmd
	host string
	port string
}

func NewKafkaMockCluster(ctx context.Context) (*KafkaMockCluster, error) {
	//cluster, err := kafka.NewMockCluster(brokerCount)
	//if err != nil {
	//	return nil, err
	//}
	//bootstrapServers := cluster.BootstrapServers()

	// TODO: read path from environment
	cmd := exec.Command /*Context(ctx, */ ("java", "-jar", "/home/ts/src/beam/playground/kafka-emulator/build/libs/beam-playground-kafka-emulator-2.46.0-SNAPSHOT.jar")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}

	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, err
	}

	go func() {
		for {
			buf := make([]byte, 256)
			n, _ := stderr.Read(buf)
			if n > 0 {
				logger.Infof("Emulator: %s", string(buf))
			}
		}
	}()

	//errChan := make(chan error) // TODO: So what?

	//go func() {
	//	err := cmd.Run()
	//	if err != nil {
	//		logger.Errorf("Emulator error: %v", err)
	//		errChan <- err
	//	}
	//	logger.Infof("Emulator stopped")
	//}()

	err = cmd.Start()
	if err != nil {
		return nil, err
	}

	const host = "127.0.0.1"
	var port string

	// TODO: read timeout from constant
	timeoutCtx, cancelFunc := context.WithTimeout(ctx, globalDuration)
	defer cancelFunc()

	// Emulator prints port it's opened on stdout, wait for this data
	for {
		select {
		case <-timeoutCtx.Done():
			return nil, timeoutCtx.Err()
		default:
		}

		line := make([]byte, 12, 12)
		n, err := stdout.Read(line)
		if err != nil {
			return nil, err
		}
		if n > 0 {
			_, err = fmt.Sscanf(string(line), "Port: %s", &port)
			if err != nil {
				return nil, err
			}
			break
		}
	}

	bootstrapServersArr := []string{host, port}
	bootstrapServers := fmt.Sprintf("%s%s%s", host, addressSeperator, port)

	workTicker := time.NewTicker(pauseDuration)
	globalTicker := time.NewTicker(globalDuration)
	for {
		select {
		case <-workTicker.C:
			if _, err = net.DialTimeout(networkType, bootstrapServers, pauseDuration); err == nil {
				return &KafkaMockCluster{
					cmd:  cmd,
					host: bootstrapServersArr[0],
					port: bootstrapServersArr[1]}, nil
			}
		case <-globalTicker.C:
			return nil, errors.New("timeout while a mock cluster is starting")
		}
	}
}

func (kmc *KafkaMockCluster) Stop() {
	logger.Infof("Stopping Kafka emulator")
	if kmc.cmd != nil {
		err := kmc.cmd.Process.Signal(os.Interrupt)
		if err != nil {
			logger.Errorf("Failed to send Interrupt signal to process %d: %v", kmc.cmd.Process.Pid, err)
			return
		}
		_, err = kmc.cmd.Process.Wait()
		if err != nil {
			logger.Errorf("Failed to wait for process %d: %v", kmc.cmd.Process.Pid, err)
		}
	}
}

func (kmc *KafkaMockCluster) GetAddress() string {
	return fmt.Sprintf("%s%s%s", kmc.host, addressSeperator, kmc.port)
}

type KafkaProducer struct {
	//cluster  *KafkaMockCluster
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
	return &KafkaProducer{ /*cluster: cluster,*/ producer: p}, nil
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
		if m, ok := e.(*kafka.Message); ok {
			if m.TopicPartition.Error != nil {
				return fmt.Errorf("delivery failed: %v", m.TopicPartition.Error)
			}
		} else if err, ok := e.(kafka.Error); ok {
			return fmt.Errorf("producer error: %v", err)
		} else {
			return fmt.Errorf("unknown event: %s", e.String())
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
