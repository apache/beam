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

// Package kafka contains integration tests for cross-language Kafka IO
// transforms.
package kafka

import (
	"bytes"
	"fmt"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/xlang/kafkaio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
	"github.com/google/uuid"
)

func appendUuid(prefix string) string {
	return fmt.Sprintf("%v_%v", prefix, uuid.New())
}

// writeList encodes a list of ints and sends encoded ints to Kafka.
func writeInts(s beam.Scope, expansionAddr, bootstrapAddr, topic string, inputs []int) {
	s = s.Scope("kafka_test.WriteListToKafka")

	ins := beam.CreateList(s, inputs)
	encoded := beam.ParDo(s, func(i int) ([]byte, error) {
		var buf bytes.Buffer
		err := coder.EncodeVarInt(int64(i), &buf)
		return buf.Bytes(), err
	}, ins)
	keyed := beam.ParDo(s, func(b []byte) ([]byte, []byte) {
		return []byte(""), b
	}, encoded)
	kafkaio.Write(s, expansionAddr, bootstrapAddr, topic, keyed)
}

// readList reads a set number of elements from Kafka and decodes them to ints.
func readInts(s beam.Scope, expansionAddr, bootstrapAddr, topic string, numRecords int64) beam.PCollection {
	s = s.Scope("kafka_test.ReadListFromKafka")

	reads := kafkaio.Read(s, expansionAddr, bootstrapAddr, []string{topic},
		kafkaio.MaxNumRecords(numRecords),
		kafkaio.ConsumerConfigs(map[string]string{"auto.offset.reset": "earliest"}))
	vals := beam.DropKey(s, reads)
	decoded := beam.ParDo(s, func(b []byte) (int, error) {
		buf := bytes.NewBuffer(b)
		i, err := coder.DecodeVarInt(buf)
		return int(i), err
	}, vals)
	return decoded
}

// WritePipeline creates a pipeline that writes a given slice of ints to Kafka.
func WritePipeline(expansionAddr, bootstrapAddr, topic string, inputs []int) *beam.Pipeline {
	p, s := beam.NewPipelineWithRoot()
	writeInts(s, expansionAddr, bootstrapAddr, topic, inputs)
	return p
}

// ReadPipeline creates a pipeline that reads ints from Kafka and asserts that
// they match a given slice of ints. This reads a number of records equal to
// the length of the given slice.
func ReadPipeline(expansionAddr, bootstrapAddr, topic string, inputs []int) *beam.Pipeline {
	p, s := beam.NewPipelineWithRoot()
	result := readInts(s, expansionAddr, bootstrapAddr, topic, int64(len(inputs)))

	// Validate that records read from Kafka match the given slice.
	ins := beam.CreateList(s, inputs)
	passert.Equals(s, result, ins)
	return p
}
