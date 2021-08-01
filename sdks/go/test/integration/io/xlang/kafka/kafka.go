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

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/go/pkg/beam/io/xlang/kafkaio"
	"github.com/apache/beam/sdks/go/pkg/beam/testing/passert"
	"github.com/google/uuid"
)

func appendUuid(prefix string) string {
	return fmt.Sprintf("%v_%v", prefix, uuid.New())
}

// Constants for the BasicPipeline.
const (
	numRecords = 1000
	basicTopic = "xlang_kafkaio_basic_test"
)

// BasicPipeline creates a pipeline that writes and then reads a range of ints
// to and from a Kafka topic and asserts that all elements are present. This
// function requires an expansion service address and a Kafka bootstrap server
// address.
func BasicPipeline(expansionAddr, bootstrapAddr string) *beam.Pipeline {
	topic := appendUuid(basicTopic)
	inputs := make([]int, numRecords)
	for i := 0; i < numRecords; i++ {
		inputs[i] = i
	}
	p, s := beam.NewPipelineWithRoot()
	ins := beam.CreateList(s, inputs)

	// Write to Kafka
	encoded := beam.ParDo(s, func(i int) ([]byte, error) {
		var buf bytes.Buffer
		err := coder.EncodeVarInt(int64(i), &buf)
		return buf.Bytes(), err
	}, ins)
	keyed := beam.ParDo(s, func(b []byte) ([]byte, []byte) {
		return []byte(""), b
	}, encoded)
	kafkaio.Write(s, expansionAddr, bootstrapAddr, topic, keyed)

	// Read from Kafka
	reads := kafkaio.Read(s, expansionAddr, bootstrapAddr, []string{topic})
	vals := beam.DropKey(s, reads)
	decoded := beam.ParDo(s, func(b []byte) (int, error) {
		buf := bytes.NewBuffer(b)
		i, err := coder.DecodeVarInt(buf)
		return int(i), err
	}, vals)

	passert.Equals(s, decoded, ins)

	return p
}
