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

package kafka

import (
	"flag"
	"fmt"
	"log"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/dataflow"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/flink"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/samza"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/spark"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
	"github.com/apache/beam/sdks/v2/go/test/integration"
)

// bootstrapAddr should be set by TestMain once a Kafka cluster has been
// started, and is used by each test.
var bootstrapAddr string
var expansionAddr string // Populate with expansion address labelled "io".

const (
	basicTopic = "xlang_kafkaio_basic_test"
	numRecords = 1000
)

func checkFlags(t *testing.T) {
	if bootstrapAddr == "" {
		t.Skip("No bootstrap server address provided.")
	}
}

// TestBasicPipeline basic writes and reads from Kafka with as few optional
// parameters or extra features as possible.
func TestKafkaIO_BasicReadWrite(t *testing.T) {
	integration.CheckFilters(t)
	checkFlags(t)

	inputs := make([]int, numRecords)
	for i := 0; i < numRecords; i++ {
		inputs[i] = i
	}
	topic := appendUuid(basicTopic)

	write := WritePipeline(expansionAddr, bootstrapAddr, topic, inputs)
	ptest.RunAndValidate(t, write)
	read := ReadPipeline(expansionAddr, bootstrapAddr, topic, inputs)
	ptest.RunAndValidate(t, read)
}

// TestMain starts up a Kafka cluster from integration.KafkaJar before running
// tests through ptest.Main.
func TestMain(m *testing.M) {
	flag.Parse()
	beam.Init()

	// Start local Kafka cluster and defer its shutdown.
	if *integration.BootstrapServers != "" {
		bootstrapAddr = *integration.BootstrapServers
	} else if *integration.KafkaJar != "" {
		cluster, err := runLocalKafka(*integration.KafkaJar, *integration.KafkaJarTimeout)
		if err != nil {
			panic(fmt.Errorf("kafka cluster failed to start: %v", err))
		}
		defer func() { cluster.Shutdown() }()
		bootstrapAddr = cluster.bootstrapAddr
	}

	services := integration.NewExpansionServices()
	defer func() { services.Shutdown() }()
	addr, err := services.GetAddr("io")
	if err != nil {
		log.Printf("skipping missing expansion service: %v", err)
	} else {
		expansionAddr = addr
	}

	ptest.MainRet(m)
}
