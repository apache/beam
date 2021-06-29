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
	"log"
	"os"
	"testing"

	_ "github.com/apache/beam/sdks/go/pkg/beam/runners/dataflow"
	_ "github.com/apache/beam/sdks/go/pkg/beam/runners/flink"
	_ "github.com/apache/beam/sdks/go/pkg/beam/runners/spark"
	"github.com/apache/beam/sdks/go/pkg/beam/testing/ptest"
	"github.com/apache/beam/sdks/go/test/integration"
)

// bootstrapAddr should be set by TestMain once a Kafka cluster has been
// started, and is used by each test.
var bootstrapAddr string

func checkFlags(t *testing.T) {
	if *integration.IoExpansionAddr == "" {
		t.Skip("No IO expansion address provided.")
	}
	if bootstrapAddr == "" {
		t.Skip("No bootstrap server address provided.")
	}
}

// TestBasicPipeline tests a basic Kafka pipeline that writes to and reads from
// Kafka with no optional parameters or extra features.
func TestBasicPipeline(t *testing.T) {
	integration.CheckFilters(t)
	checkFlags(t)
	p := BasicPipeline(*integration.IoExpansionAddr, bootstrapAddr)
	ptest.RunAndValidate(t, p)
}

// TestMain starts up a Kafka cluster from integration.KafkaJar before running
// tests through ptest.Main.
func TestMain(m *testing.M) {
	// Defer os.Exit so it happens after other defers.
	var retCode int
	defer func() { os.Exit(retCode) }()

	// Start local Kafka cluster and defer its shutdown.
	if *integration.BootstrapServers != "" {
		bootstrapAddr = *integration.BootstrapServers
	} else if *integration.KafkaJar != "" {
		cluster, err := runLocalKafka(*integration.KafkaJar)
		if err != nil {
			log.Fatalf("Kafka cluster failed to start: %v", err)
		}
		defer func() { cluster.proc.Kill() }()
		bootstrapAddr = cluster.bootstrapAddr
	}

	retCode = ptest.MainRet(m)
}
