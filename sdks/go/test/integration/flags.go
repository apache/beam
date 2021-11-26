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

package integration

import "flag"

// The following flags are flags used in one or more integration tests, and that
// may be used by scripts that execute "go test ./sdks/go/test/integration/...".
// Because any flags used with those commands are used for each package, every
// integration test package must import these flags, even if they are not used.
var (
	// TestExpansionAddr is the endpoint for the expansion service for test-only
	// cross-language transforms.
	TestExpansionAddr = flag.String("test_expansion_addr", "", "Address of Expansion Service for test cross-language transforms.")

	// IoExpansionAddr is the endpoint for the expansion service for
	// cross-language IO transforms.
	IoExpansionAddr = flag.String("io_expansion_addr", "", "Address of Expansion Service for cross-language IOs.")

	// BootstrapServers is the address of the bootstrap servers for a Kafka
	// cluster, used for Kafka IO tests.
	BootstrapServers = flag.String("bootstrap_servers", "",
		"URL of the bootstrap servers for the Kafka cluster. Should be accessible by the runner.")

	// KafkaJar is a filepath to a jar for starting a Kafka cluster, used for
	// Kafka IO tests.
	KafkaJar = flag.String("kafka_jar", "",
		"The filepath to a jar for starting up a Kafka cluster. Only used if boostrap_servers is unspecified.")

	// KafkaJarTimeout attempts to apply an auto-shutdown timeout to the Kafka
	// cluster jar. Only used for Kafka IO tests.
	KafkaJarTimeout = flag.String("kafka_jar_timeout", "10m",
		"Sets an auto-shutdown timeout to the Kafka cluster. "+
			"Requires the timeout command to be present in Path, unless the value is set to \"\".")
)
