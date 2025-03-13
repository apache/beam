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

import (
	"flag"
	"fmt"
	"strings"
)

// The following flags are flags used in one or more integration tests, and that
// may be used by scripts that execute "go test ./sdks/go/test/integration/...".
// Because any flags used with those commands are used for each package, every
// integration test package must import these flags, even if they are not used.
var (
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

	// BigQueryDataset is the name of the dataset to create tables in for
	// BigQuery integration tests.
	BigQueryDataset = flag.String("bq_dataset", "",
		"Name of the dataset to create tables in for BigQuery tests.")

	// BigtableInstance is the name of the Bigtable instance to create tables in
	// for Bigtable integration tests.
	BigtableInstance = flag.String("bt_instance", "",
		"Name of the Bigtable instance to create tables in for Bigtable tests.")

	// ExpansionJars contains elements in the form "label:jar" describing jar
	// filepaths for expansion services to use in integration tests, and the
	// corresponding labels. Once provided through this flag, those jars can
	// be used in tests via the ExpansionServices struct.
	ExpansionJars stringSlice

	// ExpansionAddrs contains elements in the form "label:address" describing
	// endpoints for expansion services to use in integration tests, and the
	// corresponding labels. Once provided through this flag, those addresses
	// can be used in tests via the ExpansionServices struct.
	ExpansionAddrs stringSlice

	// ExpansionTimeout attempts to apply an auto-shutdown timeout to any
	// expansion services started by integration tests.
	ExpansionTimeout = flag.Duration("expansion_timeout", 0,
		"Sets an auto-shutdown timeout to any started expansion services. "+
			"Requires the timeout command to be present in Path, unless the value is set to 0.")
)

func init() {
	flag.Var(&ExpansionJars, "expansion_jar",
		"Define jar locations for expansion services. Each entry consists of "+
			"two values, an arbitrary label and a jar filepath, separated by a "+
			"\":\", in the form \"label:jar\". Jars provided through this flag "+
			"can be started by tests.")
	flag.Var(&ExpansionAddrs, "expansion_addr",
		"Define addresses for expansion services. Each entry consists of "+
			"two values, an arbitrary label and an address, separated by a "+
			"\":\", in the form \"label:address\". Addresses provided through "+
			"this flag can be used as expansion addresses by tests.")
}

// GetExpansionJars gets all the jars given to --expansion_jar as a map of label to jar location.
func GetExpansionJars() map[string]string {
	ret := make(map[string]string)
	for _, jar := range ExpansionJars {
		splits := strings.SplitN(jar, ":", 2)
		ret[splits[0]] = splits[1]
	}
	return ret
}

// GetExpansionAddrs gets all the addresses given to --expansion_addr as a map of label to address.
func GetExpansionAddrs() map[string]string {
	ret := make(map[string]string)
	for _, addr := range ExpansionAddrs {
		splits := strings.SplitN(addr, ":", 2)
		ret[splits[0]] = splits[1]
	}
	return ret
}

// stringSlice is a flag.Value implementation for string slices, that allows
// multiple strings to be assigned to one flag by specifying multiple instances
// of the flag.
//
// Example:
//
//	var myFlags stringSlice
//	flag.Var(&myFlags, "my_flag", "A list of flags")
//
// With the example above, the slice can be set to contain ["foo", "bar"]:
//
//	cmd -my_flag foo -my_flag bar
type stringSlice []string

// String implements the String method of flag.Value. This outputs the value
// of the flag as a string.
func (s *stringSlice) String() string {
	return fmt.Sprintf("%v", *s)
}

// Set implements the Set method of flag.Value. This stores a string input to
// the flag into a stringSlice representation.
func (s *stringSlice) Set(value string) error {
	*s = append(*s, value)
	return nil
}

// Get returns the instance itself.
func (s stringSlice) Get() any {
	return s
}
