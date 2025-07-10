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

// taxi is an example using a cross-language Kafka pipeline to write and read
// to Kafka. This example reads from the PubSub NYC Taxi stream described in
// https://github.com/googlecodelabs/cloud-dataflow-nyc-taxi-tycoon, writes to
// a given Kafka topic and then reads back from the same Kafka topic, logging
// every element. This is done as a streaming pipeline and will not end
// unless the pipeline is stopped externally.
//
// Running this example requires a Kafka cluster accessible to the runner, and
// a cross-language expansion service that can expand Kafka read and write
// transforms. An address to a persistent expansion service can be provided as
// a flag, or if none is specified then the SDK will attempt to automatically
// start an appropriate expansion service.
//
// # Setting Up a Kafka Cluster
//
// Setting up a Kafka cluster is more involved than can be covered in this
// example. In order for this example to work, all that is necessary is a Kafka
// cluster accessible through a bootstrap server address that is passed in as
// a flag. Some instructions for setting up a single node Kafka cluster in GCE
// can be found here: https://github.com/GoogleCloudPlatform/java-docs-samples/tree/master/dataflow/flex-templates/kafka_to_bigquery
//
// # Running an Expansion Server
//
// If the automatic expansion service functionality is not available for your
// environment, or if you want improved performance, you will need to start a
// persistent expansion service. These instructions will cover running the Java
// IO Expansion Service, and therefore requires a JDK installation in a version
// supported by Beam. Depending on whether you are running this from a numbered
// Beam release, or a development environment, there are two sources you may
// use for the Expansion service.
//
// Numbered release: The expansion service jar is vendored as module
// org.apache.beam:beam-sdks-java-io-expansion-service in Maven Repository.
// This jar can be executed directly with the following command:
//
//	`java -jar <jar_name> <port_number>`
//
// Development env: This requires that the JAVA_HOME environment variable
// points to your JDK installation. From the root `beam/` directory of the
// Apache Beam repository, the jar can be built (or built and run) with the
// following commands:
//
//	Build: ./gradlew :sdks:java:io:expansion-service:build
//	Build and Run: ./gradlew :sdks:java:io:expansion-service:runExpansionService -PconstructionService.port=<port_num>
//
// # Running the Example on GCP
//
// Running this pipeline requires providing an address for the Expansion Service
// and for the Kafka cluster's bootstrap servers as flags, in addition to the
// usual flags for pipelines.
//
// An example command for executing this pipeline on GCP is as follows:
//
//	export PROJECT="$(gcloud config get-value project)"
//	export TEMP_LOCATION="gs://MY-BUCKET/temp"
//	export REGION="us-central1"
//	export JOB_NAME="kafka-taxi-`date +%Y%m%d-%H%M%S`"
//	export BOOTSTRAP_SERVERS="123.45.67.89:1234"
//	export EXPANSION_ADDR="localhost:1234"
//	cd ./sdks/go
//	go run ./examples/kafka/taxi.go \
//	  --runner=DataflowRunner \
//	  --temp_location=$TEMP_LOCATION \
//	  --staging_location=$STAGING_LOCATION \
//	  --project=$PROJECT \
//	  --region=$REGION \
//	  --job_name="${JOB_NAME}" \
//	  --bootstrap_servers=$BOOTSTRAP_SERVER \
//	  --expansion_addr=$EXPANSION_ADDR
//
// # Running the Example From a Git Clone
//
// When running on a development environment, a custom container will likely
// need to be provided for the cross-language SDK. First this will require
// building and pushing the SDK container to container repository, such as
// Docker Hub.
//
//	export DOCKER_ROOT="Your Docker Repository Root"
//	./gradlew :sdks:java:container:java11:docker -Pdocker-repository-root=$DOCKER_ROOT -Pdocker-tag=latest
//	docker push $DOCKER_ROOT/beam_java11_sdk:latest
//
// For runners in local mode, simply building the container using the default
// values for docker-repository-root and docker-tag will work to have it
// accessible locally.
//
// Additionally, you must provide the location of your custom container to the
// pipeline with the --sdk_harness_container_image_override flag for Java, or
// --environment_config flag for Go. For example:
//
//	--sdk_harness_container_image_override=".*java.*,${DOCKER_ROOT}/beam_java11_sdk:latest" \
//	--environment_config=${DOCKER_ROOT}/beam_go_sdk:latest
package main

import (
	"context"
	"flag"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/pubsubio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/xlang/kafkaio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
)

var (
	expansionAddr = flag.String("expansion_addr", "",
		"Address of Expansion Service. If not specified, attempts to automatically start an appropriate expansion service.")
	bootstrapServers = flag.String("bootstrap_servers", "",
		"(Required) URL of the bootstrap servers for the Kafka cluster. Should be accessible by the runner.")
	topic = flag.String("topic", "kafka_taxirides_realtime", "Kafka topic to write to and read from.")
)

func init() {
	register.DoFn2x0[context.Context, []byte](&LogFn{})
}

// LogFn is a DoFn to log rides.
type LogFn struct{}

// ProcessElement logs each element it receives.
func (fn *LogFn) ProcessElement(ctx context.Context, elm []byte) {
	log.Infof(ctx, "Ride info: %v", string(elm))
}

// FinishBundle waits a bit so the job server finishes receiving logs.
func (fn *LogFn) FinishBundle() {
	time.Sleep(2 * time.Second)
}

func main() {
	flag.Parse()
	beam.Init()

	ctx := context.Background()

	p := beam.NewPipeline()
	s := p.Root()

	// Read from Pubsub and write to Kafka.
	data := pubsubio.Read(s, "pubsub-public-data", "taxirides-realtime", nil)
	kvData := beam.ParDo(s, func(elm []byte) ([]byte, []byte) { return []byte(""), elm }, data)
	windowed := beam.WindowInto(s, window.NewFixedWindows(15*time.Second), kvData)
	kafkaio.Write(s, *expansionAddr, *bootstrapServers, *topic, windowed)

	// Simultaneously read from Kafka and log any element received.
	read := kafkaio.Read(s, *expansionAddr, *bootstrapServers, []string{*topic})
	vals := beam.DropKey(s, read)
	beam.ParDo0(s, &LogFn{}, vals)

	if err := beamx.Run(ctx, p); err != nil {
		log.Fatalf(ctx, "Failed to execute job: %v", err)
	}
}
