/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// beam-playground:
//   name: kafka-write
//   description: KafkaIO write example
//   multifile: false
//   default_example: false
//   context_line: 45
//   categories:
//     - IO
//   complexity: ADVANCED

package main

import (
	"context"
	"flag"
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"time"
	//"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
	//"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	//"github.com/apache/beam/sdks/v2/go/pkg/beam/io/pubsubio"
	//"github.com/apache/beam/sdks/v2/go/pkg/beam/io/xlang/kafkaio"
)

var (
	expansionAddr = flag.String("expansion_addr", "localhost:1234",
		"Address of Expansion Service. If not specified, attempts to automatically start an appropriate expansion service.")
	bootstrapServers = flag.String("bootstrap_servers", "kafka_server:9092",
		"(Required) URL of the bootstrap servers for the Kafka cluster. Should be accessible by the runner.")
	topic = flag.String("topic", "kafka_taxirides_realtime", "Kafka topic to write to and read from.")
)

// func init() {
//  register.DoFn2x0[context.Context, []byte](&LogFn{})
// }

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
	/*
		ctx := context.Background()

		p := beam.NewPipeline()
		s := p.Root()

		// In the main function, the code creates a Beam pipeline, reads from the Pub/Sub source, transforms the data into a key-value pair, applies a windowing function to the data, and writes the windowed data to a Kafka topic.

		data := pubsubio.Read(s, "pubsub-public-data", "taxirides-realtime", nil)
		kvData := beam.ParDo(s, func(elm []byte) ([]byte, []byte) { return []byte(""), elm }, data)
		windowed := beam.WindowInto(s, window.NewFixedWindows(15*time.Second), kvData)
		kafkaio.Write(s, *expansionAddr, *bootstrapServers, *topic, windowed)

		if err := beamx.Run(ctx, p); err != nil {
			log.Fatalf(ctx, "Failed to execute job: %v", err)
		}
	*/
}
