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

package main

import (
	"context"
	"flag"
	"github.com/apache/beam/sdks/go/pkg/beam/io/kafkaio"
	log "github.com/apache/beam/sdks/go/pkg/beam/log"
	"github.com/apache/beam/sdks/go/pkg/beam/x/beamx"
	"reflect"
	"time"

	"github.com/apache/beam/sdks/go/pkg/beam"
)

var (
	expansionAddr    = flag.String("expansion_addr", "", "Address of Expansion Service")
	bootstrapServers = flag.String("bootstrap_servers", "",
		"URL of the bootstrap servers for the Kafka cluster. Should be accessible by the runner.")
	topic = flag.String("topic", "kafka_taxirides_realtime", "Kafka topic to write to and read from.")
)

func init() {
	beam.RegisterType(reflect.TypeOf((*LogFn)(nil)).Elem())
}

// LogFn is a DoFn to log rides.
type LogFn struct{}

// ProcessElement logs each element it receives.
func (fn *LogFn) ProcessElement(ctx context.Context, elm []byte) {
	log.Infof(ctx, "Ride timestamp %v with info: %v", string(elm))
}

// FinishBundle waits a bit so the job server finishes receiving logs.
func (fn *LogFn) FinishBundle() {
	time.Sleep(2 * time.Second)
}

func main() {
	flag.Parse()
	beam.Init()

	ctx := context.Background()
	if *expansionAddr == "" {
		log.Fatal(ctx, "No expansion address provided")
	}

	p := beam.NewPipeline()
	s := p.Root()

	log.Infof(ctx, "Hello debug!")
	read := kafkaio.Read(s, *expansionAddr, *bootstrapServers, []string{*topic},
		kafkaio.MaxNumRecords(60))
	vals := beam.DropKey(s, read)
	beam.ParDo0(s, &LogFn{}, vals)

	if err := beamx.Run(ctx, p); err != nil {
		log.Fatalf(ctx, "Failed to execute job: %v", err)
	}
}
