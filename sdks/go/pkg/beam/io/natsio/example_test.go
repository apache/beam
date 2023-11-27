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

package natsio_test

import (
	"context"
	"log"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/natsio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
	"github.com/nats-io/nats.go"
)

func ExampleWrite() {
	beam.Init()

	p, s := beam.NewPipelineWithRoot()

	uri := "nats://localhost:4222"
	msgs := []natsio.ProducerMessage{
		{
			Subject: "events.1",
			ID:      "123",
			Data:    []byte("hello"),
			Headers: nats.Header{"key": []string{"val1"}},
		},
		{
			Subject: "events.2",
			ID:      "124",
			Data:    []byte("world"),
			Headers: nats.Header{"key": []string{"val2"}},
		},
	}

	input := beam.CreateList(s, msgs)
	natsio.Write(s, uri, input)

	if err := beamx.Run(context.Background(), p); err != nil {
		log.Fatalf("Failed to execute job: %v", err)
	}
}
