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

// readavro is a simple Avro read/write Example
// This example uses a 500 Byte sample avro file [twitter.avro]
// download here: https://s3-eu-west-1.amazonaws.com/daidokoro-dev/apache/twitter.avro
package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/avroio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/debug"
)

var (
	input  = flag.String("input", "./twitter.avro", "input avro file")
	output = flag.String("output", "./output.avro", "output avro file")
)

// Doc type used to unmarshal avro json data
type Doc struct {
	Stamp int64  `json:"timestamp"`
	Tweet string `json:"tweet"`
	User  string `json:"username"`
}

// Note that the schema is only required for Writing avro.
// not Reading.
const schema = `{
	"type": "record",
	"name": "tweet",
	"namespace": "twitter",
	"fields": [
		{ "name": "timestamp", "type": "double" },
		{ "name": "tweet", "type": "string" },
		{ "name": "username", "type": "string" }
	]
}`

func main() {
	flag.Parse()
	beam.Init()

	p := beam.NewPipeline()
	s := p.Root()

	// read rows and return JSON string PCollection - PCollection<string>
	rows := avroio.Read(s, *input, reflect.TypeOf(""))
	debug.Print(s, rows)

	// read rows and return Doc Type PCollection - PCollection<Doc>
	docs := avroio.Read(s, *input, reflect.TypeOf(Doc{}))
	debug.Print(s, docs)

	// update all values with a single user and tweet.
	format := beam.ParDo(s, func(d Doc, emit func(string)) {
		d.User = "daidokoro"
		d.Tweet = "I was here......"

		b, _ := json.Marshal(d)
		emit(string(b))
	}, docs)

	debug.Print(s, format)

	// write output
	avroio.Write(s, *output, schema, format)

	if err := beamx.Run(context.Background(), p); err != nil {
		log.Fatalf("Failed to execute job: %v", err)
	}
}
