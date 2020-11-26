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
    "github.com/apache/beam/sdks/go/pkg/beam"
    "github.com/apache/beam/sdks/go/pkg/beam/io/synthetic"
    "github.com/apache/beam/sdks/go/pkg/beam/log"
    "github.com/apache/beam/sdks/go/pkg/beam/x/beamx"
)

var (
    windowCount = flag.Int(
        "window_count",
        1,
        "The number of fixed sized windows to subdivide the side input into.")
    accessPercentage = flag.Int(
        "access_percentage",
        100,
        "Specifies the percentage of elements in the side input to be accessed.")
    syntheticSourceConfig = flag.String(
        "input_options",
        "{"+
            "\"num_records\": 300, "+
            "\"key_size\": 5, "+
            "\"value_size\": 15}",
        "A JSON object that describes the configuration for synthetic source")
)

func parseSyntheticSourceConfig() synthetic.SourceConfig {
    if *syntheticSourceConfig == "" {
        panic("--input_options not provided")
    } else {
        encoded := []byte(*syntheticSourceConfig)
        return synthetic.DefaultSourceConfig().BuildFromJSON(encoded)
    }
}

type doFn struct {
    elementsToAccess int
}

func (fn *doFn) ProcessElement(key []byte){
}


func main() {
    flag.Parse()
    beam.Init()
    ctx := context.Background()
    p, s := beam.NewPipelineWithRoot()

    syntheticSourceConfig := parseSyntheticSourceConfig()
    // elementsPerWindow := syntheticSourceConfig.NumElements / *windowCount
    // elementsToAccess := elementsPerWindow * *accessPercentage / 100

    beam.ParDo0(s, func(_ []byte, kv func (key *[]byte) bool) {
        return
    }, beam.Impulse(s),
        beam.SideInput{Input: synthetic.SourceSingle(s, syntheticSourceConfig)})

    if err := beamx.Run(ctx, p); err != nil {
        log.Exitf(ctx, "Failed to execute job: %v", err)
    }
}
