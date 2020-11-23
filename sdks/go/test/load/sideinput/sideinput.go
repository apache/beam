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
    sideInputType = flag.String("side_input_type", "iter",
        "Specifies how the side input will be materialized in ParDo operation. One of [dict, iter, list].")
    windowCount = flag.Int("window_count", 1,
        "The number of fixed sized windows to subdivide the side input into.")
    accessPercentage = flag.Int("access_percentage", 100,
        "Specifies the percentage of elements in the side input to be accessed.")
    sdfInitialElements = flag.Int("sdf_initial_elements", 1000,
        "The number of SDF Initial Elements.")
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


type sourceSingleFromSourceConfig struct {
    s beam.Scope
}

func (fn *sourceSingleFromSourceConfig) ProcessElement (ctx context.Context, _ []uint8, config synthetic.SourceConfig) beam.PCollection {
    return synthetic.SourceSingle(fn.s, config)
}

type syntheticSDFAsSource struct {
    s beam.Scope
}

func (fn *syntheticSDFAsSource) ProcessElement (ctx context.Context, _ []int) beam.PCollection {
    return beam.ParDo(fn.s, &sourceSingleFromSourceConfig{s: fn.s}, beam.Impulse(fn.s),
        beam.SideInput{Input: beam.Create(fn.s, parseSyntheticSourceConfig())})
}

type doFn struct {
    s beam.Scope
}

type innerDoFn struct {}

func (fn *innerDoFn) ProcessElement (ctx context.Context, test []uint8) {
    //fmt.Println(test)
}

type sequenceSideInputTestDoFn struct {
    s beam.Scope
    elementsToAccess int
}

func (fn *doFn) ProcessElement (ctx context.Context, inner beam.PCollection) {
    beam.ParDo0(fn.s, &innerDoFn{}, inner)
}

func (fn *sequenceSideInputTestDoFn) ProcessElement (ctx context.Context, _ []uint8, collection beam.PCollection) {
    beam.ParDo0(fn.s, &doFn{s: fn.s}, collection)
}


func main() {
    flag.Parse()
    beam.Init()
    ctx := context.Background()
    p, s := beam.NewPipelineWithRoot()

    syntheticSourceConfig := parseSyntheticSourceConfig()
    elementsPerWindow := syntheticSourceConfig.NumElements / *windowCount
    elementsToAccess := elementsPerWindow * *accessPercentage / 100

    var mainInputValues []int
    for i := 0; i < *windowCount; i++ {
        mainInputValues = append(mainInputValues, i)
    }
    mainInput := beam.Create(s, mainInputValues)

    var sideInput beam.PCollection
    initialElements := *sdfInitialElements
    if *windowCount > 1 {
        sideInput = mainInput
        initialElements = *windowCount
    } else {
        var sideInputValues []int
        for i := 0; i < initialElements; i++ {
            sideInputValues = append(sideInputValues, i)
        }
        sideInput = beam.Create(s, sideInputValues)
    }
    sideInput = beam.ParDo(s, &syntheticSDFAsSource{s: s}, sideInput)

    beam.ParDo0(s, &sequenceSideInputTestDoFn{s: s, elementsToAccess: elementsToAccess}, beam.Impulse(s),
        beam.SideInput{Input: sideInput})

    if err := beamx.Run(ctx, p); err != nil {
        log.Exitf(ctx, "Failed to execute job: %v", err)
    }
}
