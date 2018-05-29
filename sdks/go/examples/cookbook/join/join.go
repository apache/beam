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
	"reflect"

	"fmt"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/io/bigqueryio"
	"github.com/apache/beam/sdks/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/go/pkg/beam/log"
	"github.com/apache/beam/sdks/go/pkg/beam/options/gcpopts"
	"github.com/apache/beam/sdks/go/pkg/beam/x/beamx"
)

// See: https://github.com/apache/beam/blob/master/examples/java/src/main/java/org/apache/beam/examples/cookbook/JoinExamples.java

const (
	gdeltEventsTable  = "clouddataflow-readonly:samples.gdelt_sample"
	countryCodesTable = "gdelt-bq:full.crosswalk_geocountrycodetohuman"
)

var (
	output = flag.String("output", "", "Output filename")
)

type Code string

type CountryInfoRow struct {
	Code Code   `bigquery:"FIPSCC"`
	Name string `bigquery:"HumanName"`
}

type EventDataRow struct {
	Code Code   `bigquery:"ActionGeo_CountryCode"`
	Date int    `bigquery:"SQLDATE"`
	Name string `bigquery:"Actor1Name"`
	URL  string `bigquery:"SOURCEURL"`
}

func joinEvents(s beam.Scope, events, countries beam.PCollection) beam.PCollection {
	joined := beam.CoGroupByKey(s,
		beam.ParDo(s, extractEventDataFn, events),
		beam.ParDo(s, extractCountryInfoFn, countries))
	result := beam.ParDo(s, processFn, joined)
	return beam.ParDo(s, formatFn, result)
}

func extractEventDataFn(row EventDataRow) (Code, string) {
	return row.Code, fmt.Sprintf("Date: %v, Actor1: %v, url: %v", row.Date, row.Name, row.URL)
}

func extractCountryInfoFn(row CountryInfoRow) (Code, string) {
	return row.Code, row.Name
}

func processFn(code Code, events, countries func(*string) bool, emit func(Code, string)) {
	name := "none"
	countries(&name) // grab first (and only) country name, if any

	var event string
	for events(&event) {
		emit(code, fmt.Sprintf("Country name: %v, Event info: %v", name, event))
	}
}

func formatFn(code Code, info string) string {
	return fmt.Sprintf("Country code: %v, %v", code, info)
}

func main() {
	flag.Parse()
	beam.Init()

	ctx := context.Background()

	if *output == "" {
		log.Exit(ctx, "No output filename specified. Use --output=<filename>")
	}
	project := gcpopts.GetProject(ctx)

	log.Info(ctx, "Running join")

	p := beam.NewPipeline()
	s := p.Root()
	events := bigqueryio.Read(s, project, gdeltEventsTable, reflect.TypeOf(EventDataRow{}))
	countries := bigqueryio.Read(s, project, countryCodesTable, reflect.TypeOf(CountryInfoRow{}))
	formatted := joinEvents(s, events, countries)
	textio.Write(s, *output, formatted)

	if err := beamx.Run(ctx, p); err != nil {
		log.Exitf(ctx, "Failed to execute job: %v", err)
	}
}
