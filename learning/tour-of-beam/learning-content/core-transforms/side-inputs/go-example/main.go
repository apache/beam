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

// beam-playground:
//   name: side-inputs
//   description: Side-inputs example.
//   multifile: false
//   context_line: 41
//   categories:
//     - Quickstart
//   complexity: MEDIUM
//   tags:
//     - hellobeam

package main

import (
	"context"
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/debug"
)

type Person struct {
	Name, City, Country string
}

func main() {
	ctx := context.Background()

	p, s := beam.NewPipelineWithRoot()

	// List of elements
	citiesToCountriesKV := beam.ParDo(s, func(_ []byte, emit func(string, string)) {
		emit("Beijing", "China")
		emit("London", "United Kingdom")
		emit("San Francisco", "United States")
		emit("Singapore", "Singapore")
		emit("Sydney", "Australia")
	}, beam.Impulse(s))

	persons := beam.Create(s,
		Person{Name: "Henry", City: "Singapore"},
		Person{Name: "Jane", City: "San Francisco"},
		Person{Name: "Lee", City: "Beijing"},
		Person{Name: "John", City: "Sydney"},
		Person{Name: "Alfred", City: "London"},
	)

	// The applyTransform() converts [input] to [output]
	output := applyTransform(s, persons, citiesToCountriesKV)

	debug.Print(s, output)

	err := beamx.Run(ctx, p)

	if err != nil {
		log.Exitf(context.Background(), "Failed to execute job: %v", err)
	}
}

// Create from citiesToCountries view "citiesToCountriesView" and used in ParDo
func applyTransform(s beam.Scope, personsKV beam.PCollection, citiesToCountries beam.PCollection) beam.PCollection {
	citiesToCountriesView := beam.SideInput{
		Input: citiesToCountries,
	}
	return beam.ParDo(s, joinFn, personsKV, citiesToCountriesView)
}

// Compares cities and unites
func joinFn(person Person, citiesToCountriesIter func(*string, *string) bool, emit func(Person)) {
	var city, country string
	for citiesToCountriesIter(&city, &country) {
		if person.City == city {
			emit(Person{
				Name:    person.Name,
				City:    city,
				Country: country,
			})
			break
		}
	}
}
