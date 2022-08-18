
package main

import (
	"beam.apache.org/learning/katas/core_transforms/side_input/side_input/pkg/task"
	"context"
	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/log"
	"github.com/apache/beam/sdks/go/pkg/beam/x/beamx"
	"github.com/apache/beam/sdks/go/pkg/beam/x/debug"
)

type Person struct {
	Name, City, Country string
}

func main() {
	ctx := context.Background()

	p, s := beam.NewPipelineWithRoot()

    // List of elements
	citiesToCountriesKV := beam.ParDo(s, func(_ []byte, emit func(string, string)){
		emit("Beijing", "China")
		emit("London", "United Kingdom")
		emit("San Francisco", "United States")
		emit("Singapore", "Singapore")
		emit("Sydney", "Australia")
	}, beam.Impulse(s))

	persons := beam.Create(s,
		task.Person{Name: "Henry", City: "Singapore"},
		task.Person{Name: "Jane", City: "San Francisco"},
		task.Person{Name: "Lee", City: "Beijing"},
		task.Person{Name: "John", City: "Sydney"},
		task.Person{Name: "Alfred", City: "London"},
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

