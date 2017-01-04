package main

// See: https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/examples/src/main/java/com/google/cloud/dataflow/examples/cookbook/JoinExamples.java

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"

	"beam"
	"bigqueryio"
	"dataflow"
	"textio"
)

var (
	output = flag.String("output", "gs://foo/join", "Prefix of output.")
)

const (
	eventTable       = "clouddataflow-readonly:samples.gdelt_sample"
	countryCodeTable = "gdelt-bq:full.crosswalk_geocountrycodetohuman"
)

type EventRow struct {
	Country string `bq:"ActionGeo_CountryCode"`
	Date    string `bq:"SQLDATE"`
	Actor   string `bq:"Actor1Name"`
	Source  string `bq:"SOURCEURL"`
}

type CountryCodeRow struct {
	Country string `bq:"FIPSCC"`
	Name    string `bq:"HumanName"`
}

type KV struct {
	Country string `beam:"key"`
	Value   string `beam:"value"`
}

func ExtractEventData(row EventRow) KV {
	info := fmt.Sprintf("Date: %v, Actor1: %v, url: %v", row.Date, row.Actor, row.Source)
	return KV{row.Country, info}
}

// NOTE: we could have annotated the CountryCodeRow instead to avoid this conversion. CoGBK does
// not need the same types for the components.

func ExtractCountryInfo(row CountryCodeRow) KV {
	return KV{row.Country, row.Name}
}

// TODO: for CoGBK results (and elsewhere), we would in theory accept non-channel types
// to indicate singleton or finite results, instead of manually drain the channels. However,
// this may make classification too hard. However, doing so may also help us make it more
// clear that re-iteration is not needed, say (if specifying a chan).

// BAD: The result of GBK and CoGBK has some special structure that can be tricky to
// match up correctly for users, due to the weak typing at this level.

func Explode(key string, country string, events []string, out chan<- KV) {
	for _, event := range events {
		out <- KV{key, fmt.Sprintf("Country name: %v, Event info: %v", country, event)}
	}
}

func Format(kv KV) string {
	return fmt.Sprintf("Country code: %v, %v", kv.Country, kv.Value)
}

func joinEvents(p *beam.Pipeline, events, countries *beam.PCollection) (*beam.PCollection, error) {
	keyedEvents, err := beam.ParDo(p, ExtractEventData, events)
	if err != nil {
		return nil, err
	}
	keyedCountries, err := beam.ParDo(p, ExtractCountryInfo, countries)
	if err != nil {
		return nil, err
	}

	// TODO: replicate tuple tag? We're just positional here, for now.

	ret, err := beam.CoGBK(p, keyedCountries, keyedEvents)
	if err != nil {
		return nil, err
	}
	expl, err := beam.ParDo(p, Explode, ret)
	if err != nil {
		return nil, err
	}
	return beam.ParDo(p, Format, expl)
}

func main() {
	flag.Parse()
	*output = os.ExpandEnv(*output)

	p := &beam.Pipeline{}

	// (1) build pipeline

	events, err := bigqueryio.Read(p, eventTable, EventRow{})
	if err != nil {
		log.Fatalf("Failed to constuct pipeline: %v", err)
	}
	countries, err := bigqueryio.Read(p, countryCodeTable, CountryCodeRow{})
	if err != nil {
		log.Fatalf("Failed to constuct pipeline: %v", err)
	}
	out, err := joinEvents(p, events, countries)
	if err != nil {
		log.Fatalf("Failed to constuct pipeline: %v", err)
	}
	if err := textio.Write(p, *output, out); err != nil {
		log.Fatalf("Failed to constuct pipeline: %v", err)
	}

	// (2) execute it on Dataflow

	if err := dataflow.Execute(context.Background(), p); err != nil {
		log.Fatalf("Failed to execute job: %v", err)
	}
	log.Print("Success!")
}
