package main

// See: https://github.com/GoogleCloudPlatform/DataflowJavaSDK-examples/blob/master/src/main/java/com/google/cloud/dataflow/examples/cookbook/BigQueryTornadoes.java

import (
	"context"
	"flag"
	"log"
	"os"

	"beam"
	"bigqueryio"
	"count"
	"dataflow"
)

// Options used purely at pipeline construction-time can just be flags.
var (
	input  = flag.String("weather_data", "clouddataflow-readonly:samples.weather_stations", "Weather data BQ table.")
	output = flag.String("output", "clouddfe:$USER-tornadoes", "Output BQ table.")
)

type WeatherDataRow struct {
	Tornado bool `bq:"tornado"`
	Month   int  `bq:"month"`
}

// TornadoRow can be reused as a BQ schema and GBK result to avoid the conversions needed in java.
type TornadoRow struct {
	Month int `bq:"month",beam:"key"`
	Count int `bq:"tornado_count",beam:"value"`
}

// Extract outputs the month iff a tornado happened.
func Extract(row WeatherDataRow, out chan<- int) {
	if row.Tornado {
		out <- row.Month
	}
}

func CountTornadoes(p *beam.Pipeline, rows *beam.PCollection) (*beam.PCollection, error) {
	months, err := beam.ParDo(p, Extract, rows)
	if err != nil {
		return nil, err
	}
	return count.PerElement(p, months)
}

func main() {
	flag.Parse()
	*output = os.ExpandEnv(*output)

	p := &beam.Pipeline{}

	// (1) build pipeline

	rows, err := bigqueryio.Read(p, *input, WeatherDataRow{})
	if err != nil {
		log.Fatalf("Failed to constuct pipeline: %v", err)
	}
	out, err := CountTornadoes(p, rows)
	if err != nil {
		log.Fatalf("Failed to constuct pipeline: %v", err)
	}
	if err := bigqueryio.Write(p, *output, TornadoRow{}, bigqueryio.CreateIfNeeded, out); err != nil {
		log.Fatalf("Failed to constuct pipeline: %v", err)
	}

	// (2) execute it on Dataflow

	if err := dataflow.Execute(context.Background(), p); err != nil {
		log.Fatalf("Failed to execute job: %v", err)
	}
	log.Print("Success!")
}
