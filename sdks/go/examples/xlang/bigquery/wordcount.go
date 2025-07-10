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

// Wordcount is an example using cross-language BigQuery transforms to read and write to BigQuery.
// This example runs a batch pipeline that reads from the public table "shakespeare" described here:
// https://cloud.google.com/bigquery/public-data#sample_tables. It reads the data of word counts per
// different work, aggregates them to find total word counts in all works, as well as the average
// number of times a word appears if it appears in a work, and then writes all that data to a given
// output table.
//
// This example is only expected to work on Dataflow, and requires a cross-language expansion
// service that can expand BigQuery read and write transforms. An address to a persistent expansion
// service can be provided as a flag, or if none is specified then the SDK will attempt to
// automatically start an appropriate expansion service.
//
// # Running an Expansion Server
//
// If the automatic expansion service functionality is not available for your environment, or if
// you want improved performance, you will need to start a persistent expansion service. These
// instructions will cover running the Java SchemaIO Expansion Service, and therefore requires a JDK
// installation in a version supported by Beam. Depending on whether you are running this from a
// numbered Beam release, or a development environment, there are two sources you may use for the
// Expansion service.
//
// Numbered release: The expansion service jar is vendored as module
// org.apache.beam:beam-sdks-java-io-google-cloud-platform-expansion-service in Maven Repository.
// This jar can be executed directly with the following command:
//
//	`java -jar <jar_name> <port_number>`
//
// Development env: This requires that the JAVA_HOME environment variable points to your JDK
// installation. From the root `beam/` directory of the Apache Beam repository, the jar can be
// built (or built and run) with the following commands:
//
//	./gradlew :sdks:java:io:google-cloud-platform:expansion-service:build
//	./gradlew :sdks:java:io:google-cloud-platform:expansion-service:runExpansionService -PconstructionService.port=<port_num>
//
// # Running the Example on GCP
//
// An example command for executing this pipeline on GCP is as follows:
//
//	export PROJECT="$(gcloud config get-value project)"
//	export TEMP_LOCATION="gs://MY-BUCKET/temp"
//	export REGION="us-central1"
//	export JOB_NAME="bigquery-wordcount-`date +%Y%m%d-%H%M%S`"
//	export OUTPUT_TABLE="123.45.67.89:1234"
//	export EXPANSION_ADDR="localhost:1234"
//	export OUTPUT_TABLE="project_id:dataset_id.table_id"
//	go run ./sdks/go/examples/kafka/types/types.go \
//	  --runner=DataflowRunner \
//	  --temp_location=$TEMP_LOCATION \
//	  --staging_location=$STAGING_LOCATION \
//	  --project=$PROJECT \
//	  --region=$REGION \
//	  --job_name="${JOB_NAME}" \
//	  --bootstrap_servers=$BOOTSTRAP_SERVER \
//	  --expansion_addr=$EXPANSION_ADDR \
//	  --out_table=$OUTPUT_TABLE
//
// # Running the Example From a Git Clone
//
// When running on a development environment, a custom container will likely need to be provided
// for the cross-language SDK. First this will require building and pushing the SDK container to
// container repository, such as Docker Hub.
//
//	export DOCKER_ROOT="Your Docker Repository Root"
//	./gradlew :sdks:java:container:java11:docker -Pdocker-repository-root=$DOCKER_ROOT -Pdocker-tag=latest
//	docker push $DOCKER_ROOT/beam_java11_sdk:latest
//
// For runners in local mode, simply building the container using the default values for
// docker-repository-root and docker-tag will work to have it accessible locally.
//
// Additionally, you must provide the location of your custom container to the pipeline with the
// --sdk_harness_container_image_override flag for Java, or --environment_config flag for Go. For
// example:
//
//	--sdk_harness_container_image_override=".*java.*,${DOCKER_ROOT}/beam_java11_sdk:latest" \
//	--environment_config=${DOCKER_ROOT}/beam_go_sdk:latest
package main

import (
	"context"
	"flag"
	"math"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/xlang/bigqueryio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
)

var (
	// Set this to the address of the expansion service to use for BigQuery read and write, or leave
	// unspecified to attempt to automatically start an expansion service.
	expansionAddr = flag.String("expansion_addr", "",
		"Address of Expansion Service. If not specified, attempts to automatically start an appropriate expansion service.")
	// Set this required option to specify where to write the output. If the table does not exist,
	// a new one will be created. If the table already exists, elements will be appended to it.
	outTable = flag.String("out_table", "", "Output table (required).")
)

func init() {
	register.Combiner3[WordsAccum, ShakesRow, CountsRow](&WordsCombine{})
}

// ShakesRow is a struct corresponding to the schema of the Shakespeare input table. In order to
// be read properly, field names must match names from the BigQuery table, so some fields must
// include underlines.
type ShakesRow struct {
	Word        string `beam:"word"`
	Word_count  int64  `beam:"word_count"`
	Corpus      string `beam:"corpus"`
	Corpus_date int64  `beam:"corpus_date"`
}

// CountsRow is a struct corresponding to the schema of the output table. For writes, field names
// are derived from the Beam schema names specified below as struct tags.
type CountsRow struct {
	// Word is the word being counted.
	Word string `beam:"word"`
	// WordCount is the count of how many times the word appears in all works combined.
	WordCount int64 `beam:"word_count"`
	// CorpusCount is the count of how many works the word appears in.
	CorpusCount int64 `beam:"corpus_count"`
	// AvgCount is the average number of times a word appears in all works that it appears in. In
	// other words, this is equivalent to WordCount divided by CorpusCount.
	AvgCount float64 `beam:"avg_count"`
}

// WordsAccum is an accumulator for combining Shakespeare word counts in order to get averages of
// word counts.
type WordsAccum struct {
	// Word is the word being counted.
	Word string
	// Count is the number of times this word has appeared, or in other words the number of corpuses
	// it appears in (assuming that the input never repeats a word and corpus pair.
	Count int64
	// Sum is the sum of word counts from inputs.
	Sum int64
}

// WordsCombine is a CombineFn that adds up word counts and calculates average number of counts.
type WordsCombine struct{}

// CreateAccumulator creates a default WordsAccum.
func (fn *WordsCombine) CreateAccumulator() WordsAccum {
	return WordsAccum{}
}

// AddInput sums up word counts and increments the corpus count.
func (fn *WordsCombine) AddInput(a WordsAccum, row ShakesRow) WordsAccum {
	a.Word = row.Word
	a.Count += 1
	a.Sum += row.Word_count
	return a
}

// MergeAccumulators sums up the various counts being accumulated.
func (fn *WordsCombine) MergeAccumulators(a, v WordsAccum) WordsAccum {
	return WordsAccum{Word: a.Word, Count: a.Count + v.Count, Sum: a.Sum + v.Sum}
}

// ExtractOutput calculates the average and fills out the output rows.
func (fn *WordsCombine) ExtractOutput(a WordsAccum) CountsRow {
	row := CountsRow{
		Word:        a.Word,
		WordCount:   a.Sum,
		CorpusCount: a.Count,
	}
	if a.Count == 0 {
		row.AvgCount = math.NaN()
	} else {
		row.AvgCount = float64(a.Sum) / float64(a.Count)
	}
	return row
}

func main() {
	flag.Parse()
	beam.Init()

	p := beam.NewPipeline()
	s := p.Root()

	// Read from the public BigQuery table.
	inType := reflect.TypeOf((*ShakesRow)(nil)).Elem()
	rows := bigqueryio.Read(s, inType,
		bigqueryio.FromTable("bigquery-public-data:samples.shakespeare"),
		bigqueryio.ReadExpansionAddr(*expansionAddr))

	// Combine the data per word.
	keyed := beam.ParDo(s, func(elm ShakesRow) (string, ShakesRow) {
		return elm.Word, elm
	}, rows)
	counts := beam.CombinePerKey(s, &WordsCombine{}, keyed)
	countVals := beam.DropKey(s, counts)

	// Write the data to the given BigQuery table destination, creating the table if needed.
	bigqueryio.Write(s, *outTable, countVals,
		bigqueryio.CreateDisposition(bigqueryio.CreateIfNeeded),
		bigqueryio.WriteExpansionAddr(*expansionAddr))

	ctx := context.Background()
	if err := beamx.Run(ctx, p); err != nil {
		log.Fatalf(ctx, "Failed to execute job: %v", err)
	}
}
