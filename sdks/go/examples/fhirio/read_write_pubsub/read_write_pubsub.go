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

// read_write_pubsub is a pipeline example using the fhirio connector to read
// FHIR resources from GCS, write them to a GCP FHIR store, and, if a PubSub
// topic is provided, read the written resources from the FHIR store and log them
// based on the PubSub notifications about store updates.
//
// Pre-requisites:
// 1. NDJSON-encoded FHIR resources stored in GCS.
// 2. Dataflow Runner enabled: https://cloud.google.com/dataflow/docs/quickstarts.
// 3. A Google Cloud FHIR store. Optionally, PubSub notifications set up on the store.
// (see: https://cloud.google.com/healthcare-api/docs/concepts/pubsub).
//
// Running this pipeline requires providing a fully qualified GCS address
// (potentially containing wildcards) to where your FHIR resources are stored, a
// path to the FHIR store where the resources should be written to, and,
// optionally, the PubSub topic name your FHIR store is sending notifications to,
// in addition to the usual flags for the Dataflow runner.
//
// An example command for executing this pipeline on GCP is as follows:
//
//	export PROJECT="$(gcloud config get-value project)"
//	export TEMP_LOCATION="gs://MY-BUCKET/temp"
//	export STAGING_LOCATION="gs://MY-BUCKET/staging"
//	export REGION="us-central1"
//	export SOURCE_GCS_LOCATION="gs://MY_BUCKET/path/to/resources/**"
//	export FHIR_STORE_PATH="MY_FHIR_STORE_PATH"
//	export PUBSUB_TOPIC="MY_FHIR_STORE_TOPIC"
//	cd ./sdks/go
//	go run ./examples/fhirio/read_write_pubsub/read_write_pubsub.go \
//	  --runner=dataflow \
//	  --temp_location=$TEMP_LOCATION \
//	  --staging_location=$STAGING_LOCATION \
//	  --project=$PROJECT \
//	  --region=$REGION \
//	  --worker_harness_container_image=apache/beam_go_sdk:latest \
//	  --sourceGcsLocation=$SOURCE_GCS_LOCATION \
//	  --fhirStore=$FHIR_STORE_PATH \
//	  --pubsubTopic=$PUBSUB_TOPIC
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/fhirio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/pubsubio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/options/gcpopts"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
)

var (
	// Required flag with the source directory for GCS files to read, including
	// wildcards. Directory should contain the resources files in NDJSON format.
	sourceGcsLocation = flag.String("sourceGcsLocation", "", "The source directory for GCS files to read, including wildcards.")

	// Required flag with target FHIR store to write data to, must be of the full format:
	// "projects/project_id/locations/location/datasets/DATASET_ID/fhirStores/FHIR_STORE_ID"
	fhirStore = flag.String("fhirStore", "", "The target FHIR Store to write data to, must be of the full format.")

	// Optional flag with the pubsub topic of your FHIR store to read and log upon store updates.
	pubsubTopic = flag.String("pubsubTopic", "", "The PubSub topic to listen to.")
)

func init() {
	register.Function1x1[string, string](WrapInBundle)
	register.DoFn2x0[context.Context, string](&LoggerFn{})
}

// WrapInBundle takes a FHIR resource string and wraps it as a Bundle resource.
// Useful so we can publish the given resource through ExecuteBundles.
func WrapInBundle(resource string) string {
	var r struct {
		ResourceType string `json:"resourceType"`
	}
	json.NewDecoder(strings.NewReader(resource)).Decode(&r)
	return fmt.Sprintf(`{
        "resourceType": "Bundle",
        "type": "batch",
        "entry": [
        	{
        		"request": {
        			"method": "POST",
        			"url": "%s"
        		},
        		"resource": %s
        	}
		]
	}`, r.ResourceType, resource)
}

// LoggerFn is a helper DoFn to log elements received.
type LoggerFn struct {
	LogPrefix string
}

// ProcessElement logs each element it receives.
func (fn *LoggerFn) ProcessElement(ctx context.Context, elm string) {
	log.Infof(ctx, "%s: %v", fn.LogPrefix, elm)
}

// FinishBundle waits a bit so the job server finishes receiving logs.
func (fn *LoggerFn) FinishBundle() {
	time.Sleep(2 * time.Second)
}

func main() {
	flag.Parse()
	beam.Init()

	p, s := beam.NewPipelineWithRoot()

	// Read resources from GCS.
	resourcesInGcs := textio.Read(s, *sourceGcsLocation)
	resourceBundles := beam.ParDo(s, WrapInBundle, resourcesInGcs)

	// Write resources to store.
	_, failedWritesErrorMessage := fhirio.ExecuteBundles(s, *fhirStore, resourceBundles)
	beam.ParDo0(s, &LoggerFn{"Failed Write"}, failedWritesErrorMessage)

	if *pubsubTopic != "" {
		// PubSub notifications will be emitted containing the path of the resource once
		// it is written to the store. Simultaneously read notifications and resources
		// from PubSub and store, respectively.
		resourceNotifications := pubsubio.Read(s, *gcpopts.Project, *pubsubTopic, nil)
		resourcesInFhirStore, deadLetters := fhirio.Read(s, resourceNotifications)

		// Log the read resources or read errors to the server.
		beam.ParDo0(s, &LoggerFn{"Read Resource"}, resourcesInFhirStore)
		beam.ParDo0(s, &LoggerFn{"Got Dead Letter"}, deadLetters)
	}

	ctx := context.Background()
	if err := beamx.Run(ctx, p); err != nil {
		log.Fatalf(ctx, "Failed to execute job: %v", err)
	}
}
