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

// import is a pipeline example using the fhirio connector to bulk import FHIR
// resources from GCS into a given FHIR store.
//
// Pre-requisites:
// 1. NDJSON-encoded FHIR resources stored in GCS.
// 2. Dataflow Runner enabled: https://cloud.google.com/dataflow/docs/quickstarts.
// 3. A Google Cloud FHIR store.
//
// Running this pipeline requires providing a fully qualified GCS address
// (potentially containing wildcards) to where your FHIR resources are stored, a
// path to the FHIR store where the resources should be written to, in addition
// to the usual flags for the Dataflow runner.
//
// An example command for executing this pipeline on GCP is as follows:
//
//	export PROJECT="$(gcloud config get-value project)"
//	export TEMP_LOCATION="gs://MY-BUCKET/temp"
//	export STAGING_LOCATION="gs://MY-BUCKET/staging"
//	export REGION="us-central1"
//	export SOURCE_GCS_LOCATION="gs://MY_BUCKET/path/to/resources/**"
//	export FHIR_STORE_PATH="MY_FHIR_STORE_PATH"
//	cd ./sdks/go
//	go run ./examples/fhirio/import/import.go \
//	  --runner=dataflow \
//	  --temp_location=$TEMP_LOCATION \
//	  --staging_location=$STAGING_LOCATION \
//	  --project=$PROJECT \
//	  --region=$REGION \
//	  --worker_harness_container_image=apache/beam_go_sdk:latest \
//	  --sourceGcsLocation=$SOURCE_GCS_LOCATION \
//	  --fhirStore=$FHIR_STORE_PATH
package main

import (
	"context"
	"flag"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/fhirio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
)

var (
	// Required flag with the source directory for GCS files to read, including
	// wildcards. Directory should contain the resources files in NDJSON format.
	sourceGcsLocation = flag.String("sourceGcsLocation", "", "The source directory for GCS files to read, including wildcards.")

	// Required flag with target FHIR store to write data to, must be of the full format:
	// "projects/project_id/locations/location/datasets/DATASET_ID/fhirStores/FHIR_STORE_ID"
	fhirStore = flag.String("fhirStore", "", "The target FHIR Store to write data to, must be of the full format.")
)

func main() {
	flag.Parse()
	beam.Init()

	p, s := beam.NewPipelineWithRoot()

	// Read resources from GCS.
	resourcesInGcs := textio.Read(s, *sourceGcsLocation)

	// Import the read resources to the provided FHIR store.
	fhirio.Import(s, *fhirStore, "", "", fhirio.ContentStructureResource, resourcesInGcs)

	ctx := context.Background()
	if err := beamx.Run(ctx, p); err != nil {
		log.Fatalf(ctx, "Failed to execute job: %v", err)
	}
}
