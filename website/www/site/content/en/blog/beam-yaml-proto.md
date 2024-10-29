---
title: "Efficient Streaming Data Processing with Beam YAML and Protobuf"
date: "2024-09-20T11:53:38+02:00"
categories:
  - blog
authors:
  - ffernandez92
---
<!--
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Efficient Streaming Data Processing with Beam YAML and Protobuf

As streaming data processing grows, so do its maintenance, complexity, and costs.
This post explains how to efficiently scale pipelines by using [Protobuf](https://protobuf.dev/),
which ensures that pipelines are reusable and quick to deploy. The goal is to keep this process simple
for engineers to implement using [Beam YAML](https://beam.apache.org/documentation/sdks/yaml/).

<!--more-->

## Simplify pipelines with Beam YAML

Creating a pipeline in Beam can be somewhat difficult, especially for new Apache Beam users.
Setting up the project, managing dependencies, and so on can be challenging.
Beam YAML eliminates most of the boilerplate code,
which allows you to focus on the most important part of the work: data transformation.

Some of the key benefits of Beam YAML include:

*   **Readability:** By using a declarative language ([YAML](https://yaml.org/)), the pipeline configuration is more human readable.
*   **Reusability:** Reusing the same components across different pipelines is simplified.
*   **Maintainability:** Pipeline maintenance and updates are easier.

The following template shows an example of reading events from a [Kafka](https://kafka.apache.org/intro) topic and
writing them into [BigQuery](https://cloud.google.com/bigquery?hl=en).

```yaml
pipeline:
  transforms:
    - type: ReadFromKafka
      name: ReadProtoMovieEvents
      config:
        topic: 'TOPIC_NAME'
        format: RAW/AVRO/JSON/PROTO
        bootstrap_servers: 'BOOTSTRAP_SERVERS'
        schema: 'SCHEMA'
    - type: WriteToBigQuery
      name: WriteMovieEvents
      input: ReadProtoMovieEvents
      config:
        table: 'PROJECT_ID.DATASET.MOVIE_EVENTS_TABLE'
        useAtLeastOnceSemantics: true

options:
  streaming: true
  dataflow_service_options: [streaming_mode_at_least_once]
```

## The complete workflow

This section demonstrates the complete workflow for this pipeline.

### Create a simple proto event

The following code creates a simple movie event.

```protobuf
// events/v1/movie_event.proto

syntax = "proto3";

package event.v1;

import "bq_field.proto";
import "bq_table.proto";
import "buf/validate/validate.proto";
import "google/protobuf/wrappers.proto";

message MovieEvent {
  option (gen_bq_schema.bigquery_opts).table_name = "movie_table";
  google.protobuf.StringValue event_id = 1 [(gen_bq_schema.bigquery).description = "Unique Event ID"];
  google.protobuf.StringValue user_id = 2 [(gen_bq_schema.bigquery).description = "Unique User ID"];
  google.protobuf.StringValue movie_id = 3 [(gen_bq_schema.bigquery).description = "Unique Movie ID"];
  google.protobuf.Int32Value rating = 4 [(buf.validate.field).int32 = {
    // validates the average rating is at least 0
    gte: 0,
    // validates the average rating is at most 100
    lte: 100
  }, (gen_bq_schema.bigquery).description = "Movie rating"];
  string event_dt = 5 [
    (gen_bq_schema.bigquery).type_override = "DATETIME",
    (gen_bq_schema.bigquery).description = "UTC Datetime representing when we received this event. Format: YYYY-MM-DDTHH:MM:SS",
    (buf.validate.field) = {
      string: {
        pattern: "^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}$"
      },
      ignore_empty: false,
    }
  ];
}
```

Because these events are written to BigQuery,
the [`bq_field`](https://buf.build/googlecloudplatform/bq-schema-api/file/main:bq_field.proto) proto
and the [`bq_table`](https://buf.build/googlecloudplatform/bq-schema-api/file/main:bq_table.proto) proto are imported.
These proto files help generate the BigQuery JSON schema.
This example also demonstrates a shift-left approach, which moves testing, quality,
and performance as early as possible in the development process. For example, to ensure that only valid events are generated from the source, the `buf.validate` elements are included.

After you create the `movie_event.proto` proto in the `events/v1` folder, you can generate
the necessary [file descriptor](https://buf.build/docs/reference/descriptors).
A file descriptor is a compiled representation of the schema that allows various tools and systems
to understand and work with protobuf data dynamically. To simplify the process, this example uses Buf,
which requires the following configuration files.


<b>Buf configuration:</b>

```yaml
# buf.yaml

version: v2
deps:
  - buf.build/googlecloudplatform/bq-schema-api
  - buf.build/bufbuild/protovalidate
breaking:
  use:
    - FILE
lint:
  use:
    - DEFAULT
```

```yaml
# buf.gen.yaml

version: v2
managed:
  enabled: true
plugins:
  # Python Plugins
  - remote: buf.build/protocolbuffers/python
    out: gen/python
  - remote: buf.build/grpc/python
    out: gen/python

  # Java Plugins
  - remote: buf.build/protocolbuffers/java:v25.2
    out: gen/maven/src/main/java
  - remote: buf.build/grpc/java
    out: gen/maven/src/main/java

  # BQ Schemas
  - remote: buf.build/googlecloudplatform/bq-schema:v1.1.0
    out: protoc-gen/bq_schema

```

Run the following two commands to generate the necessary Java, Python, BigQuery schema, and Descriptor file:

```bash
// Generate the buf.lock file
buf deps update

// It generates the descriptor in descriptor.binp.
buf build . -o descriptor.binp --exclude-imports

// It generates the Java, Python and BigQuery schema as described in buf.gen.yaml
buf generate --include-imports
```

### Make the Beam YAML read proto

Make the following modifications to the to the YAML file:

```yaml
# movie_events_pipeline.yml

pipeline:
  transforms:
    - type: ReadFromKafka
      name: ReadProtoMovieEvents
      config:
        topic: 'movie_proto'
        format: PROTO
        bootstrap_servers: '<BOOTSTRAP_SERVERS>'
        file_descriptor_path: 'gs://my_proto_bucket/movie/v1.0.0/descriptor.binp'
        message_name: 'event.v1.MovieEvent'
    - type: WriteToBigQuery
      name: WriteMovieEvents
      input: ReadProtoMovieEvents
      config:
        table: '<PROJECT_ID>.raw.movie_table'
        useAtLeastOnceSemantics: true
options:
  streaming: true
  dataflow_service_options: [streaming_mode_at_least_once]
```

This step changes the format to `PROTO` and adds the `file_descriptor_path` and the `message_name`.

### Deploy the pipeline with Terraform

You can use [Terraform](https://www.terraform.io/) to deploy the Beam YAML pipeline
with [Dataflow](https://cloud.google.com/products/dataflow?hl=en) as the runner.
The following Terraform code example demonstrates how to achieve this:

```hcl
// Enable Dataflow API.
resource "google_project_service" "enable_dataflow_api" {
  project = var.gcp_project_id
  service = "dataflow.googleapis.com"
}

// DF Beam YAML
resource "google_dataflow_flex_template_job" "data_movie_job" {
  provider                     = google-beta
  project                      = var.gcp_project_id
  name                         = "movie-proto-events"
  container_spec_gcs_path      = "gs://dataflow-templates-${var.gcp_region}/latest/flex/Yaml_Template"
  region                       = var.gcp_region
  on_delete                    = "drain"
  machine_type                 = "n2d-standard-4"
  enable_streaming_engine      = true
  subnetwork                   = var.subnetwork
  skip_wait_on_job_termination = true
  parameters = {
    yaml_pipeline_file = "gs://${var.bucket_name}/yamls/${var.package_version}/movie_events_pipeline.yml"
    max_num_workers    = 40
    worker_zone        = var.gcp_zone
  }
  depends_on = [google_project_service.enable_dataflow_api]
}
```

Assuming the BigQuery table exists, which you can do by using Terraform and Proto,
this code creates a Dataflow job by using the Beam YAML code that reads Proto events from
Kafka and writes them into BigQuery.

## Improvements and conclusions

The following community contributions could improve the Beam YAML code in this example:

* **Support schema registries:** Integrate with schema registries such as Buf Registry or Apicurio for
better schema management. The current workflow generates the descriptors by using Buf and store them in Google Cloud Storage.
The descriptors could be stored in a schema registry instead.


* **Enhanced Monitoring:** Implement advanced monitoring and alerting mechanisms to quickly identify and address
issues in the data pipeline.

Leveraging Beam YAML and Protobuf lets us streamline the creation and maintenance of
data processing pipelines, significantly reducing complexity. This approach ensures that engineers can more
efficiently implement and scale robust, reusable pipelines without needs to manually write Beam code.

## Contribute

Developers who want to help build out and add functionalities are welcome to start contributing to the effort in the
[Beam YAML module](https://github.com/apache/beam/tree/master/sdks/python/apache_beam/yaml).

There is also a list of open [bugs](https://github.com/apache/beam/issues?q=is%3Aopen+is%3Aissue+label%3Ayaml) found
on the GitHub repo - now marked with the `yaml` tag.

Although Beam YAML is marked stable as of Beam 2.52, it is still under heavy development, with new features being
added with each release. Those who want to be part of the design decisions and give insights to how the framework is
being used are highly encouraged to join the [dev mailing list](https://beam.apache.org/community/contact-us/), where those discussions are occurring.
