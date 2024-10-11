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

As streaming data processing grows, so do its maintenance, complexity, and costs.
This post explains how to efficiently scale pipelines by using [Protobuf](https://protobuf.dev/),
which ensures that pipelines are reusable and quick to deploy. The goal is to keep this process simple
for engineers to implement using [Beam YAML](https://beam.apache.org/documentation/sdks/yaml/).

<!--more-->

## Simplify pipelines with Beam YAML

Creating a pipeline in Beam can be somewhat difficult, especially for newcomers with little experience with Beam.
Setting up the project, managing dependencies, and so on can be challenging.
Beam YAML helps eliminate most of the boilerplate code,
allowing you to focus solely on the most important part: data transformation.

Some of the main key benefits include:

*   **Readability:** By using a declarative language ([YAML](https://yaml.org/)), we improve the human readability
aspect of the pipeline configuration.
*   **Reusability:** It is much simpler to reuse the same components across different pipelines.
*   **Maintainability:** It simplifies pipeline maintenance and updates.

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

# Bringing It All Together

### Let's create a simple proto event:

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

As you can see here, there are important points to consider. Since we are planning to write these events to BigQuery,
we have imported the *[bq_field](https://buf.build/googlecloudplatform/bq-schema-api/file/main:bq_field.proto)*
and *[bq_table](https://buf.build/googlecloudplatform/bq-schema-api/file/main:bq_table.proto)* proto.
These proto files help generate the BigQuery JSON schema.
In our example, we are also advocating for a shift-left approach, which means we want to move testing, quality,
and performance as early as possible in the development process. This is why we have included the *buf.validate*
elements to ensure that only valid events are generated from the source.

Once we have our *movie_event.proto* in the *events/v1* folder, we can generate
the necessary [file descriptor](https://buf.build/docs/reference/descriptors).
Essentially, a file descriptor is a compiled representation of the schema that allows various tools and systems
to understand and work with Protobuf data dynamically. To simplify the process, we are using Buf in this example,
so we will need the following configuration files.


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

Running the following two commands we will generate the necessary Java, Python, BigQuery schema, and Descriptor File:

```bash
// Generate the buf.lock file
buf deps update

// It will generate the descriptor in descriptor.binp.
buf build . -o descriptor.binp --exclude-imports

// It will generate the Java, Python and BigQuery schema as described in buf.gen.yaml
buf generate --include-imports
```

# Let’s make our Beam YAML read proto:

These are the modifications we need to make to the YAML file:

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

As you can see, we just changed the format to be *PROTO* and added the *file_descriptor_path* and the *message_name*.

### Let’s use Terraform to deploy it

We can consider using [Terraform](https://www.terraform.io/) to deploy our Beam YAML pipeline
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

Assuming we have created the BigQuery table, which can also be done using Terraform and Proto as described earlier,
the previous code should create a Dataflow job using our Beam YAML code that reads Proto events from
Kafka and writes them into BigQuery.

# Improvements and Conclusions

Some potential improvements that can be done as part of community contributions to the previous Beam YAML code are:

* **Supporting Schema Registries:** Integrate with schema registries such as Buf Registry or Apicurio for
better schema management. In the current solution, we generate the descriptors via Buf and store them in GCS.
We could store them in a schema registry instead.


* **Enhanced Monitoring:** Implement advanced monitoring and alerting mechanisms to quickly identify and address
issues in the data pipeline.

As a conclusion, by leveraging Beam YAML and Protobuf, we have streamlined the creation and maintenance of
data processing pipelines, significantly reducing complexity. This approach ensures that engineers can more
efficiently implement and scale robust, reusable pipelines, compared to writing the equivalent Beam code manually.

## Contributing

Developers who wish to help build out and add functionalities are welcome to start contributing to the effort in the
Beam YAML module found [here](https://github.com/apache/beam/tree/master/sdks/python/apache_beam/yaml).

There is also a list of open [bugs](https://github.com/apache/beam/issues?q=is%3Aopen+is%3Aissue+label%3Ayaml) found
on the GitHub repo - now marked with the 'yaml' tag.

While Beam YAML has been marked stable as of Beam 2.52, it is still under heavy development, with new features being
added with each release. Those who wish to be part of the design decisions and give insights to how the framework is
being used are highly encouraged to join the dev mailing list as those discussions will be directed there. A link to
the dev list can be found [here](https://beam.apache.org/community/contact-us/).
