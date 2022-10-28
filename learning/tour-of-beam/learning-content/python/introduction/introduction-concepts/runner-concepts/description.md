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
### Overview

Apache Beam provides a portable API layer for building sophisticated data-parallel processing `pipelines` that may be executed across a diversity of execution engines, or `runners`. The core concepts of this layer are based upon the Beam Model (formerly referred to as the Dataflow Model), and implemented to varying degrees in each Beam `runner`.

### Direct runner
The Direct Runner executes pipelines on your machine and is designed to validate that pipelines adhere to the Apache Beam model as closely as possible. Instead of focusing on efficient pipeline execution, the Direct Runner performs additional checks to ensure that users do not rely on semantics that are not guaranteed by the model. Some of these checks include:

* enforcing immutability of elements
* enforcing encodability of elements
* elements are processed in an arbitrary order at all points
* serialization of user functions (DoFn, CombineFn, etc.)

Using the Direct Runner for testing and development helps ensure that pipelines are robust across different Beam runners. In addition, debugging failed runs can be a non-trivial task when a pipeline executes on a remote cluster. Instead, it is often faster and simpler to perform local unit testing on your pipeline code. Unit testing your pipeline locally also allows you to use your preferred local debugging tools.In the SDK Python, the default is runner **DirectRunner**.

Additionally, you can read [here](https://beam.apache.org/documentation/runners/direct/)

#### Run example

```
python -m apache_beam.examples.wordcount --input YOUR_INPUT_FILE --output counts
```

### Google Cloud Dataflow runner

The Google Cloud Dataflow uses the Cloud Dataflow managed service. When you run your pipeline with the Cloud Dataflow service, the runner uploads your executable code and dependencies to a Google Cloud Storage bucket and creates a Cloud Dataflow job, which executes your pipeline on managed resources in Google Cloud Platform. The Cloud Dataflow Runner and service are suitable for large scale, continuous jobs, and provide:
* a fully managed service
* autoscaling of the number of workers throughout the lifetime of the job
* dynamic work rebalancing

Additionally, you can read [here](https://beam.apache.org/documentation/runners/dataflow/)

#### Run example

```
# As part of the initial setup, install Google Cloud Platform specific extra components.
pip install apache-beam[gcp]
python -m apache_beam.examples.wordcount --input gs://dataflow-samples/shakespeare/kinglear.txt \
                                         --output gs://YOUR_GCS_BUCKET/counts \
                                         --runner DataflowRunner \
                                         --project YOUR_GCP_PROJECT \
                                         --region YOUR_GCP_REGION \
                                         --temp_location gs://YOUR_GCS_BUCKET/tmp/
```

### Apache Flink runner

The Apache Flink Runner can be used to execute Beam pipelines using Apache Flink. For execution, you can choose between a cluster execution mode (e.g. Yarn/Kubernetes/Mesos) or a local embedded execution mode which is useful for testing pipelines. The Flink Runner and Flink are suitable for large scale, continuous jobs, and provide:
  * A streaming-first runtime that supports both batch processing and data streaming programs
  * A runtime that supports very high throughput and low event latency at the same time
  * Fault-tolerance with exactly-once processing guarantees
  * Natural back-pressure in streaming programs
  * Custom memory management for efficient and robust switching between in-memory and out-of-core data processing algorithms
  * Integration with YARN and other components of the Apache Hadoop ecosystem

Additionally, you can read [here](https://beam.apache.org/documentation/runners/flink/)

#### Run example

1. Starting with Beam 2.18.0, pre-built Flink Job Service Docker images are available at Docker Hub: `Flink 1.10`, `Flink 1.11`, `Flink 1.12`, `Flink 1.13`, `Flink 1.14`.
2. Start the JobService endpoint: `docker run --net=host apache/beam_flink1.10_job_server:latest`
3. Submit the pipeline to the above endpoint by using the PortableRunner, job_endpoint set to localhost:8099 (this is the default address of the JobService). Optionally set environment_type set to LOOPBACK. For example:

```
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

options = PipelineOptions([
    "--runner=PortableRunner",
    "--job_endpoint=localhost:8099",
    "--environment_type=LOOPBACK"
])
with beam.Pipeline(options) as p:
    ...
```

### Apache Spark runner

The Apache Spark Runner can be used to execute Beam pipelines using Apache Spark. The Spark Runner can execute Spark pipelines just like a native Spark application; deploying a self-contained application for local mode, running on Spark’s Standalone RM, or using YARN or Mesos.

The Spark Runner executes Beam pipelines on top of Apache Spark, providing:

* Batch and streaming (and combined) pipelines.
* The same fault-tolerance guarantees as provided by RDDs and DStreams.
* The same security features Spark provides.
* Built-in metrics reporting using Spark’s metrics system, which reports Beam Aggregators as well.
* Native support for Beam side-inputs via spark’s Broadcast variables.

Additionally, you can read [here](https://beam.apache.org/documentation/runners/spark/)

#### Run example

1. Start the JobService endpoint:
   * with Docker (preferred): docker run --net=host apache/beam_spark_job_server:latest
   * or from Beam source code: ./gradlew :runners:spark:3:job-server:runShadow
2. Submit the Python pipeline to the above endpoint by using the PortableRunner, job_endpoint set to localhost:8099 (this is the default address of the JobService), and environment_type set to LOOPBACK. For example:
```
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

options = PipelineOptions([
    "--runner=PortableRunner",
    "--job_endpoint=localhost:8099",
    "--environment_type=LOOPBACK"
])
with beam.Pipeline(options) as p:
    ...
```

Console:
```
python -m apache_beam.examples.wordcount --input /path/to/inputfile \
                                         --output /path/to/write/counts \
                                         --runner SparkRunner
```