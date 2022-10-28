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

Using the Direct Runner for testing and development helps ensure that pipelines are robust across different Beam runners. In addition, debugging failed runs can be a non-trivial task when a pipeline executes on a remote cluster. Instead, it is often faster and simpler to perform local unit testing on your pipeline code. Unit testing your pipeline locally also allows you to use your preferred local debugging tools.In the SDK Go, the default is runner **DirectRunner**.

Additionally, you can read [here](https://beam.apache.org/documentation/runners/direct/)

#### Run example

```
$ go install github.com/apache/beam/sdks/v2/go/examples/wordcount
$ wordcount --input <PATH_TO_INPUT_FILE> --output counts
```

### Google Cloud Dataflow runner

The Google Cloud Dataflow uses the Cloud Dataflow managed service. When you run your pipeline with the Cloud Dataflow service, the runner uploads your executable code and dependencies to a Google Cloud Storage bucket and creates a Cloud Dataflow job, which executes your pipeline on managed resources in Google Cloud Platform. The Cloud Dataflow Runner and service are suitable for large scale, continuous jobs, and provide:
* a fully managed service
* autoscaling of the number of workers throughout the lifetime of the job
* dynamic work rebalancing

Additionally, you can read [here](https://beam.apache.org/documentation/runners/dataflow/)

#### Run example

```
$ go install github.com/apache/beam/sdks/v2/go/examples/wordcount
# As part of the initial setup, for non linux users - install package unix before run
$ go get -u golang.org/x/sys/unix
$ wordcount --input gs://dataflow-samples/shakespeare/kinglear.txt \
--output gs://<your-gcs-bucket>/counts \
--runner dataflow \
--project your-gcp-project \
--region your-gcp-region \
--temp_location gs://<your-gcs-bucket>/tmp/ \
--staging_location gs://<your-gcs-bucket>/binaries/ \
--worker_harness_container_image=apache/beam_go_sdk:latest
```