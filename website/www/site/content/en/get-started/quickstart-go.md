---
title: "Beam Quickstart for Go"
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

# WordCount quickstart for Go

This Quickstart will walk you through executing your first Beam pipeline to run [WordCount](/get-started/wordcount-example), written using Beam's [Go SDK](/documentation/sdks/go), on a [runner](/documentation#runners) of your choice.

If you're interested in contributing to the Apache Beam Go codebase, see the [Contribution Guide](/contribute).

{{< toc >}}

## Set up your environment

The Beam SDK for Go requires `go` version 1.20 or newer. It can be downloaded [here](https://golang.org/). Check what go version you have by running:

{{< highlight >}}
go version
{{< /highlight >}}

If you are unfamiliar with Go, see the [Get Started With Go Tutorial](https://go.dev/doc/tutorial/getting-started).

## Run wordcount

The Apache Beam
[examples](https://github.com/apache/beam/tree/master/sdks/go/examples)
directory has many examples. All examples can be run by passing the
required arguments described in the examples.

For example, to run `wordcount`, run:

{{< runner direct >}}
go run github.com/apache/beam/sdks/v2/go/examples/wordcount@latest --input "gs://apache-beam-samples/shakespeare/kinglear.txt" --output counts
less counts
{{< /runner >}}

{{< runner dataflow >}}
go run github.com/apache/beam/sdks/v2/go/examples/wordcount@latest --input gs://dataflow-samples/shakespeare/kinglear.txt \
            --output gs://<your-gcs-bucket>/counts \
            --runner dataflow \
            --project your-gcp-project \
            --region your-gcp-region \
            --temp_location gs://<your-gcs-bucket>/tmp/ \
            --staging_location gs://<your-gcs-bucket>/binaries/
{{< /runner >}}

{{< runner spark >}}
# Build and run the Spark job server from Beam source.
# -PsparkMasterUrl is optional. If it is unset the job will be run inside an embedded Spark cluster.
./gradlew :runners:spark:3:job-server:runShadow -PsparkMasterUrl=spark://localhost:7077

# In a separate terminal, run:
go run github.com/apache/beam/sdks/v2/go/examples/wordcount@latest --input <PATH_TO_INPUT_FILE> \
            --output counts \
            --runner spark \
            --endpoint localhost:8099
{{< /runner >}}

## Next Steps

* Learn more about the [Beam SDK for Go](/documentation/sdks/go/)
  and look through the [godoc](https://pkg.go.dev/github.com/apache/beam/sdks/v2/go/pkg/beam).
* Walk through these WordCount examples in the [WordCount Example Walkthrough](/get-started/wordcount-example).
* Clone the [Beam Go starter project](https://github.com/apache/beam-starter-go).
* Take a self-paced tour through our [Learning Resources](/documentation/resources/learning-resources).
* Dive in to some of our favorite [Videos and Podcasts](/get-started/resources/videos-and-podcasts).
* Join the Beam [users@](/community/contact-us) mailing list.

Please don't hesitate to [reach out](/community/contact-us) if you encounter any issues!
