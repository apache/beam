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

# Apache Beam Go SDK Quickstart

This Quickstart will walk you through executing your first Beam pipeline to run [WordCount](/get-started/wordcount-example), written using Beam's [Go SDK](/documentation/sdks/go), on a [runner](/documentation#runners) of your choice.

If you're interested in contributing to the Apache Beam Go codebase, see the [Contribution Guide](/contribute).

{{< toc >}}

## Set up your environment

The Beam SDK for Go requires `go` version 1.10 or newer. It can be downloaded [here](https://golang.org/). Check that you have version 1.10 by running:

{{< highlight >}}
$ go version
{{< /highlight >}}

## Get the SDK and the examples

The easiest way to obtain the Apache Beam Go SDK is via `go get`:

{{< highlight >}}
$ go get -u github.com/apache/beam/sdks/go/...
{{< /highlight >}}

For development of the Go SDK itself, see [BUILD.md](https://github.com/apache/beam/blob/master/sdks/go/BUILD.md) for details.

## Run wordcount

The Apache Beam
[examples](https://github.com/apache/beam/tree/master/sdks/go/examples)
directory has many examples. All examples can be run by passing the
required arguments described in the examples.

For example, to run `wordcount`, run:

{{< highlight class="runner-direct" >}}
$ go install github.com/apache/beam/sdks/go/examples/wordcount
$ wordcount --input <PATH_TO_INPUT_FILE> --output counts
{{< /highlight >}}

{{< highlight class="runner-dataflow" >}}
$ go install github.com/apache/beam/sdks/go/examples/wordcount
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
{{< /highlight >}}

{{< highlight class="runner-nemo" >}}
This runner is not yet available for the Go SDK.
{{< /highlight >}}

## Next Steps

* Learn more about the [Beam SDK for Go](/documentation/sdks/go/)
  and look through the [godoc](https://godoc.org/github.com/apache/beam/sdks/go/pkg/beam).
* Walk through these WordCount examples in the [WordCount Example Walkthrough](/get-started/wordcount-example).
* Take a self-paced tour through our [Learning Resources](/documentation/resources/learning-resources).
* Dive in to some of our favorite [Videos and Podcasts](/documentation/resources/videos-and-podcasts).
* Join the Beam [users@](/community/contact-us) mailing list.

Please don't hesitate to [reach out](/community/contact-us) if you encounter any issues!
