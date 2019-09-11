---
layout: section
title: "Beam Quickstart for Go"
permalink: /get-started/quickstart-go/
section_menu: section-menu/get-started.html
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

This Quickstart will walk you through executing your first Beam pipeline to run [WordCount]({{ site.baseurl }}/get-started/wordcount-example), written using Beam's [Go SDK]({{ site.baseurl }}/documentation/sdks/go), on a [runner]({{ site.baseurl }}/documentation#runners) of your choice.

If you're interested in contributing to the Apache Beam Go codebase, see the [Contribution Guide]({{ site.baseurl }}/contribute).

* TOC
{:toc}

## Set up your environment

The Beam SDK for Go requires `go` version 1.10 or newer. It can be downloaded [here](https://golang.org/). Check that you have version 1.10 by running:

```
$ go version
```

## Get the SDK and the examples

The easiest way to obtain the Apache Beam Go SDK is via `go get`:

```
$ go get -u github.com/apache/beam/sdks/go/...
```

For development of the Go SDK itself, see [BUILD.md](https://github.com/apache/beam/blob/master/sdks/go/BUILD.md) for details.

## Run wordcount

The Apache Beam
[examples](https://github.com/apache/beam/tree/master/sdks/go/examples)
directory has many examples. All examples can be run by passing the
required arguments described in the examples.

For example, to run `wordcount`, run:

{:.runner-direct}
```
$ go install github.com/apache/beam/sdks/go/examples/wordcount
$ wordcount --input <PATH_TO_INPUT_FILE> --output counts
```

{:.runner-dataflow}
```
$ go install github.com/apache/beam/sdks/go/examples/wordcount
# As part of the initial setup, for non linux users - install package unix before run
$ go get -u golang.org/x/sys/unix
$ wordcount --input gs://dataflow-samples/shakespeare/kinglear.txt \
            --output gs://<your-gcs-bucket>/counts \
            --runner dataflow \
            --project your-gcp-project \
            --temp_location gs://<your-gcs-bucket>/tmp/ \
            --staging_location gs://<your-gcs-bucket>/binaries/ \
            --worker_harness_container_image=apachebeam/go_sdk:latest
```

{:.runner-nemo}
```
This runner is not yet available for the Go SDK.
```

## Next Steps

* Learn more about the [Beam SDK for Go]({{ site.baseurl }}/documentation/sdks/go/)
  and look through the [godoc](https://godoc.org/github.com/apache/beam/sdks/go/pkg/beam).
* Walk through these WordCount examples in the [WordCount Example Walkthrough]({{ site.baseurl }}/get-started/wordcount-example).
* Take a self-paced tour through our [Learning Resources]({{ site.baseurl }}/documentation/resources/learning-resources).
* Dive in to some of our favorite [Videos and Podcasts]({{ site.baseurl }}/documentation/resources/videos-and-podcasts).
* Join the Beam [users@]({{ site.baseurl }}/community/contact-us) mailing list.

Please don't hesitate to [reach out]({{ site.baseurl }}/community/contact-us) if you encounter any issues!
