---
layout: section
title: "Beam Quickstart for Go"
permalink: /get-started/quickstart-go/
section_menu: section-menu/get-started.html
---

# Apache Beam Go SDK Quickstart

This Quickstart will walk you through executing your first Beam pipeline to run [WordCount]({{ site.baseurl }}/get-started/wordcount-example), written using Beam's [Go SDK]({{ site.baseurl }}/documentation/sdks/go), on a [runner]({{ site.baseurl }}/documentation#runners) of your choice.

* TOC
{:toc}

## Set up your environment

The Beam SDK for Go requires `go` version 1.10 or newer. It can be downloaded [here](https://golang.org/). Check that you have version 1.10 by running:

```
$ go --version
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
$ wordcount --input gs://dataflow-samples/shakespeare/kinglear.txt \
            --output gs://<your-gcs-bucket>/counts \
            --runner dataflow \
            --project your-gcp-project \
            --temp_location gs://<your-gcs-bucket>/tmp/ \
            --worker_harness_container_image=herohde-docker-apache.bintray.io/beam/go:20180514
```

## Next Steps

* Learn more about the [Beam SDK for Go]({{ site.baseurl }}/documentation/sdks/go/)
  and look through the [godoc](https://godoc.org/github.com/apache/beam/sdks/go/pkg/beam).
* Walk through these WordCount examples in the [WordCount Example Walkthrough]({{ site.baseurl }}/get-started/wordcount-example).
* Dive in to some of our favorite [articles and presentations]({{ site.baseurl }}/documentation/resources).
* Join the Beam [users@]({{ site.baseurl }}/get-started/support#mailing-lists) mailing list.

Please don't hesitate to [reach out]({{ site.baseurl }}/get-started/support) if you encounter any issues!
