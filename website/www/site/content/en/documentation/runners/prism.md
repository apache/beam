---
type: runners
title: "Prism Runner"
aliases: /learn/runners/prism/
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

# Overview

The Apache Beam Prism Runner can be used to execute Beam pipelines locally using [Beam Portability](/roadmap/portability/).

The Prism runner is suitable for small scale local testing and provides:

* A statically compiled, single binary for simple deployment without additional configuration.
* A web UI when executing in stand alone mode.
* A direct implementation of Beam execution semantics.
* A streaming-first runtime that supports batch processing and data streaming programs.
* Fast, in-memory execution for to simplify SDK, Transform, and Pipeline development.
* Cross Language Transform support.

Written in [Go](https://go.dev), it is the default runner for the [Go SDK](/roadmap/go-sdk/), but can be used in other SDKs as well (see below).

# Capabilities

While Prism already supports a great deal of Beam features, it doesn't yet support everything.
Prism is under active development to close these gaps.

With the exception of timer issues, use of unsupported features should fail the pipeline at job submission time.

In the [2.59.0 release](/blog/beam-2.59.0/), Prism passes most runner validations tests with the exceptions of pipelines using the following features:

OrderedListState, OnWindowExpiry (eg. GroupIntoBatches), CustomWindows, MergingWindowFns, Trigger and WindowingStrategy associated features, Bundle Finalization, Looping Timers, and some Coder related issues such as with Python combiner packing, and Java Schema transforms, and heterogenous flatten coders.
Processing Time timers do not yet have real time support.


See the [Roadmap](/roadmap/prism-runner/) for how to find current progress.
Specific feature support information will soon migrate to the [Runner Capability Matrix](/documentation/runners/capability-matrix/).

# Using the Prism Runner

{{< language-switcher go java py >}}

<span class="language-go">Prism is the default runner for the Go SDK and is used automatically. Set the runner with the flag `--runner=PrismRunner`. </span>
<span class="language-java">Set the runner to `PrismRunner`. </span>
<span class="language-py">Set the runner to `PrismRunner`. </span>

For other SDKs, Prism is included as an asset on [Beam Github Releases](https://github.com/apache/beam/releases/tag/v{{< param release_latest >}}) for download and stand alone use.

Here are some resources with information about how to test your pipelines.
<ul>
  <li><a href="/documentation/pipelines/test-your-pipeline/">Test Your Pipeline</a></li>
  <li>The <a href="/get-started/wordcount-example/#testing-your-pipeline-with-asserts">Apache Beam WordCount Walkthrough</a> contains an example of logging and testing a pipeline with asserts.
  <!-- Java specific links -->
  <li class="language-java"><a href="/blog/2016/10/20/test-stream.html">Testing Unbounded Pipelines in Apache Beam</a> talks about the use of Java classes <a href="https://beam.apache.org/releases/javadoc/{{< param release_latest >}}/index.html?org/apache/beam/sdk/testing/PAssert.html">PAssert</a> and <a href="https://beam.apache.org/releases/javadoc/{{< param release_latest >}}/index.html?org/apache/beam/sdk/testing/TestStream.html">TestStream</a> to test your pipelines.</li>
</ul>

### Specify your dependency

<span class="language-java">When using Java, you must specify your dependency on the Direct Runner in your `pom.xml`.</span>
{{< highlight java >}}
<dependency>
   <groupId>org.apache.beam</groupId>
   <artifactId>beam-runners-prism-java</artifactId>
   <version>{{< param release_latest >}}</version>
   <scope>runtime</scope>
</dependency>
{{< /highlight >}}

<span class="language-py">This section is not applicable to the Beam SDK for Python. Prism is built in.</span>
<span class="language-go">This section is not applicable to the Beam SDK for Go. Prism is built in.</span>

Except for the Go SDK, Prism is included as an asset on [Beam Github Releases](https://github.com/apache/beam/releases/tag/v{{< param release_latest >}}) for automatic download, startup, and shutdown on SDKs.
The binary is cached locally for subsequent executions.

## Pipeline options for the Prism Runner

Prism aims to have minimal configuration required, and does not currently present user pipeline options.

## Running Prism Standalone

Prism can be executed as a stand alone binary and will present a basic UI for listing jobs, and job status.
This is an optional mode for Prism that is useful for demos or rapid iteration.
It is not a requirement for using Prism in the Java or Python SDKs.

This can be done in two ways, downloading an asset from the github release, or building the binary locally with Go installed.

In either case, Prism serves a JobManagement API endpoint, and a Webpage UI locally.
Jobs can be submitted using `--runner=PortableRunner --endpoint=<endpoint address>` and monitored using the webpage UI.

Example output from the Prism binary:

```
2024/09/30 09:56:42 INFO Serving JobManagement endpoint=localhost:8073
2024/09/30 09:56:42 INFO Serving WebUI endpoint=http://localhost:8074
```

The binary has the following optional flags:

* `--job_port` sets the port for the Job management server (defaults to 8073)
* `--web_port` sets the port for the web ui (defaults to 8074)
* `--serve_http` enables or disables the web ui (defaults to true)
* `---idle_shutdown_timeout` sets a duration that Prism will wait for a new job before automatically shutting itself down. Uses duration format like `10s`, `5m`,`2h`. Defaults to not shutting down.

### Download a release asset

This approach doesn't require other dependencies or runtimes installed.
This is recommended if you want to deploy Prism on some other machine.

Navigate to the latest [Beam Release Github page](https://github.com/apache/beam/releases/tag/v{{< param release_latest >}}), scroll to the bottom, and download the correct asset for where you want to run Prism.

For example, if you want to execute Prism on a newer MacBook, you'd download the `darwin-arm64` asset. For executing on many cloud machines, you'd download the `linux-amd64` asset.

This requires downloading the right asset for the machine Prism will run on, such as your development machine.

Simply unzip, and execute.

### Build from the release with Go.

This approach requires a [recent version of Go installed](https://go.dev/dl/).
This is recommended if you only want to run Prism on your local machine.

You can insall Prism with `go install`:

```sh
go install github.com/apache/beam/sdks/v2/go/cmd/prism@latest
prism
```

Or simply build and execute the binary immeadiately using `go run`:

```sh
go run github.com/apache/beam/sdks/v2/go/cmd/prism@latest
```
