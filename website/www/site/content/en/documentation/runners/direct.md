---
type: runners
title: "Direct Runner"
aliases: /learn/runners/direct/
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
# Using the Direct Runner

{{< language-switcher java py go >}}

The Direct Runner executes pipelines on your machine and is designed to validate that pipelines adhere to the Apache Beam model as closely as possible. Instead of focusing on efficient pipeline execution, the Direct Runner performs additional checks to ensure that users do not rely on semantics that are not guaranteed by the model. Some of these checks include:

* enforcing immutability of elements
* enforcing encodability of elements
* elements are processed in an arbitrary order at all points
* serialization of user functions (`DoFn`, `CombineFn`, etc.)
  <span class="language-java">See [Serializability of DoFns](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/transforms/ParDo.html) for details.</span>

Using the Direct Runner for testing and development helps ensure that pipelines are robust across different Beam runners. In addition, debugging failed runs can be a non-trivial task when a pipeline executes on a remote cluster. Instead, it is often faster and simpler to perform local unit testing on your pipeline code. Unit testing your pipeline locally also allows you to use your preferred local debugging tools.

Here are some resources with information about how to test your pipelines.
<ul>
  <li><a href="/documentation/pipelines/test-your-pipeline/">Test Your Pipeline</a></li>
  <!-- Java specific links -->
  <li class="language-java"><a href="/blog/2016/10/20/test-stream.html">Testing Unbounded Pipelines in Apache Beam</a> talks about the use of Java classes <a href="https://beam.apache.org/releases/javadoc/{{< param release_latest >}}/index.html?org/apache/beam/sdk/testing/PAssert.html">PAssert</a> and <a href="https://beam.apache.org/releases/javadoc/{{< param release_latest >}}/index.html?org/apache/beam/sdk/testing/TestStream.html">TestStream</a> to test your pipelines.</li>
  <li class="language-java">The <a href="/get-started/wordcount-example/#testing-your-pipeline-with-asserts">Apache Beam WordCount Walkthrough</a> contains an example of logging and testing a pipeline with <a href="https://beam.apache.org/releases/javadoc/{{< param release_latest >}}/index.html?org/apache/beam/sdk/testing/PAssert.html">PAssert</a>.</li>

  <!-- Python specific links -->
  <li class="language-py">The <a href="/get-started/wordcount-example/#testing-your-pipeline-with-asserts">Apache Beam WordCount Walkthrough</a> contains an example of logging and testing a pipeline with <code>assert_that</code>.</li>
</ul>

  <!-- Go specific links -->
  <li class="language-go">The <a href="/get-started/wordcount-example/#testing-your-pipeline-with-asserts">Apache Beam Go SDK WordCount Walkthrough</a> contains an example of testing a pipeline with <code>beamx.Run</code> and checking output collections.</li>
</ul>

The Direct Runner is not designed for production pipelines, because it's optimized for correctness rather than performance. The Direct Runner must fit all user data in memory, whereas the Flink and Spark runners can spill data to disk if it doesn't fit in memory. Consequently, Flink and Spark runners are able to run larger pipelines and are better suited to production workloads.

## Direct Runner prerequisites and setup

### Specify your dependency

<span class="language-java">When using Java, you must specify your dependency on the Direct Runner in your `pom.xml`.</span>
{{< highlight java >}}
<dependency>
   <groupId>org.apache.beam</groupId>
   <artifactId>beam-runners-direct-java</artifactId>
   <version>{{< param release_latest >}}</version>
   <scope>runtime</scope>
</dependency>
{{< /highlight >}}

<span class="language-py">This section is not applicable to the Beam SDK for Python.</span>

<span class="language-go">No additional dependency setup is required. The Go SDK uses the Direct Runner by default via `beamx.Run()` for local execution.</span>

## Pipeline options for the Direct Runner

For general instructions on how to set pipeline options, see the [programming guide](/documentation/programming-guide/#configuring-pipeline-options).

When executing your pipeline from the command-line, set `runner` to `direct` or `DirectRunner`. The default values for the other pipeline options are generally sufficient.

See the reference documentation for the
<span class="language-java">[`DirectOptions`](https://beam.apache.org/releases/javadoc/{{< param release_latest >}}/index.html?org/apache/beam/runners/direct/DirectOptions.html)</span>
<span class="language-py">[`DirectOptions`](https://beam.apache.org/releases/pydoc/{{< param release_latest >}}/apache_beam.options.pipeline_options.html#apache_beam.options.pipeline_options.DirectOptions)</span>
interface for defaults and additional pipeline configuration options.

## Additional information and caveats

### Memory considerations

Local execution is limited by the memory available in your local environment. It is highly recommended that you run your pipeline with data sets small enough to fit in local memory. You can create a small in-memory data set using a
<span class="language-java">[`Create`](https://beam.apache.org/releases/javadoc/{{< param release_latest >}}/index.html?org/apache/beam/sdk/transforms/Create.html)</span>
<span class="language-py">[`Create`](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/transforms/core.py)</span>
<span class="language-go">[`beam.Create`](https://pkg.go.dev/github.com/apache/beam/sdks/v2/go/pkg/beam#Create)</span>
transform, or you can use a
<span class="language-java">[`Read`](https://beam.apache.org/releases/javadoc/{{< param release_latest >}}/index.html?org/apache/beam/sdk/io/Read.html)</span>
<span class="language-py">[`Read`](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/io/iobase.py)</span>
<span class="language-go">[`textio.Read`](https://pkg.go.dev/github.com/apache/beam/sdks/v2/go/pkg/beam/io/textio#Read)</span>
transform to work with small local or remote files.

### Streaming execution

{{< paragraph class="language-py" >}}
Streaming support for Python DirectRunner is limited. For known issues, see: <https://github.com/apache/beam/issues/24528>.
{{< /paragraph >}}

{{< paragraph class="language-go" >}}
The Go SDK does not currently support streaming execution with the Direct Runner. For the latest updates, refer to [Beam Go SDK GitHub issues](https://github.com/apache/beam/issues?q=is%3Aopen+is%3Aissue+%22Go+SDK%22).
</span>

{{< /paragraph >}}

If your pipeline uses an unbounded data source or sink, you must set the `streaming` option to `true`.

### Parallel execution

{{< paragraph class="language-py" >}}
Python [FnApiRunner](/contribute/runner-guide/#the-fn-api) supports multi-threading and multi-processing mode.
{{< /paragraph >}}

### Setting parallelism

{{< paragraph class="language-java" >}} The number of worker threads is defined by the targetParallelism pipeline option.
By default, targetParallelism is the greater of the number of available processors and 3. {{< /paragraph >}}

{{< paragraph class="language-py" >}} Number of threads or subprocesses is defined by setting the direct_num_workers pipeline option.
From 2.22.0, direct_num_workers = 0 is supported. When direct_num_workers is set to 0, it will set the number of threads/subprocesses to the number of cores of the machine where the pipeline is running. {{< /paragraph >}}

{{< paragraph class="language-py" >}} <strong>Setting running mode</strong> {{< /paragraph >}}

{{< paragraph class="language-py" >}} In Beam 2.19.0 and newer, you can use the direct_running_mode pipeline option to set the running mode.
direct_running_mode can be one of ['in_memory', 'multi_threading', 'multi_processing']. {{< /paragraph >}}

{{< paragraph class="language-py" >}} <b>in_memory</b>: Runner and workers' communication happens in memory (not through gRPC). This is the default mode. {{< /paragraph >}}

{{< paragraph class="language-py" >}} <b>multi_threading</b>: Runner and workers communicate through gRPC and each worker runs in a thread. {{< /paragraph >}}

{{< paragraph class="language-py" >}} <b>multi_processing</b>: Runner and workers communicate through gRPC and each worker runs in a subprocess. {{< /paragraph >}}

{{< paragraph class="language-go" >}} The Go Direct Runner currently uses a single-threaded in-memory execution model.
Parallelism in Go pipelines depends on how concurrency is handled within the pipeline code itself (e.g., via ParDo functions that use goroutines internally).
There are no exposed pipeline options (like in Java or Python) to explicitly configure parallel worker threads in the Go SDK at this time. {{< /paragraph >}}

### Before deploying pipeline to remote runner

While testing on the direct runner is convenient, it can still behave differently from remote runners beyond Beam model semantics, especially for runtime environment related issues. In general, it is recommended to test your pipeline on targeted remote runner in small scale before fully deploying into production.
