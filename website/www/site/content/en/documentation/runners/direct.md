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

{{< language-switcher java py >}}

The Direct Runner executes pipelines on your machine and is designed to validate that pipelines adhere to the Apache Beam model as closely as possible. Instead of focusing on efficient pipeline execution, the Direct Runner performs additional checks to ensure that users do not rely on semantics that are not guaranteed by the model. Some of these checks include:

* enforcing immutability of elements
* enforcing encodability of elements
* elements are processed in an arbitrary order at all points
* serialization of user functions (`DoFn`, `CombineFn`, etc.)

Using the Direct Runner for testing and development helps ensure that pipelines are robust across different Beam runners. In addition, debugging failed runs can be a non-trivial task when a pipeline executes on a remote cluster. Instead, it is often faster and simpler to perform local unit testing on your pipeline code. Unit testing your pipeline locally also allows you to use your preferred local debugging tools.

Here are some resources with information about how to test your pipelines.
<ul>
  <!-- Java specific links -->
  <li class="language-java"><a href="/blog/2016/10/20/test-stream.html">Testing Unbounded Pipelines in Apache Beam</a> talks about the use of Java classes <a href="https://beam.apache.org/releases/javadoc/{{< param release_latest >}}/index.html?org/apache/beam/sdk/testing/PAssert.html">PAssert</a> and <a href="https://beam.apache.org/releases/javadoc/{{< param release_latest >}}/index.html?org/apache/beam/sdk/testing/TestStream.html">TestStream</a> to test your pipelines.</li>
  <li class="language-java">The <a href="/get-started/wordcount-example/#testing-your-pipeline-with-asserts">Apache Beam WordCount Walkthrough</a> contains an example of logging and testing a pipeline with <a href="https://beam.apache.org/releases/javadoc/{{< param release_latest >}}/index.html?org/apache/beam/sdk/testing/PAssert.html">PAssert</a>.</li>

  <!-- Python specific links -->
  <li class="language-py">The <a href="/get-started/wordcount-example/#testing-your-pipeline-with-asserts">Apache Beam WordCount Walkthrough</a> contains an example of logging and testing a pipeline with <a href="https://beam.apache.org/releases/pydoc/{{< param release_latest >}}/apache_beam.testing.util.html#apache_beam.testing.util.assert_that">assert_that</a>.</li>
</ul>

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

## Pipeline options for the Direct Runner

When executing your pipeline from the command-line, set `runner` to `direct` or `DirectRunner`. The default values for the other pipeline options are generally sufficient.

See the reference documentation for the
<span class="language-java">[`DirectOptions`](https://beam.apache.org/releases/javadoc/{{< param release_latest >}}/index.html?org/apache/beam/runners/direct/DirectOptions.html)</span>
<span class="language-py">[`DirectOptions`](https://beam.apache.org/releases/pydoc/{{< param release_latest >}}/apache_beam.options.pipeline_options.html#apache_beam.options.pipeline_options.DirectOptions)</span>
interface for defaults and additional pipeline configuration options.

## Additional information and caveats

### Memory considerations

Local execution is limited by the memory available in your local environment. It is highly recommended that you run your pipeline with data sets small enough to fit in local memory. You can create a small in-memory data set using a <span class="language-java">[`Create`](https://beam.apache.org/releases/javadoc/{{< param release_latest >}}/index.html?org/apache/beam/sdk/transforms/Create.html)</span><span class="language-py">[`Create`](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/transforms/core.py)</span> transform, or you can use a <span class="language-java">[`Read`](https://beam.apache.org/releases/javadoc/{{< param release_latest >}}/index.html?org/apache/beam/sdk/io/Read.html)</span><span class="language-py">[`Read`](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/io/iobase.py)</span> transform to work with small local or remote files.

### Streaming execution

If your pipeline uses an unbounded data source or sink, you must set the `streaming` option to `true`.

### Execution Mode

Python [FnApiRunner](https://beam.apache.org/contribute/runner-guide/#the-fn-api) supports multi-threading and multi-processing mode.

<strong>Setting parallelism</strong>

Number of threads or subprocesses is defined by setting the `direct_num_workers` option.
From 2.22.0, `direct_num_workers = 0` is supported. When `direct_num_workers` is set to 0, it will set the number of threads/subprocess to the number of cores of the machine where the pipelien is running.

There are several ways to set this option.

* Passing through CLI when executing a pipeline.
```
python wordcount.py --input xx --output xx --direct_num_workers 2
```

* Setting with `PipelineOptions`.
```
from apache_beam.options.pipeline_options import PipelineOptions
pipeline_options = PipelineOptions(['--direct_num_workers', '2'])
```

* Adding to existing `PipelineOptions`.
```
from apache_beam.options.pipeline_options import DirectOptions
pipeline_options = PipelineOptions(xxx)
pipeline_options.view_as(DirectOptions).direct_num_workers = 2
```



<strong>Setting running mode</strong>

From 2.19, a new option was added to set running mode. We can use `direct_running_mode` option to set the running mode.
`direct_running_mode` can be one of [`'in_memory'`, `'multi_threading'`, `'multi_processing'`].

<b>in_memory</b>: Runner and workers' communication happens in memory (not through gRPC). This is a default mode.

<b>multi_threading</b>: Runner and workers communicate through gRPC and each worker runs in a thread.

<b>multi_processing</b>: Runner and workers communicate through gRPC and each worker runs in a subprocess.

Same as other options, `direct_running_mode` can be passed through CLI or set with `PipelineOptions`.

For the versions before 2.19.0, the running mode should be set with `FnApiRunner()`. Please refer following examples.

#### Running with multi-threading mode

```
import argparse

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.runners.portability import fn_api_runner
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.portability import python_urns

parser = argparse.ArgumentParser()
parser.add_argument(...)
known_args, pipeline_args = parser.parse_known_args(argv)
pipeline_options = PipelineOptions(pipeline_args)

p = beam.Pipeline(options=pipeline_options,
      runner=fn_api_runner.FnApiRunner(
          default_environment=beam_runner_api_pb2.Environment(
          urn=python_urns.EMBEDDED_PYTHON_GRPC)))
```

#### Running with multi-processing mode

```
import argparse
import sys

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.runners.portability import fn_api_runner
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.portability import python_urns

parser = argparse.ArgumentParser()
parser.add_argument(...)
known_args, pipeline_args = parser.parse_known_args(argv)
pipeline_options = PipelineOptions(pipeline_args)

p = beam.Pipeline(options=pipeline_options,
      runner=fn_api_runner.FnApiRunner(
          default_environment=beam_runner_api_pb2.Environment(
              urn=python_urns.SUBPROCESS_SDK,
              payload=b'%s -m apache_beam.runners.worker.sdk_worker_main'
                        % sys.executable.encode('ascii'))))
```
