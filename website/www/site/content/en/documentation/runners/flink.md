---
type: runners
title: "Apache Flink Runner"
aliases: /learn/runners/flink/
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

The Apache Flink Runner can be used to execute Beam pipelines using [Apache
Flink](https://flink.apache.org). For execution you can choose between a cluster
execution mode (e.g. Yarn/Kubernetes/Mesos) or a local embedded execution mode
which is useful for testing pipelines.

The Flink Runner and Flink are suitable for large scale, continuous jobs, and provide:

* A streaming-first runtime that supports both batch processing and data streaming programs
* A runtime that supports very high throughput and low event latency at the same time
* Fault-tolerance with *exactly-once* processing guarantees
* Natural back-pressure in streaming programs
* Custom memory management for efficient and robust switching between in-memory and out-of-core data processing algorithms
* Integration with YARN and other components of the Apache Hadoop ecosystem

# Using the Apache Flink Runner

It is important to understand that the Flink Runner comes in two flavors:

1. The original *classic Runner* which supports only Java (and other JVM-based languages)
2. The newer *portable Runner* which supports Java/Python/Go

You may ask why there are two Runners?

Beam and its Runners originally only supported JVM-based languages
(e.g. Java/Scala/Kotlin). Python and Go SDKs were added later on. The
architecture of the Runners had to be changed significantly to support executing
pipelines written in other languages.

If your applications only use Java, then you should currently go with the classic
Runner. Eventually, the portable Runner will replace the classic Runner because
it contains the generalized framework for executing Java, Python, Go, and more
languages in the future.

If you want to run Python pipelines with Beam on Flink you want to use the
portable Runner. For more information on
portability, please visit the [Portability page](/roadmap/portability/).

Consequently, this guide is split into parts to document the classic and
the portable functionality of the Flink Runner.
In addition, Python provides convenience wrappers to handle the full lifecycle of the runner,
and so is further split depending on whether to manage the portability
components automatically (recommended) or manually.
Please use the switcher below to select the appropriate mode for the Runner:

<nav class="language-switcher">
  <strong>Adapt for:</strong>
  <ul>
    <li data-value="java">Classic (Java)</li>
    <li data-value="py">Portable (Python)</li>
    <li data-value="portable">Portable (Java/Python/Go)</li>
  </ul>
</nav>


## Prerequisites and Setup

If you want to use the local execution mode with the Flink Runner you don't have
to complete any cluster setup. You can simply run your Beam pipeline. Be sure to
set the Runner to <span class="language-java language-py">`FlinkRunner`</span><span class="language-portable">`PortableRunner`</span>.

To use the Flink Runner for executing on a cluster, you have to setup a Flink cluster by following the
Flink [Setup Quickstart](https://ci.apache.org/projects/flink/flink-docs-stable/quickstart/setup_quickstart.html#setup-download-and-start-flink).

### Dependencies

{{< paragraph class="language-java" >}}
You must specify your dependency on the Flink Runner
in your `pom.xml` or `build.gradle`. Use the Beam version and the artifact id
from the [compatibility table](#flink-version-compatibility) below. For example:
{{< /paragraph >}}

{{< highlight java >}}
<dependency>
  <groupId>org.apache.beam</groupId>
  <artifactId>beam-runners-flink-1.18</artifactId>
  <version>{{< param release_latest >}}</version>
</dependency>
{{< /highlight >}}

{{< paragraph class="language-py" >}}
You will need Docker to be installed in your execution environment.
To run an embedded flink cluster or use the Flink runner for Python < 3.6
you will also need to have java available in your execution environment.
{{< /paragraph >}}

{{< paragraph class="language-portable" >}}
You will need Docker to be installed in your execution environment.
{{< /paragraph >}}

### Executing a Beam pipeline on a Flink Cluster

{{< paragraph class="language-java" >}}
For executing a pipeline on a Flink cluster you need to package your program
along with all dependencies in a so-called fat jar. How you do this depends on
your build system but if you follow along the [Beam Quickstart](/get-started/quickstart/) this is the command that you have to run:
{{< /paragraph >}}

{{< highlight java >}}
$ mvn package -Pflink-runner
{{< /highlight >}}

{{< paragraph class="language-java" >}}
Look for the output JAR of this command in the
`target` folder.
{{< /paragraph >}}

{{< paragraph class="language-java" >}}
The Beam Quickstart Maven project is setup to use the Maven Shade plugin to
create a fat jar and the `-Pflink-runner` argument makes sure to include the
dependency on the Flink Runner.
{{< /paragraph >}}

{{< paragraph class="language-java" >}}
For running the pipeline the easiest option is to use the `flink` command which
is part of Flink:
{{< /paragraph >}}

{{< paragraph class="language-java" >}}
$ bin/flink run -c org.apache.beam.examples.WordCount /path/to/your.jar
--runner=FlinkRunner --other-parameters
{{< /highlight >}}

{{< paragraph class="language-java" >}}
Alternatively you can also use Maven's exec command. For example, to execute the
WordCount example:
{{< /paragraph >}}

{{< highlight java >}}
mvn exec:java -Dexec.mainClass=org.apache.beam.examples.WordCount \
    -Pflink-runner \
    -Dexec.args="--runner=FlinkRunner \
      --inputFile=/path/to/pom.xml \
      --output=/path/to/counts \
      --flinkMaster=<flink master url> \
      --filesToStage=target/word-count-beam-bundled-0.1.jar"
{{< /highlight >}}
<!-- Span implicitly ended -->

{{< paragraph class="language-java" >}}
If you have a Flink `JobManager` running on your local machine you can provide `localhost:8081` for
`flinkMaster`. Otherwise an embedded Flink cluster will be started for the job.
{{< /paragraph >}}

{{< paragraph class="language-py" >}}
To run a pipeline on Flink, set the runner to `FlinkRunner`
and `flink_master` to the master URL of a Flink cluster.
In addition, optionally set `environment_type` set to `LOOPBACK`. For example,
after starting up a [local flink cluster](https://ci.apache.org/projects/flink/flink-docs-release-1.10/getting-started/tutorials/local_setup.html),
one could run:
{{< /paragraph >}}

{{< highlight py >}}
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

options = PipelineOptions([
    "--runner=FlinkRunner",
    "--flink_master=localhost:8081",
    "--environment_type=LOOPBACK"
])
with beam.Pipeline(options) as p:
    ...
{{< /highlight >}}

{{< paragraph class="language-py" >}}
To run on an embedded Flink cluster, simply omit the `flink_master` option
and an embedded Flink cluster will be automatically started and shut down for the job.
{{< /paragraph >}}

{{< paragraph class="language-py" >}}
The optional `flink_version` option may be required as well for older versions of Python.
{{< /paragraph >}}



{{< paragraph class="language-portable" >}}
Starting with Beam 2.18.0, pre-built Flink Job Service Docker images are available at Docker Hub:
[Flink 1.16](https://hub.docker.com/r/apache/beam_flink1.16_job_server).
[Flink 1.17](https://hub.docker.com/r/apache/beam_flink1.17_job_server).
[Flink 1.18](https://hub.docker.com/r/apache/beam_flink1.18_job_server).
{{< /paragraph >}}

<!-- TODO(BEAM-10214): Use actual lists here and below. -->
{{< paragraph class="language-portable" >}}
To run a pipeline on an embedded Flink cluster:
{{< /paragraph >}}

{{< paragraph class="language-portable" >}}
(1) Start the JobService endpoint: `docker run --net=host apache/beam_flink1.10_job_server:latest`
{{< /paragraph >}}

{{< paragraph class="language-portable" >}}
The JobService is the central instance where you submit your Beam pipeline to.
The JobService will create a Flink job for the pipeline and execute the job.
{{< /paragraph >}}

{{< paragraph class="language-portable" >}}
(2) Submit the Python pipeline to the above endpoint by using the `PortableRunner`, `job_endpoint` set to `localhost:8099` (this is the default address of the JobService).
Optionally set `environment_type` set to `LOOPBACK`. For example:
{{< /paragraph >}}

{{< highlight portable >}}
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

options = PipelineOptions([
    "--runner=PortableRunner",
    "--job_endpoint=localhost:8099",
    "--environment_type=LOOPBACK"
])
with beam.Pipeline(options) as p:
    ...
{{< /highlight >}}
<!-- Span implicitly ended -->

{{< paragraph class="language-portable" >}}
To run on a separate [Flink cluster](https://ci.apache.org/projects/flink/flink-docs-release-1.10/getting-started/tutorials/local_setup.html):
{{< /paragraph >}}

{{< paragraph class="language-portable" >}}
(1) Start a Flink cluster which exposes the Rest interface (e.g. `localhost:8081` by default).
{{< /paragraph >}}

{{< paragraph class="language-portable" >}}
(2) Start JobService with Flink Rest endpoint: `docker run --net=host apache/beam_flink1.10_job_server:latest --flink-master=localhost:8081`.
{{< /paragraph >}}

{{< paragraph class="language-portable" >}}
(3) Submit the pipeline as above.
{{< /paragraph >}}

{{< highlight portable >}}
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

options = PipelineOptions([
    "--runner=PortableRunner",
    "--job_endpoint=localhost:8099",
    "--environment_type=LOOPBACK"
])
with beam.Pipeline(options=options) as p:
    ...
{{< /highlight >}}

{{< paragraph class="language-py language-portable" >}}
Note that `environment_type=LOOPBACK` is only intended for local testing,
and will not work on remote clusters.
See [here](/documentation/runtime/sdk-harness-config/) for details.
{{< /paragraph >}}

## Additional information and caveats

### Monitoring your job

You can monitor a running Flink job using the Flink JobManager Dashboard or its Rest interfaces. By default, this is available at port `8081` of the JobManager node. If you have a Flink installation on your local machine that would be `http://localhost:8081`. Note: When you use the `[local]` mode an embedded Flink cluster will be started which does not make a dashboard available.

### Streaming Execution

If your pipeline uses an unbounded data source or sink, the Flink Runner will automatically switch to streaming mode. You can enforce streaming mode by using the `--streaming` flag.

Note: The Runner will print a warning message when unbounded sources are used and checkpointing is not enabled.
Many sources like `PubSubIO` rely on their checkpoints to be acknowledged which can only be done when checkpointing is enabled for the `FlinkRunner`. To enable checkpointing, please set <span class="language-java">`checkpointingInterval`</span><span class="language-py">`checkpointing_interval`</span> to the desired checkpointing interval in milliseconds.

## Pipeline options for the Flink Runner

When executing your pipeline with the Flink Runner, you can set these pipeline options.

The following list of Flink-specific pipeline options is generated automatically from the
[FlinkPipelineOptions](https://beam.apache.org/releases/javadoc/{{< param release_latest >}}/index.html?org/apache/beam/runners/flink/FlinkPipelineOptions.html)
reference class:

<!-- Java Options -->
<div class="language-java">

{{< flink_java_pipeline_options >}}

</div>

<!-- Python Options -->
<div class="language-py">

{{< flink_python_pipeline_options >}}

</div>

For general Beam pipeline options see the
[PipelineOptions](https://beam.apache.org/releases/javadoc/{{< param release_latest >}}/index.html?org/apache/beam/sdk/options/PipelineOptions.html)
reference.

## Flink Version Compatibility

The Flink cluster version has to match the minor version used by the FlinkRunner.
The minor version is the first two numbers in the version string, e.g. in `1.18.0` the
minor version is `1.18`.

We try to track the latest version of Apache Flink at the time of the Beam release.
A Flink version is supported by Beam for the time it is supported by the Flink community.
The Flink community supports the last two minor versions. When support for a Flink version is dropped, it may be deprecated and removed also from Beam.
To find out which version of Flink is compatible with Beam please see the table below:

<table class="table table-bordered">
<tr>
  <th>Flink Version</th>
  <th>Artifact Id</th>
  <th>Supported Beam Versions</th>
</tr>
<tr>
  <td>1.19.x</td>
  <td>beam-runners-flink-1.19</td>
  <td>&ge; 2.61.0</td>
</tr>
<tr>
  <td>1.18.x</td>
  <td>beam-runners-flink-1.18</td>
  <td>&ge; 2.57.0</td>
</tr>
<tr>
  <td>1.17.x</td>
  <td>beam-runners-flink-1.17</td>
  <td>&ge; 2.56.0</td>
</tr>
<tr>
  <td>1.16.x</td>
  <td>beam-runners-flink-1.16</td>
  <td>2.47.0 - 2.60.0</td>
</tr>
<tr>
  <td>1.15.x</td>
  <td>beam-runners-flink-1.15</td>
  <td>2.40.0 - 2.60.0</td>
</tr>
<tr>
  <td>1.14.x</td>
  <td>beam-runners-flink-1.14</td>
  <td>2.38.0 - 2.56.0</td>
</tr>
<tr>
  <td>1.13.x</td>
  <td>beam-runners-flink-1.13</td>
  <td>2.31.0 - 2.55.0</td>
</tr>
<tr>
  <td>1.12.x</td>
  <td>beam-runners-flink-1.12</td>
  <td>2.27.0 - 2.55.0</td>
</tr>
<tr>
  <td>1.11.x</td>
  <td>beam-runners-flink-1.11</td>
  <td>2.25.0 - 2.38.0</td>
</tr>
<tr>
  <td>1.10.x</td>
  <td>beam-runners-flink-1.10</td>
  <td>2.21.0 - 2.30.0</td>
</tr>
<tr>
  <td>1.9.x</td>
  <td>beam-runners-flink-1.9</td>
  <td>2.17.0 - 2.29.0</td>
</tr>
<tr>
  <td>1.8.x</td>
  <td>beam-runners-flink-1.8</td>
  <td>2.13.0 - 2.29.0</td>
</tr>
<tr>
  <td>1.7.x</td>
  <td>beam-runners-flink-1.7</td>
  <td>2.10.0 - 2.20.0</td>
</tr>
<tr>
  <td>1.6.x</td>
  <td>beam-runners-flink-1.6</td>
  <td>2.10.0 - 2.16.0</td>
</tr>
<tr>
  <td>1.5.x</td>
  <td>beam-runners-flink_2.11</td>
  <td>2.6.0 - 2.16.0</td>
</tr>
<tr>
  <td>1.4.x with Scala 2.11</td>
  <td>beam-runners-flink_2.11</td>
  <td>2.3.0 - 2.5.0</td>
</tr>
<tr>
  <td>1.3.x with Scala 2.10</td>
  <td>beam-runners-flink_2.10</td>
  <td>2.1.x - 2.2.0</td>
</tr>
<tr>
  <td>1.2.x with Scala 2.10</td>
  <td>beam-runners-flink_2.10</td>
  <td>2.0.0</td>
</tr>
</table>

For retrieving the right Flink version, see the [Flink downloads page](https://flink.apache.org/downloads.html).

For more information, the [Flink Documentation](https://ci.apache.org/projects/flink/flink-docs-stable/) can be helpful.

## Beam Capability

The [Beam Capability Matrix](/documentation/runners/capability-matrix/) documents the
capabilities of the classic Flink Runner.

The [Portable Capability
Matrix](https://s.apache.org/apache-beam-portability-support-table) documents
the capabilities of the portable Flink Runner.
