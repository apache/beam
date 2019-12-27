---
layout: section
title: "Apache Flink Runner"
section_menu: section-menu/runners.html
permalink: /documentation/runners/flink/
redirect_from: /learn/runners/flink/
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
portability, please visit the [Portability page]({{site.baseurl
}}/roadmap/portability/).

Consequently, this guide is split into two parts to document the classic and
the portable functionality of the Flink Runner. Please use the switcher below to
select the appropriate Runner:

<nav class="language-switcher">
  <strong>Adapt for:</strong>
  <ul>
    <li data-type="language-java">Classic (Java)</li>
    <li data-type="language-py">Portable (Java/Python/Go)</li>
  </ul>
</nav>


## Prerequisites and Setup

If you want to use the local execution mode with the Flink Runner you don't have
to complete any cluster setup. You can simply run your Beam pipeline. Be sure to
set the Runner to <span class="language-java">`FlinkRunner`</span><span class="language-py">`PortableRunner`</span>.

To use the Flink Runner for executing on a cluster, you have to setup a Flink cluster by following the
Flink [Setup Quickstart](https://ci.apache.org/projects/flink/flink-docs-stable/quickstart/setup_quickstart.html#setup-download-and-start-flink).

## Version Compatibility

The Flink cluster version has to match the minor version used by the FlinkRunner.
The minor version is the first two numbers in the version string, e.g. in `1.7.0` the
minor version is `1.7`.

We try to track the latest version of Apache Flink at the time of the Beam release.
A Flink version is supported by Beam for the time it is supported by the Flink community.
The Flink community typially supports the last two minor versions. When support for a Flink
version is dropped, it may be deprecated and removed also from Beam, with the exception of
Beam LTS releases. LTS releases continue to receive bug fixes for long as the LTS support
period.

To find out which version of Flink is compatible with Beam please see the table below:

<table class="table table-bordered">
<tr>
  <th>Beam Version</th>
  <th>Flink Version</th>
  <th>Artifact Id</th>
</tr>
<tr>
  <td rowspan="3">2.17.0</td>
  <td>1.9.x</td>
  <td>beam-runners-flink-1.9</td>
</tr>
<tr>
  <td>1.8.x</td>
  <td>beam-runners-flink-1.8</td>
</tr>
<tr>
  <td>1.7.x</td>
  <td>beam-runners-flink-1.7</td>
</tr>
<tr>
  <td rowspan="4">2.13.0 - 2.16.0</td>
  <td>1.8.x</td>
  <td>beam-runners-flink-1.8</td>
</tr>
<tr>
  <td>1.7.x</td>
  <td>beam-runners-flink-1.7</td>
</tr>
<tr>
  <td>1.6.x</td>
  <td>beam-runners-flink-1.6</td>
</tr>
<tr>
  <td>1.5.x</td>
  <td>beam-runners-flink_2.11</td>
</tr>
<tr>
  <td rowspan="3">2.10.0 - 2.16.0</td>
  <td>1.7.x</td>
  <td>beam-runners-flink-1.7</td>
</tr>
<tr>
  <td>1.6.x</td>
  <td>beam-runners-flink-1.6</td>
</tr>
<tr>
  <td>1.5.x</td>
  <td>beam-runners-flink_2.11</td>
</tr>
<tr>
  <td>2.9.0</td>
  <td rowspan="4">1.5.x</td>
  <td rowspan="4">beam-runners-flink_2.11</td>
</tr>
<tr>
  <td>2.8.0</td>
</tr>
<tr>
  <td>2.7.0</td>
</tr>
<tr>
  <td>2.6.0</td>
</tr>
<tr>
  <td>2.5.0</td>
  <td rowspan="3">1.4.x with Scala 2.11</td>
  <td rowspan="3">beam-runners-flink_2.11</td>
</tr>
<tr>
  <td>2.4.0</td>
</tr>
<tr>
  <td>2.3.0</td>
</tr>
<tr>
  <td>2.2.0</td>
  <td rowspan="2">1.3.x with Scala 2.10</td>
  <td rowspan="2">beam-runners-flink_2.10</td>
</tr>
<tr>
  <td>2.1.x</td>
</tr>
<tr>
  <td>2.0.0</td>
  <td>1.2.x with Scala 2.10</td>
  <td>beam-runners-flink_2.10</td>
</tr>
</table>

For retrieving the right Flink version, see the [Flink downloads page](https://flink.apache.org/downloads.html).

For more information, the [Flink Documentation](https://ci.apache.org/projects/flink/flink-docs-stable/) can be helpful.

### Dependencies

<span class="language-java">You must specify your dependency on the Flink Runner
in your `pom.xml` or `build.gradle`. Use the Beam version and the artifact id
from the above table. For example:
</span>

```java
<dependency>
  <groupId>org.apache.beam</groupId>
  <artifactId>beam-runners-flink-1.6</artifactId>
  <version>{{ site.release_latest }}</version>
</dependency>
```

<span class="language-py">
You will need Docker to be installed in your execution environment. To develop
Apache Beam with Python you have to install the Apache Beam Python SDK: `pip
install apache_beam`. Please refer to the [Python documentation]({{ site.baseurl }}/documentation/sdks/python/)
on how to create a Python pipeline.
</span>

```python
pip install apache_beam
```

### Executing a Beam pipeline on a Flink Cluster

<span class="language-java">
For executing a pipeline on a Flink cluster you need to package your program
along with all dependencies in a so-called fat jar. How you do this depends on
your build system but if you follow along the [Beam Quickstart]({{ site.baseurl
}}/get-started/quickstart/) this is the command that you have to run:
</span>

```java
$ mvn package -Pflink-runner
```
<span class="language-java">Look for the output JAR of this command in the
install apache_beam``target` folder.
<span>

<span class="language-java">
The Beam Quickstart Maven project is setup to use the Maven Shade plugin to
create a fat jar and the `-Pflink-runner` argument makes sure to include the
dependency on the Flink Runner.
</span>

<span class="language-java">
For running the pipeline the easiest option is to use the `flink` command which
is part of Flink:
</span>

```java
$ bin/flink run -c org.apache.beam.examples.WordCount /path/to/your.jar
--runner=FlinkRunner --other-parameters
```

<span class="language-java">
Alternatively you can also use Maven's exec command. For example, to execute the
WordCount example:
</span>

```java
mvn exec:java -Dexec.mainClass=org.apache.beam.examples.WordCount \
    -Pflink-runner \
    -Dexec.args="--runner=FlinkRunner \
      --inputFile=/path/to/pom.xml \
      --output=/path/to/counts \
      --flinkMaster=<flink master url> \
      --filesToStage=target/word-count-beam-bundled-0.1.jar"
```
<!-- Span implictly ended -->

<span class="language-java">
If you have a Flink `JobManager` running on your local machine you can provide `localhost:8081` for
`flinkMaster`. Otherwise an embedded Flink cluster will be started for the job.
</span>

<span class="language-py">
As of now you will need a copy of Apache Beam's source code. You can
download it on the [Downloads page]({{ site.baseurl }}/get-started/downloads/).

Pre-built Docker images are available at Docker-Hub:
[Python 2.7](https://hub.docker.com/r/apachebeam/python2.7_sdk),
[Python 3.5](https://hub.docker.com/r/apachebeam/python3.5_sdk),
[Python 3.6](https://hub.docker.com/r/apachebeam/python3.6_sdk),
[Python 3.7](https://hub.docker.com/r/apachebeam/python3.7_sdk).

To run a pipeline on an embedded Flink cluster:
</span>

<span class="language-py">1. Start the JobService endpoint: `./gradlew :runners:flink:1.9:job-server:runShadow`
</span>

<span class="language-py">
The JobService is the central instance where you submit your Beam pipeline to.
The JobService will create a Flink job for the pipeline and execute the job.
To execute the job on a Flink cluster, the Beam JobService needs to be
provided with the Flink JobManager address.
</span>

<span class="language-py">2. Submit the Python pipeline to the above endpoint by using the `PortableRunner`, `job_endpoint` set to `localhost:8099` (this is the default address of the JobService), and `environment_type` set to `LOOPBACK`. For example:
</span>

```py
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

<span class="language-py">
To run on a separate [Flink cluster](https://ci.apache.org/projects/flink/flink-docs-release-1.8/tutorials/local_setup.html):
</span>

<span class="language-py">1. Start a Flink cluster which exposes the Rest interface on `localhost:8081` by default.
</span>

<span class="language-py">2. Start JobService with Flink Rest endpoint: `./gradlew :runners:flink:1.9:job-server:runShadow -PflinkMasterUrl=localhost:8081`.
</span>

<span class="language-py">3. Submit the pipeline as above.
Note however that `environment_type=LOOPBACK` is only intended for local testing.
See [here]({{ site.baseurl }}/roadmap/portability/#sdk-harness-config) for details.
</span>

<span class="language-py">Steps 2 and 3 can be automated in Python by using the `FlinkRunner`,
plus the optional `flink_version` and `flink_master`* options, e.g.:
</span>

```py
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

options = PipelineOptions([
    "--runner=FlinkRunner",
    "--flink_version=1.8",
    "--flink_master=localhost:8081",
    "--environment_type=LOOPBACK"
])
with beam.Pipeline(options) as p:
    ...
```

\* Note: For Beam versions < 2.17.0, use `flink_master_url` instead of `flink_master`.

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
[FlinkPipelineOptions](https://beam.apache.org/releases/javadoc/{{ site.release_latest }}/index.html?org/apache/beam/runners/flink/FlinkPipelineOptions.html)
reference class:

<!-- Java Options -->
<div class="language-java">
{% include flink_java_pipeline_options.html %}
</div>
<!-- Python Options -->
<div class="language-py">
{% include flink_python_pipeline_options.html %}
</div>

For general Beam pipeline options see the
[PipelineOptions](https://beam.apache.org/releases/javadoc/{{ site.release_latest }}/index.html?org/apache/beam/sdk/options/PipelineOptions.html)
reference.

## Capability

The [Beam Capability Matrix]({{ site.baseurl
}}/documentation/runners/capability-matrix/) documents the
capabilities of the classic Flink Runner.

The [Portable Capability
Matrix](https://s.apache.org/apache-beam-portability-support-table) documents
the capabilities of the portable Flink Runner.

