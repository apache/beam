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

1. A *legacy Runner* which supports only Java (and other JVM-based languages)
2. A *portable Runner* which supports Java/Python/Go

You may ask why there are two Runners?

Beam and its Runners originally only supported JVM-based languages
(e.g. Java/Scala/Kotlin). Python and Go SDKs were added later on. The
architecture of the Runners had to be changed significantly to support executing
pipelines written in other languages.

If your applications only use Java, then you should currently go with the legacy
Runner. Eventually, the portable Runner will replace the legacy Runner because
it contains the generalized framework for executing Java, Python, Go, and more
languages in the future.

If you want to run Python pipelines with Beam on Flink you want to use the
portable Runner. For more information on
portability, please visit the [Portability page]({{site.baseurl
}}/roadmap/portability/).

Consequently, this guide is split into two parts to document the legacy and
the portable functionality of the Flink Runner. Please use the switcher below to
select the appropriate Runner:

<nav class="language-switcher">
  <strong>Adapt for:</strong>
  <ul>
    <li data-type="language-java">Legacy (Java)</li>
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
  <td>>=2.13.0</td>
  <td>1.8.x</td>
  <td>beam-runners-flink-1.8</td>
</tr>
<tr>
  <td rowspan="3">>=2.10.0</td>
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
$ bin/flink -c org.apache.beam.examples.WordCount /path/to/your.jar
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
download it on the [Downloads page]({{ site.baseurl
}}/get-started/downloads/). In the future there will be pre-built Docker images
available.
</span>

<span class="language-py">1. *Only required once:* Build the SDK harness container: `./gradlew :beam-sdks-python-container:docker`
</span>

<span class="language-py">2. Start the JobService endpoint: `./gradlew :beam-runners-flink_2.11-job-server:runShadow`
</span>

<span class="language-py">
The JobService is the central instance where you submit your Beam pipeline to.
The JobService will create a Flink job for the pipeline and execute the job
job. To execute the job on a Flink cluster, the Beam JobService needs to be
provided with the Flink JobManager address.
</span>

<span class="language-py">3. Submit the Python pipeline to the above endpoint by using the `PortableRunner` and `job_endpoint` set to `localhost:8099` (this is the default address of the JobService). For example:
</span>

```py
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

options = PipelineOptions(["--runner=PortableRunner", "--job_endpoint=localhost:8099"])
p = beam.Pipeline(options)
..
p.run()
```

<span class="language-py">
To run on a separate [Flink cluster](https://ci.apache.org/projects/flink/flink-docs-release-1.5/quickstart/setup_quickstart.html):
</span>

<span class="language-py">1. Start a Flink cluster which exposes the Rest interface on `localhost:8081` by default.
</span>

<span class="language-py">2. Start JobService with Flink Rest endpoint: `./gradlew :beam-runners-flink_2.11-job-server:runShadow -PflinkMasterUrl=localhost:8081`.
</span>

<span class="language-py">3. Submit the pipeline as above.
</span>

## Additional information and caveats

### Monitoring your job

You can monitor a running Flink job using the Flink JobManager Dashboard or its Rest interfaces. By default, this is available at port `8081` of the JobManager node. If you have a Flink installation on your local machine that would be `http://localhost:8081`. Note: When you use the `[local]` mode an embedded Flink cluster will be started which does not make a dashboard available.

### Streaming Execution

If your pipeline uses an unbounded data source or sink, the Flink Runner will automatically switch to streaming mode. You can enforce streaming mode by using the `streaming` setting mentioned below.

Note: The Runner will print a warning message when unbounded sources are used and checkpointing is not enabled.
Many sources like `PubSubIO` rely on their checkpoints to be acknowledged which can only be done when checkpointing is enabled for the `FlinkRunner`. To enable checkpointing, please set <span class="language-java">`checkpointingInterval`</span><span class="language-py">`checkpointing_interval`</span> to the desired checkpointing interval in milliseconds.

## Pipeline options for the Flink Runner

When executing your pipeline with the Flink Runner, you can set these pipeline options.

See the reference documentation for the<span class="language-java">
[FlinkPipelineOptions](https://beam.apache.org/releases/javadoc/{{ site.release_latest }}/index.html?org/apache/beam/runners/flink/FlinkPipelineOptions.html)
</span><span class="language-py">
[PipelineOptions](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/options/pipeline_options.py)
</span>interface (and its subinterfaces) for the complete list of pipeline configuration options.


<!-- Java Options -->
<div class="language-java">
<table class="table table-bordered">
<tr>
  <th>Field</th>
  <th>Description</th>
  <th>Default Value</th>
</tr>
<tr>
  <td><code>runner</code></td>
  <td>The pipeline runner to use. This option allows you to determine the pipeline runner at runtime.</td>
  <td>Set to <code>FlinkRunner</code> to run using Flink.</td>
</tr>
<tr>
  <td><code>streaming</code></td>
  <td>Whether streaming mode is enabled or disabled; <code>true</code> if enabled. Set to <code>true</code> if running pipelines with unbounded <code>PCollection</code>s.</td>
  <td><code>false</code></td>
</tr>
<tr>
  <td><code>flinkMaster</code></td>
  <td>The url of the Flink JobManager on which to execute pipelines. This can either be the address of a cluster JobManager, in the form <code>"host:port"</code> or one of the special Strings <code>"[local]"</code> or <code>"[auto]"</code>. <code>"[local]"</code> will start a local Flink Cluster in the JVM while <code>"[auto]"</code> will let the system decide where to execute the pipeline based on the environment.</td>
  <td><code>[auto]</code></td>
</tr>
<tr>
  <td><code>filesToStage</code></td>
  <td>Jar Files to send to all workers and put on the classpath. Here you have to put the fat jar that contains your program along with all dependencies.</td>
  <td>empty</td>
</tr>
<tr>
  <td><code>parallelism</code></td>
  <td>The degree of parallelism to be used when distributing operations onto workers.</td>
  <td>For local execution: <code>Number of available CPU cores</code>
            For remote execution: <code>Default parallelism configuerd at remote cluster</code>
            Otherwise: <code>1</code>
            </td>
</tr>
<tr>
  <td><code>maxParallelism</code></td>
  <td>The pipeline wide maximum degree of parallelism to be used. The maximum parallelism specifies the upper limit for dynamic scaling and the number of key groups used for partitioned state.</td>
  <td><code>-1L</code>, meaning same as the parallelism</td>
</tr>
<tr>
  <td><code>checkpointingInterval</code></td>
  <td>The interval between consecutive checkpoints (i.e. snapshots of the current pipeline state used for fault tolerance).</td>
  <td><code>-1L</code>, i.e. disabled</td>
</tr>
<tr>
  <td><code>checkpointMode</code></td>
  <td>The checkpointing mode that defines consistency guarantee.</td>
  <td><code>EXACTLY_ONCE</code></td>
</tr>
<tr>
  <td><code>checkpointTimeoutMillis</code></td>
  <td>The maximum time in milliseconds that a checkpoint may take before being discarded</td>
  <td><code>-1</code>, the cluster default</td>
</tr>
<tr>
  <td><code>minPauseBetweenCheckpoints</code></td>
  <td>The minimal pause in milliseconds before the next checkpoint is triggered.</td>
  <td><code>-1</code>, the cluster default</td>
</tr>
<tr>
  <td><code>failOnCheckpointingErrors</code></td>
  <td>
  Sets the expected behaviour for tasks in case that they encounter an error in their
            checkpointing procedure. If this is set to true, the task will fail on checkpointing error.
            If this is set to false, the task will only decline a the checkpoint and continue running.
  </td>
  <td><code>-1</code>, the cluster default</td>
</tr>
<tr>
  <td><code>numberOfExecutionRetries</code></td>
  <td>Sets the number of times that failed tasks are re-executed. A value of <code>0</code> effectively disables fault tolerance. A value of <code>-1</code> indicates that the system default value (as defined in the configuration) should be used.</td>
  <td><code>-1</code></td>
</tr>
<tr>
  <td><code>executionRetryDelay</code></td>
  <td>Sets the delay between executions. A value of <code>-1</code> indicates that the default value should be used.</td>
  <td><code>-1</code></td>
</tr>
<tr>
  <td><code>objectReuse</code></td>
  <td>Sets the behavior of reusing objects.</td>
  <td><code>false</code>, no Object reuse</td>
</tr>
<tr>
  <td><code>stateBackend</code></td>
  <td>Sets the state backend to use in streaming mode. The default is to read this setting from the Flink config.</td>
  <td><code>empty</code>, i.e. read from Flink config</td>
</tr>
<tr>
  <td><code>enableMetrics</code></td>
  <td>Enable/disable Beam metrics in Flink Runner</td>
  <td>Default: <code>true</code></td>
</tr>
<tr>
  <td><code>externalizedCheckpointsEnabled</code></td>
  <td>Enables or disables externalized checkpoints. Works in conjunction with CheckpointingInterval</td>
  <td>Default: <code>false</code></td>
</tr>
<tr>
  <td><code>retainExternalizedCheckpointsOnCancellation</code></td>
  <td>Sets the behavior of externalized checkpoints on cancellation.</td>
  <td>Default: <code>false</code></td>
</tr>
<tr>
  <td><code>maxBundleSize</code></td>
  <td>The maximum number of elements in a bundle.</td>
  <td>Default: <code>1000</code></td>
</tr>
<tr>
  <td><code>maxBundleTimeMills</code></td>
  <td>The maximum time to wait before finalising a bundle (in milliseconds).</td>
  <td>Default: <code>1000</code></td>
</tr>
<tr>
  <td><code>shutdownSourcesOnFinalWatermark</code></td>
  <td>If set, shutdown sources when their watermark reaches +Inf.</td>
  <td>Default: <code>false</code></td>
</tr>
<tr>
  <td><code>latencyTrackingInterval</code></td>
  <td>Interval in milliseconds for sending latency tracking marks from the sources to the sinks. Interval value <= 0 disables the feature.</td>
  <td>Default: <code>0</code></td>
</tr>
<tr>
  <td><code>autoWatermarkInterval</code></td>
  <td>The interval in milliseconds for automatic watermark emission.</td>
</tr>
<tr>
  <td><code>executionModeForBatch</code></td>
  <td>Flink mode for data exchange of batch pipelines. Reference {@link org.apache.flink.api.common.ExecutionMode}. Set this to BATCH_FORCED if pipelines get blocked, see https://issues.apache.org/jira/browse/FLINK-10672</td>
  <td>Default: <code>PIPELINED</code></td>
</tr>
<tr>
  <td><code>savepointPath</code></td>
  <td>Savepoint restore path. If specified, restores the streaming pipeline from the provided path.</td>
  <td>Default: None</td>
</tr>
<tr>
  <td><code>allowNonRestoredState</code></td>
  <td>Flag indicating whether non restored state is allowed if the savepoint contains state for an operator that is no longer part of the pipeline.</td>
  <td>Default: <code>false</code></td>
</tr>
</table>
</div>

<!-- Python Options -->
<div class="language-py">
<table class="table table-bordered">

<tr>
  <td><code>files_to_stage</code></td>
  <td>Jar-Files to send to all workers and put on the classpath. The default value is all files from the classpath.</td>
</tr>
<tr>
  <td><code>flink_master</code></td>
  <td>Address of the Flink Master where the Pipeline should be executed. Can either be of the form "host:port" or one of the special values [local], [collection] or [auto].</td>
  <td>Default: <code>[auto]</code></td>
</tr>
<tr>
  <td><code>parallelism</code></td>
  <td>The degree of parallelism to be used when distributing operations onto workers. If the parallelism is not set, the configured Flink default is used, or 1 if none can be found.</td>
  <td>Default: <code>-1</code></td>
</tr>
<tr>
  <td><code>max_parallelism</code></td>
  <td>The pipeline wide maximum degree of parallelism to be used. The maximum parallelism specifies the upper limit for dynamic scaling and the number of key groups used for partitioned state.</td>
  <td>Default: <code>-1</code></td>
</tr>
<tr>
  <td><code>checkpointing_interval</code></td>
  <td>The interval in milliseconds at which to trigger checkpoints of the running pipeline. Default: No checkpointing.</td>
  <td>Default: <code>-1</code></td>
</tr>
<tr>
  <td><code>checkpointing_mode</code></td>
  <td>The checkpointing mode that defines consistency guarantee.</td>
  <td>Default: <code>EXACTLY_ONCE</code></td>
</tr>
<tr>
  <td><code>checkpoint_timeout_millis</code></td>
  <td>The maximum time in milliseconds that a checkpoint may take before being discarded.</td>
  <td>Default: <code>-1</code></td>
</tr>
<tr>
  <td><code>min_pause_between_checkpoints</code></td>
  <td>The minimal pause in milliseconds before the next checkpoint is triggered.</td>
  <td>Default: <code>-1</code></td>
</tr>
<tr>
  <td><code>fail_on_checkpointing_errors</code></td>
  <td>Sets the expected behaviour for tasks in case that they encounter an error in their checkpointing procedure. If this is set to true, the task will fail on checkpointing error. If this is set to false, the task will only decline a the checkpoint and continue running. </td>
  <td>Default: <code>true</code></td>
</tr>
<tr>
  <td><code>number_of_execution_retries</code></td>
  <td>Sets the number of times that failed tasks are re-executed. A value of zero effectively disables fault tolerance. A value of -1 indicates that the system default value (as defined in the configuration) should be used.</td>
  <td>Default: <code>-1</code></td>
</tr>
<tr>
  <td><code>execution_retry_delay</code></td>
  <td>Sets the delay in milliseconds between executions. A value of {@code -1} indicates that the default value should be used.</td>
  <td>Default: <code>-1</code></td>
</tr>
<tr>
  <td><code>object_reuse</code></td>
  <td>Sets the behavior of reusing objects.</td>
  <td>Default: <code>false</code></td>
</tr>
<tr>
  <td><code>state_backend</code></td>
  <td>Sets the state backend to use in streaming mode. Otherwise the default is read from the Flink config.</td>
</tr>
<tr>
  <td><code>enable_metrics</code></td>
  <td>Enable/disable Beam metrics in Flink Runner</td>
  <td>Default: <code>true</code></td>
</tr>
<tr>
  <td><code>externalized_checkpoints_enabled</code></td>
  <td>Enables or disables externalized checkpoints. Works in conjunction with CheckpointingInterval</td>
  <td>Default: <code>false</code></td>
</tr>
<tr>
  <td><code>retain_externalized_checkpoints_on_cancellation</code></td>
  <td>Sets the behavior of externalized checkpoints on cancellation.</td>
  <td>Default: <code>false</code></td>
</tr>
<tr>
  <td><code>max_bundle_size</code></td>
  <td>The maximum number of elements in a bundle.</td>
  <td>Default: <code>1000</code></td>
</tr>
<tr>
  <td><code>max_bundle_time_mills</code></td>
  <td>The maximum time to wait before finalising a bundle (in milliseconds).</td>
  <td>Default: <code>1000</code></td>
</tr>
<tr>
  <td><code>shutdown_sources_on_final_watermark</code></td>
  <td>If set, shutdown sources when their watermark reaches +Inf.</td>
  <td>Default: <code>false</code></td>
</tr>
<tr>
  <td><code>latency_tracking_interval</code></td>
  <td>Interval in milliseconds for sending latency tracking marks from the sources to the sinks. Interval value <= 0 disables the feature.</td>
  <td>Default: <code>0</code></td>
</tr>
<tr>
  <td><code>auto_watermark_interval</code></td>
  <td>The interval in milliseconds for automatic watermark emission.</td>
</tr>
<tr>
  <td><code>execution_mode_for_batch</code></td>
  <td>Flink mode for data exchange of batch pipelines. Reference {@link org.apache.flink.api.common.ExecutionMode}. Set this to BATCH_FORCED if pipelines get blocked, see https://issues.apache.org/jira/browse/FLINK-10672</td>
  <td>Default: <code>PIPELINED</code></td>
</tr>
<tr>
  <td><code>savepoint_path</code></td>
  <td>Savepoint restore path. If specified, restores the streaming pipeline from the provided path.</td>
</tr>
<tr>
  <td><code>allow_non_restored_state</code></td>
  <td>Flag indicating whether non restored state is allowed if the savepoint contains state for an operator that is no longer part of the pipeline.</td>
  <td>Default: <code>false</code></td>
</tr>

</table>
</div>

## Capability

The [Beam Capability Matrix]({{ site.baseurl
}}/documentation/runners/capability-matrix/) documents the
capabilities of the legacy Flink Runner.

The [Portable Capability
Matrix](https://s.apache.org/apache-beam-portability-support-table) documents
the capabilities of the portable Flink Runner.

