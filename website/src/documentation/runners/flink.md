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
# Using the Apache Flink Runner

The old Flink Runner will eventually be replaced by the Portable Runner which enables to run pipelines in other languages than Java. Please see the [Portability page]({{ site.baseurl }}/contribute/portability/) for the latest state.

<nav class="language-switcher">
  <strong>Adapt for:</strong>
  <ul>
    <li data-type="language-java">Java SDK</li>
    <li data-type="language-py">Python SDK</li>
  </ul>
</nav>

The Apache Flink Runner can be used to execute Beam pipelines using [Apache Flink](https://flink.apache.org). When using the Flink Runner you will create a jar file containing your job that can be executed on a regular Flink cluster. It's also possible to execute a Beam pipeline using Flink's local execution mode without setting up a cluster. This is helpful for development and debugging of your pipeline.

The Flink Runner and Flink are suitable for large scale, continuous jobs, and provide:

* A streaming-first runtime that supports both batch processing and data streaming programs
* A runtime that supports very high throughput and low event latency at the same time
* Fault-tolerance with *exactly-once* processing guarantees
* Natural back-pressure in streaming programs
* Custom memory management for efficient and robust switching between in-memory and out-of-core data processing algorithms
* Integration with YARN and other components of the Apache Hadoop ecosystem

The [Beam Capability Matrix]({{ site.baseurl }}/documentation/runners/capability-matrix/) documents the supported capabilities of the Flink Runner.

## Flink Runner prerequisites and setup

If you want to use the local execution mode with the Flink Runner you don't have to complete any setup.
You can simply run your Beam pipeline. Be sure to set the Runner to `FlinkRunner`.

To use the Flink Runner for executing on a cluster, you have to setup a Flink cluster by following the
Flink [Setup Quickstart](https://ci.apache.org/projects/flink/flink-docs-stable/quickstart/setup_quickstart.html#setup-download-and-start-flink).

### Version Compatibility

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
  <td rowspan="3">2.10.0</td>
  <td>1.5.x</td>
  <td>beam-runners-flink_2.11</td>
</tr>
<tr>
  <td>1.6.x</td>
  <td>beam-runners-flink-1.6</td>
</tr>
<tr>
  <td>1.7.x</td>
  <td>beam-runners-flink-1.7</td>
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

### Specify your dependency

<span class="language-java">When using Java, you must specify your dependency on the Flink Runner in your `pom.xml`.</span>

Use the Beam version and the artifact id from the above table. For example:

```java
<dependency>
  <groupId>org.apache.beam</groupId>
  <artifactId>beam-runners-flink-1.6</artifactId>
  <version>{{ site.release_latest }}</version>
</dependency>
```

<span class="language-py">This section is not applicable to the Beam SDK for Python.</span>

## Executing a pipeline on a Flink cluster

For executing a pipeline on a Flink cluster you need to package your program along will all dependencies in a so-called fat jar. How you do this depends on your build system but if you follow along the [Beam Quickstart]({{ site.baseurl }}/get-started/quickstart/) this is the command that you have to run:

```
$ mvn package -Pflink-runner
```
The Beam Quickstart Maven project is setup to use the Maven Shade plugin to create a fat jar and the `-Pflink-runner` argument makes sure to include the dependency on the Flink Runner.

For actually running the pipeline you would use this command
```
$ mvn exec:java -Dexec.mainClass=org.apache.beam.examples.WordCount \
    -Pflink-runner \
    -Dexec.args="--runner=FlinkRunner \
      --inputFile=/path/to/pom.xml \
      --output=/path/to/counts \
      --flinkMaster=<flink master url> \
      --filesToStage=target/word-count-beam-bundled-0.1.jar"
```
If you have a Flink `JobManager` running on your local machine you can provide `localhost:8081` for
`flinkMaster`. Otherwise an embedded Flink cluster will be started for the WordCount job.

## Additional information and caveats

### Monitoring your job

You can monitor a running Flink job using the Flink JobManager Dashboard or its Rest interfaces. By default, this is available at port `8081` of the JobManager node. If you have a Flink installation on your local machine that would be `http://localhost:8081`. Note: When you use the `[local]` mode an embedded Flink cluster will be started which does not make a dashboard available.

### Streaming Execution

If your pipeline uses an unbounded data source or sink, the Flink Runner will automatically switch to streaming mode. You can enforce streaming mode by using the `streaming` setting mentioned below.

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
