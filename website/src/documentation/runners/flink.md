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

If you want to use the local execution mode with the Flink runner to don't have to complete any setup.

To use the Flink Runner for executing on a cluster, you have to setup a Flink cluster by following the Flink [setup quickstart](https://ci.apache.org/projects/flink/flink-docs-stable/quickstart/setup_quickstart.html).

### Version Compatibility

The Flink cluster version has to match the version used by the FlinkRunner. To find out which version of Flink please see the table below:

<table class="table table-bordered">
<tr>
  <th>Beam version</th>
  <th>Flink version</th>
</tr>
<tr>
  <td>2.7.0, 2.6.0</td>
  <td>1.5.x</td>
</tr>
<tr>
  <td>2.5.0, 2.4.0, 2.3.0</td>
  <td>1.4.x</td>
</tr>
<tr>
  <td>2.2.0</td>
  <td>1.3.x with Scala 2.10</td>
</tr>
<tr>
  <td>2.2.0, 2.1.x</td>
  <td>1.3.x with Scala 2.10</td>
</tr>
<tr>
  <td>2.0.0</td>
  <td>1.2.x with Scala 2.10</td>
</tr>
</table>

For retrieving the right version, see the [Flink downloads page](https://flink.apache.org/downloads.html).

For more information, the [Flink Documentation](https://ci.apache.org/projects/flink/flink-docs-stable/) can be helpful.

### Specify your dependency

<span class="language-java">When using Java, you must specify your dependency on the Flink Runner in your `pom.xml`.</span>
```java
<dependency>
  <groupId>org.apache.beam</groupId>
  <artifactId>beam-runners-flink_2.11</artifactId>
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
      --filesToStage=target/word-count-beam--bundled-0.1.jar"
```
If you have a Flink `JobManager` running on your local machine you can give `localhost:8081` for
`flinkMaster`.

## Pipeline options for the Flink Runner

When executing your pipeline with the Flink Runner, you can set these pipeline options.

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
  <td><code>1</code></td>
</tr>
<tr>
  <td><code>checkpointingInterval</code></td>
  <td>The interval between consecutive checkpoints (i.e. snapshots of the current pipeline state used for fault tolerance).</td>
  <td><code>-1L</code>, i.e. disabled</td>
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
  <td><code>stateBackend</code></td>
  <td>Sets the state backend to use in streaming mode. The default is to read this setting from the Flink config.</td>
  <td><code>empty</code>, i.e. read from Flink config</td>
</tr>
</table>

See the reference documentation for the  <span class="language-java">[FlinkPipelineOptions]({{ site.baseurl }}/documentation/sdks/javadoc/{{ site.release_latest }}/index.html?org/apache/beam/runners/flink/FlinkPipelineOptions.html)</span><span class="language-py">[PipelineOptions](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/options/pipeline_options.py)</span> interface (and its subinterfaces) for the complete list of pipeline configuration options.

## Additional information and caveats

### Monitoring your job

You can monitor a running Flink job using the Flink JobManager Dashboard. By default, this is available at port `8081` of the JobManager node. If you have a Flink installation on your local machine that would be `http://localhost:8081`.

### Streaming Execution

If your pipeline uses an unbounded data source or sink, the Flink Runner will automatically switch to streaming mode. You can enforce streaming mode by using the `streaming` setting mentioned above.
