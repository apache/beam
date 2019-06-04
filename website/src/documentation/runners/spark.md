---
layout: section
title: "Apache Spark Runner"
section_menu: section-menu/runners.html
permalink: /documentation/runners/spark/
redirect_from: /learn/runners/spark/
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
# Using the Apache Spark Runner

The Apache Spark Runner can be used to execute Beam pipelines using [Apache Spark](http://spark.apache.org/).
The Spark Runner can execute Spark pipelines just like a native Spark application; deploying a self-contained application for local mode, running on Spark's Standalone RM, or using YARN or Mesos.

The Spark Runner executes Beam pipelines on top of Apache Spark, providing:

* Batch and streaming (and combined) pipelines.
* The same fault-tolerance [guarantees](http://spark.apache.org/docs/latest/streaming-programming-guide.html#fault-tolerance-semantics) as provided by RDDs and DStreams.
* The same [security](http://spark.apache.org/docs/latest/security.html) features Spark provides.
* Built-in metrics reporting using Spark's metrics system, which reports Beam Aggregators as well.
* Native support for Beam side-inputs via spark's Broadcast variables.

The [Beam Capability Matrix]({{ site.baseurl }}/documentation/runners/capability-matrix/) documents the currently supported capabilities of the Spark Runner.

_**Note:**_ _support for the Beam Model in streaming is currently experimental, follow-up in the [mailing list]({{ site.baseurl }}/get-started/support/) for status updates._

## Spark Runner prerequisites and setup

The Spark runner currently supports Spark's 2.x branch, and more specifically any version greater than 2.2.0.

You can add a dependency on the latest version of the Spark runner by adding to your pom.xml the following:
```java
<dependency>
  <groupId>org.apache.beam</groupId>
  <artifactId>beam-runners-spark</artifactId>
  <version>{{ site.release_latest }}</version>
</dependency>
```

### Deploying Spark with your application

In some cases, such as running in local mode/Standalone, your (self-contained) application would be required to pack Spark by explicitly adding the following dependencies in your pom.xml:
```java
<dependency>
  <groupId>org.apache.spark</groupId>
  <artifactId>spark-core_2.10</artifactId>
  <version>${spark.version}</version>
</dependency>

<dependency>
  <groupId>org.apache.spark</groupId>
  <artifactId>spark-streaming_2.10</artifactId>
  <version>${spark.version}</version>
</dependency>
```

And shading the application jar using the maven shade plugin:
```java
<plugin>
  <groupId>org.apache.maven.plugins</groupId>
  <artifactId>maven-shade-plugin</artifactId>
  <configuration>
    <createDependencyReducedPom>false</createDependencyReducedPom>
    <filters>
      <filter>
        <artifact>*:*</artifact>
        <excludes>
          <exclude>META-INF/*.SF</exclude>
          <exclude>META-INF/*.DSA</exclude>
          <exclude>META-INF/*.RSA</exclude>
        </excludes>
      </filter>
    </filters>
  </configuration>
  <executions>
    <execution>
      <phase>package</phase>
      <goals>
        <goal>shade</goal>
      </goals>
      <configuration>
        <shadedArtifactAttached>true</shadedArtifactAttached>
        <shadedClassifierName>shaded</shadedClassifierName>
        <transformers>
          <transformer
            implementation="org.apache.maven.plugins.shade.resource.ServicesResourceTransformer"/>
        </transformers>
      </configuration>
    </execution>
  </executions>
</plugin>
```

After running <code>mvn package</code>, run <code>ls target</code> and you should see (assuming your artifactId is `beam-examples` and the version is `1.0.0`):
```
beam-examples-1.0.0-shaded.jar
```

To run against a Standalone cluster simply run:
```
spark-submit --class com.beam.examples.BeamPipeline --master spark://HOST:PORT target/beam-examples-1.0.0-shaded.jar --runner=SparkRunner
```

### Running on a pre-deployed Spark cluster

Deploying your Beam pipeline on a cluster that already has a Spark deployment (Spark classes are available in container classpath) does not require any additional dependencies.
For more details on the different deployment modes see: [Standalone](http://spark.apache.org/docs/latest/spark-standalone.html), [YARN](http://spark.apache.org/docs/latest/running-on-yarn.html), or [Mesos](http://spark.apache.org/docs/latest/running-on-mesos.html).

## Pipeline options for the Spark Runner

When executing your pipeline with the Spark Runner, you should consider the following pipeline options.

<table class="table table-bordered">
<tr>
  <th>Field</th>
  <th>Description</th>
  <th>Default Value</th>
</tr>
<tr>
  <td><code>runner</code></td>
  <td>The pipeline runner to use. This option allows you to determine the pipeline runner at runtime.</td>
  <td>Set to <code>SparkRunner</code> to run using Spark.</td>
</tr>
<tr>
  <td><code>sparkMaster</code></td>
  <td>The url of the Spark Master. This is the equivalent of setting <code>SparkConf#setMaster(String)</code> and can either be <code>local[x]</code> to run local with x cores, <code>spark://host:port</code> to connect to a Spark Standalone cluster, <code>mesos://host:port</code> to connect to a Mesos cluster, or <code>yarn</code> to connect to a yarn cluster.</td>
  <td><code>local[4]</code></td>
</tr>
<tr>
  <td><code>storageLevel</code></td>
  <td>The <code>StorageLevel</code> to use when caching RDDs in batch pipelines. The Spark Runner automatically caches RDDs that are evaluated repeatedly. This is a batch-only property as streaming pipelines in Beam are stateful, which requires Spark DStream's <code>StorageLevel</code> to be <code>MEMORY_ONLY</code>.</td>
  <td>MEMORY_ONLY</td>
</tr>
<tr>
  <td><code>batchIntervalMillis</code></td>
  <td>The <code>StreamingContext</code>'s <code>batchDuration</code> - setting Spark's batch interval.</td>
  <td><code>1000</code></td>
</tr>
<tr>
  <td><code>enableSparkMetricSinks</code></td>
  <td>Enable reporting metrics to Spark's metrics Sinks.</td>
  <td>true</td>
</tr>
<tr>
  <td><code>cacheDisabled</code></td>
  <td>Disable caching of reused PCollections for whole Pipeline. It's useful when it's faster to recompute RDD rather than save.</td>
  <td>false</td>
</tr>
</table>

## Additional notes

### Using spark-submit

When submitting a Spark application to cluster, it is common (and recommended) to use the <code>spark-submit</code> script that is provided with the spark installation.
The <code>PipelineOptions</code> described above are not to replace <code>spark-submit</code>, but to complement it.
Passing any of the above mentioned options could be done as one of the <code>application-arguments</code>, and setting <code>--master</code> takes precedence.
For more on how to generally use <code>spark-submit</code> checkout Spark [documentation](http://spark.apache.org/docs/latest/submitting-applications.html#launching-applications-with-spark-submit).

### Monitoring your job

You can monitor a running Spark job using the Spark [Web Interfaces](http://spark.apache.org/docs/latest/monitoring.html#web-interfaces). By default, this is available at port `4040` on the driver node. If you run Spark on your local machine that would be `http://localhost:4040`.
Spark also has a history server to [view after the fact](http://spark.apache.org/docs/latest/monitoring.html#viewing-after-the-fact).
Metrics are also available via [REST API](http://spark.apache.org/docs/latest/monitoring.html#rest-api).
Spark provides a [metrics system](http://spark.apache.org/docs/latest/monitoring.html#metrics) that allows reporting Spark metrics to a variety of Sinks. The Spark runner reports user-defined Beam Aggregators using this same metrics system and currently supports <code>GraphiteSink</code> and <code>CSVSink</code>, and providing support for additional Sinks supported by Spark is easy and straight-forward.

### Streaming Execution

If your pipeline uses an <code>UnboundedSource</code> the Spark Runner will automatically set streaming mode. Forcing streaming mode is mostly used for testing and is not recommended.

### Using a provided SparkContext and StreamingListeners

If you would like to execute your Spark job with a provided <code>SparkContext</code>, such as when using the [spark-jobserver](https://github.com/spark-jobserver/spark-jobserver), or use <code>StreamingListeners</code>, you can't use <code>SparkPipelineOptions</code> (the context or a listener cannot be passed as a command-line argument anyway).
Instead, you should use <code>SparkContextOptions</code> which can only be used programmatically and is not a common <code>PipelineOptions</code> implementation.
