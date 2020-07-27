---
type: runners
title: "Apache Spark Runner"
aliases: /learn/runners/spark/
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

The Apache Spark Runner can be used to execute Beam pipelines using [Apache Spark](https://spark.apache.org/).
The Spark Runner can execute Spark pipelines just like a native Spark application; deploying a self-contained application for local mode, running on Spark's Standalone RM, or using YARN or Mesos.

The Spark Runner executes Beam pipelines on top of Apache Spark, providing:

* Batch and streaming (and combined) pipelines.
* The same fault-tolerance [guarantees](https://spark.apache.org/docs/latest/streaming-programming-guide.html#fault-tolerance-semantics) as provided by RDDs and DStreams.
* The same [security](https://spark.apache.org/docs/latest/security.html) features Spark provides.
* Built-in metrics reporting using Spark's metrics system, which reports Beam Aggregators as well.
* Native support for Beam side-inputs via spark's Broadcast variables.

The [Beam Capability Matrix](/documentation/runners/capability-matrix/) documents the currently supported capabilities of the Spark Runner.

## Three flavors of the Spark runner
The Spark runner comes in three flavors:

1. A *legacy Runner* which supports only Java (and other JVM-based languages) and that is based on Spark RDD/DStream
2. An *Structured Streaming Spark Runner* which supports only Java (and other JVM-based languages) and that is based on Spark Datasets and the [Apache Spark Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html) framework.
> **Note:** It is still experimental, its coverage of the Beam model is partial. As for now it only supports batch mode.
3. A *portable Runner* which supports Java, Python, and Go

This guide is split into two parts to document the non-portable and
the portable functionality of the Spark Runner. Please use the switcher below to
select the appropriate Runner:

## Which runner to use: portable or non portable runner?

Beam and its Runners originally only supported JVM-based languages
(e.g. Java/Scala/Kotlin). Python and Go SDKs were added later on. The
architecture of the Runners had to be changed significantly to support executing
pipelines written in other languages.

If your applications only use Java, then you should currently go with one of the java based runners.
If you want to run Python or Go pipelines with Beam on Spark, you need to use
the portable Runner. For more information on portability, please visit the
[Portability page](/roadmap/portability/).


<nav class="language-switcher">
  <strong>Adapt for:</strong>
  <ul>
    <li data-type="language-java">Non portable (Java)</li>
    <li data-type="language-py">Portable (Java/Python/Go)</li>
  </ul>
</nav>


## Spark Runner prerequisites and setup

The Spark runner currently supports Spark's 2.x branch, and more specifically any version greater than 2.4.0.

{{< paragraph class="language-java" >}}
You can add a dependency on the latest version of the Spark runner by adding to your pom.xml the following:
{{< /paragraph >}}

{{< highlight java >}}
<dependency>
  <groupId>org.apache.beam</groupId>
  <artifactId>beam-runners-spark</artifactId>
  <version>{{< param release_latest >}}</version>
</dependency>
{{< /highlight >}}

### Deploying Spark with your application

{{< paragraph class="language-java" >}}
In some cases, such as running in local mode/Standalone, your (self-contained) application would be required to pack Spark by explicitly adding the following dependencies in your pom.xml:
{{< /paragraph >}}

{{< highlight java >}}
<dependency>
  <groupId>org.apache.spark</groupId>
  <artifactId>spark-core_2.11</artifactId>
  <version>${spark.version}</version>
</dependency>

<dependency>
  <groupId>org.apache.spark</groupId>
  <artifactId>spark-streaming_2.11</artifactId>
  <version>${spark.version}</version>
</dependency>
{{< /highlight >}}

{{< paragraph class="language-java" >}}
And shading the application jar using the maven shade plugin:
{{< /paragraph >}}

{{< highlight java >}}
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
{{< /highlight >}}

{{< paragraph class="language-java" >}}
After running <code>mvn package</code>, run <code>ls target</code> and you should see (assuming your artifactId is `beam-examples` and the version is `1.0.0`):
{{< /paragraph >}}

{{< highlight java >}}
beam-examples-1.0.0-shaded.jar
{{< /highlight >}}

{{< paragraph class="language-java" >}}
To run against a Standalone cluster simply run:
{{< /paragraph >}}

{{< paragraph class="language-java" >}}
<br><b>For RDD/DStream based runner:</b><br>
{{< /paragraph >}}

{{< highlight java >}}
spark-submit --class com.beam.examples.BeamPipeline --master spark://HOST:PORT target/beam-examples-1.0.0-shaded.jar --runner=SparkRunner
{{< /highlight >}}

{{< paragraph class="language-java" >}}
<br><b>For Structured Streaming based runner:</b><br>
{{< /paragraph >}}

{{< highlight java >}}
spark-submit --class com.beam.examples.BeamPipeline --master spark://HOST:PORT target/beam-examples-1.0.0-shaded.jar --runner=SparkStructuredStreamingRunner
{{< /highlight >}}

{{< paragraph class="language-py" >}}
You will need Docker to be installed in your execution environment. To develop
Apache Beam with Python you have to install the Apache Beam Python SDK: `pip
install apache_beam`. Please refer to the [Python documentation](/documentation/sdks/python/)
on how to create a Python pipeline.
{{< /paragraph >}}

{{< highlight py >}}
pip install apache_beam
{{< /highlight >}}

{{< paragraph class="language-py" >}}
Starting from Beam 2.20.0, pre-built Spark Job Service Docker images are available at 
[Docker Hub](https://hub.docker.com/r/apache/beam_spark_job_server).
{{< /paragraph >}}

{{< paragraph class="language-py" >}}
For older Beam versions, you will need a copy of Apache Beam's source code. You can
download it on the [Downloads page](/get-started/downloads/).
{{< /paragraph >}}

{{< paragraph class="language-py" >}}
1. Start the JobService endpoint:
    * with Docker (preferred): `docker run --net=host apache/beam_spark_job_server:latest`
    * or from Beam source code: `./gradlew :runners:spark:job-server:runShadow`
{{< /paragraph >}}

{{< paragraph class="language-py" >}}
The JobService is the central instance where you submit your Beam pipeline.
The JobService will create a Spark job for the pipeline and execute the
job. To execute the job on a Spark cluster, the Beam JobService needs to be
provided with the Spark master address.
{{< /paragraph >}}

{{< paragraph class="language-py" >}}2. Submit the Python pipeline to the above endpoint by using the `PortableRunner`, `job_endpoint` set to `localhost:8099` (this is the default address of the JobService), and `environment_type` set to `LOOPBACK`. For example:{{< /paragraph >}}

{{< highlight py >}}
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

### Running on a pre-deployed Spark cluster

Deploying your Beam pipeline on a cluster that already has a Spark deployment (Spark classes are available in container classpath) does not require any additional dependencies.
For more details on the different deployment modes see: [Standalone](https://spark.apache.org/docs/latest/spark-standalone.html), [YARN](https://spark.apache.org/docs/latest/running-on-yarn.html), or [Mesos](https://spark.apache.org/docs/latest/running-on-mesos.html).

{{< paragraph class="language-py" >}}1. Start a Spark cluster which exposes the master on port 7077 by default.{{< /paragraph >}}

{{< paragraph class="language-py" >}}
2. Start JobService that will connect with the Spark master:
    * with Docker (preferred): `docker run --net=host apache/beam_spark_job_server:latest --spark-master-url=spark://localhost:7077`
    * or from Beam source code: `./gradlew :runners:spark:job-server:runShadow -PsparkMasterUrl=spark://localhost:7077`
{{< /paragraph >}}

{{< paragraph class="language-py" >}}3. Submit the pipeline as above.
Note however that `environment_type=LOOPBACK` is only intended for local testing.
See [here](/roadmap/portability/#sdk-harness-config) for details.{{< /paragraph >}}

{{< paragraph class="language-py" >}}
(Note that, depending on your cluster setup, you may need to change the `environment_type` option.
See [here](/roadmap/portability/#sdk-harness-config) for details.)
{{< /paragraph >}}

## Pipeline options for the Spark Runner

When executing your pipeline with the Spark Runner, you should consider the following pipeline options.

{{< paragraph class="language-java" >}}
<br><b>For RDD/DStream based runner:</b><br>
{{< /paragraph >}}

<table class="language-java table table-bordered">
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

{{< paragraph class="language-java" >}}
<br><b>For Structured Streaming based runner:</b><br>
{{< /paragraph >}}

<table class="language-java table table-bordered">
<tr>
  <th>Field</th>
  <th>Description</th>
  <th>Default Value</th>
</tr>
<tr>
  <td><code>runner</code></td>
  <td>The pipeline runner to use. This option allows you to determine the pipeline runner at runtime.</td>
  <td>Set to <code>SparkStructuredStreamingRunner</code> to run using Spark Structured Streaming.</td>
</tr>
<tr>
  <td><code>sparkMaster</code></td>
  <td>The url of the Spark Master. This is the equivalent of setting <code>SparkConf#setMaster(String)</code> and can either be <code>local[x]</code> to run local with x cores, <code>spark://host:port</code> to connect to a Spark Standalone cluster, <code>mesos://host:port</code> to connect to a Mesos cluster, or <code>yarn</code> to connect to a yarn cluster.</td>
  <td><code>local[4]</code></td>
</tr>
<tr>
  <td><code>testMode</code></td>
  <td>Enable test mode that gives useful debugging information: catalyst execution plans and Beam DAG printing</td>
  <td>false</td>
</tr>
<tr>
  <td><code>enableSparkMetricSinks</code></td>
  <td>Enable reporting metrics to Spark's metrics Sinks.</td>
  <td>true</td>
</tr>
<tr>
  <td><code>checkpointDir</code></td>
  <td>A checkpoint directory for streaming resilience, ignored in batch. For durability, a reliable filesystem such as HDFS/S3/GS is necessary.</td>
  <td>local dir in /tmp</td>
</tr>
<tr>
  <td><code>filesToStage</code></td>
  <td>Jar-Files to send to all workers and put on the classpath.</td>
  <td>all files from the classpath</td>
</tr>
<tr>
  <td><code>EnableSparkMetricSinks</code></td>
  <td>Enable/disable sending aggregator values to Spark's metric sinks</td>
  <td>true</td>
</tr>
</table>

<table class="language-py table table-bordered">
<tr>
  <th>Field</th>
  <th>Description</th>
  <th>Value</th>
</tr>
<tr>
  <td><code>--runner</code></td>
  <td>The pipeline runner to use. This option allows you to determine the pipeline runner at runtime.</td>
  <td>Set to <code>PortableRunner</code> to run using Spark.</td>
</tr>
<tr>
  <td><code>--job_endpoint</code></td>
  <td>Job service endpoint to use. Should be in the form hostname:port, e.g. localhost:3000</td>
  <td>Set to match your job service endpoint (localhost:8099 by default)</td>
</tr>
</table>

## Additional notes

### Using spark-submit

When submitting a Spark application to cluster, it is common (and recommended) to use the <code>spark-submit</code> script that is provided with the spark installation.
The <code>PipelineOptions</code> described above are not to replace <code>spark-submit</code>, but to complement it.
Passing any of the above mentioned options could be done as one of the <code>application-arguments</code>, and setting <code>--master</code> takes precedence.
For more on how to generally use <code>spark-submit</code> checkout Spark [documentation](https://spark.apache.org/docs/latest/submitting-applications.html#launching-applications-with-spark-submit).

### Monitoring your job

You can monitor a running Spark job using the Spark [Web Interfaces](https://spark.apache.org/docs/latest/monitoring.html#web-interfaces). By default, this is available at port `4040` on the driver node. If you run Spark on your local machine that would be `http://localhost:4040`.
Spark also has a history server to [view after the fact](https://spark.apache.org/docs/latest/monitoring.html#viewing-after-the-fact).
{{< paragraph class="language-java" >}}
Metrics are also available via [REST API](https://spark.apache.org/docs/latest/monitoring.html#rest-api).
Spark provides a [metrics system](https://spark.apache.org/docs/latest/monitoring.html#metrics) that allows reporting Spark metrics to a variety of Sinks. The Spark runner reports user-defined Beam Aggregators using this same metrics system and currently supports <code>GraphiteSink</code> and <code>CSVSink</code>, and providing support for additional Sinks supported by Spark is easy and straight-forward.
{{< /paragraph >}}
{{< paragraph class="language-py" >}}Spark metrics are not yet supported on the portable runner.{{< /paragraph >}}

### Streaming Execution

{{< paragraph class="language-java" >}}
<br><b>For RDD/DStream based runner:</b><br>
If your pipeline uses an <code>UnboundedSource</code> the Spark Runner will automatically set streaming mode. Forcing streaming mode is mostly used for testing and is not recommended.
<br>
<br><b>For Structured Streaming based runner:</b><br>
Streaming mode is not implemented yet in the Spark Structured Streaming runner.
{{< /paragraph >}}

{{< paragraph class="language-py" >}}
Streaming is not yet supported on the Spark portable runner.
{{< /paragraph >}}

### Using a provided SparkContext and StreamingListeners

{{< paragraph class="language-java" >}}
<br><b>For RDD/DStream based runner:</b><br>
If you would like to execute your Spark job with a provided <code>SparkContext</code>, such as when using the [spark-jobserver](https://github.com/spark-jobserver/spark-jobserver), or use <code>StreamingListeners</code>, you can't use <code>SparkPipelineOptions</code> (the context or a listener cannot be passed as a command-line argument anyway).
Instead, you should use <code>SparkContextOptions</code> which can only be used programmatically and is not a common <code>PipelineOptions</code> implementation.
<br>
<br><b>For Structured Streaming based runner:</b><br>
Provided SparkSession and StreamingListeners are not supported on the Spark Structured Streaming runner
{{< /paragraph >}}

{{< paragraph class="language-py" >}}
Provided SparkContext and StreamingListeners are not supported on the Spark portable runner.
{{< /paragraph >}}
