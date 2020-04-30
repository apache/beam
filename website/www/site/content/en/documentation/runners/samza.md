---
type: runners
title: "Apache Samza Runner"
aliases: /learn/runners/Samza/
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

# Using the Apache Samza Runner

The Apache Samza Runner can be used to execute Beam pipelines using [Apache Samza](https://samza.apache.org/). The Samza Runner executes Beam pipeline in a Samza application and can run locally. The application can further be built into a .tgz file, and deployed to a YARN cluster or Samza standalone cluster with Zookeeper.

The Samza Runner and Samza are suitable for large scale, stateful streaming jobs, and provide:

* First class support for local state (with RocksDB store). This allows fast state access for high frequency streaming jobs.
* Fault-tolerance with support for incremental checkpointing of state instead of full snapshots. This enables Samza to scale to applications with very large state.
* A fully asynchronous processing engine that makes remote calls efficient.
* Flexible deployment model for running the the applications in any hosting environment with Zookeeper.
* Features like canaries, upgrades and rollbacks that support extremely large deployments with minimal downtime.

The [Beam Capability Matrix](/documentation/runners/capability-matrix/) documents the currently supported capabilities of the Samza Runner.

## Samza Runner prerequisites and setup

The Samza Runner is built on Samza version greater than 1.0.

### Specify your dependency

<span class="language-java">You can specify your dependency on the Samza Runner by adding the following to your `pom.xml`:</span>
{{< highlight java >}}
<dependency>
  <groupId>org.apache.beam</groupId>
  <artifactId>beam-runners-samza</artifactId>
  <version>{{< param release_latest >}}</version>
  <scope>runtime</scope>
</dependency>

<!-- Samza dependencies -->
<dependency>
  <groupId>org.apache.samza</groupId>
  <artifactId>samza-api</artifactId>
  <version>${samza.version}</version>
</dependency>

<dependency>
  <groupId>org.apache.samza</groupId>
  <artifactId>samza-core_2.11</artifactId>
  <version>${samza.version}</version>
</dependency>

<dependency>
  <groupId>org.apache.samza</groupId>
  <artifactId>samza-kafka_2.11</artifactId>
  <version>${samza.version}</version>
  <scope>runtime</scope>
</dependency>

<dependency>
  <groupId>org.apache.samza</groupId>
  <artifactId>samza-kv_2.11</artifactId>
  <version>${samza.version}</version>
  <scope>runtime</scope>
</dependency>

<dependency>
  <groupId>org.apache.samza</groupId>
  <artifactId>samza-kv-rocksdb_2.11</artifactId>
  <version>${samza.version}</version>
  <scope>runtime</scope>
</dependency>
    
{{< /highlight >}}

## Executing a pipeline with Samza Runner

If you run your pipeline locally or deploy it to a standalone cluster with all the jars and resource files, no packaging is required. For example, the following command runs the WordCount example:

```
$ mvn exec:java -Dexec.mainClass=org.apache.beam.examples.WordCount \
    -Psamza-runner \
    -Dexec.args="--runner=SamzaRunner \
      --inputFile=/path/to/input \
      --output=/path/to/counts"
```

To deploy your pipeline to a YARN cluster, here is the [instructions](https://samza.apache.org/startup/hello-samza/latest/) of deploying a sample Samza job. First you need to package your application jars and resource files into a `.tgz` archive file, and make it available to download for Yarn containers. In your config, you need to specify the URI of this TGZ file location:

```
yarn.package.path=${your_job_tgz_URI}

job.name=${your_job_name}
job.factory.class=org.apache.samza.job.yarn.YarnJobFactory
job.coordinator.system=${job_coordinator_system}
job.default.system=${job_default_system}
```

For more details on the configuration, see [Samza Configuration Reference](https://samza.apache.org/learn/documentation/latest/jobs/configuration-table.html).

The config file will be passed in by setting the command line arg `--configFilePath=/path/to/config.properties`. With that, you can run your main class of Beam pipeline in a Yarn Resource Manager, and the Samza Runner will submit a Yarn job under the hood. 

Check out our [Samza Beam example from Github](https://github.com/apache/samza-beam-examples)

## Pipeline options for the Samza Runner

When executing your pipeline with the Samza Runner, you can use the following pipeline options.

<table class="table table-bordered">
<tr>
  <th>Field</th>
  <th>Description</th>
  <th>Default Value</th>
</tr>
<tr>
  <td><code>runner</code></td>
  <td>The pipeline runner to use. This option allows you to determine the pipeline runner at runtime.</td>
  <td>Set to <code>SamzaRunner</code> to run using Samza.</td>
</tr>
<tr>
  <td><code>configFilePath</code></td>
  <td>The config for Samza using a properties file.</td>
  <td><code>empty</code>, i.e. use local execution.</td>
</tr>
<tr>
  <td><code>configFactory</code></td>
  <td>The factory to read config file from config file path.</td>
  <td><code>PropertiesConfigFactory</code>, reading configs as a property file.</td>
</tr>
<tr>
  <td><code>configOverride</code></td>
  <td>The config override to set programmatically.</td>
  <td><code>empty</code>, i.e. use config file or local execution.</td>
</tr>
<tr>
  <td><code>jobInstance</code></td>
  <td>The instance name of the job.</td>
  <td><code>1</code></td>
</tr>
<tr>
  <td><code>samzaExecutionEnvironment</code></td>
  <td>Samza application execution environment. See <code>SamzaExecutionEnvironment</code> for more details.</td>
  <td><code>LOCAL</code></td>
</tr>
<tr>
  <td><code>watermarkInterval</code></td>
  <td>The interval to check for watermarks in milliseconds.</td>
  <td><code>1000</code></td>
</tr>
<tr>
  <td><code>systemBufferSize</code></td>
  <td>The maximum number of messages to buffer for a given system.</td>
  <td><code>5000</code></td>
</tr>
<tr>
  <td><code>maxSourceParallelism</code></td>
  <td>The maximum parallelism allowed for any data source.</td>
  <td><code>1</code></td>
</tr>
<tr>
  <td><code>storeBatchGetSize</code></td>
  <td>The batch get size limit for the state store.</td>
  <td><code>10000</code></td>
</tr>
<tr>
  <td><code>enableMetrics</code></td>
  <td>Enable/disable Beam metrics in Samza Runne.</td>
  <td><code>true</code></td>
</tr>
<tr>
  <td><code>stateDurable</code></td>
  <td>The config for state to be durable.</td>
  <td><code>false</code></td>
</tr>
<tr>
  <td><code>maxBundleSize</code></td>
  <td>The maximum number of elements in a bundle.</td>
  <td><code>1</code> (by default the auto bundling is disabled)</td>
</tr>
<tr>
  <td><code>maxBundleTimeMs</code></td>
  <td>The maximum time to wait before finalising a bundle (in milliseconds)..</td>
  <td><code>1000</code></td>
</tr>
</table>

## Monitoring your job

You can monitor your pipeline job using metrics emitted from both Beam and Samza, e.g. Beam source metrics such as `elements_read` and `backlog_elements`, and Samza job metrics such as `job-healthy` and `process-envelopes`. A complete list of Samza metrics is in [Samza Metrics Reference](https://samza.apache.org/learn/documentation/latest/container/metrics-table.html). You can view your job's metrics via JMX in development, and send the metrics to graphing system such as [Graphite](https://graphiteapp.org/). For more details, please see [Samza Metrics](https://samza.apache.org/learn/documentation/latest/container/metrics.html).

For a running Samza YARN job, you can use YARN web UI to monitor the job status and check logs.
