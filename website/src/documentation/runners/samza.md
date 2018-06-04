---
layout: section
title: "Apache Samza Runner"
section_menu: section-menu/runners.html
permalink: /documentation/runners/samza/
redirect_from: /learn/runners/Samza/
---
# Using the Apache Samza Runner

The Apache Samza Runner can be used to execute Beam pipelines using [Apache Samza](http://samza.apache.org/). The Samza Runner executes Beam pipeline in a Samza application and can run locally. The application can further be built into a .tgz file, and deployed to a YARN cluster or Samza standalone cluster with Zookeeper.

The Samza Runner and Samza are suitable for large scale, stateful streaming jobs, and provide:

* First class support for local state (with RocksDB store). This allows fast state access for high frequency streaming jobs.
* Fault-tolerance with support for incremental checkpointing of state instead of full snapshots. This enables Samza to scale to applications with very large state.
* A fully asynchronous processing engine that makes remote calls efficient.
* Flexible deployment model for running the the applications in any hosting environment with Zookeeper.
* Features like canaries, upgrades and rollbacks that support extremely large deployments with minimal downtime.

The [Beam Capability Matrix]({{ site.baseurl }}/documentation/runners/capability-matrix/) documents the currently supported capabilities of the Samza Runner.

## Samza Runner prerequisites and setup

The Samza Runner is built on Samza version greater than 0.14.1, and uses Scala version 2.11.

### Specify your dependency

<span class="language-java">You can specify your dependency on the Samza Runner by adding the following to your `pom.xml`:</span>
```java
<dependency>
  <groupId>org.apache.beam</groupId>
  <artifactId>beam-runners-samza_2.11</artifactId>
  <version>{{ site.release_latest }}</version>
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
    
```

## Executing a pipeline with Samza Runner

If you run your pipeline locally or deploy it to a standalone cluster bundled with all the jars and resource files, no packaging is required. For example, the following command runs the WordCount example:

```
$ mvn exec:java -Dexec.mainClass=org.apache.beam.examples.WordCount \
    -Psamza-runner \
    -Dexec.args="--runner=SamzaRunner \
      --inputFile=/path/to/input \
      --output=/path/to/counts"
```

To deploy your pipeline to a YARN cluster, you need to package your application jars and resource files into a `.tgz` archive file. In your config, you need to specify the URI of the TGZ file for Samza Runner to download:

```
yarn.package.path=${your_job_tgz_URI}

job.name=${your_job_name}
job.factory.class=org.apache.samza.job.yarn.YarnJobFactory
job.coordinator.system=${job_coordinator_system}
job.default.system=${job_default_system}
```

The config file can be passed to Samza Runner by setting the command line arg `--configFilePath=/path/to/config.properties`. For more details on the Samza configuration, see [Samza Configuration Reference](https://samza.apache.org/learn/documentation/latest/jobs/configuration-table.html).

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
  <td><code>samzaConfig</code></td>
  <td>The config for Samza runner.</td>
  <td>Config for running locally.</td>
</tr>
<tr>
  <td><code>ConfigFilePath</code></td>
  <td>The config for Samza runner using a properties file.</td>
  <td><code>empty</code>, i.e. use default samzaConfig</td>
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
</table>

## Monitoring your job

You can monitor your pipeline job using metrics emitted from both Beam and Samza, e.g. Beam source metrics such as `elements_read` and `backlog_elements`, and Samza job metrics such as `job-healthy` and `process-envelopes`. A complete list of Samza metrics is in [Samza Metrics Reference](https://samza.apache.org/learn/documentation/latest/container/metrics-table.html). You can view your job's metrics via JMX in development, and send the metrics to graphing system such as [Graphite](http://graphiteapp.org/). For more details, please see [Samza Metrics](https://samza.apache.org/learn/documentation/latest/container/metrics.html).

For a running Samza YARN job, you can use YARN web UI to monitor the job status and check logs.