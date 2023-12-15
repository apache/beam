---
layout: post
title:  "Scaling a streaming workload on Apache Beam, 1 million events per second and beyond"
date:   2023-12-01 00:00:01 -0800
categories:
  - blog
authors:
  - pabs
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

# Scaling a streaming workload on Apache Beam

<img class="center-block"
    src="/images/blog/scaling-streaming-workload/0-intro.png"
    alt="Streaming Processing">

Scaling a streaming workload is critical for ensuring that a pipeline can handle large amounts of data being processed, while minimizing latency and executing efficiently. Without proper scaling, a pipeline may experience performance issues or even fail entirely, delaying the time to insights for the business.

When using Apache Beam, developing a streaming pipeline can be made easy given the support for the sources and sinks needed by the workload. One can focus on the needed processing (transformations, enrichments or aggregations) and setting the right configurations on each case.

However, it is important to identify the key performance bottlenecks and make sure that the pipeline has the resources it needs to handle the load efficiently. This may involve right-sizing the number of workers, understanding the settings needed for the source and sinks of the pipeline, optimizing the processing logic, and even the transport formats in use.

This article aims to illustrate how to attack the problem of scaling and optimizing a streaming workload developed in Apache Beam and run on GCP Cloud Dataflow. The goal is to reach 1 million events per second, while also trying to minimize latency and resources in use for the execution. The workload will be grounded in Pub/Sub as the streaming source and BigQuery as the sink. We plan to describe the reasoning behind the configuration settings and code changes in place to help the workload achieve the desired scale and beyond.

Finally, it is worth mentioning progression described in this article maps the evolution of a real-life workload, with lots of simplifications. After the initial business requirements for the pipeline were achieved, the focus was on optimizing the performance and reducing the resources needed for the execution.

## Execution setup

For this article we created a test suite which takes care of creating the necessary components for the pipelines to execute. The code can be found in this Github [repository](https://github.com/prodriguezdefino/apache-beam-streaming-tests), and the subsequent changes which are introduced on every run can be found in this [folder](https://github.com/prodriguezdefino/apache-beam-streaming-tests/tree/main/scaling-streaming-workload-blog) as scripts that can be executed to achieve similar results.

All the execution scripts will also execute a Terraform based automation to create a PubSub topic and subscription, BigQuery dataset and table to run the workload. Also, it will launch 2 pipelines, one data generation pipeline that will be pushing events to the PubSub topic and the ingestion pipeline which is our main focus to understand the potential improvement points.

In all cases the pipelines start with an empty PubSub topic and subscription, and an empty BigQuery table. The plan is to quickly reach the generation of 1 million events per second and after a few minutes review how the ingestion pipeline scales with time. The data being auto generated is based on provided schemas or IDL (given the configuration) and the goal is to have messages ranging between 800 bytes and 2KB, adding up to ~1GB/s volume throughput. Also, the ingestion pipelines are using the same worker type configuration on all runs (`n2d-standard-4` GCE machines) and capping the max workers number to avoid very large fleets.

As previously mentioned, all the executions run on GCP Cloud Dataflow, but all the configurations and format changes can be applied to the suite while executing on other supported Apache Beam runners (changes and recommendations are not runner specific).

### Local Environment Requirements

Before launching the startup scripts, some requisites for the local environment should be fulfilled:

*   gcloud must be installed and configured with the correct permissions.
*   terraform must be installed.
*   JDK 17 or later must be installed.
*   Maven 3.6 or later must be present.

See the [requirements](https://github.com/prodriguezdefino/apache-beam-streaming-tests#requisites) section on the repository for the full details on them.

Also, it is important to review the service quotas and resources available in the GCP project that will be in use, more specifically: PubSub regional capacity, BigQuery ingestion quota and GCE instances available in the selected region for the tests.

### Workload Description

Focusing on the ingestion pipeline, our workload is [very simple](https://github.com/prodriguezdefino/apache-beam-streaming-tests/blob/main/canonical-streaming-pipelines/src/main/java/com/google/cloud/pso/beam/pipelines/StreamingSourceToBigQuery.java#L55): read data in a specific format from PubSub (Apache Thrift in this case), deal with potential compression and batching settings (not enabled by default), execute a UDF (identity function by default), transform the input format to one of the formats supported by the BigQueryIO transform and then write the data to the configured table.

<img class="center-block"
    src="/images/blog/scaling-streaming-workload/0-pipeline.png"
    alt="Example Workload">

The pipeline we used for the tests is highly configurable, see the [options](https://github.com/prodriguezdefino/apache-beam-streaming-tests/blob/main/canonical-streaming-pipelines/src/main/java/com/google/cloud/pso/beam/pipelines/StreamingSourceToBigQuery.java#L39) in the file for more details on how to tweak the ingestion. On each of our steps no code changes are needed, the execution scripts will take care of the configurations needed.

Although these tests are focused on reading data from PubSub, the ingestion pipeline is capable of reading data from a generic streaming source. The repository contains other [examples](https://github.com/prodriguezdefino/apache-beam-streaming-tests/tree/main/example-suite-scripts) on how to launch this same test suite but instead reading data from PubSubLite and Kafka as well (in all cases taking care of the automation needed to set up the streaming infrastructure).

Finally as seen in the configuration [options](https://github.com/prodriguezdefino/apache-beam-ptransforms/blob/a0dd229081625c7b593512543614daf995a9f870/common/src/main/java/com/google/cloud/pso/beam/common/formats/options/TransportFormatOptions.java), the pipeline supports many transport format options for the input (Thrift, Avro and JSON). This suite focuses on Thrift because it is a common open source format and generates a format transformation need (the intent is to put some strain in the workload processing), but similar tests can be executed for Avro and JSON input data. The streaming data generator pipeline can generate random data for the [3 supported formats](https://github.com/prodriguezdefino/apache-beam-streaming-tests/tree/main/streaming-data-generator/src/main/java/com/google/cloud/pso/beam/generator/formats) by walking directly on the schema (Avro and JSON) or IDL (Thrift) provided for execution.

## First Run : Default settings

The default values for the execution implies writing the data to BigQuery using `STREAMING_INSERTS` mode for BigQueryIO, which correlates with the [tableData insertAll API](https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/insertAll) for BigQuery. This API supports data in JSON format. From the Apache Beam perspective by using `BigQueryIO.writeTableRows` method we can easily resolve the writes into BigQuery.

For our ingestion pipeline it means that the Thrift format needs to be transformed into TableRow, and to do that we need to discover first how to translate the Thrift IDL into a BigQuery table schema. That can be achieved by translating the Thrift IDL into an Avro schema, and then using Beam utilities the table schema for BigQuery can be easily translated. Luckily this only needs to be done at bootstrap and that schema transformation is cached at the DoFn level.

After setting up the data generation and ingestion pipelines, and after letting the pipelines execute for some minutes, we quickly see problems to sustain the desired throughput.

<img class="center-block"
    src="/images/blog/scaling-streaming-workload/1-default-ps.png"
    alt="PubSub metrics">

As seen in the previous image, the number of messages that are not being processed by the ingestion pipeline start to show as unacknowledged messages in PubSub metrics.

<img class="center-block"
    src="/images/blog/scaling-streaming-workload/1-default-throughput.png"
    alt="Throughput">

By reviewing the per stage performance metrics, we clearly see that the pipeline shows a saw-like shape normally associated with the throttling mechanisms the Dataflow runner uses when some of the stages are acting as bottlenecks for the throughput. Also, we clearly see that the Reshuffle step on the BigQueryIO write transform does not scale as expected.

This happens because by default the `[BigQueryOptions](https://github.com/apache/beam/blob/master/sdks/java/io/google-cloud-platform/src/main/java/org/apache/beam/sdk/io/gcp/bigquery/BigQueryOptions.java#L57)` uses 50 different keys to shuffle data to workers before the writes happen on BigQuery. To solve this problem we can include a configuration to our launch script that will enable the write operations to scale to a larger number of workers, improving the performance.


## Second Run : Improve write bottleneck

After increasing the number of streaming keys to a higher number, 512 keys in our case, we restarted the test suite and started to see how the PubSub metrics started to improve. After an initial ramp on the size of the backlog we see the curve to start to ease out.

<img class="center-block"
    src="/images/blog/scaling-streaming-workload/2-skeys-ps.png"
    alt="PubSub metrics">

This is good, but we should take a look at the throughput per stage numbers to understand if we are achieving the goal we set up for this exercise.

<img class="center-block"
    src="/images/blog/scaling-streaming-workload/2-skeys-throughput.png"
    alt="Throughput">

Although the performance has clearly improved, and the PubSub backlog no longer increases monotonically, we are still far from the goal of processing 1 million events per second (1GB/s) for our ingestion pipeline. In fact, the throughput metrics jump all over the time, indicating that there are bottlenecks preventing the processing to further scale.

## Third Run : Unleash autoscale

Luckily for us, auto-scaling the writes is an option when writing into BigQuery. This simplifies the configuration, preventing us from guessing on the right number of shards. We switched the pipeline’s configuration and enabled this setting for the next launch [script](https://github.com/prodriguezdefino/apache-beam-streaming-tests/blob/main/scaling-streaming-workload-blog/3-ps2bq-si-tr-streamingautoshard.sh).

<img class="center-block"
    src="/images/blog/scaling-streaming-workload/3-autoshard-parallelism.png"
    alt="Key Parallelism">

Right off the bat, we can see the auto-sharding mechanism tweaks the number of keys very aggressively and in a dynamic way, this is good since different moments in time may have different scale needs (early backlog recoveries, spikes in the execution, etc.).

<img class="center-block"
    src="/images/blog/scaling-streaming-workload/3-autoshard-throughput-tr.png"
    alt="Throughput">

By inspecting the throughput performance per stage we can clearly see that as the number of keys gets increased the performance of the writes increases as well, in fact reaching very large numbers!

Also once the initial backlog was consumed and the pipeline stabilized we can see that the performance numbers we planned as objectives are reached. The pipeline can sustain processing quite more than a million events per second from PubSub and processing several GB/s of BigQuery ingestion (yay!).

But we can try to do better. There are several improvements we can introduce to the pipeline to make the execution more efficient, and in most cases those are simply configuration changes. We just need to know where to focus next.

<img class="center-block"
    src="/images/blog/scaling-streaming-workload/3-autoshard-autoscale.png"
    alt="Resources">

As seen in the previous image, the number of workers needed to sustain this throughput is quite high still. The workload itself is not really CPU intensive, most of the cost is spent on transforming formats and on IO interactions (shuffles and the actual writes). So we can look closer at the transport formats first to understand what can be improved.

<img class="center-block"
    src="/images/blog/scaling-streaming-workload/3-autoshard-tr-input.png"
    alt="Thrift Input Size">
<img class="center-block"
    src="/images/blog/scaling-streaming-workload/3-autoshard-tr-output.png"
    alt="TableRow Output Size">

When looking at the input size, right before the identity UDF execution, the data format is binary Thrift which is a decently compact format even when no compression is used; but while comparing the PCollection approximated size with the TableRow format needed for BigQuery ingestion we can see a clear size increase. This is something we can act on, by changing the BigQuery write API in use.

<img class="center-block"
    src="/images/blog/scaling-streaming-workload/3-autoshard-tr-overhead.png"
    alt="Translation Overhead">

If we inspect the StoreInBigQuery transform, we can see that the large majority of the wall time is spent on the actual writes. Also, we note that the wall time spent converting data to the destination format (TableRows) compared with how much is spent in the actual writes is quite large (13 times bigger for the writes). This can be improved by switching the pipeline write mode.

## Fourth Run : In with the new (using StorageWrites API)

Enabling StorageWrite API for this pipeline is simple, we just need to set the write mode as `STORAGE_WRITE_API` and define a write triggering frequency. We will be writing data at most every 10 seconds for this test. The write triggering frequency controls how long the per-stream data will be accumulated, a higher number will define a larger output to be written after the stream assignment but also impose a larger end to end latency for every element read from PubSub. Similar to the `STREAMING_WRITES` configuration, BigQueryIO can handle auto-sharding for the writes, this showed already to be the best setting for performance.

After both pipelines, data generator and the ingestion one, become stable we can quickly and clearly spot the performance benefits seen when using StorageWrites API in BigQueryIO. The first thing is about the wall time rate between the format transformation and write operation. After enabling the new implementation this rate becomes much smaller since the wall time spent on writes is only ~34% larger than the format transformation.

<img class="center-block"
    src="/images/blog/scaling-streaming-workload/4-format-transformation.png"
    alt="Translation Overhead">

We also see that the pipeline throughput is quite smooth after stabilization. This enables the runner to quickly and steadily downscale the pipeline resources needed to sustain the desired throughput.

<img class="center-block"
    src="/images/blog/scaling-streaming-workload/4-throughput.png"
    alt="Throughput">

Also, taking a look at the resource scale needed to process the data we see another dramatical improvement, the streaming inserts based pipeline needed more than 80 workers while being stable to sustain the throughput, the storage writes pipeline only needs 49 (a sizable 40% improvement).

<img class="center-block"
    src="/images/blog/scaling-streaming-workload/4-ingestion-scale.png"
    alt="Resources">

We can use the reference of the data generation pipeline, which only needs to randomly generate data and write the events to PubSub. It executes steadily with an average of 40 workers; the improvements on the ingestion pipeline using the right configuration for the workload makes it closer to those resources needed for the generation.

Similarly to the streaming inserts based pipeline, to write the data into BigQuery a format translation needs to be executed, from Thrift to TableRow in the former and Thrift to protobuf on the latter. In fact, since we are using the `BigQueryIO.writeTableRows` method we are adding another hop in the format translation. And also, as seen previously, the TableRow format implies

Increasing the size of the PCollection being processed, this is an improvement area we can delve further into.

## Fifth Run : A better write format

The BigQueryIO transform, when using `STORAGE_WRITE_API` exposes a method that can be used to write Beam Row type directly into BigQuery. This is very useful for a large number of pipelines given to the flexibility the Row type provides for interoperability and schema management. Also, it is quite efficient for shuffling and compared with TableRow is more dense, so we can expect smaller PCollection sizes for our pipeline.

For the next run, we will be decreasing the triggering frequency when writing to BigQuery since our data volume is not small, and given that we will be using a different format a slightly different code will be running (the test pipeline script will get configured with the flag `--formatToStore=BEAM_ROW` for this change).

<img class="center-block"
    src="/images/blog/scaling-streaming-workload/5-input-size.png"
    alt="Thrift input size">
<img class="center-block"
    src="/images/blog/scaling-streaming-workload/5-output-size.png"
    alt="Row output size">

As expected we can see that the PCollection size that gets written into BigQuery is considerably smaller than the one on previous executions, in fact for this particular execution the Beam Row format implies a smaller size compared with Thrift format. This could be an important factor given that a larger PCollection conformed by bigger per element sizes can put a non trivial memory pressure in smaller worker configurations, reducing the overall throughput.

<img class="center-block"
    src="/images/blog/scaling-streaming-workload/5-format-trasformation.png"
    alt="Translation overhead">

Also, the wall clock rate for the format transformation and the actual BigQuery writes maintain a very similar rate. Handling the Beam Row format does not impose a performance penalty in the format translation and subsequent writes. This is confirmed also by the number of workers in use by the pipeline once the throughput becomes stable, slightly smaller than the previous run but clearly in the same ballpark.

<img class="center-block"
    src="/images/blog/scaling-streaming-workload/5-ingestion-scale.png"
    alt="Resources">

We are in a much better position than when we started, but given our test pipeline input format we can improve things a little bit more.

## Sixth Run : Further reduce the format translation effort

There is another supported format for the input PCollection in the BigQueryIO transform that can be very advantageous for our input format, the method `writeGenericRecords` enables the transform to transform Avro GenericRecords directly into protobuf before the write operation. Apache Thrift can be transformed into Avro GenericRecords very efficiently so we can make another test run configuring our test ingestion pipeline by setting the option `--formatToStore=AVRO_GENERIC_RECORD` on our execution script.

This time, the difference between format translation and writes increases significantly, given performance improvement we were expecting, we see now that the translation to Avro GenericRecords is only a 20% of the write effort spent on writing those records into BigQuery. Given that all the test pipelines had similar runtimes and the wall clock seen in the WriteIntoBigQuery stage is also aligned with other StorageWrite related runs, it is clear that for this particular workload using this format is the right call.

<img class="center-block"
    src="/images/blog/scaling-streaming-workload/6-format-transformation.png"
    alt="Translation overhead">

Also when checking on resource utilization we see further gains, considering we need less CPU time to execute the format translations for our workload while achieving the desired throughput.

<img class="center-block"
    src="/images/blog/scaling-streaming-workload/6-ingestion-scale.png"
    alt="Resources">

This pipeline compares better with the previous run, running steadily on 42 workers once throughput has been stabilized. Given the worker configuration used (`nd2-standard-4), and the volume throughput of the workload process (~1 GB/s), we are achieving ~6 MB/s throughput per CPU core which is quite impressive for a streaming pipeline with exactly once semantics.

<img class="center-block"
    src="/images/blog/scaling-streaming-workload/6-latencies.png"
    alt="Latencies">

Finally the latency seen at this scale deserves a callout, achieving sub-second E2E latencies during sustained periods of time (when adding up all the stages executed in the main path of the pipeline).

Given the workload requirements and the implemented pipeline code this is the best performance we can extract without further tuning the runner’s specific settings (which are outside of the scope for this article).

## Seventh Run : Lets just relax (at least some constraints)

When using the STORAGE\_WRITE\_API setting for BigQueryIO we are enforcing exactly once semantics on the writes. This is great for a lot of use cases that need strong consistency on the data that gets processed, but it imposes a performance and cost penalty.

From a high level perspective, writes into BigQuery are made in batches which are released given the current sharding and the triggering frequency. If writes fail during the execution of the particular bundle it will be retried, and a bundle of data will be committed into BigQuery only when all the data in that particular bundle was correctly appended to a stream. This implementation needs to shuffle the full volume of data to create the batches that will be written, and also the information of the finished batches for later commit (although this last piece is very small compared with the first).

<img class="center-block"
    src="/images/blog/scaling-streaming-workload/7-previous-data-input.png"
    alt="Read data size">

When looking at the previous pipeline execution, we can see that the total data being processed from Streaming Engine perspective for the pipeline is quite larger than the data being read from PubSub. In these images we see 7 TB of data being read from PubSub, and the processing of data for the whole execution of the pipeline implies moving 25 TB of data to and from Streaming Engine to achieve the level of consistency desired.

<img class="center-block"
    src="/images/blog/scaling-streaming-workload/7-previous-shuffle-total.png"
    alt="Streamed data size">

In the cases where data consistency is not a hard requirement for the ingestion, we can relax the BigQueryIO write mode to use an at-least-once semantic. By doing this, the implementation will avoid shuffling and grouping data for the writes, but this decision may cause a small number of repeated rows being written into the destination table (this could happen in case of append errors, infrequent worker restarts, and other even less frequent errors).

Adding the configuration to use `STORAGE_API_AT_LEAST_ONCE` write mode. We are also adding the configuration flag `–useStorageApiConnectionPool` to instruct the StorageWrite client to reuse connections while writing data, this only works for the `STORAGE_API_AT_LEAST_ONCE` mode and will reduce the occurrences of warnings similars to `Storage Api write delay more than 8 seconds`.

<img class="center-block"
    src="/images/blog/scaling-streaming-workload/7-resources.png"
    alt="Resources">

Once we let the pipeline throughput stabilize we see a very similar pattern on resource utilization for the workload. The number of workers in use reaches 40, a small improvement compared with the last run. But, and as expected, the amount of data being moved from the Streaming Engine perspective becomes quite close to the data read from PubSub.


<img class="center-block"
    src="/images/blog/scaling-streaming-workload/7-current-input.png"
    alt="Read data size">
<img class="center-block"
    src="/images/blog/scaling-streaming-workload/7-current-shuffle-total.png"
    alt="Streamed data size">

Considering all these factors, we have further optimized the workload achieving a throughput of 6.4 MB/s per CPU core, a small improvement compared to the same workload when using consistent writes into BigQuery, but with a much smaller total on the streaming data being incurred. This configuration represents the most optimal setup for our workload, seeing the highest throughput per resource and being the option that streams less data across workers.

<img class="center-block"
    src="/images/blog/scaling-streaming-workload/7-latency.png"
    alt="Streamed data size">

One final look at the latency seen for our pipeline shows an impressively low latency for the E2E processing. Given that the main path of our pipeline has been fused in a single execution stage from reads to writes, we see that even at p99 the latency tends to be below 300 milliseconds at a quite large volume throughput (as previously mentioned around 1 GB/s).

## Recap

Optimizing Apache Beam streaming workloads for low latency and efficient execution requires careful analysis, decision-making and setting the right configurations.

Considering the scenario discussed in this article, it is essential to consider factors like overall CPU utilization, throughput and latency per stage, PCollection sizes, wall time per stage, write mode, and transport formats, in addition to writing the right pipeline for the workload.

Our experiments revealed that using the StorageWrite API, auto sharding for writes, and Avro GenericRecords as the transport format yielded the most efficient results. Relaxing the consistency for writes can further improve performance.

The accompanying [Github repository](https://github.com/prodriguezdefino/apache-beam-streaming-tests) contains a test suite that you can use to replicate the analysis on your GCP project or with a different runner setup, feel free to take it for a spin (comments and PRs are always welcomed).
