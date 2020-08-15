---
type: languages
title: "Nexmark benchmark suite"
aliases: /documentation/sdks/java/nexmark/
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
# Nexmark benchmark suite

## What it is

Nexmark is a suite of pipelines inspired by the 'continuous data stream'
queries in [Nexmark research
paper](https://web.archive.org/web/20100620010601/http://datalab.cs.pdx.edu/niagaraST/NEXMark/)

These are multiple queries over a three entities model representing on online
auction system:

 - **Person** represents a person submitting an item for auction and/or making
   a bid on an auction.
 - **Auction** represents an item under auction.
 - **Bid** represents a bid for an item under auction.

## The queries

The queries exercise many aspects of Beam model:

* **Query1** or **CURRENCY_CONVERSION**: What are the bid values in Euro's?
  Illustrates a simple map.
* **Query2** or **SELECTION**: What are the auctions with particular auction numbers?
  Illustrates a simple filter.
* **Query3** or **LOCAL_ITEM_SUGGESTION**: Who is selling in particular US states?
  Illustrates an incremental join (using per-key state and timer) and filter.
* **Query4** or **AVERAGE_PRICE_FOR_CATEGORY**: What is the average selling price for each auction
  category?
  Illustrates complex join (using custom window functions) and
  aggregation.
* **Query5** or **HOT_ITEMS**: Which auctions have seen the most bids in the last period?
  Illustrates sliding windows and combiners.
* **Query6** or **AVERAGE_SELLING_PRICE_BY_SELLER**: What is the average selling price per seller for their
  last 10 closed auctions.
  Shares the same 'winning bids' core as for **Query4**, and
  illustrates a specialized combiner.
* **Query7** or **HIGHEST_BID**: What are the highest bids per period?
  Deliberately implemented using a side input to illustrate fanout.
* **Query8** or **MONITOR_NEW_USERS**: Who has entered the system and created an auction in
  the last period?
  Illustrates a simple join.

We have augmented the original queries with five more:

* **Query0** or **PASSTHROUGH**: Pass-through.
  Allows us to measure the monitoring overhead.
* **Query9** or **WINNING_BIDS**: Winning-bids.
  A common sub-query shared by **Query4** and **Query6**.
* **Query10** or **LOG_TO_SHARDED_FILES**: Log all events to GCS files.
  Illustrates windows with large side effects on firing.
* **Query11** or **USER_SESSIONS**: How many bids did a user make in each session they
  were active?
  Illustrates session windows.
* **Query12** or **PROCESSING_TIME_WINDOWS**: How many bids does a user make within a fixed
  processing time limit?
  Illustrates working in processing time in the Global window, as
  compared with event time in non-Global windows for all the other
  queries.
* **BOUNDED_SIDE_INPUT_JOIN**: Joins a stream to a bounded side input, modeling basic stream enrichment.


## Benchmark workload configuration

Here are some of the knobs of the benchmark workload (see
[NexmarkConfiguration.java](https://github.com/apache/beam/blob/master/sdks/java/testing/nexmark/src/main/java/org/apache/beam/sdk/nexmark/NexmarkConfiguration.java)).

These configuration items can be passed to the launch command line.

### Events generation (defaults)

* 100 000 events generated
* 100 generator threads
* Event rate in SIN curve
* Initial event rate of 10 000
* Event rate step of 10 000
* 100 concurrent auctions
* 1000 concurrent persons bidding / creating auctions

### Windows (defaults)

* size 10s
* sliding period 5s
* watermark hold for 0s

### Events Proportions (defaults)

* Hot Auctions = ½
* Hot Bidders =¼
* Hot Sellers=¼

### Technical

* Artificial CPU load
* Artificial IO load

## Nexmark output

Here is an example output of the Nexmark benchmark run in streaming mode with
the SMOKE suite on the (local) direct runner:

<pre>
Performance:
  Conf       Runtime(sec)         Events(/sec)         Results
  0000                5,5              18138,9          100000
  0001                4,2              23657,4           92000
  0002                2,2              45683,0             351
  0003                3,9              25348,5             444
  0004                1,6               6207,3              40
  0005                5,0              20173,5              12
  0006                0,9              11376,6             401
  0007              121,4                823,5               1
  0008                2,5              40273,9            6000
  0009                0,9              10695,2             298
  0010                4,0              25025,0               1
  0011                4,4              22655,2            1919
  0012                3,5              28208,7            1919
</pre>

## Benchmark launch configuration

The Nexmark launcher accepts the `--runner` argument as usual for programs that
use Beam PipelineOptions to manage their command line arguments. In addition
to this, the necessary dependencies must be configured.

When running via Gradle, the following two parameters control the execution:

    -P nexmark.args
        The command line to pass to the Nexmark main program.

    -P nexmark.runner
	The Gradle project name of the runner, such as ":runners:direct-java" or
	":runners:flink:1.10. The project names can be found in the root
        `settings.gradle`.

Test data is deterministically synthesized on demand. The test
data may be synthesized in the same pipeline as the query itself,
or may be published to Pub/Sub or Kafka.

The query results may be:

* Published to Pub/Sub or Kafka.
* Written to text files as plain text.
* Written to text files using an Avro encoding.
* Sent to BigQuery.
* Discarded.

### Common configuration parameters

Decide if batch or streaming:

    --streaming=true

Number of events generators:

    --numEventGenerators=4

Queries can be run by their name or by their number (number is still there for backward compatibility, only the queries 0 to 12 have a number)

Run query **N**:

    --query=N

Run query called **PASSTHROUGH**:

    --query=PASSTHROUGH

### Available Suites
The suite to run can be chosen using this configuration parameter:

    --suite=SUITE

Available suites are:
* DEFAULT: Test default configuration with query 0.
    * SMOKE: Run all the queries with the default configuration.
* STRESS: Like smoke but for 1m events.
* FULL_THROTTLE: Like SMOKE but 100m events.

### Google Cloud Dataflow runner specific configuration

    --manageResources=false --monitorJobs=true \
    --enforceEncodability=false --enforceImmutability=false
    --project=<your project> \
    --zone=<your zone> \
    --workerMachineType=n1-highmem-8 \
    --stagingLocation=gs://<a gs path for staging> \
    --runner=DataflowRunner \
    --tempLocation=gs://<a gs path for temporary files> \
    --filesToStage=target/beam-sdks-java-nexmark-{{< param release_latest >}}.jar

### Direct runner specific configuration

    --manageResources=false --monitorJobs=true \
    --enforceEncodability=false --enforceImmutability=false

### Flink runner specific configuration

    --manageResources=false --monitorJobs=true \
    --flinkMaster=[local] --parallelism=#numcores

### Spark runner specific configuration

    --manageResources=false --monitorJobs=true \
    --sparkMaster=local \
    -Dspark.ui.enabled=false -DSPARK_LOCAL_IP=localhost -Dsun.io.serialization.extendedDebugInfo=true
	
### Kafka source/sink configuration parameters

Set Kafka host/ip (for example, "localhost:9092"):

    --bootstrapServers=<kafka host/ip> 

Write results into Kafka topic:

    --sinkType=KAFKA

Set topic name which will be used for benchmark results:

	--kafkaResultsTopic=<topic name>

Write or/and read events into/from Kafka topic:

    --sourceType=KAFKA
	
Set topic name which will be used for benchmark events:

	--kafkaTopic=<topic name>

## Current status

These tables contain statuses of the queries runs in the different runners. Google Cloud Dataflow status is yet to come.


### Batch / Synthetic / Local

<table class="table table-bordered">
    <tr>
      <th>Query</th>
      <th>Direct</th>
      <th>Spark</th>
      <th>Flink</th>
    </tr>
    <tr>
      <td>0</td>
      <td>ok</td>
      <td>ok</td>
      <td>ok</td>
    </tr>
    <tr>
      <td>1</td>
      <td>ok</td>
      <td>ok</td>
      <td>ok</td>
    </tr>
    <tr>
      <td>2</td>
      <td>ok</td>
      <td>ok</td>
      <td>ok</td>
    </tr>
    <tr>
      <td>3</td>
      <td>ok</td>
      <td>ok</td>
      <td>ok</td>
    </tr>
    <tr>
      <td>4</td>
      <td>ok</td>
      <td>ok</td>
      <td>ok</td>
    </tr>
    <tr>
      <td>5</td>
      <td>ok</td>
      <td>ok</td>
      <td>ok</td>
    </tr>
    <tr>
      <td>6</td>
      <td>ok</td>
      <td>ok</td>
      <td>ok</td>
    </tr>
    <tr>
      <td>7</td>
      <td>ok</td>
      <td>ok</td>
      <td>ok</td>
    </tr>
    <tr>
      <td>8</td>
      <td>ok</td>
      <td>ok</td>
      <td>ok</td>
    </tr>
    <tr>
      <td>9</td>
      <td>ok</td>
      <td>ok</td>
      <td>ok</td>
    </tr>
    <tr>
      <td>10</td>
      <td>ok</td>
      <td>ok</td>
      <td>ok</td>
    </tr>
    <tr>
      <td>11</td>
      <td>ok</td>
      <td>ok</td>
      <td>ok</td>
    </tr>
    <tr>
      <td>12</td>
      <td>ok</td>
      <td>ok</td>
      <td>ok</td>
    </tr>
    <tr>
      <td>BOUNDED_SIDE_INPUT_JOIN</td>
      <td>ok</td>
      <td>ok</td>
      <td>ok</td>
    </tr>
</table>

### Streaming / Synthetic / Local

<table class="table table-bordered">
    <tr>
      <th>Query</th>
      <th>Direct</th>
      <th>Spark <a href="https://issues.apache.org/jira/browse/BEAM-2847">BEAM-2847</a></th>
      <th>Flink</th>
    </tr>
    <tr>
      <td>0</td>
      <td>ok</td>
      <td>ok</td>
      <td>ok</td>
    </tr>
    <tr>
      <td>1</td>
      <td>ok</td>
      <td>ok</td>
      <td>ok</td>
    </tr>
    <tr>
      <td>2</td>
      <td>ok</td>
      <td>ok</td>
      <td>ok</td>
    </tr>
    <tr>
      <td>3</td>
      <td>ok</td>
      <td><a href="https://issues.apache.org/jira/browse/BEAM-2176">BEAM-2176</a>, <a href="https://issues.apache.org/jira/browse/BEAM-3961">BEAM-3961</a></td>
      <td>ok</td>
    </tr>
    <tr>
      <td>4</td>
      <td>ok</td>
      <td>ok</td>
      <td>ok</td>
    </tr>
    <tr>
      <td>5</td>
      <td>ok</td>
      <td>ok</td>
      <td>ok</td>
    </tr>
    <tr>
      <td>6</td>
      <td>ok</td>
      <td>ok</td>
      <td>ok</td>
    </tr>
    <tr>
      <td>7</td>
      <td>ok</td>
      <td><a href="https://issues.apache.org/jira/browse/BEAM-2112">BEAM-2112</a></td>
      <td>ok</td>
    </tr>
    <tr>
      <td>8</td>
      <td>ok</td>
      <td>ok</td>
      <td>ok</td>
    </tr>
    <tr>
      <td>9</td>
      <td>ok</td>
      <td>ok</td>
      <td>ok</td>
    </tr>
    <tr>
      <td>10</td>
      <td>ok</td>
      <td>ok</td>
      <td>ok</td>
    </tr>
    <tr>
      <td>11</td>
      <td>ok</td>
      <td>ok</td>
      <td>ok</td>
    </tr>
    <tr>
      <td>12</td>
      <td>ok</td>
      <td>ok</td>
      <td>ok</td>
    </tr>
        <tr>
          <td>BOUNDED_SIDE_INPUT_JOIN</td>
          <td>ok</td>
          <td><a href="https://issues.apache.org/jira/browse/BEAM-2112">BEAM-2112</a></td>
          <td>ok</td>
        </tr>
</table>

### Batch / Synthetic / Cluster

Yet to come

### Streaming / Synthetic / Cluster

Yet to come

## Running Nexmark

### Running SMOKE suite on the DirectRunner (local)

The DirectRunner is default, so it is not required to pass `-Pnexmark.runner`.
Here we do it for maximum clarity.

The direct runner does not have separate batch and streaming modes, but the
Nexmark launch does.

These parameters leave on many of the DirectRunner's extra safety checks so the
SMOKE suite can make sure there is nothing broken in the Nexmark suite.

Batch Mode:

    ./gradlew :sdks:java:testing:nexmark:run \
        -Pnexmark.runner=":runners:direct-java" \
        -Pnexmark.args="
            --runner=DirectRunner
            --streaming=false
            --suite=SMOKE
            --manageResources=false
            --monitorJobs=true
            --enforceEncodability=true
            --enforceImmutability=true"

Streaming Mode:

    ./gradlew :sdks:java:testing:nexmark:run \
        -Pnexmark.runner=":runners:direct-java" \
        -Pnexmark.args="
            --runner=DirectRunner
            --streaming=true
            --suite=SMOKE
            --manageResources=false
            --monitorJobs=true
            --enforceEncodability=true
            --enforceImmutability=true"

### Running SMOKE suite on the SparkRunner (local)

The SparkRunner is special-cased in the Nexmark gradle launch. The task will
provide the version of Spark that the SparkRunner is built against, and
configure logging.

Batch Mode:

    ./gradlew :sdks:java:testing:nexmark:run \
        -Pnexmark.runner=":runners:spark" \
        -Pnexmark.args="
            --runner=SparkRunner
            --suite=SMOKE
            --streamTimeout=60
            --streaming=false
            --manageResources=false
            --monitorJobs=true"

Streaming Mode:

    ./gradlew :sdks:java:testing:nexmark:run \
        -Pnexmark.runner=":runners:spark" \
        -Pnexmark.args="
            --runner=SparkRunner
            --suite=SMOKE
            --streamTimeout=60
            --streaming=true
            --manageResources=false
            --monitorJobs=true"

### Running SMOKE suite on the FlinkRunner (local)

Batch Mode:

    ./gradlew :sdks:java:testing:nexmark:run \
        -Pnexmark.runner=":runners:flink:1.10" \
        -Pnexmark.args="
            --runner=FlinkRunner
            --suite=SMOKE
            --streamTimeout=60
            --streaming=false
            --manageResources=false
            --monitorJobs=true
            --flinkMaster=[local]"

Streaming Mode:

    ./gradlew :sdks:java:testing:nexmark:run \
        -Pnexmark.runner=":runners:flink:1.10" \
        -Pnexmark.args="
            --runner=FlinkRunner
            --suite=SMOKE
            --streamTimeout=60
            --streaming=true
            --manageResources=false
            --monitorJobs=true
            --flinkMaster=[local]"

### Running SMOKE suite on Google Cloud Dataflow

Set these up first so the below command is valid

    PROJECT=<your project>
    ZONE=<your zone>
    STAGING_LOCATION=gs://<a GCS path for staging>
    PUBSUB_TOPCI=<existing pubsub topic>

Launch:

    ./gradlew :sdks:java:testing:nexmark:run \
        -Pnexmark.runner=":runners:google-cloud-dataflow-java" \
        -Pnexmark.args="
            --runner=DataflowRunner
            --suite=SMOKE
            --streamTimeout=60
            --streaming=true
            --manageResources=false
            --monitorJobs=true
            --project=${PROJECT}
            --zone=${ZONE}
            --workerMachineType=n1-highmem-8
            --stagingLocation=${STAGING_LOCATION}
            --sourceType=PUBSUB
            --pubSubMode=PUBLISH_ONLY
            --pubsubTopic=${PUBSUB_TOPIC}
            --resourceNameMode=VERBATIM
            --manageResources=false
            --numEventGenerators=64
            --numWorkers=16
            --maxNumWorkers=16
            --firstEventRate=100000
            --nextEventRate=100000
            --ratePeriodSec=3600
            --isRateLimited=true
            --avgPersonByteSize=500
            --avgAuctionByteSize=500
            --avgBidByteSize=500
            --probDelayedEvent=0.000001
            --occasionalDelaySec=3600
            --numEvents=0
            --useWallclockEventTime=true
            --usePubsubPublishTime=true
            --experiments=enable_custom_pubsub_sink"

### Running query 0 on a Spark cluster with Apache Hadoop YARN

Building package:

    ./gradlew :sdks:java:testing:nexmark:assemble

Submit to the cluster:

    spark-submit \
        --class org.apache.beam.sdk.nexmark.Main \
        --master yarn-client \
        --driver-memory 512m \
        --executor-memory 512m \
        --executor-cores 1 \
        sdks/java/testing/nexmark/build/libs/beam-sdks-java-nexmark-{{< param release_latest >}}-spark.jar \
            --runner=SparkRunner \
            --query=0 \
            --streamTimeout=60 \
            --streaming=false \
            --manageResources=false \
            --monitorJobs=true"

## Nexmark dashboards
Below dashboards are used as a CI mechanism to detect no-regression on the Beam components. They are not supposed to be benchmark comparision of the runners or engines. Especially because:
- Parameters of the runners are not the same
- Nexmark is run with the runners in local (most of the time embedded) mode
- Nexmark runs on a shared machine that also run all the CI and build.
- Runners have different support of the Beam model
- Runners have different strengths that make comparison difficult:
    - Some runners were designed to be batch oriented, others streaming oriented
    - Some are designed towards sub-second latency, others support auto-scaling

### Dashboards content
At each commit on master, Nexmark suites are run and plots are created on the graphs. All metrics dashboards are hosted at [metrics.beam.apache.org](http://metrics.beam.apache.org/).

There are 2 kinds of dashboards:
- one for performances (run times of the queries)
- one for the size of the output PCollection (which should be constant)

There are dashboards for these runners (others to come):
- spark
- flink
- direct runner
- Dataflow

Each dashboard contains:
- graphs in batch mode
- graphs in streaming mode
- graphs for all the queries.

