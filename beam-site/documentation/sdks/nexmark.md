---
layout: section
title: "Nexmark benchmark suite"
section_menu: section-menu/sdks.html
permalink: /documentation/sdks/java/nexmark/
---
# Nexmark benchmark suite

## What it is

Nexmark is a suite of pipelines inspired by the 'continuous data stream'
queries in [Nexmark research paper](http://datalab.cs.pdx.edu/niagaraST/NEXMark/)

These are multiple queries over a three entities model representing on online auction system:

 - **Person** represents a person submitting an item for auction and/or making a bid
    on an auction.
 - **Auction** represents an item under auction.
 - **Bid** represents a bid for an item under auction.

## The queries

The queries exercise many aspects of Beam model:

* **Query1**: What are the bid values in Euro's?
  Illustrates a simple map.
* **Query2**: What are the auctions with particular auction numbers?
  Illustrates a simple filter.
* **Query3**: Who is selling in particular US states?
  Illustrates an incremental join (using per-key state and timer) and filter.
* **Query4**: What is the average selling price for each auction
  category?
  Illustrates complex join (using custom window functions) and
  aggregation.
* **Query5**: Which auctions have seen the most bids in the last period?
  Illustrates sliding windows and combiners.
* **Query6**: What is the average selling price per seller for their
  last 10 closed auctions.
  Shares the same 'winning bids' core as for **Query4**, and
  illustrates a specialized combiner.
* **Query7**: What are the highest bids per period?
  Deliberately implemented using a side input to illustrate fanout.
* **Query8**: Who has entered the system and created an auction in
  the last period?
  Illustrates a simple join.

We have augmented the original queries with five more:

* **Query0**: Pass-through.
  Allows us to measure the monitoring overhead.
* **Query9**: Winning-bids.
  A common sub-query shared by **Query4** and **Query6**.
* **Query10**: Log all events to GCS files.
  Illustrates windows with large side effects on firing.
* **Query11**: How many bids did a user make in each session they
  were active?
  Illustrates session windows.
* **Query12**: How many bids does a user make within a fixed
  processing time limit?
  Illustrates working in processing time in the Global window, as
  compared with event time in non-Global windows for all the other
  queries.

## Benchmark workload configuration
Here are some of the knobs of the benchmark workload (see [NexmarkConfiguration.java](https://github.com/apache/beam/blob/master/sdks/java/nexmark/src/main/java/org/apache/beam/sdk/nexmark/NexmarkConfiguration.java)).

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
Here is an example output of the Nexmark benchmark run in streaming mode with the SMOKE suite on the (local) direct runner:

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

We can specify the Beam runner to use with maven profiles, available profiles are:

    direct-runner
    spark-runner
    flink-runner
    apex-runner

The runner must also be specified like in any other Beam pipeline using:

    --runner


Test data is deterministically synthesized on demand. The test
data may be synthesized in the same pipeline as the query itself,
or may be published to Pub/Sub.

The query results may be:

* Published to Pub/Sub.
* Written to text files as plain text.
* Written to text files using an Avro encoding.
* Sent to BigQuery.
* Discarded.

### Common configuration parameters

Decide if batch or streaming:

    --streaming=true

Number of events generators:

    --numEventGenerators=4

Run query N:

    --query=N

### Available Suites
The suite to run can be chosen using this configuration parameter:

    --suite=SUITE

Available suites are:
* DEFAULT: Test default configuration with query 0.
* SMOKE: Run the 12 default configurations.
* STRESS: Like smoke but for 1m events.
* FULL_THROTTLE: Like SMOKE but 100m events.


### Apex runner specific configuration

    --manageResources=false --monitorJobs=false

### Google Cloud Dataflow runner specific configuration

    --manageResources=false --monitorJobs=true \
    --enforceEncodability=false --enforceImmutability=false
    --project=<your project> \
    --zone=<your zone> \
    --workerMachineType=n1-highmem-8 \
    --stagingLocation=<a gs path for staging> \
    --runner=DataflowRunner \
    --tempLocation=gs://[your temp location] \
    --stagingLocation=gs://[your staging location] \
    --filesToStage=target/beam-sdks-java-nexmark-{{ site.release_latest }}.jar

### Direct runner specific configuration

    --manageResources=false --monitorJobs=true \
    --enforceEncodability=false --enforceImmutability=false

### Flink runner specific configuration

    --manageResources=false --monitorJobs=true \
    --flinkMaster=local --parallelism=#numcores

### Spark runner specific configuration

    --manageResources=false --monitorJobs=true \
    --sparkMaster=local \
    -Dspark.ui.enabled=false -DSPARK_LOCAL_IP=localhost -Dsun.io.serialization.extendedDebugInfo=true

## Current status

These tables contain statuses of the queries runs in the different runners. Google Cloud Dataflow and Apache Gearpump statuses are yet to come.


### Batch / Synthetic / Local

<table class="table table-bordered">
    <tr>
      <th>Query</th>
      <th>Direct</th>
      <th>Spark</th>
      <th>Flink</th>
      <th>Apex</th>
    </tr>
    <tr>
      <td>0</td>
      <td>ok</td>
      <td>ok</td>
      <td>ok</td>
      <td>ok</td>
    </tr>
    <tr>
      <td>1</td>
      <td>ok</td>
      <td>ok</td>
      <td>ok</td>
      <td>ok</td>
    </tr>
    <tr>
      <td>2</td>
      <td>ok</td>
      <td>ok</td>
      <td>ok</td>
      <td>ok</td>
    </tr>
    <tr>
      <td>3</td>
      <td>ok</td>
      <td>ok</td>
      <td>ok</td>
      <td><a href="https://issues.apache.org/jira/browse/BEAM-1114">BEAM-1114</a></td>
    </tr>
    <tr>
      <td>4</td>
      <td>ok</td>
      <td>ok</td>
      <td>ok</td>
      <td>ok</td>
    </tr>
    <tr>
      <td>5</td>
      <td>ok</td>
      <td>ok</td>
      <td>ok</td>
      <td>ok</td>
    </tr>
    <tr>
      <td>6</td>
      <td>ok</td>
      <td>ok</td>
      <td>ok</td>
      <td>ok</td>
    </tr>
    <tr>
      <td>7</td>
      <td>ok</td>
      <td>ok</td>
      <td>ok</td>
      <td>ok</td>
    </tr>
    <tr>
      <td>8</td>
      <td>ok</td>
      <td>ok</td>
      <td>ok</td>
      <td>ok</td>
    </tr>
    <tr>
      <td>9</td>
      <td>ok</td>
      <td>ok</td>
      <td>ok</td>
      <td>ok</td>
    </tr>
    <tr>
      <td>10</td>
      <td>ok</td>
      <td>ok</td>
      <td>ok</td>
      <td>ok</td>
    </tr>
    <tr>
      <td>11</td>
      <td>ok</td>
      <td>ok</td>
      <td>ok</td>
      <td>ok</td>
    </tr>
    <tr>
      <td>12</td>
      <td>ok</td>
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
      <th>Apex</th>
    </tr>
    <tr>
      <td>0</td>
      <td>ok</td>
      <td>ok</td>
      <td>ok</td>
      <td>ok</td>
    </tr>
    <tr>
      <td>1</td>
      <td>ok</td>
      <td>ok</td>
      <td>ok</td>
      <td>ok</td>
    </tr>
    <tr>
      <td>2</td>
      <td>ok</td>
      <td>ok</td>
      <td>ok</td>
      <td>ok</td>
    </tr>
    <tr>
      <td>3</td>
      <td>ok</td>
      <td><a href="https://issues.apache.org/jira/browse/BEAM-2176">BEAM-2176</a>, <a href="https://issues.apache.org/jira/browse/BEAM-3961">BEAM-3961</a></td>
      <td>ok</td>
      <td><a href="https://issues.apache.org/jira/browse/BEAM-1114">BEAM-1114</a></td>
    </tr>
    <tr>
      <td>4</td>
      <td>ok</td>
      <td>ok</td>
      <td>ok</td>
      <td>ok</td>
    </tr>
    <tr>
      <td>5</td>
      <td>ok</td>
      <td>ok</td>
      <td>ok</td>
      <td>ok</td>
    </tr>
    <tr>
      <td>6</td>
      <td>ok</td>
      <td>ok</td>
      <td>ok</td>
      <td>ok</td>
    </tr>
    <tr>
      <td>7</td>
      <td>ok</td>
      <td><a href="https://issues.apache.org/jira/browse/BEAM-2112">BEAM-2112</a></td>
      <td>ok</td>
      <td>ok</td>
    </tr>
    <tr>
      <td>8</td>
      <td>ok</td>
      <td>ok</td>
      <td>ok</td>
      <td>ok</td>
    </tr>
    <tr>
      <td>9</td>
      <td>ok</td>
      <td>ok</td>
      <td>ok</td>
      <td>ok</td>
    </tr>
    <tr>
      <td>10</td>
      <td>ok</td>
      <td>ok</td>
      <td>ok</td>
      <td>ok</td>
    </tr>
    <tr>
      <td>11</td>
      <td>ok</td>
      <td>ok</td>
      <td>ok</td>
      <td>ok</td>
    </tr>
    <tr>
      <td>12</td>
      <td>ok</td>
      <td>ok</td>
      <td>ok</td>
      <td>ok</td>
    </tr>
</table>

### Batch / Synthetic / Cluster

Yet to come

### Streaming / Synthetic / Cluster

Yet to come

## Running Nexmark

### Running SMOKE suite on the DirectRunner (local)

Batch Mode:

    mvn exec:java -Dexec.mainClass=org.apache.beam.sdk.nexmark.Main -Pdirect-runner -Dexec.args="--runner=DirectRunner --suite=SMOKE --streaming=false --manageResources=false --monitorJobs=true --enforceEncodability=true --enforceImmutability=true"

Streaming Mode:

    mvn exec:java -Dexec.mainClass=org.apache.beam.sdk.nexmark.Main -Pdirect-runner -Dexec.args="--runner=DirectRunner --suite=SMOKE --streaming=true --manageResources=false --monitorJobs=true --enforceEncodability=true --enforceImmutability=true"


### Running SMOKE suite on the SparkRunner (local)

Batch Mode:

    mvn exec:java -Dexec.mainClass=org.apache.beam.sdk.nexmark.Main -Pspark-runner "-Dexec.args=--runner=SparkRunner --suite=SMOKE --streamTimeout=60 --streaming=false --manageResources=false --monitorJobs=true"

Streaming Mode:

    mvn exec:java -Dexec.mainClass=org.apache.beam.sdk.nexmark.Main -Pspark-runner "-Dexec.args=--runner=SparkRunner --suite=SMOKE --streamTimeout=60 --streaming=true --manageResources=false --monitorJobs=true"


### Running SMOKE suite on the FlinkRunner (local)

Batch Mode:

    mvn exec:java -Dexec.mainClass=org.apache.beam.sdk.nexmark.Main -Pflink-runner "-Dexec.args=--runner=FlinkRunner --suite=SMOKE --streamTimeout=60 --streaming=false --manageResources=false --monitorJobs=true  --flinkMaster=local"

Streaming Mode:

    mvn exec:java -Dexec.mainClass=org.apache.beam.sdk.nexmark.Main -Pflink-runner "-Dexec.args=--runner=FlinkRunner --suite=SMOKE --streamTimeout=60 --streaming=true --manageResources=false --monitorJobs=true  --flinkMaster=local"


### Running SMOKE suite on the ApexRunner (local)

Batch Mode:

    mvn exec:java -Dexec.mainClass=org.apache.beam.sdk.nexmark.Main -Papex-runner "-Dexec.args=--runner=ApexRunner --suite=SMOKE --streamTimeout=60 --streaming=false --manageResources=false --monitorJobs=false"

Streaming Mode:

    mvn exec:java -Dexec.mainClass=org.apache.beam.sdk.nexmark.Main -Papex-runner "-Dexec.args=--runner=ApexRunner --suite=SMOKE --streamTimeout=60 --streaming=true --manageResources=false --monitorJobs=false"


### Running SMOKE suite on Google Cloud Dataflow

Building package:

    mvn clean package -Pdataflow-runner

Submit to Google Dataflow service:


```
java -cp sdks/java/nexmark/target/beam-sdks-java-nexmark-bundled-{{ site.release_latest }}.jar \
  org.apache.beam.sdk.nexmark.Main \
  --runner=DataflowRunner
  --project=<your project> \
  --zone=<your zone> \
  --workerMachineType=n1-highmem-8 \
  --stagingLocation=<a gs path for staging> \
  --streaming=true \
  --sourceType=PUBSUB \
  --pubSubMode=PUBLISH_ONLY \
  --pubsubTopic=<an existing Pubsub topic> \
  --resourceNameMode=VERBATIM \
  --manageResources=false \
  --monitorJobs=false \
  --numEventGenerators=64 \
  --numWorkers=16 \
  --maxNumWorkers=16 \
  --suite=SMOKE \
  --firstEventRate=100000 \
  --nextEventRate=100000 \
  --ratePeriodSec=3600 \
  --isRateLimited=true \
  --avgPersonByteSize=500 \
  --avgAuctionByteSize=500 \
  --avgBidByteSize=500 \
  --probDelayedEvent=0.000001 \
  --occasionalDelaySec=3600 \
  --numEvents=0 \
  --useWallclockEventTime=true \
  --usePubsubPublishTime=true \
  --experiments=enable_custom_pubsub_sink
```

```
java -cp sdks/java/nexmark/target/beam-sdks-java-nexmark-bundled-{{ site.release_latest }}.jar \
  org.apache.beam.sdk.nexmark.Main \
  --runner=DataflowRunner
  --project=<your project> \
  --zone=<your zone> \
  --workerMachineType=n1-highmem-8 \
  --stagingLocation=<a gs path for staging> \
  --streaming=true \
  --sourceType=PUBSUB \
  --pubSubMode=SUBSCRIBE_ONLY \
  --pubsubSubscription=<an existing Pubsub subscription to above topic> \
  --resourceNameMode=VERBATIM \
  --manageResources=false \
  --monitorJobs=false \
  --numWorkers=64 \
  --maxNumWorkers=64 \
  --suite=SMOKE \
  --usePubsubPublishTime=true \
  --outputPath=<a gs path under which log files will be written> \
  --windowSizeSec=600 \
  --occasionalDelaySec=3600 \
  --maxLogEvents=10000 \
  --experiments=enable_custom_pubsub_source
```

### Running query 0 on a Spark cluster with Apache Hadoop YARN


Building package:

    mvn clean package -Pspark-runner

Submit to the cluster:

    spark-submit --master yarn-client --class org.apache.beam.sdk.nexmark.Main --driver-memory 512m --executor-memory 512m --executor-cores 1 beam-sdks-java-nexmark-bundled-{{ site.release_latest }}.jar --runner=SparkRunner --query=0 --streamTimeout=60 --streaming=false --manageResources=false --monitorJobs=true
