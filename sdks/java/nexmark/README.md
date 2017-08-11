<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
-->

# NEXMark test suite

This is a suite of pipelines inspired by the 'continuous data stream'
queries in [http://datalab.cs.pdx.edu/niagaraST/NEXMark/]
(http://datalab.cs.pdx.edu/niagaraST/NEXMark/).

These are multiple queries over a three entities model representing on online auction system:

 - **Person** represents a person submitting an item for auction and/or making a bid
    on an auction.
 - **Auction** represents an item under auction.
 - **Bid** represents a bid for an item under auction.

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

We can specify the Beam runner to use with maven profiles, available profiles are:

* direct-runner
* spark-runner
* flink-runner
* apex-runner

The runner must also be specified like in any other Beam pipeline using

    --runner


Test data is deterministically synthesized on demand. The test
data may be synthesized in the same pipeline as the query itself,
or may be published to Pubsub.

The query results may be:

* Published to Pubsub.
* Written to text files as plain text.
* Written to text files using an Avro encoding.
* Send to BigQuery.
* Discarded.

# Configuration

## Common configuration parameters

Decide if batch or streaming:

    --streaming=true

Number of events generators

    --numEventGenerators=4

Run query N

    --query=N

## Available Suites
The suite to run can be chosen using this configuration parameter:

    --suite=SUITE

Available suites are:
* DEFAULT: Test default configuration with query 0.
* SMOKE: Run the 12 default configurations.
* STRESS: Like smoke but for 1m events.
* FULL_THROTTLE: Like SMOKE but 100m events.

   

## Apex specific configuration

    --manageResources=false --monitorJobs=false

## Dataflow specific configuration

    --manageResources=false --monitorJobs=true \
    --enforceEncodability=false --enforceImmutability=false
    --project=<your project> \
    --zone=<your zone> \
    --workerMachineType=n1-highmem-8 \
    --stagingLocation=<a gs path for staging> \
    --runner=DataflowRunner \
    --tempLocation=gs://talend-imejia/nexmark/temp/ \
    --stagingLocation=gs://talend-imejia/nexmark/temp/staging/ \
    --filesToStage=target/beam-sdks-java-nexmark-2.1.0-SNAPSHOT.jar

## Direct specific configuration

    --manageResources=false --monitorJobs=true \
    --enforceEncodability=false --enforceImmutability=false

## Flink specific configuration

    --manageResources=false --monitorJobs=true \
    --flinkMaster=local --parallelism=#numcores

## Spark specific configuration

    --manageResources=false --monitorJobs=true \
    --sparkMaster=local \
    -Dspark.ui.enabled=false -DSPARK_LOCAL_IP=localhost -Dsun.io.serialization.extendedDebugInfo=true

# Current Status

Open issues are tracked [here](https://github.com../../../../../issues):

## Batch / Synthetic / Local

| Query | Direct | Spark                                                        | Flink                                                      | Apex                                                         |
| ----: | ------ | ------------------------------------------------------------ | ---------------------------------------------------------- | ------------------------------------------------------------ |
|     0 | ok     | ok                                                           | ok                                                         | ok                                                           |
|     1 | ok     | ok                                                           | ok                                                         | ok                                                           |
|     2 | ok     | ok                                                           | ok                                                         | ok                                                           |
|     3 | ok     | [BEAM-1035](https://issues.apache.org/jira/browse/BEAM-1035) | ok                                                         | [BEAM-1037](https://issues.apache.org/jira/browse/BEAM-1037) |
|     4 | ok     | ok                                                           | ok                                                         | ok                                                           |
|     5 | ok     | ok                                                           | ok                                                         | ok                                                           |
|     6 | ok     | ok                                                           | ok                                                         | ok                                                           |
|     7 | ok     | ok                                                           | ok                                                         | ok                                                           |
|     8 | ok     | ok                                                           | ok                                                         | ok                                                           |
|     9 | ok     | ok                                                           | ok                                                         | ok                                                           |
|    10 | ok     | ok                                                           | ok                                                         | ok                                                           |
|    11 | ok     | ok                                                           | ok                                                         | ok                                                           |
|    12 | ok     | ok                                                           | ok                                                         | ok                                                           |

## Streaming / Synthetic / Local

| Query | Direct | Spark                                                        | Flink                                                      | Apex                                                         |
| ----: | ------ | ------------------------------------------------------------ | ---------------------------------------------------------- | ------------------------------------------------------------ |
|     0 | ok     | ok                                                           | ok                                                         | ok                                                           |
|     1 | ok     | ok                                                           | ok                                                         | ok                                                           |
|     2 | ok     | ok                                                           | ok                                                         | ok                                                           |
|     3 | ok     | [BEAM-1035](https://issues.apache.org/jira/browse/BEAM-1035) | ok                                                         | [BEAM-1037](https://issues.apache.org/jira/browse/BEAM-1037) |
|     4 | ok     | ok                                                           | ok                                                         | ok                                                           |
|     5 | ok     | ok                                                           | ok                                                         | ok                                                           |
|     6 | ok     | ok                                                           | ok                                                         | ok                                                           |
|     7 | ok     | [BEAM-2112](https://issues.apache.org/jira/browse/BEAM-2112) | ok                                                         | ok                                                           |
|     8 | ok     | ok                                                           | ok                                                         | ok                                                           |
|     9 | ok     | ok                                                           | ok                                                         | ok                                                           |
|    10 | ok     | ok                                                           | ok                                                         | ok                                                           |
|    11 | ok     | ok                                                           | ok                                                         | ok                                                           |
|    12 | ok     | ok                                                           | ok                                                         | ok                                                           |

## Batch / Synthetic / Cluster

TODO

| Query | Dataflow                       | Spark                          | Flink                          | Apex                           |
| ----: | ------------------------------ | ------------------------------ | ------------------------------ | ------------------------------ |
|     0 |                                |                                |                                |                                |

## Streaming / Synthetic / Cluster

TODO

| Query | Dataflow                       | Spark                          | Flink                          | Apex                           |
| ----: | ------------------------------ | ------------------------------ | ------------------------------ | ------------------------------ |
|     0 |                                |                                |                                |                                |

# Running Nexmark

## Running SMOKE suite on the DirectRunner (local)

Batch Mode

    mvn exec:java -Dexec.mainClass=org.apache.beam.sdk.nexmark.Main -Pdirect-runner -Dexec.args="--runner=DirectRunner --suite=SMOKE --streaming=false --manageResources=false --monitorJobs=true --enforceEncodability=true --enforceImmutability=true"

Streaming Mode

    mvn exec:java -Dexec.mainClass=org.apache.beam.sdk.nexmark.Main -Pdirect-runner -Dexec.args="--runner=DirectRunner --suite=SMOKE --streaming=true --manageResources=false --monitorJobs=true --enforceEncodability=true --enforceImmutability=true"


## Running SMOKE suite on the SparkRunner (local)

Batch Mode

    mvn exec:java -Dexec.mainClass=org.apache.beam.sdk.nexmark.Main -Pspark-runner "-Dexec.args=--runner=SparkRunner --suite=SMOKE --streamTimeout=60 --streaming=false --manageResources=false --monitorJobs=true"

Streaming Mode

    mvn exec:java -Dexec.mainClass=org.apache.beam.sdk.nexmark.Main -Pspark-runner "-Dexec.args=--runner=SparkRunner --suite=SMOKE --streamTimeout=60 --streaming=true --manageResources=false --monitorJobs=true"


## Running SMOKE suite on the FlinkRunner (local)

Batch Mode

    mvn exec:java -Dexec.mainClass=org.apache.beam.sdk.nexmark.Main -Pflink-runner "-Dexec.args=--runner=FlinkRunner --suite=SMOKE --streamTimeout=60 --streaming=false --manageResources=false --monitorJobs=true  --flinkMaster=local"

Streaming Mode

    mvn exec:java -Dexec.mainClass=org.apache.beam.sdk.nexmark.Main -Pflink-runner "-Dexec.args=--runner=FlinkRunner --suite=SMOKE --streamTimeout=60 --streaming=true --manageResources=false --monitorJobs=true  --flinkMaster=local"


## Running SMOKE suite on the ApexRunner (local)

Batch Mode

    mvn exec:java -Dexec.mainClass=org.apache.beam.sdk.nexmark.Main -Papex-runner "-Dexec.args=--runner=ApexRunner --suite=SMOKE --streamTimeout=60 --streaming=false --manageResources=false --monitorJobs=false"

Streaming Mode

    mvn exec:java -Dexec.mainClass=org.apache.beam.sdk.nexmark.Main -Papex-runner "-Dexec.args=--runner=ApexRunner --suite=SMOKE --streamTimeout=60 --streaming=true --manageResources=false --monitorJobs=false"


## Running SMOKE suite on Google Cloud Dataflow

Building package

    mvn clean package -Pdataflow-runner

Submit to Google Dataflow service


```
java -cp sdks/java/nexmark/target/beam-sdks-java-nexmark-bundled-2.1.0-SNAPSHOT.jar \
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
java -cp sdks/java/nexmark/target/beam-sdks-java-nexmark-bundled-2.1.0-SNAPSHOT.jar \
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

## Running query 0 on a Spark cluster with yarn

Building package

    mvn clean package -Pspark-runner

Submit to the cluster

    spark-submit --master yarn-client --class org.apache.beam.sdk.nexmark.Main --driver-memory 512m --executor-memory 512m --executor-cores 1 beam-sdks-java-nexmark-bundled-2.1.0-SNAPSHOT.jar --runner=SparkRunner --query=0 --streamTimeout=60 --streaming=false --manageResources=false --monitorJobs=true

