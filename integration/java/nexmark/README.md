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

# NEXMark integration suite

This is a suite of pipelines inspired by the 'continuous data stream'
queries in [http://datalab.cs.pdx.edu/niagaraST/NEXMark/]
(http://datalab.cs.pdx.edu/niagaraST/NEXMark/).

These are multiple queries over a three entities model representing on online auction system:

 - **Person** represents a person submitting an item for auction and/or making a bid
    on an auction.
 - **Auction** represents an item under auction.
 - **Bid** represents a bid for an item under auction.

The queries exercise many aspects of dataflow model on Beam:

* **Query1**: What are the bid values in Euro's?
  Illustrates a simple map.
* **Query2**: What are the auctions with particular auction numbers?
  Illustrates a simple filter.
* **Query3**: Who is selling in particular US states?
  Illustrates an incremental join (using per-key state) and filter.
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

The queries can be executed using a 'Driver' for a given backend.
Currently the supported drivers are:

* **NexmarkDirectDriver** for running locally on a single machine.
* **NexmarkGoogleDriver** for running on the Google Cloud Dataflow
  service. Requires a Google Cloud account.
* **NexmarkFlinkDriver** for running on a Flink cluster. Requires the
  cluster to be established and the Nexmark jar to be distributed to
  each worker.
* **NexmarkSparkDriver** for running on a Spark cluster.
  
Other drivers are straightforward.

Test data is deterministically synthesized on demand. The test
data may be synthesized in the same pipeline as the query itself,
or may be published to Pubsub.

The query results may be:

* Published to Pubsub.
* Written to text files as plain text.
* Written to text files using an Avro encoding.
* Send to BigQuery.
* Discarded.

Options are provided for measuring progress, measuring overall
pipeline performance, and comparing that performance against a known
baseline. However that machinery has only been implemented against
the Google Cloud Dataflow driver.

# Configuration

Common configuration parameters:

Available Suites:

- DEFAULT: Test default configuration with query 0.
- SMOKE: Run the 12 default configurations.
- STRESS: Like smoke but for 1m events.
- FULL_THROTTLE: Like SMOKE but 100m events.

        --suite=SMOKE

Decide if batch or streaming:

    --streaming=true

Number of events generators

    --numEventGenerators=4

## Apex specific configuration

--suite=SMOKE --manageResources=false --monitorJobs=true

## Direct specific configuration

--suite=SMOKE --manageResources=false --monitorJobs=true \
--enforceEncodability=false --enforceImmutability=false

## Flink specific configuration

--suite=SMOKE --manageResources=false --monitorJobs=true \
--flinkMaster=local

## Spark specific configuration

--suite=SMOKE --manageResources=false --monitorJobs=true --sparkMaster=local \
-Dspark.ui.enabled=false -DSPARK_LOCAL_IP=localhost -Dsun.io.serialization.extendedDebugInfo=true

# Current Status

Open issues are tracked [here](https://github.com../../../../../issues):

## Batch / Synthetic / Local

| Query | Direct                         | Spark                          | Flink                          | Apex                            |
| ----: | ------------------------------ | ------------------------------ | ------------------------------ | ------------------------------- |
|     0 | ok                             | [#1](../../../../../issues/1)  | ok                             | ok                              |
|     1 | ok                             | [#1](../../../../../issues/1)  | ok                             | ok                              |
|     2 | ok                             | [#1](../../../../../issues/1)  | ok                             | ok                              |
|     3 | [#7](../../../../../issues/7)  | [#7](../../../../../issues/7)  | [#7](../../../../../issues/7)  | [#7](../../../../../issues/7)   |
|     4 | ok                             | ok                             | [#2](../../../../../issues/2)  | ok                              |
|     5 | ok                             | [#3](../../../../../issues/3)  | ok                             | ok                              |
|     6 | ok                             | ok                             | [#2](../../../../../issues/2)  | ok                              |
|     7 | ok                             | [#1](../../../../../issues/1)  | ok                             | [#24](../../../../../issues/24) |
|     8 | ok                             | [#1](../../../../../issues/1)  | ok                             | ok                              |
|     9 | ok                             | ok                             | [#2](../../../../../issues/2)  | ok                              |
|    10 | [#5](../../../../../issues/5)  | [#4](../../../../../issues/4)  | ok                             | ok                              |
|    11 | ok                             | [#1](../../../../../issues/1)  | ok                             | ok                              |
|    12 | ok                             | [#1](../../../../../issues/1)  | ok                             | ok                              |

## Streaming / Synthetic / Local

| Query | Direct                         | Spark                          | Flink                          | Apex                           |
| ----: | ------------------------------ | ------------------------------ | ------------------------------ | ------------------------------ |
|     0 | ok                             |                                |                                | ok                             |
|     1 | ok                             |                                |                                | ok                             |
|     2 | ok                             |                                |                                | ok                             |
|     3 | [#7](../../../../../issues/7)  |                                |                                | [#7](../../../../../issues/7)  |
|     4 | ok                             |                                |                                | ok                             |
|     5 | ok                             |                                |                                | ok                             |
|     6 | ok                             |                                |                                | ok                             |
|     7 | ok                             |                                |                                | ?                              |
|     8 | ok                             |                                |                                | ok                             |
|     9 | ok                             |                                |                                | ok                             |
|    10 | [#5](../../../../../issues/5)  |                                |                                | ?                              |
|    11 | ok                             |                                |                                | Ok                             |
|    12 | ok                             |                                |                                | Ok                             |

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

## Running on the DirectRunner (local)

Batch Mode

-Dexec.classpathScope="test"

    mvn exec:java -Dexec.mainClass=org.apache.beam.integration.nexmark.NexmarkDirectDriver -Dexec.args="--suite=SMOKE --streaming=false --numEventGenerators=4 --manageResources=false --monitorJobs=false --enforceEncodability=false --enforceImmutability=false"

Streaming Mode

    mvn exec:java -Dexec.mainClass=org.apache.beam.integration.nexmark.NexmarkDirectDriver -Dexec.args="--suite=SMOKE --streaming=true --numEventGenerators=4 --manageResources=false --monitorJobs=false --enforceEncodability=false --enforceImmutability=false"

## Running on Google Cloud Dataflow

An example invocation for **Query10** on the Google Cloud Dataflow
service.

```
java -cp integration/java/target/java-integration-all-bundled-0.2.0-incubating-SNAPSHOT.jar \
  org.apache.beam.integration.nexmark.NexmarkGoogleDriver \
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
  --query=10 \
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
java -cp integration/java/target/java-integration-all-bundled-0.2.0-incubating-SNAPSHOT.jar \
  org.apache.beam.integration.nexmark.NexmarkGoogleDriver \
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
  --query=10 \
  --usePubsubPublishTime=true \
  --outputPath=<a gs path under which log files will be written> \
  --windowSizeSec=600 \
  --occasionalDelaySec=3600 \
  --maxLogEvents=10000 \
  --experiments=enable_custom_pubsub_source
```

## Running on Flink

See [BEAM_ON_FLINK_ON_GCP](./BEAM_ON_FLINK_ON_GCP.md) for instructions
on running a NexMark pipeline using Flink hosted on a Google Compute
Platform cluster.
