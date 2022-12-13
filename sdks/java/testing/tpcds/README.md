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

# TPC-DS Benchmark

## Google Dataflow Runner

To execute TPC-DS benchmark for 1Gb dataset on Google Dataflow, run the following example command from the command line:

```bash
./gradlew :sdks:java:testing:tpcds:run -Ptpcds.args="--dataSize=1G \
  --runner=DataflowRunner \
  --queries=3,26,55 \
  --tpcParallel=2 \
  --dataDirectory=/path/to/tpcds_data/ \
  --project=apache-beam-testing \
  --stagingLocation=gs://beamsql_tpcds_1/staging \
  --tempLocation=gs://beamsql_tpcds_2/temp \
  --dataDirectory=/path/to/tpcds_data/ \
  --region=us-west1 \
  --maxNumWorkers=10"
```

To run a query using ZetaSQL planner (currently Query96 can be run using ZetaSQL), set the plannerName as below. If not specified, the default planner is Calcite.

```bash
./gradlew :sdks:java:testing:tpcds:run -Ptpcds.args="--dataSize=1G \
  --runner=DataflowRunner \
  --queries=96 \
  --tpcParallel=2 \
  --dataDirectory=/path/to/tpcds_data/ \
  --plannerName=org.apache.beam.sdk.extensions.sql.zetasql.ZetaSQLQueryPlanner \
  --project=apache-beam-testing \
  --stagingLocation=gs://beamsql_tpcds_1/staging \
  --tempLocation=gs://beamsql_tpcds_2/temp \
  --region=us-west1 \
  --maxNumWorkers=10"
```

## Spark Runner

To execute TPC-DS benchmark with Query3 for 1Gb dataset on Apache Spark 3.x, run the following example command from the command line:

```bash
./gradlew :sdks:java:testing:tpcds:run -Ptpcds.runner=":runners:spark:3" -Ptpcds.args=" \
  --runner=SparkRunner \
  --queries=3 \
  --tpcParallel=1 \
  --dataDirectory=/path/to/tpcds_data/ \
  --dataSize=1G \
  --resultsDirectory=/path/to/tpcds_results/"
```
