---
type: languages
title: "TPC-DS benchmark suite"
aliases: /documentation/sdks/java/tpcds/
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
# TPC Benchmark™ DS (TPC-DS) benchmark suite

## What it is
From TPC-DS [specification document](https://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp):
> "TPC-DS is a decision support benchmark that models several generally applicable aspects of a decision support system,
> including queries and data maintenance. The benchmark provides a representative evaluation of performance as a general
> purpose decision support system."

In general, TPC-DS is:
- Industry standard benchmark (OLAP/Data Warehouse);
  - https://www.tpc.org/tpcds/
- Implemented for many analytical processing systems - RDBMS, Apache Spark, Apache Flink, etc;
- It provides a wide range of different queries (SQL);
- It incorporates the tools to generate input data of different sizes.

## Table schemas
### Schema Overview
From TPC-DS [specification document](https://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp):
> The TPC-DS schema models the sales and sales returns process for an organization that employs three primary
sales channels: stores, catalogs, and the Internet. The schema includes seven fact tables:
>
> - A pair of fact tables focused on the product sales and returns for each of the three channels
> - A single fact table that models inventory for the catalog and internet sales channels.
>
> In addition, the schema includes 17 dimension tables that are associated with all sales channels.

### Tables
- **store_sales** - Every row represents a single lineitem for a sale made through the store channel and recorded in the store_sales fact table
- **store_returns** - Every row represents a single lineitem for the return of an item sold through the store channel and recorded in the store_returns fact table.
- **catalog_sales** - Every row represents a single lineitem for a sale made through the catalog channel and recorded in the catalog_sales fact table.
- **catalog_returns** - Every row represents a single lineitem for the return of an item sold through the catalog channel and recorded in the catalog_returns table.
- **web_sales** - Every row represents a single lineitem for a sale made through the web channel and recorded in the web_sales fact table.
- **web_returns** - Every row represents a single lineitem for the return of an item sold through the web sales channel and recorded in the web_returns table.
- **inventory** - Every row represents the quantity of a particular item on-hand at a given warehouse during a specific week.
- **store** - Every row represents details of a store.
- **call_center** - Every row represents details of a call center.
- **catalog_page** - Every row represents details of a catalog page.
- **web_site** - Every row represents details of a web site.
- **web_page** - Every row represents details of a web page within a web site.
- **warehouse** - Every row represents a warehouse where items are stocked.
- **customer** - Every row represents a customer.
- **customer_address** - Every row represents a unique customer address (each customer can have more than one address).
- **customer_demographics** - The customer demographics table contains one row for each unique combination of customer demographic information.
- **date_dim** - Every row represents one calendar day. The surrogate key (d_date_sk) for a given row is derived from the julian date being described by the row.
- **household_demographics** - Every row defines a household demographic profile.
- **item** - Every row represents a unique product formulation (e.g., size, color, manufacturer, etc.).
- **income_band** - Every row represents details of an income range.
- **promotion** - Every row represents details of a specific product promotion (e.g., advertising, sales, PR).
- **reason** - Every row represents a reason why an item was returned.
- **ship_mode** - Every row represents a shipping mode.
- **time_dim** - Every row represents one second.

## The queries

TPC-DS benchmark contains 99 distinct SQL-99 queries (including OLAP extensions). Each query answers a business
question, which illustrates the business context in which the query could be used.

All queries are “templated” with random input parameters and used to compare completeness and performance of SQL implementations.

## Input data
Input data source:

- Input files (CSV) are generated with CLI tool `dsdgen`
- Input datasets can be generated for different scale factor sizes:
  - 1GB / 10GB / 100GB / 1000GB
- The tool constrains the minimum amount of data to be generated to 1GB

## TPC-DS extension in Beam

Beam provides a [simplified implementation](https://github.com/apache/beam/tree/master/sdks/java/testing/tpcds) of TPC-DS benchmark.

### Reasons

There are several reasons to have TPC-DS benchmarks in Beam:

- Compare the performance of Beam SQL against native SQL implementations for different runners;
- Exercise Beam SQL on different runtime environments;
- Identify missing or incorrect Beam SQL features;
- Identify performance issues in Beam and Beam SQL.

### Queries
All TPC-DS queries in Beam are pre-generated and stored in the provided artifacts.

For the moment, 28 out of 103 SQL queries (99 + 4) successfully pass by running with Beam SQL transform since not all
SQL-99 operations are supported.

Currently (as of Beam 2.40.0 release) supported queries are:
 - 3, 7, 10, 22, 25, 26, 29, 35, 38, 40, 42, 43, 50, 52, 55, 69, 78, 79, 83, 84, 87, 93, 96, 97, 99

### Tables
All TPC-DS table schemas are stored in the provided artifacts.

### Input data
CSV and Parquet input data has been pre-generated and staged in the Google Cloud Storage bucket `gs://beam-tpcds`.

#### CSV datasets
Staged in `gs://beam-tpcds/datasets/text/*` bucket, spread by different data scale factors.

#### Parquet datasets
Staged in `gs://beam-tpcds/datasets/parquet/nonpartitioned/` and `gs://beam-tpcds/datasets/parquet/partitioned/`, spreaded by different data scale factors.

For `partitioned` version, some large tables has been pre-partitioned by a date column into several files in the bucket.

### Runtime
TPC-DS extension for Beam can only be run in **Batch** mode and supports these runners for the moment (not tested with other runners):
- Spark Runner
- Flink Runner
- Dataflow Runner

## Benchmark launch configuration

The TPC-DS launcher accepts the `--runner` argument as usual for programs that
use Beam PipelineOptions to manage their command line arguments. In addition
to this, the necessary dependencies must be configured.

When running via Gradle, the following two parameters control the execution:

    -P tpcds.args
        The command line to pass to the TPC-DS main program.

    -P tpcds.runner
	The Gradle project name of the runner, such as ":runners:spark:3" or
	":runners:flink:1.17. The project names can be found in the root
        `settings.gradle.kts`.

Test data has to be generated before running a suite and stored to accessible file system. The query results will be written into output files.

### Common configuration parameters

Scale factor size of input dataset (1GB / 10GB / 100GB / 1000GB):

    --dataSize=<1GB|10GB|100GB|1000GB>

Path to input datasets directory:

    --dataDirectory=<path to dir>

Path to results directory:

    --resultsDirectory=<path to dir>

Format of input files:

    --sourceType=<CSV|PARQUET>

Select queries to run (comma separated list of query numbers or `all` for all queries):

    --queries=<1,2,...N|all>

Number of queries **N** to run in parallel:

    --tpcParallel=N

## Running TPC-DS

Here are some examples demonstrating how to run TPC-DS benchmarks on different runners.

Running suite on the SparkRunner (local) with Query3 against 1Gb dataset in Parquet format:

    ./gradlew :sdks:java:testing:tpcds:run \
        -Ptpcds.runner=":runners:spark:3" \
        -Ptpcds.args="
            --runner=SparkRunner
            --dataSize=1GB
            --sourceType=PARQUET
            --dataDirectory=gs://beam-tpcds/datasets/parquet/partitioned
            --resultsDirectory=/tmp/beam-tpcds/results/spark/
            --tpcParallel=1
            --queries=3"

Running suite on the FlinkRunner (local) with Query7 and Query10 in parallel against 10Gb dataset in CSV format:

    ./gradlew :sdks:java:testing:tpcds:run \
        -Ptpcds.runner=":runners:flink:1.13" \
        -Ptpcds.args="
            --runner=FlinkRunner
            --parallelism=2
            --dataSize=10GB
            --sourceType=CSV
            --dataDirectory=gs://beam-tpcds/datasets/csv
            --resultsDirectory=/tmp/beam-tpcds/results/flink/
            --tpcParallel=2
            --queries=7,10"

Running suite on the DataflowRunner with all queries against 100GB dataset in PARQUET format:

    ./gradlew :sdks:java:testing:tpcds:run \
        -Ptpcds.runner=":runners:google-cloud-dataflow-java" \
        -Ptpcds.args="
            --runner=DataflowRunner
            --region=<region_name>
            --project=<project_name>
            --numWorkers=4
            --maxNumWorkers=4
            --autoscalingAlgorithm=NONE
            --dataSize=100GB
            --sourceType=PARQUET
            --dataDirectory=gs://beam-tpcds/datasets/parquet/partitioned
            --resultsDirectory=/tmp/beam-tpcds/results/dataflow/
            --tpcParallel=4
            --queries=all"

[//]: # (## TPC-DS dashboards)
[//]: # (Work in progress.)

