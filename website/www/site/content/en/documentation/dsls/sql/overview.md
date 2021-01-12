---
type: languages
title: "Beam SQL: Overview"
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

# Beam SQL overview

Beam SQL allows a Beam user (currently only available in Beam Java) to query
bounded and unbounded `PCollections` with SQL statements. Your SQL query
is translated to a `PTransform`, an encapsulated segment of a Beam pipeline.
You can freely mix SQL `PTransforms` and other `PTransforms` in your pipeline.

Beam SQL includes the following dialects:

- [Beam Calcite SQL](https://calcite.apache.org)
- [Beam ZetaSQL](https://github.com/google/zetasql)

Beam Calcite SQL is a variant of Apache Calcite, a dialect widespread in
big data processing. Beam Calcite SQL is the default Beam SQL dialect. Beam ZetaSQL is more compatible with BigQuery, so it's especially useful in pipelines that [write to or read from BigQuery tables](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/gcp/bigquery/BigQueryIO.html).

To change dialects, pass [the dialect's full package name](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/extensions/sql/package-summary.html) to the [`setPlannerName`](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/extensions/sql/impl/BeamSqlPipelineOptions.html#setPlannerName-java.lang.String-) method in the [`PipelineOptions`](https://beam.apache.org/releases/javadoc/2.15.0/org/apache/beam/sdk/options/PipelineOptions.html) interface.

There are two additional concepts you need to know to use SQL in your pipeline:

 - [SqlTransform](https://beam.apache.org/releases/javadoc/{{< param release_latest >}}/index.html?org/apache/beam/sdk/extensions/sql/SqlTransform.html): the interface for creating `PTransforms` from SQL queries.
 - [Row](https://beam.apache.org/releases/javadoc/{{< param release_latest >}}/index.html?org/apache/beam/sdk/values/Row.html):
   the type of elements that Beam SQL operates on. A `PCollection<Row>` plays the role of a table.

## Walkthrough
The [SQL pipeline walkthrough](/documentation/dsls/sql/walkthrough) works through how to use Beam SQL with example code.

## Shell
The Beam SQL shell allows you to write pipelines as SQL queries without using the Java SDK.
The [Shell page](/documentation/dsls/sql/shell) describes how to work with the interactive Beam SQL shell.

## Apache Calcite dialect
The [Beam Calcite SQL overview](/documentation/dsls/sql/calcite/overview) summarizes Apache Calcite operators,
functions, syntax, and data types supported by Beam Calcite SQL.

## ZetaSQL dialect
For more information on the ZetaSQL features in Beam SQL, see the [Beam ZetaSQL dialect reference](/documentation/dsls/sql/zetasql/overview).

To switch to Beam ZetaSQL, configure the [pipeline options](https://beam.apache.org/releases/javadoc/2.15.0/org/apache/beam/sdk/options/PipelineOptions.html) as follows:
```
PipelineOptions options = ...;
options
    .as(BeamSqlPipelineOptions.class)
    .setPlannerName("org.apache.beam.sdk.extensions.sql.zetasql.ZetaSQLQueryPlanner");
```
Note, Use of the `ZetaSQLQueryPlanner` requires an additional dependency on `beam-sdks-java-extensions-sql-zetasql` in addition to the `beam-sdks-java-extensions-sql` package required for `CalciteQueryPlanner`.

## Beam SQL extensions
Beam SQL has additional extensions leveraging Beam’s unified batch/streaming model and processing complex data types. You can use these extensions with all Beam SQL dialects.
