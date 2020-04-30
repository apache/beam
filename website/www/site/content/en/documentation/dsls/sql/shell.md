---
type: languages
title: "Beam SQL: Shell"
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

# Beam SQL Shell

## Overview

Starting with version 2.6.0, Beam SQL includes an interactive shell, called the Beam SQL shell. The shell allows you to write pipelines as SQL queries without needing the Java SDK. By default, Beam uses the `DirectRunner` to execute the queries as Beam pipelines.

This page describes how to work with the shell, but does not focus on specific features of Beam SQL. For a more thorough overview of the features used in the examples on this page, see the corresponding sections in the [Beam SQL documentation](/documentation/dsls/sql/overview/).

## Quickstart

To use Beam SQL shell, you must first clone the [Beam SDK repository](https://github.com/apache/beam). Then, from the root of the repository clone, execute the following commands to run the shell:

```
./gradlew -p sdks/java/extensions/sql/shell -Pbeam.sql.shell.bundled=':runners:flink:1.10,:sdks:java:io:kafka' installDist

./sdks/java/extensions/sql/shell/build/install/shell/bin/shell
```

After you run the commands,  the SQL shell starts and you can type queries:

```
Welcome to Beam SQL 2.6.0-SNAPSHOT (based on sqlline version 1.4.0)
0: BeamSQL> 
```

_Note: If you haven't built the project before running the Gradle command, the command will take a few minutes as Gradle must build all dependencies first._

The shell converts the queries into Beam pipelines, runs them using `DirectRunner`, and returns the results as tables when the pipelines finish:

```
0: BeamSQL> SELECT 'foo' AS NAME, 'bar' AS TYPE, 'num' AS NUMBER;
+------+------+--------+
| NAME | TYPE | NUMBER |
+------+------+--------+
| foo  | bar  | num    |
+------+------+--------+
1 row selected (0.826 seconds)
```

## Declaring Tables

Before reading data from a source or writing data to a destination, you must declare a virtual table using the `CREATE EXTERNAL TABLE` statement. For example, if you have a local CSV file `"test-file.csv"` in the current folder, you can create a table with the following statement:

```
0: BeamSQL> CREATE EXTERNAL TABLE csv_file (field1 VARCHAR, field2 INTEGER) TYPE text LOCATION 'test-file.csv';

No rows affected (0.042 seconds)
```

The `CREATE EXTERNAL TABLE` statement registers the CSV file as a table in Beam SQL and specifies the table's schema. This statement does not directly create a persistent physical table; it only describes the source/sink to Beam SQL so that you can use the table in the queries that read data and write data.

_For more information about `CREATE EXTERNAL TABLE` syntax and supported table types, see the [CREATE EXTERNAL TABLE reference page](/documentation/dsls/sql/create-external-table/)._

## Reading and Writing Data

To read data from the local CSV file that you declared in the previous section, execute the following query:

```
0: BeamSQL> SELECT field1 AS field FROM csv_file;
+--------+
| field  |
+--------+
| baz    |
| foo    |
| bar    |
| bar    |
| foo    |
+--------+
```

_For more information about `SELECT` syntax, see the [Query syntax page](/documentation/dsls/sql/calcite/query-syntax/)._

To write data to the CSV file, use the `INSERT INTO … SELECT ...` statement:

```
0: BeamSQL> INSERT INTO csv_file SELECT 'foo', 'bar';
```
Read and write behavior depends on the type of the table. For example:

*   The table type `text` is implemented using `TextIO`, so writing to a `text` table can produce multiple numbered files. 
*   The table type `pubsub` is an unbounded source, so reading from a `pubsub` table never completes.

## Developing with unbounded Sources

When you want to inspect the data from an unbounded source during development, you must specify the `LIMIT x` clause at the end of the `SELECT` statement to limit the output to `x` number of records. Otherwise, the pipeline will never finish.

```
0: BeamSQL> SELECT field1 FROM unbounded_source LIMIT 10 ;
```

The example queries shown so far are fast queries that execute locally. These queries are helpful when you are investigating the data and iteratively designing the pipeline. Ideally, you want the queries to finish quickly and return output when complete. 

When you're satisfied with the logic of your SQL statements, you can submit the statements as long-running jobs by dropping the `LIMIT x` statement. Then, the pipeline can potentially run forever if one of the tables represents an unbounded source.

## Specifying the Runner

By default, Beam uses the `DirectRunner` to run the pipeline on the machine where you're executing the commands. If you want to run the pipeline with a different runner, you must perform two steps:

1.  Make sure the SQL shell includes the desired runner. Add the corresponding project id to the `-Pbeam.sql.shell.bundled` parameter of the Gradle invocation ([source code](https://github.com/apache/beam/blob/master/sdks/java/extensions/sql/shell/build.gradle), [project ids](https://github.com/apache/beam/blob/master/settings.gradle)). For example, use the following command to include Flink runner and KafkaIO:

    ```
    ./gradlew -p sdks/java/extensions/sql/shell -Pbeam.sql.shell.bundled=':runners:flink:1.10,:sdks:java:io:kafka' installDist
    ```

    _Note: You can bundle multiple runners (using a comma-separated list) or other additional components in the same manner. For example, you can add support for more I/Os._

1.  Then, specify the runner using the `SET` command ([reference page](/documentation/dsls/sql/set/)):

    ```
    0: BeamSQL> SET runner='FlinkRunner';
    ```

Beam will submit all future `INSERT` statements as pipelines to the specified runner. In this case, the Beam SQL shell does not display the query results. You must manage the submitted jobs through the corresponding runner's UI (for example, using the Flink UI or command line).

## Specifying the PipelineOptions

To configure the runner, you must specify `PipelineOptions` by using the `SET` command ([details](/documentation/dsls/sql/set/)):

```
0: BeamSQL> SET projectId='gcpProjectId';
0: BeamSQL> SET tempLocation='/tmp/tempDir';
```
## Packaging the SQL Shell

You can also build your own standalone package for SQL shell using `distZip` or `distTar` tasks. For example:

```
./gradlew -p sdks/java/extensions/sql/shell -Pbeam.sql.shell.bundled=':runners:flink:1.10,:sdks:java:io:kafka' distZip

ls ./sdks/java/extensions/sql/shell/build/distributions/
beam-sdks-java-extensions-sql-shell-2.6.0-SNAPSHOT.tar beam-sdks-java-extensions-sql-shell-2.6.0-SNAPSHOT.zip
```

