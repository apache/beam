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

# Example Pipelines for Beam SQL and Schema Transforms

The examples included in this module serve to demonstrate the basic
functionality of Apache Beam SQL, and act as starting points for
the development of more complex pipelines.

## SQL transform

An example that leverage the powerful SQL syntax in Beam [SqlTransform](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/extensions/sql/SqlTransform.html) directly in your Beam pipelines.


[`SqlTransformExample`](https://github.com/apache/beam/blob/master/examples/java/sql/src/main/java/org/apache/beam/examples/sql/SqlTransformExample.java) is a simple pipeline that calculates multiple metrics per key : Min, Max and Sum.


## Schema transform

[Beam Schemas](https://beam.apache.org/documentation/programming-guide/#schemas) offer a flexible way of writing in code the same operations that are so easy to express in SQL.

[`SchemaTransformExample`](https://github.com/apache/beam/blob/master/examples/java/sql/src/main/java/org/apache/beam/examples/sql/SchemaTransformExample.java) is a simple pipeline that calculates multiple metrics per key : Min, Max and Sum.

## Running Examples

See [Apache Beam WordCount Example](https://beam.apache.org/get-started/wordcount-example/) for information on running these examples.

Gradle cmd line can be similar to:

`./gradlew clean :examples:java:sql:execute  --args="--runner=DirectRunner" -Pdirect-runner -PmainClass=org.apache.beam.examples.sql.SqlTransformExample`

## Beyond SQL and Schemas

Both SQL and Schema Transforms leverage Row type.
The same results can be achieved directly using Beam transforms on a KV input PCollection. See [Composed Cobiners example](https://github.com/apache/beam/blob/master/examples/java/src/main/java/org/apache/beam/examples/CoCombineTransformExample.java).
