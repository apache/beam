---
layout: section
title: "Google BigQuery I/O connector"
section_menu: section-menu/documentation.html
permalink: /documentation/io/built-in/google-bigquery/
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

[Built-in I/O Transforms]({{site.baseurl}}/documentation/io/built-in/)

# Google BigQuery I/O connector

<nav class="language-switcher">
  <strong>Adapt for:</strong>
  <ul>
    <li data-type="language-java" class="active">Java SDK</li>
    <li data-type="language-py">Python SDK</li>
  </ul>
</nav>

The Beam SDKs include built-in transforms that can read data from and write data
to [Google BigQuery](https://cloud.google.com/bigquery) tables.

## Before you start

<!-- Java specific -->

{:.language-java}
To use BigQueryIO, add the Maven artifact dependency to your `pom.xml` file.

```java
<dependency>
    <groupId>org.apache.beam</groupId>
    <artifactId>beam-sdks-java-io-google-cloud-platform</artifactId>
    <version>{{ site.release_latest }}</version>
</dependency>
```

{:.language-java}
Additional resources:

{:.language-java}
* [BigQueryIO source code](https://github.com/apache/beam/tree/master/sdks/java/io/google-cloud-platform/src/main/java/org/apache/beam/sdk/io/gcp/bigquery)
* [BigQueryIO Javadoc](https://beam.apache.org/releases/javadoc/{{ site.release_latest }}/org/apache/beam/sdk/io/gcp/bigquery/BigQueryIO.html)
* [Google BigQuery documentation](https://cloud.google.com/bigquery/docs)


<!-- Python specific -->

{:.language-py}
To use BigQueryIO, you must install the Google Cloud Platform dependencies by
running `pip install apache-beam[gcp]`.

{:.language-py}
Additional resources:

{:.language-py}
* [BigQueryIO source code](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/io/gcp/bigquery.py)
* [BigQueryIO Pydoc](https://beam.apache.org/releases/pydoc/{{ site.release_latest }}/apache_beam.io.gcp.bigquery.html)
* [Google BigQuery documentation](https://cloud.google.com/bigquery/docs)


## BigQuery basics

### Table names

To read or write from a BigQuery table, you must provide a fully-qualified
BigQuery table name (for example, `bigquery-public-data:github_repos.sample_contents`).
A fully-qualified BigQuery table name consists of three parts:

 * **Project ID**: The ID for your Google Cloud Project. The default value comes
   from your pipeline options object.
 * **Dataset ID**: The BigQuery dataset ID, which is unique within a given Cloud
   Project.
 * **Table ID**: A BigQuery table ID, which is unique within a given dataset.

A table name can also include a [table decorator](https://cloud.google.com/bigquery/table-decorators)
if you are using [time-partitioned tables](#using-time-partitioning).

To specify a BigQuery table, you can use either the table's fully-qualified name as
a string, or use a
<span class="language-java">
  [TableReference](https://developers.google.com/resources/api-libraries/documentation/bigquery/v2/java/latest/index.html?com/google/api/services/bigquery/model/TableReference.html)
</span>
<span class="language-py">
  [TableReference](https://github.com/googleapis/google-cloud-python/blob/master/bigquery/google/cloud/bigquery/table.py#L153)
</span>
object.

#### Using a string

To specify a table with a string, use the format
`[project_id]:[dataset_id].[table_id]` to specify the fully-qualified BigQuery
table name.

```java
{% github_sample /apache/beam/blob/master/examples/java/src/main/java/org/apache/beam/examples/snippets/Snippets.java tag:BigQueryTableSpec
%}```
```py
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/snippets.py tag:model_bigqueryio_table_spec
%}```

You can also omit `project_id` and use the `[dataset_id].[table_id]` format. If
you omit the project ID, Beam uses the default project ID from your
<span class="language-java">
  [pipeline options](https://beam.apache.org/releases/javadoc/{{ site.release_latest }}/org/apache/beam/sdk/extensions/gcp/options/GcpOptions.html).
</span>
<span class="language-py">
  [pipeline options](https://beam.apache.org/releases/pydoc/{{ site.release_latest }}/apache_beam.options.pipeline_options.html#apache_beam.options.pipeline_options.GoogleCloudOptions).
</span>

```java
{% github_sample /apache/beam/blob/master/examples/java/src/main/java/org/apache/beam/examples/snippets/Snippets.java tag:BigQueryTableSpecWithoutProject
%}```
```py
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/snippets.py tag:model_bigqueryio_table_spec_without_project
%}```

#### Using a TableReference

To specify a table with a `TableReference`, create a new `TableReference` using
the three parts of the BigQuery table name.

```java
{% github_sample /apache/beam/blob/master/examples/java/src/main/java/org/apache/beam/examples/snippets/Snippets.java tag:BigQueryTableSpecObject
%}```
```py
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/snippets.py tag:model_bigqueryio_table_spec_object
%}```

<!-- Java specific -->

{:.language-java}
The Beam SDK for Java also provides the [`parseTableSpec`](https://beam.apache.org/releases/javadoc/{{ site.release_latest }}/org/apache/beam/sdk/io/gcp/bigquery/BigQueryHelpers.html)
helper method, which constructs a `TableReference` object from a String that
contains the fully-qualified BigQuery table name. However, the static factory
methods for BigQueryIO transforms accept the table name as a String and
construct a `TableReference` object for you.


### Table rows

BigQueryIO read and write transforms produce and consume data as a `PCollection`
of dictionaries, where each element in the `PCollection` represents a single row
in the table.

### Schemas

When writing to BigQuery, you must supply a table schema for the destination
table that you want to write to, unless you specify a [create
disposition](#create-disposition) of `CREATE_NEVER`. [Creating a table
schema](#creating-a-table-schema) covers schemas in more detail.

### Data types

BigQuery supports the following data types: STRING, BYTES, INTEGER, FLOAT,
NUMERIC, BOOLEAN, TIMESTAMP, DATE, TIME, DATETIME and GEOGRAPHY.
All possible values are described at [https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types).
BigQueryIO allows you to use all of these data types. The following example
shows the correct format for data types used when reading from and writing to
BigQuery:

```java
{% github_sample /apache/beam/blob/master/examples/java/src/main/java/org/apache/beam/examples/snippets/Snippets.java tag:BigQueryDataTypes
%}```
```py
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/snippets.py tag:model_bigqueryio_data_types
%}```

<!-- Java specific -->

{:.language-java}
As of Beam 2.7.0, the NUMERIC data type is supported. This data type supports
high-precision decimal numbers (precision of 38 digits, scale of 9 digits).
The GEOGRAPHY data type works with Well-Known Text (See [https://en.wikipedia.org/wiki/Well-known_text](https://en.wikipedia.org/wiki/Well-known_text)
format for reading and writing to BigQuery.
BigQuery IO requires values of BYTES datatype to be encoded using base64
encoding when writing to BigQuery. When bytes are read from BigQuery they are
returned as base64-encoded strings.

<!-- Python specific -->

{:.language-py}
As of Beam 2.7.0, the NUMERIC data type is supported. This data type supports
high-precision decimal numbers (precision of 38 digits, scale of 9 digits).
The GEOGRAPHY data type works with Well-Known Text (See [https://en.wikipedia.org/wiki/Well-known_text](https://en.wikipedia.org/wiki/Well-known_text)
format for reading and writing to BigQuery.
BigQuery IO requires values of BYTES datatype to be encoded using base64
encoding when writing to BigQuery. When bytes are read from BigQuery they are
returned as base64-encoded bytes.

## Reading from BigQuery

BigQueryIO allows you to read from a BigQuery table, or read the results of an
arbitrary SQL query string. By default, Beam invokes a [BigQuery export
request](https://cloud.google.com/bigquery/docs/exporting-data) when you apply a
BigQueryIO read transform. However, the Beam SDK for Java (version 2.11.0 and
later) adds support for the beta release of the [BigQuery Storage API](https://cloud.google.com/bigquery/docs/reference/storage/)
as an [experimental feature](https://beam.apache.org/releases/javadoc/current/index.html?org/apache/beam/sdk/annotations/Experimental.html).
See [Using the BigQuery Storage API](#storage-api) for more information and a
list of limitations.

> Beam’s use of BigQuery APIs is subject to BigQuery's
> [Quota](https://cloud.google.com/bigquery/quota-policy)
> and [Pricing](https://cloud.google.com/bigquery/pricing) policies.

<!-- Java specific -->

{:.language-java}
The Beam SDK for Java has two BigQueryIO read methods. Both of these methods
allow you to read from a table, or read fields using a query string.

{:.language-java}
1. `read(SerializableFunction)` reads Avro-formatted records and uses a
   specified parsing function to parse them into a `PCollection` of custom typed
   objects. Each element in the `PCollection` represents a single row in the
   table. The [example code](#reading-with-a-query-string) for reading with a
   query string shows how to use `read(SerializableFunction)`.

2. `readTableRows` returns a `PCollection` of BigQuery `TableRow`
   objects.  Each element in the `PCollection` represents a single row in the
   table. Integer values in the `TableRow` objects are encoded as strings to
   match BigQuery's exported JSON format. This method is convenient, but can be
   2-3 times slower in performance compared to `read(SerializableFunction)`. The
   [example code](#reading-from-a-table) for reading from a table shows how to
   use `readTableRows`.

{:.language-java}
***Note:*** `BigQueryIO.read()` is deprecated as of Beam SDK 2.2.0. Instead, use
`read(SerializableFunction<SchemaAndRecord, T>)` to parse BigQuery rows from
Avro `GenericRecord` into your custom type, or use `readTableRows()` to parse
them into JSON `TableRow` objects.

<!-- Python specific -->

{:.language-py}
To read from a BigQuery table using the Beam SDK for Python, apply a `Read`
transform on a `BigQuerySource`. Read returns a `PCollection` of dictionaries,
where each element in the `PCollection` represents a single row in the table.
Integer values in the `TableRow` objects are encoded as strings to match
BigQuery's exported JSON format.


### Reading from a table

{:.language-java}
To read an entire BigQuery table, use the `from` method with a BigQuery table
name. This example uses `readTableRows`.

{:.language-py}
To read an entire BigQuery table, use the `table` parameter with the BigQuery
table name.

The following code reads an entire table that contains weather station data and
then extracts the `max_temperature` column.

```java
{% github_sample /apache/beam/blob/master/examples/java/src/main/java/org/apache/beam/examples/snippets/Snippets.java tag:BigQueryReadTable
%}```
```py
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/snippets.py tag:model_bigqueryio_read_table
%}```


### Reading with a query string

{:.language-java}
If you don't want to read an entire table, you can supply a query string with
the `fromQuery` method. This example uses
`read(SerializableFunction)`.

{:.language-py}
If you don't want to read an entire table, you can supply a query string to
`BigQuerySource` by specifying the `query` parameter.

The following code uses a SQL query to only read the `max_temperature` column.

```java
{% github_sample /apache/beam/blob/master/examples/java/src/main/java/org/apache/beam/examples/snippets/Snippets.java tag:BigQueryReadQuery
%}```
```py
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/snippets.py tag:model_bigqueryio_read_query
%}```

You can also use BigQuery's standard SQL dialect with a query string, as shown
in the following example:

```java
{% github_sample /apache/beam/blob/master/examples/java/src/main/java/org/apache/beam/examples/snippets/Snippets.java tag:BigQueryReadQueryStdSQL
%}```
```py
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/snippets.py tag:model_bigqueryio_read_query_std_sql
%}```

### Using the BigQuery Storage API {#storage-api}

The [BigQuery Storage API](https://cloud.google.com/bigquery/docs/reference/storage/)
allows you to directly access tables in BigQuery storage. As a result, your
pipeline can read from BigQuery storage faster than previously possible.

The Beam SDK for Java (version 2.11.0 and later) adds support for the beta
release of the BigQuery Storage API as an [experimental feature](https://beam.apache.org/releases/javadoc/current/index.html?org/apache/beam/sdk/annotations/Experimental.html).
Beam's support for the BigQuery Storage API has the following limitations:

* The SDK for Python does not support the BigQuery Storage API.
* SDK versions 2.11.0 and 2.12.0 do not support reading with a query string; you
  can only read from a table.
* SDK versions before 2.15.0 do not support dynamic work rebalancing. As a
  result, reads might be less efficient in the presence of stragglers.

Because this is currently a Beam experimental feature, export based reads are
recommended for production jobs.

#### Updating your code

Use the following methods when you read from a table:

* Required: Specify [withMethod(Method.DIRECT_READ)](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/gcp/bigquery/BigQueryIO.TypedRead.html#withMethod-org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead.Method-) to use the BigQuery Storage API for the read operation.
* Optional: To use features such as [column projection and column filtering](https://cloud.google.com/bigquery/docs/reference/storage/), you must specify [withSelectedFields](https://beam.apache.org/releases/javadoc/2.17.0/org/apache/beam/sdk/io/gcp/bigquery/BigQueryIO.TypedRead.html#withSelectedFields-java.util.List-) and [withRowRestriction](https://beam.apache.org/releases/javadoc/2.17.0/org/apache/beam/sdk/io/gcp/bigquery/BigQueryIO.TypedRead.html#withRowRestriction-java.lang.String-) respectively.

The following code snippet reads from a table. This example is from the [BigQueryTornadoes
example](https://github.com/apache/beam/blob/master/examples/java/src/main/java/org/apache/beam/examples/cookbook/BigQueryTornadoes.java).
When the example's read method option is set to `DIRECT_READ`, the pipeline uses
the BigQuery Storage API and column projection to read public samples of weather
data from a BigQuery table. You can view the [full source code on
GitHub](https://github.com/apache/beam/blob/master/examples/java/src/main/java/org/apache/beam/examples/cookbook/BigQueryTornadoes.java).

```java
   rowsFromBigQuery =
       p.apply(
            BigQueryIO.readTableRows()
               .from(options.getInput())
               .withMethod(Method.DIRECT_READ)
               .withSelectedFields(Lists.newArrayList("month", "tornado"));
```
```py
# The SDK for Python does not support the BigQuery Storage API.
```

The following code snippet reads with a query string.

```java
// Snippet not yet available (BEAM-7034).
```
```py
# The SDK for Python does not support the BigQuery Storage API.
```

## Writing to BigQuery

BigQueryIO allows you to write to BigQuery tables. If you are using the Beam SDK
for Java, you can also write different rows to different tables.

> BigQueryIO write transforms use APIs that are subject to BigQuery's
> [Quota](https://cloud.google.com/bigquery/quota-policy) and
> [Pricing](https://cloud.google.com/bigquery/pricing) policies.

When you apply a write transform, you must provide the following information
for the destination table(s):

 * The table name.
 * The destination table's create disposition. The create disposition specifies
   whether the destination table must exist or can be created by the write
   operation.
 * The destination table's write disposition. The write disposition specifies
   whether the data you write will replace an existing table, append rows to an
   existing table, or write only to an empty table.

In addition, if your write operation creates a new BigQuery table, you must also
supply a table schema for the destination table.


### Create disposition

The create disposition controls whether or not your BigQuery write operation
should create a table if the destination table does not exist.

<!-- Java specific -->

{:.language-java}
Use `.withCreateDisposition` to specify the create disposition. Valid enum
values are:

{:.language-java}
 * `Write.CreateDisposition.CREATE_IF_NEEDED`: Specifies that the
   write operation should create a new table if one does not exist. If you use
   this value, you must provide a table schema with the `withSchema` method.
   `CREATE_IF_NEEDED` is the default behavior.

 * `Write.CreateDisposition.CREATE_NEVER`: Specifies that a table
   should never be created. If the destination table does not exist, the write
   operation fails.

<!-- Python specific -->

{:.language-py}
Use the `create_disposition` parameter to specify the create disposition. Valid
enum values are:

{:.language-py}
 * `BigQueryDisposition.CREATE_IF_NEEDED`: Specifies that the write operation
   should create a new table if one does not exist. If you use this value, you
   must provide a table schema. `CREATE_IF_NEEDED` is the default behavior.

 * `BigQueryDisposition.CREATE_NEVER`: Specifies that a table should never be
   created. If the destination table does not exist, the write operation fails.


If you specify `CREATE_IF_NEEDED` as the create disposition and you don't supply
a table schema, the transform might fail at runtime if the destination table does
not exist.


### Write disposition

The write disposition controls how your BigQuery write operation applies to an
existing table.

<!-- Java specific -->

{:.language-java}
Use `.withWriteDisposition` to specify the write disposition. Valid enum values
are:

{:.language-java}
 * `Write.WriteDisposition.WRITE_EMPTY`: Specifies that the write
   operation should fail at runtime if the destination table is not empty.
   `WRITE_EMPTY` is the default behavior.

 * `Write.WriteDisposition.WRITE_TRUNCATE`: Specifies that the write
   operation should replace an existing table. Any existing rows in the
   destination table are removed, and the new rows are added to the table.

 * `Write.WriteDisposition.WRITE_APPEND`: Specifies that the write
   operation should append the rows to the end of the existing table.

<!-- Python specific -->

{:.language-py}
Use the `write_disposition` parameter to specify the write disposition. Valid
enum values are:

{:.language-py}
 * `BigQueryDisposition.WRITE_EMPTY`: Specifies that the write operation should
   fail at runtime if the destination table is not empty. `WRITE_EMPTY` is the
   default behavior.

 * `BigQueryDisposition.WRITE_TRUNCATE`: Specifies that the write operation
   should replace an existing table. Any existing rows in the destination table
   are removed, and the new rows are added to the table.

 * `BigQueryDisposition.WRITE_APPEND`: Specifies that the write operation should
   append the rows to the end of the existing table.


When you use `WRITE_EMPTY`, the check for whether or not the destination table
is empty can occur before the actual write operation. This check doesn't
guarantee that your pipeline will have exclusive access to the table. Two
concurrent pipelines that write to the same output table with a write
disposition of `WRITE_EMPTY` might start successfully, but both pipelines can
fail later when the write attempts happen.


### Creating a table schema

If your BigQuery write operation creates a new table, you must provide schema
information. The schema contains information about each field in the table.

{:.language-java}
To create a table schema in Java, you can either use a `TableSchema` object, or
use a string that contains a JSON-serialized `TableSchema` object.

{:.language-py}
To create a table schema in Python, you can either use a `TableSchema` object,
or use a string that defines a list of fields. Single string based schemas do
not support nested fields, repeated fields, or specifying a BigQuery mode for
fields (the mode will always be set to `NULLABLE`).


#### Using a TableSchema

To create and use a table schema as a `TableSchema` object, follow these steps.

<!-- Java specific - TableSchema -->

{:.language-java}
1. Create a list of `TableFieldSchema` objects. Each `TableFieldSchema` object
   represents a field in the table.

2. Create a `TableSchema` object and use the `setFields` method to specify your
   list of fields.

3. Use the `withSchema` method to provide your table schema when you apply a
   write transform.


<!-- Python specific - TableSchema -->

{:.language-py}
1. Create a `TableSchema` object.

2. Create and append a `TableFieldSchema` object for each field in your table.

3. Next, use the `schema` parameter to provide your table schema when you apply
   a write transform. Set the parameter’s value to the `TableSchema` object.


<!-- Common -->

The following example code shows how to create a `TableSchema` for a table with
two fields (source and quote) of type string.

```java
{% github_sample /apache/beam/blob/master/examples/java/src/main/java/org/apache/beam/examples/snippets/Snippets.java tag:BigQuerySchemaObject
%}```
```py
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/snippets.py tag:model_bigqueryio_schema_object
%}```


#### Using a string

<!-- Java specific - string -->

{:.language-java}
To create and use a table schema as a string that contains JSON-serialized
`TableSchema` object, follow these steps.

{:.language-java}
1. Create a string that contains a JSON-serialized `TableSchema` object.

2. Use the `withJsonSchema` method to provide your table schema when you apply a
   write transform.


<!-- Python specific - string -->

{:.language-py}
To create and use a table schema as a string, follow these steps.

{:.language-py}
1. Create a single comma separated string of the form
   "field1:type1,field2:type2,field3:type3" that defines a list of fields. The
   type should specify the field’s BigQuery type.

2. Use the `schema` parameter to provide your table schema when you apply a
   write transform. Set the parameter’s value to the string.


<!-- Common -->

The following example shows how to use a string to specify the same table schema
as the previous example.

```java
{% github_sample /apache/beam/blob/master/examples/java/src/main/java/org/apache/beam/examples/snippets/Snippets.java tag:BigQuerySchemaJson
%}```
```py
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/snippets.py tag:model_bigqueryio_schema
%}```


### Setting the insertion method

{:.language-py}
> The Beam SDK for Python does not currently support specifying the insertion
method.

BigQueryIO supports two methods of inserting data into BigQuery: load jobs and
streaming inserts. Each insertion method provides different tradeoffs of cost,
quota, and data consistency. See the BigQuery documentation for
[load jobs](https://cloud.google.com/bigquery/loading-data) and
[streaming inserts](https://cloud.google.com/bigquery/streaming-data-into-bigquery)
for more information about these tradeoffs.

BigQueryIO chooses a default insertion method based on the input `PCollection`.

{:.language-py}
BigQueryIO uses load jobs when you apply a BigQueryIO write transform to a
bounded `PCollection`.

{:.language-java}
BigQueryIO uses load jobs in the following situations:

{:.language-java}
* When you apply a BigQueryIO write transform to a bounded `PCollection`.
* When you apply a BigQueryIO write transform to an unbounded `PCollection` and
  use `BigQueryIO.write().withTriggeringFrequency()` to set the triggering
  frequency.
* When you specify load jobs as the insertion method using
  `BigQueryIO.write().withMethod(FILE_LOADS)`.

{:.language-py}
BigQueryIO uses streaming inserts when you apply a BigQueryIO write transform to
an unbounded `PCollection`.

{:.language-java}
BigQueryIO uses streaming inserts in the following situations:

{:.language-java}
* When you apply a BigQueryIO write transform to an unbounded `PCollection` and
  do not set the triggering frequency.
* When you specify streaming inserts as the insertion method using
  `BigQueryIO.write().withMethod(STREAMING_INSERTS)`.

<!-- Java specific -->

{:.language-java}
You can use `withMethod` to specify the desired insertion method. See
[Write.Method](https://beam.apache.org/releases/javadoc/{{ site.release_latest }}/org/apache/beam/sdk/io/gcp/bigquery/BigQueryIO.Write.Method.html)
for the list of the available methods and their restrictions.

{:.language-java}
***Note:*** If you use batch loads in a streaming pipeline, you must use
`withTriggeringFrequency` to specify a triggering frequency and `withNumFileShards` to specify number of file shards written.


### Writing to a table

{:.language-java}
To write to a BigQuery table, apply either a `writeTableRows` or `write`
transform.

{:.language-py}
To write to a BigQuery table, apply the `WriteToBigQuery` transform.
`WriteToBigQuery` supports both batch mode and streaming mode. You must apply
the transform to a `PCollection` of dictionaries. In general, you'll need to use
another transform, such as `ParDo`, to format your output data into a
collection.

The following examples use this `PCollection` that contains quotes.

```java
{% github_sample /apache/beam/blob/master/examples/java/src/main/java/org/apache/beam/examples/snippets/Snippets.java tag:BigQueryWriteInput
%}```
```py
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/snippets.py tag:model_bigqueryio_write_input
%}```

<!-- writeTableRows and WriteToBigQuery -->

{:.language-java}
The `writeTableRows` method writes a `PCollection` of BigQuery `TableRow`
objects to a BigQuery table. Each element in the `PCollection` represents a
single row in the table. This example uses `writeTableRows` to write quotes to a
`PCollection<TableRow>`.  The write operation creates a table if needed; if the
table already exists, it will be replaced.

{:.language-py}
The following example code shows how to apply a `WriteToBigQuery` transform to
write a `PCollection` of dictionaries to a BigQuery table. The write operation
creates a table if needed; if the table already exists, it will be replaced.

```java
{% github_sample /apache/beam/blob/master/examples/java/src/main/java/org/apache/beam/examples/snippets/Snippets.java tag:BigQueryWriteTable
%}```
```py
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/snippets.py tag:model_bigqueryio_write
%}```

<!-- write -->

{:.language-java}
The `write` transform writes a `PCollection` of custom typed objects to a BigQuery
table. Use `.withFormatFunction(SerializableFunction)` to provide a formatting
function that converts each input element in the `PCollection` into a
`TableRow`.  This example uses `write` to write a `PCollection<String>`. The
write operation creates a table if needed; if the table already exists, it will
be replaced.

```java
{% github_sample /apache/beam/blob/master/examples/java/src/main/java/org/apache/beam/examples/snippets/Snippets.java tag:BigQueryWriteFunction
%}```

{:.language-java}
When you use streaming inserts, you can decide what to do with failed records.
You can either keep retrying, or return the failed records in a separate
`PCollection` using the `WriteResult.getFailedInserts()` method.

### Using dynamic destinations

{:.language-py}
> The Beam SDK for Python does not currently support dynamic destinations.

You can use the dynamic destinations feature to write elements in a
`PCollection` to different BigQuery tables, possibly with different schemas.

The dynamic destinations feature groups your user type by a user-defined
destination key, uses the key to compute a destination table and/or schema, and
writes each group's elements to the computed destination.

In addition, you can also write your own types that have a mapping function to
`TableRow`, and you can use side inputs in all `DynamicDestinations` methods.

<!-- Java specific -->

{:.language-java}
To use dynamic destinations, you must create a `DynamicDestinations` object and
implement the following methods:

{:.language-java}
* `getDestination`: Returns an object that `getTable` and `getSchema` can use as
  the destination key to compute the destination table and/or schema.

* `getTable`: Returns the table (as a `TableDestination` object) for the
  destination key. This method must return a unique table for each unique
  destination.

* `getSchema`: Returns the table schema (as a `TableSchema` object) for the
  destination key.

{:.language-java}
Then, use `write().to` with your `DynamicDestinations` object. This example
uses a `PCollection` that contains weather data and writes the data into a
different table for each year.

```java
{% github_sample /apache/beam/blob/master/examples/java/src/main/java/org/apache/beam/examples/snippets/Snippets.java tag:BigQueryWriteDynamicDestinations
%}```
```py
# The Beam SDK for Python does not currently support dynamic destinations.
```

### Using time partitioning

{:.language-py}
> The Beam SDK for Python does not currently support time partitioning.

BigQuery time partitioning divides your table into smaller partitions, which is
called a [partitioned table](https://cloud.google.com/bigquery/docs/partitioned-tables).
Partitioned tables make it easier for you to manage and query your data.

<!-- Java specific -->

{:.language-java}
To use BigQuery time partitioning, use one of these two methods:

{:.language-java}
* `withTimePartitioning`: This method takes a `TimePartitioning` class, and is
  only usable if you are writing to a single table.

* `withJsonTimePartitioning`: This method is the same as
  `withTimePartitioning`, but takes a JSON-serialized String object.

{:.language-java}
This example generates one partition per day.

```java
{% github_sample /apache/beam/blob/master/examples/java/src/main/java/org/apache/beam/examples/snippets/Snippets.java tag:BigQueryTimePartitioning
%}```
```py
# The Beam SDK for Python does not currently support time partitioning.
```


## Limitations

BigQueryIO currently has the following limitations.

1. You can’t sequence the completion of a BigQuery write with other steps of
   your pipeline.

2. If you are using the Beam SDK for Python, you might have import size quota
   issues if you write a very large dataset. As a workaround, you can partition
   the dataset (for example, using Beam's `Partition` transform) and write to
   multiple BigQuery tables. The Beam SDK for Java does not have this limitation
   as it partitions your dataset for you.


## Additional examples

You can find additional examples that use BigQuery in Beam's examples
directories.

### Java cookbook examples

These examples are from the Java [cookbook examples](https://github.com/apache/beam/tree/master/examples/java/src/main/java/org/apache/beam/examples/cookbook)
directory.

* [BigQueryTornadoes](https://github.com/apache/beam/blob/master/examples/java/src/main/java/org/apache/beam/examples/cookbook/BigQueryTornadoes.java)
  reads the public samples of weather data from BigQuery, counts the number of
  tornadoes that occur in each month, and writes the results to a BigQuery
  table.

* [CombinePerKeyExamples](https://github.com/apache/beam/blob/master/examples/java/src/main/java/org/apache/beam/examples/cookbook/CombinePerKeyExamples.java)
  reads the public Shakespeare data from BigQuery, and for each word in the
  dataset that exceeds a given length, generates a string containing the list of
  play names in which that word appears. The pipeline then writes the results to
  a BigQuery table.

* [FilterExamples](https://github.com/apache/beam/blob/master/examples/java/src/main/java/org/apache/beam/examples/cookbook/FilterExamples.java)
  reads public samples of weather data from BigQuery, performs a projection
  on the data, finds the global mean of the temperature readings, filters on
  readings for a single given month, and outputs only data (for that month)
  that has a mean temp smaller than the derived global mean.

* [JoinExamples](https://github.com/apache/beam/blob/master/examples/java/src/main/java/org/apache/beam/examples/cookbook/JoinExamples.java)
  reads a sample of the [GDELT "world event"](http://goo.gl/OB6oin) from
  BigQuery and joins the event `action` country code against a table that maps
  country codes to country names.

* [MaxPerKeyExamples](https://github.com/apache/beam/blob/master/examples/java/src/main/java/org/apache/beam/examples/cookbook/MaxPerKeyExamples.java)
  reads the public samples of weather data from BigQuery, finds the maximum
  temperature for each month, and writes the results to a BigQuery table.

* [TriggerExample](https://github.com/apache/beam/blob/master/examples/java/src/main/java/org/apache/beam/examples/cookbook/TriggerExample.java)
  performs a streaming analysis of traffic data from San Diego freeways. The
  pipeline looks at the data coming in from a text file and writes the results
  to a BigQuery table.


### Java complete examples

These examples are from the Java [complete examples](https://github.com/apache/beam/tree/master/examples/java/src/main/java/org/apache/beam/examples/complete)
directory.

* [AutoComplete](https://github.com/apache/beam/blob/master/examples/java/src/main/java/org/apache/beam/examples/complete/AutoComplete.java)
  computes the most popular hash tags for every prefix, which can be used for
  auto-completion. The pipeline can optionally write the results to a BigQuery
  table.

* [StreamingWordExtract](https://github.com/apache/beam/blob/master/examples/java/src/main/java/org/apache/beam/examples/complete/StreamingWordExtract.java)
  reads lines of text, splits each line into individual words, capitalizes those
  words, and writes the output to a BigQuery table.

* [TrafficMaxLaneFlow](https://github.com/apache/beam/blob/master/examples/java/src/main/java/org/apache/beam/examples/complete/TrafficMaxLaneFlow.java)
  reads traffic sensor data, finds the lane that had the highest recorded flow,
  and writes the results to a BigQuery table.

* [TrafficRoutes](https://github.com/apache/beam/blob/master/examples/java/src/main/java/org/apache/beam/examples/complete/TrafficRoutes.java)
  reads traffic sensor data, calculates the average speed for each window and
  looks for slowdowns in routes, and writes the results to a BigQuery table.


### Python cookbook examples

These examples are from the Python [cookbook examples](https://github.com/apache/beam/tree/master/sdks/python/apache_beam/examples/cookbook)
directory.

* [BigQuery schema](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/cookbook/bigquery_schema.py)
  creates a `TableSchema` with nested and repeated fields, generates data with
  nested and repeated fields, and writes the data to a BigQuery table.

* [BigQuery side inputs](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/cookbook/bigquery_side_input.py)
  uses BigQuery sources as a side inputs. It illustrates how to insert
  side-inputs into transforms in three different forms: as a singleton, as a
  iterator, and as a list.

* [BigQuery tornadoes](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/cookbook/bigquery_tornadoes.py)
  reads from a BigQuery table that has the 'month' and 'tornado' fields as part
  of the table schema, computes the number of tornadoes in each month, and
  outputs the results to a BigQuery table.

* [BigQuery filters](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/cookbook/filters.py)
  reads weather station data from a BigQuery table, manipulates BigQuery rows in
  memory, and writes the results to a BigQuery table.


