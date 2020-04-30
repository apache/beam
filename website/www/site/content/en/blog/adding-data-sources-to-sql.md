---
title:  "Adding new Data Sources to Beam SQL CLI"
date:   2019-06-04 00:00:01 -0800
categories:
  - blog
aliases:
  - /blog/2019/06/04/adding-data-sources-to-sql.html
authors:
  - pabloem

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

A new, exciting feature that came to Apache Beam is the ability to use
SQL in your pipelines. This is done using Beam's
[`SqlTransform`](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/extensions/sql/SqlTransform.html)
in Java pipelines.

Beam also has a fancy new SQL command line that you can use to query your
data interactively, be it Batch or Streaming. If you haven't tried it, check out
[http://bit.ly/ExploreBeamSQL](https://bit.ly/ExploreBeamSQL).

A nice feature of the SQL CLI is that you can use `CREATE EXTERNAL TABLE`
commands to *add* data sources to be accessed in the CLI. Currently, the CLI
supports creating tables from BigQuery, PubSub, Kafka, and text files. In this
post, we explore how to add new data sources, so that you will be able to
consume data from other Beam sources.

<!--more-->

The table provider we will be implementing in this post will be generating a
continuous unbounded stream of integers. It will be based on the
[`GenerateSequence` PTransform](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/GenerateSequence.html)
from the Beam SDK. In the end will be able to define and use the sequence generator
in SQL like this:

```
CREATE EXTERNAL TABLE                      -- all tables in Beam are external, they are not persisted
  sequenceTable                              -- table alias that will be used in queries
  (
         sequence BIGINT,                  -- sequence number
         event_timestamp TIMESTAMP         -- timestamp of the generated event
  )
TYPE sequence                              -- type identifies the table provider
TBLPROPERTIES '{ elementsPerSecond : 12 }' -- optional rate at which events are generated
```

And we'll be able to use it in queries like so:

```
SELECT sequence FROM sequenceTable;
```

Let's dive in!

### Implementing a `TableProvider`

Beam's `SqlTransform` works by relying on `TableProvider`s, which it uses when
one uses a `CREATE EXTERNAL TABLE` statement. If you are looking to add a new
data source to the Beam SQL CLI, then you will want to add a `TableProvider` to
do it. In this post, I will show what steps are necessary to create a new table
provider for the
[`GenerateSequence` transform](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/GenerateSequence.html) available in the Java SDK.

The `TableProvider` classes are under
[`sdks/java/extensions/sql/src/main/java/org/apache/beam/sdk/extensions/sql/meta/provider/`](https://github.com/apache/beam/tree/master/sdks/java/extensions/sql/src/main/java/org/apache/beam/sdk/extensions/sql/meta/provider). If you look in there, you can find providers, and their implementations, for all available data sources. So, you just need to add the one you want, along with an implementation of `BaseBeamTable`.

### The GenerateSequenceTableProvider

Our table provider looks like this:

{{< highlight java >}}
@AutoService(TableProvider.class)
public class GenerateSequenceTableProvider extends InMemoryMetaTableProvider {

  @Override
  public String getTableType() {
    return "sequence";
  }

  @Override
  public BeamSqlTable buildBeamSqlTable(Table table) {
    return new GenerateSequenceTable(table);
  }
}
{{< /highlight >}}

All it does is give a type to the table - and it implements the
`buildBeamSqlTable` method, which simply returns a `BeamSqlTable` defined by
our `GenerateSequenceTable` implementation.

### The GenerateSequenceTable

We want a table implementation that supports streaming properly, so we will
allow users to define the number of elements to be emitted per second. We will
define a simple table that emits sequential integers in a streaming fashion.
This looks like so:

{{< highlight java >}}
class GenerateSequenceTable extends BaseBeamTable implements Serializable {
  public static final Schema TABLE_SCHEMA =
      Schema.of(Field.of("sequence", FieldType.INT64), Field.of("event_time", FieldType.DATETIME));

  Integer elementsPerSecond = 5;

  GenerateSequenceTable(Table table) {
    super(TABLE_SCHEMA);
    if (table.getProperties().containsKey("elementsPerSecond")) {
      elementsPerSecond = table.getProperties().getInteger("elementsPerSecond");
    }
  }

  @Override
  public PCollection.IsBounded isBounded() {
    return IsBounded.UNBOUNDED;
  }

  @Override
  public PCollection<Row> buildIOReader(PBegin begin) {
    return begin
        .apply(GenerateSequence.from(0).withRate(elementsPerSecond, Duration.standardSeconds(1)))
        .apply(
            MapElements.into(TypeDescriptor.of(Row.class))
                .via(elm -> Row.withSchema(TABLE_SCHEMA).addValues(elm, Instant.now()).build()))
        .setRowSchema(getSchema());
  }

  @Override
  public POutput buildIOWriter(PCollection<Row> input) {
    throw new UnsupportedOperationException("buildIOWriter unsupported!");
  }
}
{{< /highlight >}}

## The real fun

Now that we have implemented the two basic classes (a `BaseBeamTable`, and a
`TableProvider`), we can start playing with them. After building the
[SQL CLI](https://beam.apache.org/documentation/dsls/sql/shell/), we
can now perform selections on the table:

```
0: BeamSQL> CREATE EXTERNAL TABLE input_seq (
. . . . . >   sequence BIGINT COMMENT 'this is the primary key',
. . . . . >   event_time TIMESTAMP COMMENT 'this is the element timestamp'
. . . . . > )
. . . . . > TYPE 'sequence';
No rows affected (0.005 seconds)
```

And let's select a few rows:

```
0: BeamSQL> SELECT * FROM input_seq LIMIT 5;
+---------------------+------------+
|      sequence       | event_time |
+---------------------+------------+
| 0                   | 2019-05-21 00:36:33 |
| 1                   | 2019-05-21 00:36:33 |
| 2                   | 2019-05-21 00:36:33 |
| 3                   | 2019-05-21 00:36:33 |
| 4                   | 2019-05-21 00:36:33 |
+---------------------+------------+
5 rows selected (1.138 seconds)
```

Now let's try something more interesting. Such as grouping. This will also let
us make sure that we're providing the timestamp for each row properly:

```
0: BeamSQL> SELECT
. . . . . >   COUNT(sequence) as elements,
. . . . . >   TUMBLE_START(event_time, INTERVAL '2' SECOND) as window_start
. . . . . > FROM input_seq
. . . . . > GROUP BY TUMBLE(event_time, INTERVAL '2' SECOND) LIMIT 5;
+---------------------+--------------+
|      elements       | window_start |
+---------------------+--------------+
| 6                   | 2019-06-05 00:39:24 |
| 10                  | 2019-06-05 00:39:26 |
| 10                  | 2019-06-05 00:39:28 |
| 10                  | 2019-06-05 00:39:30 |
| 10                  | 2019-06-05 00:39:32 |
+---------------------+--------------+
5 rows selected (10.142 seconds)
```

And voilà! We can start playing with some interesting streaming queries to our
sequence generator.
