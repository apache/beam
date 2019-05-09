---
layout: post
title:  "Adding new Data Sources to Beam SQL CLI"
date:   2019-05-01 00:00:01 -0800
excerpt_separator: <!--more-->
categories: blog
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
[`SqlTransform`](https://beam.apache.org/releases/javadoc/latest/org/apache/beam/sdk/extensions/sql/SqlTransform.html)
in Java pipelines.

Beam also has a fancy new SQL command line that you can use to query your
data interactively, be it Batch or Streaming. If you haven't tried it, check out
[http://bit.ly/ExploreBeamSQL](http://bit.ly/ExploreBeamSQL).

A nice feature of the SQL CLI is that you can use `CREATE TABLE` commands to
*add* data sources to be accessed in the CLI. Currently, the CLI supports
creating tables from BigQuery, PubSub, Kafka, and text files. In this post, we
explore how to add new data sources, so that you will be able to consume data
from other Beam sources.

<!--more-->

Beam's `SqlTransform` works by relying on `TableProvider`s, which it uses when
one uses a `CREATE TABLE` statement. If you are looking to add a new data source
to the Beam SQL CLI, then you will want to add a `TableProvider` to do it. In
this post, I will show what steps are necessary to create a new table provider
for the [`GenerateSequence` transform](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/GenerateSequence.html) available in the Java SDK.

The `TableProvider` classes are under
[`sdks/java/extensions/sql/src/main/java/org/apache/beam/sdk/extensions/sql/meta/provider/`](https://github.com/apache/beam/tree/master/sdks/java/extensions/sql/src/main/java/org/apache/beam/sdk/extensions/sql/meta/provider). If you look in there, you can find providers, and their implementations, for all available data sources. So, you just need to add the one you want, along with an implementation of `BaseBeamTable`.

### The GenerateSequenceTableProvider

Our table provider looks like this:

```java
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
```

### The GenerateSequenceTable

We want a table implementation that supports streaming properly, so we will
allow users to define the number of elements to be emitted per second.

```java
class GenerateSequenceTable extends BaseBeamTable implements Serializable {
  static public final Schema tableSchema = Schema.of(Field.of("sequence", FieldType.INT64));

  Integer elementsPerSecond = 5;


  GenerateSequenceTable(Table table) {
    super(tableSchema);
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
        .apply(WithTimestamps.of(elm -> Instant.now().plus(elm)))
        .apply(MapElements
            .into(TypeDescriptor.of(Row.class))
            .via(elm -> Row.withSchema(tableSchema).addValue(elm).build()))
        .setRowSchema(getSchema());
  }

  @Override
  public POutput buildIOWriter(PCollection<Row> input) {
    throw new UnsupportedOperationException("buildIOWriter unsupported!");
  }
}
```
