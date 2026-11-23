---
title: "Apache Iceberg CDC quickstart"
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

[Built-in I/O Transforms](/documentation/io/built-in/)

# Apache Iceberg CDC quickstart

In this guide, you'll keep one Iceberg table in sync with another using change data capture (CDC),
with a streaming pipeline that applies each change as it lands. Everything will run locally on your
machine.

We'll work with two pieces:

* The **Iceberg CDC source** reads a table's inserts, updates and deletes from its snapshots, in
  batch or streaming.
* The **Iceberg CDC sink** applies those changes to another table by primary key, via merge-on-read.

For the why and how behind it, read
[Stream changes between Iceberg tables with Apache Beam](/blog/iceberg-cdc-sql/).

{{< language-switcher sql java py yaml >}}

<style>
  /* The selector above sets the language for the whole page, so hide the per-section tabs. */
  div.language-switcher { display: none !important; }
  /* Without a tab bar above it, a code block doesn't need the gap or the square corner left for one. */
  .code-snippet { border-top-left-radius: 8px; }
  .code-snippet pre { margin-top: 0; }
  /* Expected rows: a small bordered table, only as wide as its content. */
  .expected-rows table { width: auto; }
  .expected-rows th, .expected-rows td { border: 1px solid #ddd; padding: 8px 12px; }
</style>

## Before you begin

{{< paragraph class="language-sql" >}}
You need Java 17 or later and the Beam SQL shell, version 2.79.0 or later. Pipelines run on your
machine, and on Dataflow when you're ready to scale up.
{{< /paragraph >}}

{{< paragraph class="language-java" >}}
You need Java 17 or later and Beam 2.79.0 or later. This guide runs on the Java DirectRunner, and later on Dataflow when you're ready to scale up.
{{< /paragraph >}}

{{< paragraph class="language-py language-yaml" >}}
You need Python 3.11 or later and Beam 2.79.0 or later. You also need Java 17 or later on your `PATH`, since the
Iceberg transforms run in Java. The streaming steps use the
[Prism runner](/documentation/runners/prism/), which Beam downloads the first time you use it.
{{< /paragraph >}}

{{< paragraph class="language-sql" >}}
Install the [Beam SQL shell](/documentation/dsls/sql/shell/#installation), and start it in your terminal with Iceberg support:
{{< /paragraph >}}

{{% section class="language-sql" %}}
```shell
./beam-sql.sh --version 2.79.0 --io iceberg
```
{{% /section %}}

{{< paragraph class="language-java" >}}
Add these dependencies. `slf4j-simple` prints Beam's logs, including the rows you'll check later:
{{< /paragraph >}}

<section class="language-java">

{{< runner maven >}}
<dependency>
  <groupId>org.apache.beam</groupId>
  <artifactId>beam-sdks-java-managed</artifactId>
  <version>{{< param release_latest >}}</version>
</dependency>
<dependency>
  <groupId>org.apache.beam</groupId>
  <artifactId>beam-sdks-java-io-iceberg</artifactId>
  <version>{{< param release_latest >}}</version>
  <scope>runtime</scope>
</dependency>
<dependency>
  <groupId>org.apache.beam</groupId>
  <artifactId>beam-runners-direct-java</artifactId>
  <version>{{< param release_latest >}}</version>
  <scope>runtime</scope>
</dependency>
<dependency>
  <groupId>org.slf4j</groupId>
  <artifactId>slf4j-simple</artifactId>
  <version>2.0.16</version>
  <scope>runtime</scope>
</dependency>
{{< /runner >}}
{{< runner gradle >}}
dependencies {
  implementation("org.apache.beam:beam-sdks-java-managed:{{< param release_latest >}}")
  runtimeOnly("org.apache.beam:beam-sdks-java-io-iceberg:{{< param release_latest >}}")
  runtimeOnly("org.apache.beam:beam-runners-direct-java:{{< param release_latest >}}")
  runtimeOnly("org.slf4j:slf4j-simple:2.0.16")
}
{{< /runner >}}

</section>

{{< paragraph class="language-py" >}}
Install Beam:
{{< /paragraph >}}

{{% section class="language-py" %}}
```shell
pip install 'apache-beam>=2.79.0'
```
{{% /section %}}

{{< paragraph class="language-yaml" >}}
Install Beam with YAML support. Save each pipeline below to a file and run it with
`python -m apache_beam.yaml.main --yaml_pipeline_file=<file>`.
{{< /paragraph >}}

{{% section class="language-yaml" %}}
```shell
pip install 'apache-beam[yaml]>=2.79.0'
```
{{% /section %}}

## 1. Set up a catalog

Use a Hadoop catalog in a local directory. No extra setup is needed. If you've run this guide
before, delete the directory first, so you start with empty tables:

```shell
rm -rf /tmp/beam-iceberg-cdc-quickstart
```

{{< paragraph class="language-sql" >}}
You'll use three shells. Start the first one, then create the catalog and a database:
{{< /paragraph >}}

{{< highlight sql >}}
CREATE CATALOG quickstart TYPE 'iceberg'
PROPERTIES (
  'type' = 'hadoop',
  'warehouse' = 'file:///tmp/beam-iceberg-cdc-quickstart'
);

CREATE DATABASE IF NOT EXISTS quickstart.shop;
USE DATABASE quickstart.shop;
{{< /highlight >}}
{{< highlight java >}}
Map<String, Object> catalog =
    Map.of(
        "catalog_name", "quickstart",
        "catalog_properties",
            Map.of("type", "hadoop", "warehouse", "file:///tmp/beam-iceberg-cdc-quickstart"));
{{< /highlight >}}
{{< highlight py >}}
import apache_beam as beam

catalog = {
    "catalog_name": "quickstart",
    "catalog_properties": {
        "type": "hadoop",
        "warehouse": "file:///tmp/beam-iceberg-cdc-quickstart",
    },
}
{{< /highlight >}}
{{< highlight yaml >}}
# Every pipeline in this guide uses this catalog:
catalog_name: quickstart
catalog_properties: {type: hadoop, warehouse: "file:///tmp/beam-iceberg-cdc-quickstart"}
{{< /highlight >}}

{{< paragraph class="language-java" >}}
You'll run the steps below as separate programs, and each one starts with this `catalog`.
{{< /paragraph >}}

{{< paragraph class="language-py" >}}
You'll run the steps below as separate scripts, and each one starts with this `catalog`.
{{< /paragraph >}}

## 2. Create the source table

{{< paragraph class="language-sql" >}}
Create the source table and open a live view of the table's changes. It's empty for now, but will
get populated when we start writing to it in step 4. Leave it open.
{{< /paragraph >}}

{{< paragraph class="language-java language-py language-yaml" >}}
Create the source table by writing a first round of changes to it with Beam's CDC sink.
{{< /paragraph >}}

{{< highlight sql >}}
CREATE EXTERNAL TABLE orders (
  id BIGINT PRIMARY KEY NOT ENFORCED,
  item VARCHAR,
  qty INTEGER
)
TYPE 'iceberg';

SELECT * FROM CHANGES(orders)
WITH ('streaming' = true, 'starting_strategy' = 'earliest', 'poll_interval_seconds' = 1);
{{< /highlight >}}

<section class="language-sql">

<img class="center-block" src="/images/iceberg-cdc/live-view-open.gif"
     alt="The SELECT is typed into the Beam SQL shell, and the live view opens with 0 rows.">

</section>

{{< highlight java >}}
// The sink creates the table on first write, with `id` as its primary key.
static final Schema CHANGE =
    Schema.builder()
        .addInt64Field("id")
        .addStringField("item")
        .addInt32Field("qty")
        .addStringField("op") // the change type
        .addInt64Field("seq") // the sequence number
        .build();

static Row change(long id, String item, int qty, String op, long seq) {
  return Row.withSchema(CHANGE).addValues(id, item, qty, op, seq).build();
}

static void applyChanges(Map<String, Object> catalog, Row... changes) {
  Map<String, Object> write = new HashMap<>(catalog);
  write.put("table", "shop.orders");
  write.put("mode", "merge-on-read");
  write.put("equality_columns", List.of("id"));
  write.put("change_type_column", "op");
  write.put("sequence_number_column", "seq");

  Pipeline p = Pipeline.create();
  p.apply(Create.of(Arrays.asList(changes)).withRowSchema(CHANGE))
      .apply(Managed.write(Managed.ICEBERG).withConfig(write));
  p.run().waitUntilFinish();
}
{{< /highlight >}}
{{< highlight py >}}
# The sink creates the table on first write, with `id` as its primary key.
def change(id, item, qty, op, seq):
  return beam.Row(id=id, item=item, qty=qty, op=op, seq=seq)

def apply_changes(*changes):
  with beam.Pipeline() as p:
    _ = (
        p
        | beam.Create(changes)
        | beam.managed.Write(beam.managed.ICEBERG, config={
            **catalog,
            "table": "shop.orders",
            "mode": "merge-on-read",
            "equality_columns": ["id"],
            "change_type_column": "op",
            "sequence_number_column": "seq",
        }))
{{< /highlight >}}

<section class="language-java language-py language-yaml">

Each change record carries two metadata values:

- the record's change type (`op`)
- the record's sequence number (`seq`), which goes up with each change to the same key. Both halves of
  an update (`UPDATE_BEFORE` and `UPDATE_AFTER`) share one.

</section>

{{< highlight java >}}
// Snapshot 1: three new orders
applyChanges(catalog,
    change(1, "apple", 3, "INSERT", 1),
    change(2, "pear", 5, "INSERT", 1),
    change(3, "plum", 2, "INSERT", 1));
{{< /highlight >}}
{{< highlight py >}}
# Snapshot 1: three new orders
apply_changes(
    change(1, "apple", 3, "INSERT", 1),
    change(2, "pear", 5, "INSERT", 1),
    change(3, "plum", 2, "INSERT", 1))
{{< /highlight >}}
{{< highlight yaml >}}
# snapshot_1.yaml: three new orders. The sink creates the table, with `id` as its key.
pipeline:
  type: chain
  transforms:
    - type: Create
      config:
        elements:
          - {id: 1, item: apple, qty: 3, op: INSERT, seq: 1}
          - {id: 2, item: pear, qty: 5, op: INSERT, seq: 1}
          - {id: 3, item: plum, qty: 2, op: INSERT, seq: 1}
    - type: WriteToIceberg
      config:
        table: shop.orders
        catalog_name: quickstart
        catalog_properties: {type: hadoop, warehouse: "file:///tmp/beam-iceberg-cdc-quickstart"}
        mode: merge-on-read
        equality_columns: [id]
        change_type_column: op
        sequence_number_column: seq
{{< /highlight >}}

## 3. Keep a table in sync (streaming)

<section class="language-sql">

Now open a **second shell**. Here, we'll run a streaming pipeline that continuously observes the
original `orders` table and applies its changes to a downstream table.

Connect to the catalog, and create a second table, `orders_mirror`.

</section>

{{< highlight sql >}}
CREATE CATALOG quickstart TYPE 'iceberg'
PROPERTIES (
  'type' = 'hadoop',
  'warehouse' = 'file:///tmp/beam-iceberg-cdc-quickstart'
);
USE DATABASE quickstart.shop;

CREATE EXTERNAL TABLE orders_mirror (
  id BIGINT PRIMARY KEY NOT ENFORCED,
  item VARCHAR,
  qty INTEGER
)
TYPE 'iceberg';
{{< /highlight >}}

{{< paragraph class="language-sql" >}}
Continuously stream changes from `orders` into `orders_mirror`. On the read side, select two
Iceberg metadata columns by name:
{{< /paragraph >}}

{{< paragraph class="language-java" >}}
Now write a **second program**. Here, we'll run a streaming pipeline that continuously observes the
original `orders` table and applies its changes to a downstream table, `orders_mirror`.
{{< /paragraph >}}

{{< paragraph class="language-py language-yaml" >}}
Now open a **second terminal**. Here, we'll run a streaming pipeline that continuously observes the
original `orders` table and applies its changes to a downstream table, `orders_mirror`.
{{< /paragraph >}}

{{< paragraph class="language-java language-py language-yaml" >}}
On the read side, request two Iceberg metadata columns with `include_metadata_columns`:
{{< /paragraph >}}

- the change type (`_change_type`)
- the snapshot sequence number (`_commit_snapshot_sequence_number`)

On the write side, point `change_type_column` at the type. You don't need to set
`sequence_number_column`, because it already defaults to `_commit_snapshot_sequence_number`. The
sink drops both columns before it writes.

Streaming writes need a commit window, which you set with `triggering_frequency_seconds`. Streaming
reads need a polling interval, which you set with `poll_interval_seconds` (defaults to 60s). A
streaming read starts at the latest snapshot by default, so set `starting_strategy` to `earliest`
to also apply the changes made before the pipeline started.

{{< highlight sql >}}
INSERT INTO orders_mirror
WITH ('mode' = 'merge-on-read',
      'change_type_column' = '_change_type',
      'triggering_frequency_seconds' = 1)
SELECT *, _change_type, _commit_snapshot_sequence_number
FROM CHANGES(orders)
WITH ('streaming' = true,
      'starting_strategy' = 'earliest',
      'poll_interval_seconds' = 1);
{{< /highlight >}}
{{< highlight java >}}
Map<String, Object> read = new HashMap<>(catalog);
read.put("table", "shop.orders");
read.put("include_metadata_columns", List.of("_change_type", "_commit_snapshot_sequence_number"));
read.put("streaming", true);
read.put("starting_strategy", "earliest");
read.put("poll_interval_seconds", 1);

Map<String, Object> write = new HashMap<>(catalog);
write.put("table", "shop.orders_mirror");
write.put("mode", "merge-on-read");
write.put("equality_columns", List.of("id"));
write.put("change_type_column", "_change_type");
write.put("triggering_frequency_seconds", 1);

Pipeline p = Pipeline.create();
p.apply(Managed.read(Managed.ICEBERG_CDC).withConfig(read))
    .getSinglePCollection()
    .apply(Managed.write(Managed.ICEBERG).withConfig(write));
p.run().waitUntilFinish(); // runs until you stop it
{{< /highlight >}}
{{< highlight py >}}
from apache_beam.options.pipeline_options import PipelineOptions

with beam.Pipeline(options=PipelineOptions(streaming=True, runner="PrismRunner")) as p:
  _ = (
      p
      | beam.managed.Read("iceberg_cdc", config={
          **catalog,
          "table": "shop.orders",
          "include_metadata_columns": [
              "_change_type", "_commit_snapshot_sequence_number"],
          "streaming": True,
          "starting_strategy": "earliest",
          "poll_interval_seconds": 1,
      })
      | beam.managed.Write(beam.managed.ICEBERG, config={
          **catalog,
          "table": "shop.orders_mirror",
          "mode": "merge-on-read",
          "equality_columns": ["id"],
          "change_type_column": "_change_type",
          "triggering_frequency_seconds": 1,
      }))
{{< /highlight >}}
{{< highlight yaml >}}
# mirror.yaml
pipeline:
  type: chain
  transforms:
    - type: ReadFromIcebergCDC
      config:
        table: shop.orders
        catalog_name: quickstart
        catalog_properties: {type: hadoop, warehouse: "file:///tmp/beam-iceberg-cdc-quickstart"}
        include_metadata_columns: [_change_type, _commit_snapshot_sequence_number]
        streaming: true
        starting_strategy: earliest
        poll_interval_seconds: 1
    - type: WriteToIceberg
      config:
        table: shop.orders_mirror
        catalog_name: quickstart
        catalog_properties: {type: hadoop, warehouse: "file:///tmp/beam-iceberg-cdc-quickstart"}
        mode: merge-on-read
        equality_columns: [id]
        change_type_column: _change_type
        triggering_frequency_seconds: 1

options:
  streaming: true
  runner: PrismRunner
{{< /highlight >}}

<section class="language-java">

Within a few seconds, the mirror catches up on this first round of changes. Leave the streaming
pipeline running and open a new program to check the contents of `orders_mirror`:

</section>

<section class="language-py language-yaml">

Within 15 seconds or so, the mirror catches up on this first round of changes. Leave the streaming
pipeline running and open a new terminal to check the contents of `orders_mirror`:

</section>

{{< highlight java >}}
static void printTable(Map<String, Object> catalog, String table) {
  Map<String, Object> read = new HashMap<>(catalog);
  read.put("table", table);

  Pipeline p = Pipeline.create();
  p.apply(Managed.read(Managed.ICEBERG).withConfig(read))
      .getSinglePCollection()
      .apply(LogElements.info());
  p.run().waitUntilFinish();
}

printTable(catalog, "shop.orders_mirror");
{{< /highlight >}}
{{< highlight py >}}
def print_table(table):
  with beam.Pipeline() as p:
    _ = (
        p
        | beam.managed.Read(beam.managed.ICEBERG, config={**catalog, "table": table})
        | beam.Map(print))

print_table("shop.orders_mirror")
{{< /highlight >}}
{{< highlight yaml >}}
# check.yaml. Run it with:
# python -m apache_beam.yaml.main --yaml_pipeline_file=check.yaml \
#     --jinja_variables='{"table": "shop.orders_mirror"}'
pipeline:
  type: chain
  transforms:
    - type: ReadFromIceberg
      config:
        table: {{table}}
        catalog_name: quickstart
        catalog_properties: {type: hadoop, warehouse: "file:///tmp/beam-iceberg-cdc-quickstart"}
    - type: LogForTesting
{{< /highlight >}}

{{< paragraph class="language-yaml" >}}
`check.yaml` takes the table as a [Jinja variable](/documentation/sdks/yaml/#jinja-templatization).
`LogForTesting` logs each row as a line like `{"id": 1, "item": "apple", "qty": 3}`, among Beam's
other log output.
{{< /paragraph >}}

<section class="language-java language-py language-yaml">

You should see these rows, in any order:

<div class="expected-rows">

| id | item  | qty |
|---:|-------|----:|
| 1  | apple | 3   |
| 2  | pear  | 5   |
| 3  | plum  | 2   |

</div>

</section>

## 4. Write some changes and watch them arrive

{{< paragraph class="language-sql" >}}
Open a third shell. Connect it to the catalog, and check that both tables are there:
{{< /paragraph >}}

{{< highlight sql >}}
CREATE CATALOG quickstart TYPE 'iceberg'
PROPERTIES (
  'type' = 'hadoop',
  'warehouse' = 'file:///tmp/beam-iceberg-cdc-quickstart'
);
USE DATABASE quickstart.shop;
SHOW TABLES;
{{< /highlight >}}

<section class="language-sql">

Now change `orders` in two rounds, one snapshot each. Each change record carries two metadata
values:

- the record's change type (`op`)
- the record's sequence number (`seq`), which goes up with each change to the same key. Both halves of
  an update (`UPDATE_BEFORE` and `UPDATE_AFTER`) share one.

</section>

{{< highlight sql >}}
-- Snapshot 1: three new orders
INSERT INTO orders
WITH (
  'mode' = 'merge-on-read',
  'change_type_column' = 'op',
  'sequence_number_column' = 'seq')
  (id, item, qty, op, seq)
VALUES (1, 'apple', 3, 'INSERT', 1),
       (2, 'pear', 5, 'INSERT', 1),
       (3, 'plum', 2, 'INSERT', 1);
{{< /highlight >}}

<section class="language-sql">

Soon you'll see three inserts show up in the live view in the **first shell**, in any order:

<img class="center-block" src="/images/iceberg-cdc/live-view-snapshot-1.gif"
     alt="The live view in the first shell: three inserts appear, for apple, pear and plum.">

Query both tables. Each should hold the same three orders.

</section>

{{< highlight sql >}}
SELECT * FROM orders;
SELECT * FROM orders_mirror;
{{< /highlight >}}

{{< paragraph class="language-sql" >}}
Now make a second round of changes:
{{< /paragraph >}}

{{< paragraph class="language-java" >}}
Now make a second round of changes from another program, using the
helpers from step 2:
{{< /paragraph >}}

{{< paragraph class="language-py" >}}
Now make a second round of changes from another script, using the helpers from step 2:
{{< /paragraph >}}

{{< paragraph class="language-yaml" >}}
Now make a second round of changes from another terminal:
{{< /paragraph >}}

{{< highlight sql >}}
-- Snapshot 2: change order 1, delete order 2, add order 4
INSERT INTO orders
WITH (
  'mode' = 'merge-on-read',
  'change_type_column' = 'op',
  'sequence_number_column' = 'seq')
  (id, item, qty, op, seq)
VALUES (1, 'apple', 3, 'UPDATE_BEFORE', 2),
       (1, 'apple', 4, 'UPDATE_AFTER', 2),
       (2, 'pear', 5, 'DELETE', 2),
       (4, 'kiwi', 1, 'INSERT', 2);
{{< /highlight >}}
{{< highlight java >}}
// Snapshot 2: change order 1, delete order 2, add order 4
applyChanges(catalog,
    change(1, "apple", 3, "UPDATE_BEFORE", 2),
    change(1, "apple", 4, "UPDATE_AFTER", 2),
    change(2, "pear", 5, "DELETE", 2),
    change(4, "kiwi", 1, "INSERT", 2));
{{< /highlight >}}
{{< highlight py >}}
# Snapshot 2: change order 1, delete order 2, add order 4
apply_changes(
    change(1, "apple", 3, "UPDATE_BEFORE", 2),
    change(1, "apple", 4, "UPDATE_AFTER", 2),
    change(2, "pear", 5, "DELETE", 2),
    change(4, "kiwi", 1, "INSERT", 2))
{{< /highlight >}}
{{< highlight yaml >}}
# snapshot_2.yaml: change order 1, delete order 2, add order 4
pipeline:
  type: chain
  transforms:
    - type: Create
      config:
        elements:
          - {id: 1, item: apple, qty: 3, op: UPDATE_BEFORE, seq: 2}
          - {id: 1, item: apple, qty: 4, op: UPDATE_AFTER, seq: 2}
          - {id: 2, item: pear, qty: 5, op: DELETE, seq: 2}
          - {id: 4, item: kiwi, qty: 1, op: INSERT, seq: 2}
    - type: WriteToIceberg
      config:
        table: shop.orders
        catalog_name: quickstart
        catalog_properties: {type: hadoop, warehouse: "file:///tmp/beam-iceberg-cdc-quickstart"}
        mode: merge-on-read
        equality_columns: [id]
        change_type_column: op
        sequence_number_column: seq
{{< /highlight >}}

<section class="language-sql">

You'll see four new changes stream into the live view:

<img class="center-block" src="/images/iceberg-cdc/live-view-snapshot-2.gif"
     alt="The live view: below the first three inserts, four more changes appear: +I for kiwi, -D for
     pear, and -U and +U for apple's old and new quantities.">

Query both tables again to verify the changes were applied correctly:

</section>

{{< highlight sql >}}
SELECT * FROM orders;
SELECT * FROM orders_mirror;
{{< /highlight >}}

<section class="language-java language-py language-yaml">

Run the check from step 3 again to verify the changes were applied correctly to `orders` and its downstream mirror:

</section>

{{< highlight java >}}
printTable(catalog, "shop.orders");
printTable(catalog, "shop.orders_mirror");
{{< /highlight >}}
{{< highlight py >}}
print_table("shop.orders")
print_table("shop.orders_mirror")
{{< /highlight >}}
{{< highlight yaml >}}
# Run check.yaml for each table:
# python -m apache_beam.yaml.main --yaml_pipeline_file=check.yaml \
#     --jinja_variables='{"table": "shop.orders"}'
# python -m apache_beam.yaml.main --yaml_pipeline_file=check.yaml \
#     --jinja_variables='{"table": "shop.orders_mirror"}'
{{< /highlight >}}

`orders_mirror` should now match `orders`, in any order:

<div class="expected-rows">

| id | item  | qty |
|---:|-------|----:|
| 1  | apple | 4   |
| 3  | plum  | 2   |
| 4  | kiwi  | 1   |

</div>

{{< paragraph class="language-sql" >}}
When you're done, press `q` in the first shell to close the live view, and stop the streaming
`INSERT` in the second shell with Ctrl+C.

{{< /paragraph >}}

{{< paragraph class="language-java language-py language-yaml" >}}
When you're done, stop the streaming pipeline with Ctrl+C.
{{< /paragraph >}}

## Run it on Dataflow

When you're ready to move off your machine, point the catalog at a REST catalog such as
[Lakehouse](/documentation/io/built-in/iceberg/#choose-your-catalog), and run on Dataflow.

{{< paragraph class="language-sql" >}}
Start the shell with the Dataflow runner. In this quickstart, do this for the
[**second shell**](#3-keep-a-table-in-sync-streaming):
{{< /paragraph >}}

{{% section class="language-sql" %}}
```shell
./beam-sql.sh --version 2.79.0 --runner dataflow --io iceberg
```
{{% /section %}}

{{< paragraph class="language-sql" >}}
Within the shell, set the following pipeline options:
{{< /paragraph >}}

{{< paragraph class="language-java" >}}
Add these dependencies. The Iceberg ones are for Lakehouse:
{{< /paragraph >}}

<section class="language-java">

{{< runner maven >}}
<dependency>
  <groupId>org.apache.beam</groupId>
  <artifactId>beam-runners-google-cloud-dataflow-java</artifactId>
  <version>{{< param release_latest >}}</version>
  <scope>runtime</scope>
</dependency>
<dependency>
  <groupId>org.apache.iceberg</groupId>
  <artifactId>iceberg-gcp</artifactId>
  <version>1.12.0</version>
  <scope>runtime</scope>
</dependency>
<dependency>
  <groupId>org.apache.iceberg</groupId>
  <artifactId>iceberg-gcp-bundle</artifactId>
  <version>1.12.0</version>
  <scope>runtime</scope>
</dependency>
{{< /runner >}}
{{< runner gradle >}}
dependencies {
  runtimeOnly("org.apache.beam:beam-runners-google-cloud-dataflow-java:{{< param release_latest >}}")
  runtimeOnly("org.apache.iceberg:iceberg-gcp:1.12.0")
  runtimeOnly("org.apache.iceberg:iceberg-gcp-bundle:1.12.0")
}
{{< /runner >}}

</section>

{{< paragraph class="language-java language-py language-yaml" >}}
Set the following pipeline options. In this quickstart, do this for the
[**mirror pipeline**](#3-keep-a-table-in-sync-streaming):
{{< /paragraph >}}

{{< highlight sql >}}
SET runner = DataflowRunner;
SET project = 'my-project';
SET region = 'us-central1';
SET tempLocation = 'gs://my-bucket/tmp';
{{< /highlight >}}
{{< highlight java >}}
Pipeline p =
    Pipeline.create(
        PipelineOptionsFactory.fromArgs(
                "--runner=DataflowRunner",
                "--project=my-project",
                "--region=us-central1",
                "--tempLocation=gs://my-bucket/tmp")
            .create());
{{< /highlight >}}
{{< highlight py >}}
# pip install 'apache-beam[gcp]>=2.79.0'
from apache_beam.options.pipeline_options import PipelineOptions

options = PipelineOptions(
    runner="DataflowRunner",
    project="my-project",
    region="us-central1",
    temp_location="gs://my-bucket/tmp",
    streaming=True)
with beam.Pipeline(options=options) as p:
  ...
{{< /highlight >}}
{{< highlight yaml >}}
# pip install 'apache-beam[gcp,yaml]>=2.79.0', then:
# python -m apache_beam.yaml.main --yaml_pipeline_file=mirror.yaml \
#     --runner=DataflowRunner --project=my-project --region=us-central1 \
#     --temp_location=gs://my-bucket/tmp
{{< /highlight >}}

On Dataflow, a streaming job keeps running after the shell or program exits. Cancel it from the
Dataflow console, or with `gcloud dataflow jobs cancel`.

## What to know before production

* Your source table needs a primary key on `NOT NULL` columns, which Iceberg calls identifier
  fields. If it doesn't have one, add it in a [Beam SQL shell](/documentation/dsls/sql/shell/#installation):
  `ALTER TABLE orders ADD PRIMARY KEY (id) NOT ENFORCED`.
* You may also stream changes from sources other than Iceberg, as long as:
  * Every row has a change type and a sequence number. As a key's sequence numbers go up, its event
    times should never go down. Both halves of an update share a sequence number.
  * Every update comes with its `UPDATE_BEFORE`, and `INSERT` only shows up for keys the target
    doesn't have yet. Otherwise the target ends up with duplicates. If your source only sends
    after-images, or might resend inserts, please set `upsert` to `true`.
  * If the target is partitioned on non-key columns, `UPDATE_BEFORE` and `DELETE` rows need to carry the
    row's old values in those partition columns. Otherwise the old row never gets deleted.
* The source skips expired snapshots, so keep your snapshot expiration long enough to survive potential
  outages in your upstream lakehouse.
* If your use-case requires you to restart streaming jobs, give the sink a fixed `sink_id`. Each run gets a random one by
  default. A job restarted with the same `sink_id` and `triggering_frequency_seconds` will be able to
  recognize and skip already-committed windows. This is helpful when your restart may overlap with a previous run.
* A running pipeline doesn't handle schema changes (e.g. an added column). To change the schema, stop the
  pipeline, make the same change to both tables, and start it again.
* If a record arrives after its commit window has closed, it will be considered late and routed to the
  sink's `dead_letter` output instead of the table. Watch the `deadLetterRecords` counter.

## Options

The most useful options for the source:

| Option | Default | What it does |
|---|---|---|
| `streaming` | `false` | Keep polling for new snapshots. |
| `poll_interval_seconds` | `60` | How often to poll, in streaming. |
| `starting_strategy` | `earliest` in batch, `latest` in streaming | Where to start reading. `latest` includes the current snapshot. Can't be combined with `from_snapshot` or `from_timestamp`. |
| `from_snapshot` / `from_timestamp` | none | Start at this snapshot ID, which must exist, or at the first snapshot committed at or after this time (epoch milliseconds). If there is none yet, a batch read returns no rows and a streaming read waits for one. |
| `to_snapshot` / `to_timestamp` | none | Stop at this snapshot ID or time (inclusive). |
| `include_metadata_columns` | none | Add `_change_type`, `_commit_snapshot_sequence_number` or `_commit_snapshot_id` to each row (v3 tables have more). When feeding the CDC sink, include only the first two. In SQL, select them by name instead. |
| `filter`, `keep`, `drop` | none | Filter rows and pick columns. Key columns can't be dropped. |
| `watermark_column` | none | Use this column as event time instead of the commit time. Leave it unset when feeding the CDC sink. |

And for the sink, when `mode` is `merge-on-read`:

| Option | Default | What it does |
|---|---|---|
| `equality_columns` | the table's identifier fields | The primary key. Required when the sink creates the table. |
| `sequence_number_column` | `_commit_snapshot_sequence_number` | Non-null `BIGINT` column that goes up with each change to a key. |
| `change_type_column` | none | Non-null string column holding the change type. Always set it. |
| `change_type_map` | none | Maps source change type values to `INSERT`, `UPDATE_BEFORE`, `UPDATE_AFTER` or `DELETE`. |
| `upsert` | `false` | Treat inserts and updates as upserts, ignoring `UPDATE_BEFORE` rows. Partition columns must be key columns. |
| `triggering_frequency_seconds` | required in streaming | Length of each commit window. |
| `sink_id` | random per run | Identifies the sink's commits so restarts skip what's done. |
| `token_heartbeat_seconds` | off | In streaming, commit an empty snapshot this often while idle, so the latest stamp survives snapshot expiry. |
| `num_shards` | `16` | Write parallelism per table. Each window writes up to this many data files, and as many delete files, per partition it touches. |
| `shards_per_partition` | `num_shards` | The most shards one partition may use. Lower it for fewer files per partition. Below `num_shards`, partition columns must be key columns. |
| `allowed_lateness_seconds` | `21600` | How late a record can be and still reach `dead_letter`, rather than being dropped. |
| `snapshot_properties` | none | Extra properties to add to each commit's snapshot summary. Keys starting with `beam.cdc.` are reserved. |
| `sorter_memory_mb` | `100` | Memory for the sort before writing, before it spills to disk. |
| `error_handling` | fail the bundle | `{"output": "errors"}` sends invalid records (an unknown change type, or a null key or sequence number) to an output named `errors`. Without it, a bad record fails its bundle, which stalls a streaming pipeline. SQL has no `errors` output yet. |

## Learn more

* [Stream changes between Iceberg tables with Apache Beam](/blog/iceberg-cdc-sql/)
* [Iceberg CDC sink design doc](https://s.apache.org/beam-iceberg-cdc-sink)
* [Apache Iceberg I/O connector](/documentation/io/built-in/iceberg/)
* [IcebergIO Javadoc](https://beam.apache.org/releases/javadoc/{{< param release_latest >}}/org/apache/beam/sdk/io/iceberg/IcebergIO.html)
* Managed I/O reference: [Iceberg CDC read](/documentation/io/managed-io/#iceberg_cdc-read) and
  [Iceberg write](/documentation/io/managed-io/#iceberg-write)
