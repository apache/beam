---
title: "Apply row-level changes to Iceberg with Apache Beam"
date: 2026-11-22 00:00:00 -0800
categories:
  - blog
authors:
  - ahmedabu98
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

You can now use Apache Beam to **apply a stream of row-level changes (inserts, updates, deletes) to an Iceberg
table** by primary key. The merge-on-read sink works in streaming and batch, and it takes changes
from any source: another Iceberg table, a database's change log, or a Kafka topic. Try it with
this **[quickstart](/documentation/io/built-in/iceberg-cdc/)**.

<!--more-->

Applying changes to a table has a few hard parts. The changes have to land in the order they
happened. Deletes have to find the rows they remove. And if a commit is retried, or the job
restarts, the same change can't be applied twice. **Beam's CDC sink handles these details for
you.**

## What the sink expects

The sink takes a collection of change rows. Besides the table's own columns, each row carries
two more:

- the change type: `INSERT`, `UPDATE_BEFORE`, `UPDATE_AFTER` or `DELETE`
- a sequence number that goes up with each change to the same key. Both halves of an update share
  one.

Here's a pipeline that applies those changes to `sales.orders`:

{{< highlight sql >}}
INSERT INTO lakehouse.sales.orders
  WITH ('mode' = 'merge-on-read',
        'change_type_column' = 'op',
        'sequence_number_column' = 'seq',
        'triggering_frequency_seconds' = 10)
SELECT id, item, qty, op, seq FROM order_changes;
{{< /highlight >}}
{{< highlight java >}}
Map<String, Object> config =
    Map.of(
        "catalog_properties", catalogProperties,
        "table", "sales.orders",
        "mode", "merge-on-read",
        "equality_columns", List.of("id"),
        "change_type_column", "op",
        "sequence_number_column", "seq",
        "triggering_frequency_seconds", 10);

changes.apply(Managed.write(Managed.ICEBERG).withConfig(config));
{{< /highlight >}}
{{< highlight py >}}
changes | beam.managed.Write(beam.managed.ICEBERG, config={
    "catalog_properties": {...},
    "table": "sales.orders",
    "mode": "merge-on-read",
    "equality_columns": ["id"],
    "change_type_column": "op",
    "sequence_number_column": "seq",
    "triggering_frequency_seconds": 10,
})
{{< /highlight >}}
{{< highlight yaml >}}
- type: WriteToIceberg
  input: Changes
  config:
    catalog_properties: {...}
    table: sales.orders
    mode: merge-on-read
    equality_columns: [id]
    change_type_column: op
    sequence_number_column: seq
    triggering_frequency_seconds: 10
{{< /highlight >}}

Setting `mode` to `merge-on-read` turns on the CDC sink. `equality_columns` is the primary key,
and the sink uses it to create the table if it doesn't exist yet. In SQL, the table has to exist
already, and the key comes from its `PRIMARY KEY`. The sink drops the `op` and `seq` columns before
writing to the table.

If your changes use their own labels, like Debezium's `c`, `u` and `d`, map them with
`change_type_map`. If your source doesn't send a before-image for each update, set `upsert` to
`true`, and the sink applies inserts and updates as upserts. For a Debezium stream, that looks
like this:

{{< highlight sql >}}
  WITH (...,
        'change_type_map' = MAP['c', 'INSERT',
                                'u', 'UPDATE_AFTER',
                                'd', 'DELETE'],
        'upsert' = true)
{{< /highlight >}}
{{< highlight java >}}
Map<String, Object> config =
    Map.of(
        ...,
        "change_type_map",
            Map.of("c", "INSERT", "u", "UPDATE_AFTER", "d", "DELETE"),
        "upsert", true);
{{< /highlight >}}
{{< highlight py >}}
config={
    ...
    "change_type_map": {"c": "INSERT", "u": "UPDATE_AFTER", "d": "DELETE"},
    "upsert": True,
}
{{< /highlight >}}
{{< highlight yaml >}}
config:
  ...
  change_type_map: {c: INSERT, u: UPDATE_AFTER, d: DELETE}
  upsert: true
{{< /highlight >}}

If your changes come from Beam's [Iceberg CDC source](/blog/iceberg-cdc-source/), point
`change_type_column` at `_change_type`. You don't need to set `sequence_number_column`, because it
already defaults to `_commit_snapshot_sequence_number`.

## How the sink works

<img class="center-block" src="/images/blog/iceberg-cdc/cdc-sink-pipeline.png"
     alt="The CDC sink: input records carry a sequence number and a change type. They are assigned
     keys, grouped by table and key shard into commit windows, sorted by key and sequence number,
     written as data files and delete files, then committed. Invalid records fail the pipeline or go
     to an error output, and late records go to a dead-letter output.">

The sink groups changes into commit windows, one every `triggering_frequency_seconds` in streaming,
and commits each window as a single Iceberg snapshot.

A key's changes have to be applied in the order they happened, otherwise the table will end up with
incorrect values. Beam doesn't keep elements in order as they move between steps, so the sink sorts
each key's changes by a monotonically increasing sequence number provided by the upstream source.

After the sort, a key's changes in a given window sit together and in order, so the writer can skip
the versions in between. For each key in a window, Beam writes **at most one data row** (the latest
version) and at most one equality delete.

In comparison, the Flink sink writes every change as it arrives, using equality deletes to remove
existing records and position deletes to cancel intermediate changes between checkpoints. Here are
a few scenarios highlighting the difference in behavior:

<style>
  /* The sink comparison: a dark header, with Beam's column highlighted in the brand orange. */
  .sink-comparison table {
    width: 100%; margin: 8px 0 24px; font-size: 15px;
    border: 1px solid #e5e5e5; border-collapse: separate; border-spacing: 0; border-radius: 8px;
    overflow: hidden; box-shadow: 0 3px 20px rgba(0, 0, 0, 0.075);
  }
  .sink-comparison th { background: #37424b; color: #fff; font-weight: 600; padding: 12px 16px; }
  .sink-comparison td { padding: 12px 16px; border-top: 1px solid #eee; vertical-align: top; }
  .sink-comparison th:last-child { background: #ff6d00; }
  .sink-comparison td:last-child { background: #ffede5; font-weight: 600; }
</style>

<div class="sink-comparison">

| One key's changes within one window/checkpoint interval | Flink writes | Beam writes |
|---|---|---|
| Existing key, updated twice | 2 data rows, 1 equality delete, 1 position delete | 1 data row, 1 equality delete |
| New key: inserted, updated twice, deleted, inserted again | 4 data rows, 3 position deletes | 1 data row |

</div>

Beam's savings are emphasized for keys that change often within a window. In a batch pipeline, the
whole run is one window. The tradeoff cost is that Beam will sort per window, which is an
unavoidable sunk cost.

### Restarts and late data

Every commit is stamped with the sink's `sink_id` and the end of its window. If a commit is
retried, or a streaming job restarts with the same `sink_id` and window length, the sink reads the
stamps and skips the windows that already landed. Each run gets a random `sink_id` by default, so
**set a fixed one if you plan to restart your job**. Don't reuse `sink_id` across batch pipelines,
as further reuses will be interpreted as duplicates and will be skipped at commit time.

A change that arrives after its window has closed will be considered late. Instead of landing in the table, it goes
to the sink's `dead_letter` output, so watch the `deadLetterRecords` counter. Changes more than
`allowed_lateness_seconds` late (six hours by default) are dropped.

## Try it

You can use the CDC sink feature starting with the Beam SDK 2.79.0 version, in Java, Python, YAML
and Beam SQL. Java also has `IcebergIO.writeCdcRows`. This works for table format v2 or v3.
Equality deletes are not allowed in v4, and the sink will later be updated to accommodate for that.

Start with the **[Iceberg CDC quickstart](/documentation/io/built-in/iceberg-cdc/)**. Before you go
to production, read its
[production notes](/documentation/io/built-in/iceberg-cdc/#what-to-know-before-production). For
the design details, see the [design doc](https://s.apache.org/beam-iceberg-cdc-sink). To see the
sink and Beam's CDC source working together, read
[Stream changes between Iceberg tables with Apache Beam](/blog/iceberg-cdc-sql/).

We'd love to hear how you use this feature. Tell us on the
[user mailing list](/community/contact-us/) or open an issue on
[GitHub](https://github.com/apache/beam/issues).
