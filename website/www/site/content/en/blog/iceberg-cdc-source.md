---
title: "Read an Iceberg table's change stream with Apache Beam"
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

You can now **continuously read an Iceberg table's row-level changes (inserts, updates and deletes)** using an Apache Beam
streaming pipeline. Beam's native Iceberg change data capture (CDC) source periodically polls a table's snapshot history
and produces row-level changes. It can read a range of commits in batch too. Try it with this
**[quickstart](/documentation/io/built-in/iceberg-cdc/)**.

Most engines only stream appends out of an Iceberg table. **Beam streams updates and deletes too.**

<!--more-->

Plenty of systems need to follow a table as it changes. A search index has to drop a product when
it's deleted, and a cache has to pick up a price when it's corrected. A copy of the table has to
do both. **Appends alone don't cover this**, because many of those changes are updates and
deletes: orders get amended, erroneous records get fixed, and customers send GDPR requests to
delete their data.

## What it looks like

Consider an `orders` table that gets three new orders, followed by an update that modifies order 1,
deletes order 2, and adds order 4. Read as a stream of changes, these commits look like this:

<img class="center-block" src="/images/blog/iceberg-cdc/changes-live-view.gif"
     alt="A Beam SQL live view of CHANGES(orders). Three inserts stream in from the first commit,
     then the second commit's changes: order 1's old and new versions, a delete of order 2, and an
     insert of order 4.">

Each row is one change to one key. The `op` column describes what happened, and
`_commit_snapshot_sequence_number` says which commit it came from. An update shows up as two rows:
the old version and the new one.

Here's how to read a table's changes as a stream:

{{< highlight sql >}}
SELECT *, _change_type, _commit_snapshot_sequence_number
FROM CHANGES(lakehouse.sales.orders)
WITH ('streaming' = true, 'poll_interval_seconds' = 5);
{{< /highlight >}}
{{< highlight java >}}
Map<String, Object> config =
    Map.of(
        "table", "sales.orders",
        "catalog_name", "lakehouse",
        "catalog_properties", catalogProperties,
        "streaming", true,
        "poll_interval_seconds", 5,
        "include_metadata_columns",
            List.of("_change_type", "_commit_snapshot_sequence_number"));

PCollection<Row> changes =
    pipeline
        .apply(Managed.read(Managed.ICEBERG_CDC).withConfig(config))
        .getSinglePCollection();

changes.apply(LogElements.stdout());
{{< /highlight >}}
{{< highlight py >}}
changes = pipeline | beam.managed.Read("iceberg_cdc", config={
    "table": "sales.orders",
    "catalog_name": "lakehouse",
    "catalog_properties": {...},
    "streaming": True,
    "poll_interval_seconds": 5,
    "include_metadata_columns": [
        "_change_type", "_commit_snapshot_sequence_number"],
})

changes | beam.Map(print)
{{< /highlight >}}
{{< highlight yaml >}}
pipeline:
  type: chain
  transforms:
    - type: ReadFromIcebergCDC
      config:
        table: sales.orders
        catalog_name: lakehouse
        catalog_properties: {...}
        streaming: true
        poll_interval_seconds: 5
        include_metadata_columns:
          [_change_type, _commit_snapshot_sequence_number]
    - type: LogForTesting
options:
  streaming: true
{{< /highlight >}}

Turning on `streaming` keeps the source polling for new snapshots every `poll_interval_seconds`, and
their changes come out as they're committed. The metadata columns are opt-in: list them in
`include_metadata_columns`, or select them by name in SQL. `_change_type` carries the same
information as the `op` column above, spelled out as `INSERT`, `UPDATE_BEFORE`, `UPDATE_AFTER` or
`DELETE`.

From there, you can send each change wherever it needs to go. To apply the changes to another
Iceberg table, use Beam's [CDC sink](/blog/iceberg-cdc-sink/). You can also choose where to start
and stop reading, by snapshot or by time. The quickstart's
[options table](/documentation/io/built-in/iceberg-cdc/#options) lists them all.

## How the source works

For each snapshot, the source looks at the rows the commit removed and the rows it added, and
groups them by primary key:

* A removed row and an added row with the same key become an `UPDATE_BEFORE` and `UPDATE_AFTER`
  pair.
* A key with only a removed row becomes a `DELETE`, and a key with only an added row becomes an
  `INSERT`.
* A copy-on-write commit rewrites whole data files, so it removes and re-adds rows that didn't
  change. The source drops those pairs, and **you only see the real delta**.

You get each commit's net effect on each key. If one commit changes a key twice, you see the version
before the commit and the version after it, and nothing in between.

Iceberg's own changelog scan doesn't handle delete files yet, so until it does, Beam uses a scan
based on [apache/iceberg#14264](https://github.com/apache/iceberg/pull/14264).

To pair changes up, the source needs the table to have a primary key on `NOT NULL` columns, which
Iceberg calls identifier fields. The quickstart's
[production notes](/documentation/io/built-in/iceberg-cdc/#what-to-know-before-production) show how
to add one.

## Try it

You can use the CDC source starting with the Beam SDK 2.79.0 version, in Java, Python, YAML and
Beam SQL. It works with tables in format v2 or v3.

Start with the **[Iceberg CDC quickstart](/documentation/io/built-in/iceberg-cdc/)**. To see the
source and sink working together, read
[Stream changes between Iceberg tables with Apache Beam](/blog/iceberg-cdc-sql/).

We'd love to hear how you use this feature. Tell us on the
[user mailing list](/community/contact-us/) or open an issue on
[GitHub](https://github.com/apache/beam/issues).
