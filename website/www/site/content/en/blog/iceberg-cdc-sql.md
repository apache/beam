---
title: "Stream changes between Iceberg tables with Apache Beam"
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

You can now use Apache Beam to continuously **keep one Iceberg table in sync with another**.
Beam's Iceberg change data capture (CDC) [source](/blog/iceberg-cdc-source/) reads a table's row-level changes (inserts, updates, deletes),
and its merge-on-read [sink](/blog/iceberg-cdc-sink/) applies them downstream by primary key. Try it with this
**[quickstart](/documentation/io/built-in/iceberg-cdc/)**.

Most engines only stream appends out of an Iceberg table. **Beam streams updates and deletes too.**

<!--more-->

It's common for teams to keep a backup copy of their Iceberg table. This backup might be a replica
in another region or different cloud provider, a copy in a new catalog they're migrating to, a
smaller table containing only the rows and columns the team needs, or a masked copy that hides
sensitive data from teams that shouldn't see it. **The source data isn't static**,
though. Orders get updated, erroneous records get fixed, and customers send GDPR requests to delete
their data. Consequently, you need a process to let these updates reach the downstream copy in
real-time.

If you're in this situation, one option you have is to **reload the entire table periodically**.
However, this approach uses a lot of compute resources, and the downstream copy still becomes
out-of-sync between reload operations. Alternatively, you could **write your own custom pipeline** to
forward the change log downstream, but then you need to deal with all the mechanics yourself, such
as applying changes in the correct order, handling deletes, and recovering from a failure without
applying the same change twice.

Beam's new Iceberg CDC source and sink handle these details for you. You just have to run
**one streaming pipeline** and it will keep the two tables continuously in sync. You also
have the option to run **periodic, sequential batch jobs**, each job picking up where the
last one left off.

## What it looks like

Consider an `orders` table that gets three new orders, followed by an update that
modifies order 1, deletes order 2, and adds order 4. Read as a stream of changes, these commits
look like this:

<img class="center-block" src="/images/blog/iceberg-cdc/changes-live-view.gif"
     alt="A Beam SQL live view of CHANGES(orders). Three inserts stream in from the first commit,
     then the second commit's changes: order 1's old and new versions, a delete of order 2, and an
     insert of order 4.">

Each row is one change to one key. The `op` column describes what happened, and
`_commit_snapshot_sequence_number` says which commit it came from. You can see each commit's net
effect on each key, and an update shows up as two rows: the old version and the new one. You can
apply these commits to a downstream table to keep that table in sync.

Here's a Beam SQL pipeline that applies commits to a downstream table continuously:

```sql
INSERT INTO lakehouse.sales.orders_mirror
  WITH ('mode' = 'merge-on-read',
        'change_type_column' = '_change_type',
        'triggering_frequency_seconds' = 15)
SELECT *, _change_type, _commit_snapshot_sequence_number
FROM CHANGES(lakehouse.sales.orders, 123)
  WITH ('streaming' = true, 'poll_interval_seconds' = 5);
```

The `CHANGES` function reads `orders` as a stream of changes, starting at snapshot `123`. Specifying
`'streaming' = true` keeps the stream reading continuously as new snapshots are observed.

On the write side, `'mode' = 'merge-on-read'` turns on the CDC sink, which applies each change to
`orders_mirror` by primary key, in 15-second commit windows.

The [Iceberg CDC quickstart](/documentation/io/built-in/iceberg-cdc/) walks through a full
step-by-step example.

### Masked copies and medallion architectures

The `SELECT` can do more than copy. It can filter rows, drop columns and mask values, and the sink
still applies each change to the right key. So the same pipeline can keep a masked copy of a table
with personal data, or move changes through a medallion architecture, one pipeline per hop. This one
cleans bronze into silver:

```sql
INSERT INTO lakehouse.silver.customers
  WITH ('mode' = 'merge-on-read',
        'change_type_column' = '_change_type',
        'triggering_frequency_seconds' = 15)
SELECT id, name,
       '***' || SUBSTRING(email FROM POSITION('@' IN email)) AS email,
       country, _change_type, _commit_snapshot_sequence_number
FROM CHANGES(lakehouse.bronze.customers)
  WITH ('streaming' = true)
WHERE country IS NOT NULL;
```

When a customer's data is deleted in bronze, the delete reaches silver too, and the next hop carries
it on to gold.

## How it works

Two shorter posts go into each half:

* [Read an Iceberg table's change stream with Apache Beam](/blog/iceberg-cdc-source/) covers
  how the source turns each snapshot into changes, and what Spark and Flink can't read yet.
* [Apply row-level changes to Iceberg with Apache Beam](/blog/iceberg-cdc-sink/) covers
  how the sink orders each key's changes, and why it writes less than Flink's sink for keys that
  change often.

## Try it

You can use the Iceberg CDC source and sink starting with the Beam SDK 2.79.0 version. This works for table format v2 or v3.
Equality deletes are not allowed in v4, and the sink will later be updated to accommodate for
that.

Start with the **[Iceberg CDC quickstart](/documentation/io/built-in/iceberg-cdc/)**. For the design details, see the
[design doc](https://s.apache.org/beam-iceberg-cdc-sink).

We'd love to hear how you use this feature. Tell us on the
[user mailing list](/community/contact-us/) or open an issue on
[GitHub](https://github.com/apache/beam/issues).
