---
type: languages
title: "Beam Java SDK Extensions"
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
# Apache Beam Java SDK Extensions

## Join-library

Join-library provides inner join, outer left join, and outer right join functions. The aim
is to simplify the most common cases of join to a simple function call.

The functions are generic and support joins of any Beam-supported types.
Input to the join functions are `PCollections` of `Key` / `Value`s. Both
the left and right `PCollection`s need the same type for the key. All the join
functions return a `Key` / `Value` where `Key` is the join key and value is
a `Key` / `Value` where the key is the left value and right is the value.

For outer joins, the user must provide a value that represents `null` because `null`
cannot be serialized.

Example usage:

```
PCollection<KV<String, String>> leftPcollection = ...
PCollection<KV<String, Long>> rightPcollection = ...

PCollection<KV<String, KV<String, Long>>> joinedPcollection =
  Join.innerJoin(leftPcollection, rightPcollection);
```


## Sorter

This module provides the `SortValues` transform, which takes a `PCollection<KV<K, Iterable<KV<K2, V>>>>` and produces a `PCollection<KV<K, Iterable<KV<K2, V>>>>` where, for each primary key `K` the paired `Iterable<KV<K2, V>>` has been sorted by the byte encoding of secondary key (`K2`). It is an efficient and scalable sorter for iterables, even if they are large (do not fit in memory).

### Caveats

- This transform performs value-only sorting; the iterable accompanying each key is sorted, but *there is no relationship between different keys*, as Beam does not support any defined relationship between different elements in a `PCollection`.
* Each `Iterable<KV<K2, V>>` is sorted on a single worker using local memory and disk. This means that `SortValues` may be a performance and/or scalability bottleneck when used in different pipelines. For example, users are discouraged from using `SortValues` on a `PCollection` of a single element to globally sort a large `PCollection`. A (rough) estimate of the number of bytes of disk space utilized if sorting spills to disk is `numRecords * (numSecondaryKeyBytesPerRecord + numValueBytesPerRecord + 16) * 3`.

### Options

* The user can customize the temporary location used if sorting requires spilling to disk and the maximum amount of memory to use by creating a custom instance of `BufferedExternalSorter.Options` to pass into `SortValues.create`.

### Example usage of `SortValues`

```
PCollection<KV<String, KV<String, Integer>>> input = ...

// Group by primary key, bringing <SecondaryKey, Value> pairs for the same key together.
PCollection<KV<String, Iterable<KV<String, Integer>>>> grouped =
    input.apply(GroupByKey.<String, KV<String, Integer>>create());

// For every primary key, sort the iterable of <SecondaryKey, Value> pairs by secondary key.
PCollection<KV<String, Iterable<KV<String, Integer>>>> groupedAndSorted =
    grouped.apply(
        SortValues.<String, String, Integer>create(BufferedExternalSorter.options()));
```
