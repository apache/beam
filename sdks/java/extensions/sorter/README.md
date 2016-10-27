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

Sorter
============

The provided Sorter can be used to sort large (more than can fit to memory)
amounts of data within the context of a Beam pipeline. It will default to in
memory sorting and spill to disk when the buffer is full.

ExternalSorter class implements the external sort algorithm (currently simply wrapping
the one from Hadoop). InMemorySorter implements the in memory sort and
BufferedExternalSorter uses fallback logic to combine the two.

SortValues is a PTransform that uses the BufferedExternalSorter to perform secondary key sorting.

Example of how to use sorter:

    PCollection<KV<String, KV<String, Integer>>> input = ...

    // Group by primary key, bringing <SecondaryKey, Value> pairs for the same key together.
    PCollection<KV<String, Iterable<KV<String, Integer>>>> grouped =
        input.apply(GroupByKey.<String, KV<String, Integer>>create());

    // For every primary key, sort the iterable of <SecondaryKey, Value> pairs by secondary key.
    PCollection<KV<String, Iterable<KV<String, Integer>>>> groupedAndSorted =
        grouped.apply(
            SortValues.<String, String, Integer>create(new BufferedExternalSorter.Options()));
