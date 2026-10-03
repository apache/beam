---
title:  "Apache Beam 2.77.0"
date:   2026-09-?? 14:00:00 -0500
categories:
  - blog
  - release
authors:
  - vterentev
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

We are happy to present the new 2.77.0 release of Beam.
This release includes both improvements and new functionality.
See the [download page](/get-started/downloads/#2770-2026-09-??) for this release.

<!--more-->

For more information on changes in 2.77.0, check out the [detailed release notes](https://github.com/apache/beam/milestone/44).

## Highlights

### I/Os

* Added `schema_update_options` to `WriteToBigQuery` file loads, allowing BigQuery load jobs to add nullable fields or relax required fields when appending data (Python) ([#21141](https://github.com/apache/beam/issues/21141)).
* BigQueryIO now supports reading BigQuery Lakehouse runtime catalog (BigLake metastore) Iceberg tables with the Storage Read API, using 4-part `project.catalog.namespace.table` identifiers (or a `TableReference` with a composite `catalog.namespace` dataset id). Previously such references were silently mis-parsed (Java) ([#39597](https://github.com/apache/beam/issues/39597)) .
* SolaceIO now supports reading and writing binary and text content data payload (Java) ([#39875](https://github.com/apache/beam/issues/39875)).
* ClickHouseIO: support writing `Decimal(P, S)` / `Decimal32/64/128/256` columns (Java) ([#39840](https://github.com/apache/beam/issues/39840)).
* SolaceIO now supports reading and writing user properties (message metadata) (Java) ([#40099](https://github.com/apache/beam/issues/40099)).
* [IcebergIO] AddFiles (`IcebergAddFiles` in YAML) can evolve the table schema before registering files, with `schema_evolution_options`, `required_columns`, `incompatible_schema_handling` and `unverifiable_file_handling` (Java/YAML, batch only) ([#40144](https://github.com/apache/beam/issues/40144)).

### New Features / Improvements

* (Java/Python) `Watch` can bound its deduplication state by event time, retiring an output key once the greatest emitted timestamp has moved more than the allowed lateness past it. Java adds `Watch.growthOf(...).withTimestampCursor()`. Python adds `allowed_lateness` for the existing `timestamp_cursor` option ([#18459](https://github.com/apache/beam/issues/18459)).
* (Java) Spark Structured Streaming runner: stateful ParDo with state, timers, `@RequiresTimeSortedInput` and tagged outputs is now supported in batch mode ([#39779](https://github.com/apache/beam/issues/39779)).
* (Python) Added support for Vertex AI Model Monitoring V2 in RunInference ([#39738](https://github.com/apache/beam/issues/39738)).
* [Flink Runner] Added opt-in static round-robin split assignment for small bounded sources via the new `sourceStaticSplitThresholdMb` pipeline option. The default of 0 keeps the existing lazy pull-based assignment ([#39873](https://github.com/apache/beam/issues/39873)).
* Added automatic caching of bounded, single-pane side-input views for classic Java Flink DataStream execution ([#39866](https://github.com/apache/beam/issues/39866)).
* (Python) Added `Sample.Any`, the Python equivalent of Java's `Sample.any`, which returns up to n arbitrary elements from a PCollection ([#18552](https://github.com/apache/beam/issues/18552)).

### Breaking Changes

* Portable Java SDK now encodes SchemaCoders in a portable way ([#34672](https://github.com/apache/beam/issues/34672)).
  - Original custom Java coder encoding can still be obtained using [StreamingOptions.setUpdateCompatibilityVersion("2.76")](https://github.com/apache/beam/blob/2cf0930e7ae1aa389c26ce6639b584877a3e31d9/sdks/java/core/src/main/java/org/apache/beam/sdk/options/StreamingOptions.java#L47) ([#34672](https://github.com/apache/beam/issues/34672)).
  - Fixes ([#36496](https://github.com/apache/beam/issues/36496)), ([#30276](https://github.com/apache/beam/issues/30276)), ([#29245](https://github.com/apache/beam/issues/29245)).
* (Python) `TensorRTEngineHandlerNumPy` now requires TensorRT 10 or later. TensorRT 8.x is no longer supported, since TensorRT 10 removed the engine binding API the handler was written against ([#36306](https://github.com/apache/beam/issues/36306)).
  - Engines serialized by TensorRT 8.x must be rebuilt, as an engine can only be deserialized by the major version that built it.
  - TensorRT 10 and later require a GPU with compute capability 7.5 or higher, which excludes NVIDIA Pascal and Volta GPUs.
  - If dropping TensorRT 8.x support is a hard blocker for you, please comment on ([#36306](https://github.com/apache/beam/issues/36306)).

### Bugfixes

* (Java) Fixed the Spark runner firing processing-time timers in reverse timestamp order ([#39824](https://github.com/apache/beam/issues/39824)).
* (Java) Fixed the Spark runner dropping the stored watermark of a streaming source with no update in a batch ([#39822](https://github.com/apache/beam/issues/39822)).
* (Python) Fixed incorrect profiler options handling on portable runners ([#39613](https://github.com/apache/beam/issues/39613)).
* (Java) KafkaIO dynamic reads no longer require the obsolete `beam_fn_api` experiment ([#29998](https://github.com/apache/beam/issues/29998)).
* (Prism) Self-checkpointing splittable DoFns now resume after their requested delay instead of immediately, so polling SDFs no longer busy-spin ([#39848](https://github.com/apache/beam/issues/39848)).
* (Java) MongoDbIO read splitting now preserves non-ObjectId `_id` types (e.g. string ids) instead of failing to parse the generated range filters ([#39900](https://github.com/apache/beam/issues/39900)).
* (Go) Fixed GCS glob matching silently dropping objects when the glob pattern contains multi-byte characters ([#39969](https://github.com/apache/beam/issues/39969)).
* (Python) Fixed `TensorRTEngineHandlerNumPy` failing with `CUDA_ERROR_INVALID_VALUE` on models with a single-element input or output tensor ([#36306](https://github.com/apache/beam/issues/36306)).
* (Python) Fixed `PickleCoder`/`_MemoizingPickleCoder.as_deterministic_coder()` raising `TypeError` instead of returning a working deterministic coder ([#28558](https://github.com/apache/beam/issues/28558)).

According to git shortlog, the following people contributed to the 2.76.0 release. Thank you to all contributors!

Abdelrahman Ibrahim, Aditya Narayan, Ahmed Abualsaud, Alex Bevilacqua, Alexander Pochill, Ali Ebrahim, Andrew Crites, Arun Pandian, Ashwin S, Bruno Volpato, Chamikara Jayalath, Chris Gavin, Claire McGinty, Danny McCormick, Derrick Williams, Eiji Ogiwara, Elia Liu, Fabian Loris, Goutam Adwant, HansMarcus01, Israel Herraiz, Jack McCluskey, Jan Lukavský, Jeremy Schoemaker, Kenneth Knowles, Lalit Yadav, Lawrence Qiu, M Junaid Shaukat, Makoto Nagai, Maksym Tymoshyk, Manvith Panyam, Mattie Fu, Michael Gruschke, Mukesh Bhandarkar, Nicolas Gibanel, Paulius Kuzmickas, Radosław Stankiewicz, Ryan Wigglesworth, Sam Whittle, Sharan Teja M, Shizuma5, Shunping Huang, SreeramaYeshwanthGowd, Tobias Kaymak, Tom Newton, Udit Jain, Vitaly Terentyev, Yi Hu, ZIHAN DAI, akshayjadiyanv, claudevdm, darshan-sj, feefs, junaiddshaukat, kellen, nitinware, parveensania, tvalentyn
