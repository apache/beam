---
title:  "Apache Beam 2.78.0"
date:   2026-11-?? 14:00:00 -0500
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

We are happy to present the new 2.78.0 release of Beam.
This release includes both improvements and new functionality.
See the [download page](/get-started/downloads/#2780-2026-11-??) for this release.

<!--more-->

For more information on changes in 2.78.0, check out the [detailed release notes](https://github.com/apache/beam/milestone/44).

## Highlights

### I/Os

* Added a SchemaTransform for SplunkIO writes, available in Beam YAML as `WriteToSplunk` (Java/YAML) ([#40470](https://github.com/apache/beam/issues/40470)).
* (Java) FirestoreIO: added `RpcQosOptions.Builder.withRampupThrottlingDisabled()`, mirroring the existing DatastoreIO option, to opt out of the 500/50/5 write ramp-up ([#27170](https://github.com/apache/beam/issues/27170)).
* Added support for attributing BigQuery API quota and billing to a specific GCP project (quota project): `quota_project_id` parameter in `ReadFromBigQuery` or `--bigquery_quota_project_id` pipeline option, honored by the Python BigQuery IO transforms (`WriteToBigQuery` with `FILE_LOADS` or `STREAMING_INSERTS`; not yet `STORAGE_WRITE_API`, which is Java-backed), `--bigQueryQuotaProjectId` pipeline option (Java), and `bigqueryio.WithQuotaProject` read/query option (Go) ([#37431](https://github.com/apache/beam/issues/37431)).
* (Java/Python/Go) DebeziumIO: `readAsJson()` can now include `op` and the remaining Debezium `source` fields (e.g. MySQL `file`, `pos`, `row`, `ts_ms`; PostgreSQL `lsn`, `txId`, `ts_ms`) in `metadata` via `withFullMetadata(true)` / `full_metadata=True` / `debeziumio.FullMetadata()`. Off by default ([#40701](https://github.com/apache/beam/issues/40701)).
* [IcebergIO] AddFiles is now available through the Managed API with `Managed.write(Managed.ICEBERG_ADD_FILES)` (Java/Python) ([#40827](https://github.com/apache/beam/issues/40827)).
* (Java) IcebergIO: `AddFiles` can register files in several tables in one transform, naming each file's table from its path. Set `path_pattern` (`withPathPattern` in Java) to a glob with `{name}` placeholders, such as `gs://lake/export/{table}/**`, and make `table` a template over what it captures, such as `default.{table}`. The schema pre-pass then runs per table, so a table whose files conflict is refused and its files reported while every other table is still registered. Requires `schema_evolution_options` and `error_handling`; not supported with `dry_run`. Error rows gain a `table` field only when `path_pattern` is set, so an existing pipeline's error output is unchanged ([#40849](https://github.com/apache/beam/issues/40849)).

### New Features / Improvements

* (Java) Flink runner: sources now forward Beam metrics to Flink at most once per second instead of after every record, which was a significant share of the CPU of reading. Metrics in the pipeline result are not affected ([#40607](https://github.com/apache/beam/issues/40607)).
* (Go) Added `wait.On`, which delays each input window until the corresponding windows in its signal PCollections have closed ([#39909](https://github.com/apache/beam/issues/39909)).
* (Python) Expanded the SDK worker heap dump (`--experiments=enable_heap_dump`) with process RSS, CPython allocator/GC stats, and glibc `mallinfo2` native-heap/fragmentation stats to help distinguish native-heap from Python-object memory growth ([#39244](https://github.com/apache/beam/issues/39244)).
* (Python) Added typed decision questions and the `EvaluateDecisions` transform, with an optional Jev model adapter ([#40220](https://github.com/apache/beam/pull/40220)).
* The `disableCounterMetrics`, `disableStringSetMetrics` and `disableBoundedTrieMetrics` experiments are now honored by the Python SDK, as they already were in Java (Python) ([#38746](https://github.com/apache/beam/issues/38746)).
* (Go) Pipeline-level `--resource_hints` are now forwarded to expansion services, so they apply to cross-language transforms as they do in the Java and Python SDKs. The `max_active_bundles_per_worker` hint name is now also accepted by `--resource_hints` ([#23893](https://github.com/apache/beam/issues/23893)).
* `QdrantWriteConfig` can now optionally store chunk content in the point payload via `content_payload_key`, so retrieved points include the chunk text (Python) ([#40476](https://github.com/apache/beam/issues/40476)).
* ReadFromBigQuery now supports Lakehouse runtime catalog (BigLake metastore) tables with `method=DIRECT_READ`, using 4-part `project.catalog.namespace.table` identifiers. Previously `project:catalog.namespace.table` was silently mis-parsed and tables that report no `numBytes` failed to split (Python) ([#39597](https://github.com/apache/beam/issues/39597)).
* (Python) Added `AwsOptions` with `--aws_*` pipeline options (e.g. `--aws_access_key_id`, `--aws_region_name`, `--aws_endpoint_url`) that set defaults for all AWS service clients. The existing `--s3_*` options in `S3Options` override these defaults for the S3 client only ([#40528](https://github.com/apache/beam/issues/40528)).
* (Python) Added `--aws_credentials_secret_manager` so AWS credentials (`--aws_access_key_id`, `--aws_secret_access_key`, `--aws_session_token` and their `--s3_*` overrides) can be secret specs resolved through a secret manager such as `GoogleCloudSecretManager`, instead of plain-text values ([#40528](https://github.com/apache/beam/issues/40528)).
* (Python) Added `AwsSecret` (`AwsSecretsManager` / `type:awssecret`) to retrieve secrets from AWS Secrets Manager, configured from `AwsOptions` (`--aws_*`) with optional per-secret connection overrides ([#40528](https://github.com/apache/beam/issues/40528)).
* (Go) Added `schema.RegisterLogicalTypeConversion` and `schema.RegisterLogicalTypeCoder` to register portable logical types in one call, and Go types for the standard `beam:logical_type:date:v1` and `beam:logical_type:micros_instant:v1` logical types, so schemas from other SDKs with these types can be used in cross-language pipelines ([#39684](https://github.com/apache/beam/issues/39684)).
* (Python, Java, Go) Added `--dataflow_container_base_repository` (Python, Go) and `--dataflowContainerBaseRepository` (Java) to set the repository (e.g. `docker.example.com/my-prefix/cloud-dataflow/v1beta3`) from which the Dataflow runner pulls its default SDK container images, including the default images of cross-language Apache Beam environments. Defaults to `gcr.io/cloud-dataflow/v1beta3` ([#40569](https://github.com/apache/beam/issues/40569)).
* (Go) GCS and Dataflow clients now support non-default universes such as Trusted Partner Cloud (TPC), using the universe domain from `GOOGLE_CLOUD_UNIVERSE_DOMAIN` or the default credentials. Added a `--gcs_endpoint` pipeline option to override the GCS endpoint ([#40569](https://github.com/apache/beam/issues/40569)).
* (Java) `GcsUtil` now uses the java-storage client (GcsUtil V2) for GCS operations by default instead of gcsio (GcsUtil V1). To go back to V1, pass `--experiments=disable_gcsutil_v2`. Pipelines with the `use_grpc_for_gcs` experiment keep using V1. The `use_gcsutil_v2` experiment is no longer needed and is now ignored ([#40712](https://github.com/apache/beam/issues/40712)).

### Breaking Changes

* (Java) Removed Beam SQL Google Cloud Data Catalog table provider (`beam-sdks-java-extensions-sql-datacatalog`) because GCP Data Catalog has been decommissioned ([#40663](https://github.com/apache/beam/issues/40663)).
* (Go) The row coder now encodes `int16` and `uint16` struct fields as 2 byte big endian INT16 values, matching the Java and Python SDKs. This is an update incompatible change for streaming pipelines that use rows with `int16` or `uint16` fields ([#40151](https://github.com/apache/beam/issues/40151)).
* (Java) JmsIO migrated to `jakarta.jms` (JMS 3.1) and ActiveMQ 6.2.5. User code implementing `JmsIO.MessageMapper`,
  `valueMapper`, `topicNameMapper`, or providing a `ConnectionFactory` must update imports from `javax.jms.*` to
  `jakarta.jms.*`. The module now requires Java 17 at runtime ([#38729](https://github.com/apache/beam/issues/38729)).
* (Java) Dataflow runner: `--apiRootUrl` no longer defaults to `https://dataflow.googleapis.com/`. When neither `--apiRootUrl` nor an absolute `--dataflowEndpoint` is set, the Google API client library resolves the Dataflow endpoint and honors `GOOGLE_CLOUD_UNIVERSE_DOMAIN`, so Dataflow jobs can be submitted in non-default universes. Code that reads `DataflowPipelineDebugOptions.getApiRootUrl()` should handle a `null` value ([#40569](https://github.com/apache/beam/issues/40569)).
* (Python) `Regex.find_all` with `group=Regex.ALL` now returns every group, as `(match, group1, group2, ...)`, instead of only group 1, and with `outputEmpty=False` it drops a match only when the whole match is empty, matching the `group=0` behavior ([#40399](https://github.com/apache/beam/pull/40399)).
* (Python) `Regex.all_matches` now returns every group of the regex, instead of failing with a `TypeError` when the regex has no groups and dropping nested or unmatched trailing groups ([#40562](https://github.com/apache/beam/issues/40562)).
* (Python) `--dataflow_endpoint` now defaults to `''` instead of `https://dataflow.googleapis.com`. When unset, the Google Cloud client library resolves the Dataflow endpoint and honors `GOOGLE_CLOUD_UNIVERSE_DOMAIN`, so Dataflow jobs can be submitted in non-default universes. Code that reads `GoogleCloudOptions.dataflow_endpoint` should handle an empty value ([#40569](https://github.com/apache/beam/issues/40569)).
* (Python) The `xgboost` extras now install `xgboost>=2.0.0,<4` and `datatable>=1.1.0,<2`, moving both the lower and upper bounds for both. The previous minimum `xgboost` and `datatable` versions (`1.6.0` and `1.0.0` respectively) are no longer allowed ([#40694](https://github.com/apache/beam/issues/40694)).
* (Java) [IcebergIO] AddFiles (`IcebergAddFiles` in YAML) now carries the destination table alongside each file through the registration chain, so that one transform can register files into many tables. The coders of its internal `PCollection`s change, which makes this an update incompatible change for streaming pipelines that use it ([#40817](https://github.com/apache/beam/pull/40817)).
* (YAML) `IcebergAddFiles` will be automatically replaced with the compatible managed transform. For older Beam versions, streaming update compatibility can be maintained by specifying the pipeline option `update_compatibility_version` ([#40827](https://github.com/apache/beam/issues/40827)).
* (Java) [IcebergIO] AddFiles now collapses batch input into the global window. Manifests and snapshots are sized per key and window, so windowing applied upstream of AddFiles only divided `manifest_file_size` into smaller manifests and more snapshots; its outputs are now globally windowed in batch. Streaming pipelines keep the windowing they had ([#40849](https://github.com/apache/beam/issues/40849)).
* Twister2 runner supported is removed ([#35905](https://github.com/apache/beam/issues/35905))).

### Deprecations

* Support for Python 3.10 was removed following its end-of-life ([#40250](https://github.com/apache/beam/issues/40250)).
* (Python) `XGBoostModelHandlerDatatable` is deprecated and will be removed in Beam 2.79.0, along with the `datatable` dependency of the `xgboost` extra. `datatable` has not had a release since 2023 and has no wheels for Python 3.13+. Use `XGBoostModelHandlerPandas` (or the NumPy/SciPy handlers) instead ([#40705](https://github.com/apache/beam/issues/40705)).

### Bugfixes

* (Python) Fixed timestamp and side-input argument binding when a `DoFn.process` or `DoFn.process_batch` method gives its element or batch parameter a default value ([#19631](https://github.com/apache/beam/issues/19631)).
* (Python) Fixed BigQuery `FILE_LOADS` silently omitting files when dynamic destinations use different spellings of the same table ([#40001](https://github.com/apache/beam/pull/40001)).
* (Python) DaskRunner now preserves outputs emitted by `DoFn.finish_bundle()` instead of silently dropping them ([#40519](https://github.com/apache/beam/issues/40519)).
* (Python) BigQuery streaming inserts now reset the retry budget and backoff for each batch instead of sharing them across a bundle ([#39995](https://github.com/apache/beam/pull/39995)).
* (Python) State write failures reported by portable runners now fail the bundle instead of silently losing state updates ([#39992](https://github.com/apache/beam/pull/39992)).
* (Python) Type inference now gives the right type for a slice of a fixed-size tuple, such as `row[1:]` in `beam.Map(lambda row: row[1:])`. The slice used to keep the type of the whole tuple, which could make a following `CombinePerKey` fail with a `TypeCheckError` or encode elements with the wrong coder ([#20899](https://github.com/apache/beam/issues/20899)).
* (Go) Fixed a data race on the Prism runner's artifact cache map in JobServices ([#32656](https://github.com/apache/beam/issues/32656)).
* (Go) Fixed the harness leaking Data/State gRPC streams after the worker stops, and a deadlock when Send returns EOF ([#40260](https://github.com/apache/beam/issues/40260)).
* (Go) Wrap last dataWriter/timerWriter send errors on Close ([#40405](https://github.com/apache/beam/issues/40405)).
* (Go) Fixed a deadlock recreating a data channel while holding the channel lock ([#40414](https://github.com/apache/beam/issues/40414)).
* (Python) Fixed `KeyedModelHandler` not enforcing `max_models_per_worker_hint` when several copies of the model handler run in the same SDK process ([#40468](https://github.com/apache/beam/issues/40468)).
* (Python) Fixed a bug where `RunInference` was serving the first batch on each worker with the original models instead of the paths from a `KeyModelPathMapping` side input ([#40665](https://github.com/apache/beam/issues/40665)).
* (Python) Fixed a bug in `RunInference` where adaptive throttling in remote inference codepath was accidentally disabled ([#40598](https://github.com/apache/beam/pull/40598)).
* (Python) Fixed an inefficiency in `RunInference` that caused unnecessary model reloading in some cases when `large_model=True` was not set ([#40441](https://github.com/apache/beam/pull/40441)).
* (Java) Fixed the declared schema of the error output of the Kafka write SchemaTransform, which wrapped the error schema a second time and did not match the rows it emits ([#39760](https://github.com/apache/beam/issues/39760)).
* (Go) Fixed pubsubio importing a `google.golang.org/genproto` package removed in recent releases, which broke builds of Go modules depending on a current `genproto` version ([#40018](https://github.com/apache/beam/issues/40018)).
* (Python) `VertexAIFeatureStoreEnrichmentHandler` now logs a warning instead of failing when the caller lacks permission to read the feature store metadata ([#40442](https://github.com/apache/beam/issues/40442)).
* (Java) BigQueryIO now treats a 404 when deleting a temporary table or dataset as success, so a replayed work item whose earlier attempt already deleted it no longer retries forever ([#24997](https://github.com/apache/beam/issues/24997)).
* (Java) Fixed BigQuery Storage Write API failure outputs returning the wrong original row after consecutive serialization errors ([#39999](https://github.com/apache/beam/pull/39999)).
* (Java) IcebergIO now writes rows containing `EnumerationType` (proto enum) fields as strings, instead of throwing `Unsupported Beam logical type Enum` ([#40299](https://github.com/apache/beam/issues/40299)).
* (Python) Fixed stateful DoFns with side inputs sometimes taking the timer key coder from a side input instead of the main input, which could make the worker fail to decode timer keys with `Unknown type tag` ([#40374](https://github.com/apache/beam/issues/40374)).
* (Python) Fixed `AsyncWrapper` finalizing bundles before generator processing, which could drop buffered writes, and nesting iterable outputs ([#40000](https://github.com/apache/beam/pull/40000)).
* (Python) `Duration` built from float seconds now rounds to the nearest microsecond instead of truncating, which could lose a microsecond ([#40263](https://github.com/apache/beam/issues/40263)).
* (Python) `ReadFromText` with `escapechar` no longer skips a delimiter that directly follows an escaped delimiter, which merged two records into one ([#40459](https://github.com/apache/beam/issues/40459)).
* (Python) DataFrame API: `agg` with a list that includes `'count'` or `'size'` now labels those rows `count`/`size` instead of `sum` ([#40504](https://github.com/apache/beam/issues/40504)).
* (Python) `ConcatSource` no longer raises `ZeroDivisionError` when every source bundle reports a weight of 0, e.g. bundles smaller than 1 MB ([#21316](https://github.com/apache/beam/issues/21316)).
* (Python) DataFrame API: `Series.value_counts(bins=...)` no longer always fails with a `TypeError` ([#40503](https://github.com/apache/beam/issues/40503)).
* (Python) `ReadBigQueryChangeHistory` no longer fails at pipeline construction with `TypeCheckError: The transform 'ExecuteQueries' requires PCollections of type ... _QueryRange` when pipeline type checking is enabled (the default); a regression in 2.76.0 ([#40618](https://github.com/apache/beam/issues/40618)).
* (Python) `ReadBigQueryChangeHistory` with `change_function='CHANGES'` no longer fails reading its temp table with a Storage Read API `500 InternalServerError`: the `_CHANGE_IS_FOR_UPDATE` pseudo-column is now excluded from the query instead of being written to the temp table ([#40618](https://github.com/apache/beam/issues/40618)).
* (Python) `ReadBigQueryChangeHistory` now backtick-quotes the column names it puts in the generated CHANGES/APPENDS query (`columns`, `change_type_column`, `change_timestamp_column`), so reserved words and names with spaces work, and `columns=[]` selects only the pseudo-columns instead of generating invalid SQL ([#40618](https://github.com/apache/beam/issues/40618)).
* (Python) Setting `max_batch_weight` on a RunInference `ModelHandler` no longer fails with a `TypeError`: `BatchElements` now supports `max_batch_weight`, which limits the total `element_size_fn` of a batch in addition to its number of elements ([#40644](https://github.com/apache/beam/issues/40644)).
* (Python) `BatchElements` now honors `element_size_fn` when `max_batch_duration_secs` is set, and no longer emits an empty batch before an element that exceeds the batch size on its own ([#40644](https://github.com/apache/beam/issues/40644)).
* (Python) `KeyedModelHandler` and `MaybeKeyedModelHandler` now apply the `element_size_fn` and `batch_length_fn` of the wrapped `ModelHandler` to examples, instead of to `(key, example)` pairs ([#40644](https://github.com/apache/beam/issues/40644)).
* (Java) SplunkIO write errors no longer lose their status code, status message and payload when encoded. The schema inferred for `SplunkWriteError` had no fields, so it now has a hand-written schema with `statusCode`, `statusMessage` and `payload` ([#40470](https://github.com/apache/beam/issues/40470)).
* (Python) FnApiRunner now length prefixes timer key coders it doesn't know, as it already did for PCollection coders, so cross-language stateful DoFns with timers keyed by e.g. Java's `VarIntCoder` no longer fail ([#40470](https://github.com/apache/beam/issues/40470)).
* (Java) Portable runners no longer deadlock when a single bundle produces more output than the Fn API data buffers hold (about 1 GB with the Python SDK, e.g. one Flink batch partition). The runner now consumes a bundle's output before waiting for its `ProcessBundleResponse` ([#40720](https://github.com/apache/beam/issues/40720)).
* (Python) DataFrame API: `drop_duplicates`, `duplicated` and `nunique` no longer fail on rows with NaN values or on categorical columns with unused categories, and `nunique(dropna=False)` now counts NaN ([#40570](https://github.com/apache/beam/issues/40570)).
* (Java) BigQuery Storage Write API `TableRow` conversion now honors `allowMissingRequiredFields` when building protocol-buffer messages, so callers that pass `true` with required descriptors get partial messages instead of a conversion-time `UninitializedMessageException` ([#40189](https://github.com/apache/beam/issues/40189)).
* (Go) Fixed generic registration (for example `register.Function1x1` or `register.DoFn1x1`) panicking on functions and methods that take or return an anonymous struct. Unnamed struct types are now only registered with the schema registry, since they have no type key for the runtime type registry ([#21789](https://github.com/apache/beam/issues/21789)).

According to git shortlog, the following people contributed to the 2.78.0 release. Thank you to all contributors!

Abdelrahman Ibrahim, Ahmed Abualsaud, Ali Ebrahim, Amir Fathi, Anavay Sharma, Andrew Crites, Anish Mehta, Arun Pandian, Ashwin S, Ayoub, Balázs Németh, Biao Geng, Bruno Volpato, CH-Abhinav, CarloMariaProietti, Chamikara Jayalath, Chetan Ruparel, Claude, Clay Johnson, Danish Amjad, Danny McCormick, Deepika Mamidipelly, Derrick Williams, Dhruv Dagar, DiezNobach, Divyansh Yadav, Elia LIU, Elia Liu, Florent Biville, GAURAV THAKUR, Ganesh Sivakumar, Goutam Adwant, Igor, Jack McCluskey, Jan Lukavský, Jean-Baptiste Onofré, Jiang Zhu, Julien Tournay, Kenneth Knowles, Khaled Hammouda, Maksym Tymoshyk, Malo Denielou, Manan Mangal, Mazlum Tosun, Nicolas Gibanel, Nitin Ware, Pablo Villanueva, Parv Arhatia, Paulius Kuzmickas, Radosław Stankiewicz, Rakesh Pai, Reuven Lax, RuiLong J., Ryan McGinnis, Sam Whittle, Shahar Epstein, Shunping Huang, Steven van Rossum, Tobias Kaymak, Udaya Chathuranga, Udit Jain, Utkarsh Bhangale, Valentyn Tymofieiev, Valient Gough, Vishal More, Vitaly Terentyev, Wangshu, XuJian, Yashvardhan Thanvi, Yi Hu, Yusuf Quazi, ZIHAN DAI, akshayjadiyanv, alisha-1000, claudevdm, darshan-sj, ddebowczyk92, ilyas ahsan, junaiddshaukat, mola, nitinware, pablo rodriguez defino, parveensania, raams95, singhmrinal02, waneal
