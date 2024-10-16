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
<!-- Template -->
<!--
# [2.XX.X] - Unreleased

## Highlights

* New highly anticipated feature X added to Python SDK ([#X](https://github.com/apache/beam/issues/X)).
* New highly anticipated feature Y added to Java SDK ([#Y](https://github.com/apache/beam/issues/Y)).

## I/Os

* Support for X source added (Java/Python) ([#X](https://github.com/apache/beam/issues/X)).

## New Features / Improvements

* X feature added (Java/Python) ([#X](https://github.com/apache/beam/issues/X)).

## Breaking Changes

* X behavior was changed ([#X](https://github.com/apache/beam/issues/X)).

## Deprecations

* X behavior is deprecated and will be removed in X versions ([#X](https://github.com/apache/beam/issues/X)).

## Bugfixes

* Fixed X (Java/Python) ([#X](https://github.com/apache/beam/issues/X)).

## Security Fixes
* Fixed (CVE-YYYY-NNNN)[https://www.cve.org/CVERecord?id=CVE-YYYY-NNNN] (Java/Python/Go) ([#X](https://github.com/apache/beam/issues/X)).

## Known Issues

* ([#X](https://github.com/apache/beam/issues/X)).
-->

# [2.57.0] - Unreleased

## Highlights

* Apache Beam adds Python 3.12 support ([#29149](https://github.com/apache/beam/issues/29149)).
* Added FlinkRunner for Flink 1.18 ([#30789](https://github.com/apache/beam/issues/30789)).

## I/Os

* Support for X source added (Java/Python) ([#X](https://github.com/apache/beam/issues/X)).
* Ensure that BigtableIO closes the reader streams ([#31477](https://github.com/apache/beam/issues/31477)).

## New Features / Improvements

* Added Feast feature store handler for enrichment transform (Python) ([#30957](https://github.com/apache/beam/issues/30964)).
* BigQuery per-worker metrics are reported by default for Streaming Dataflow Jobs (Java) ([#31015](https://github.com/apache/beam/pull/31015))
* Adds `inMemory()` variant of Java List and Map side inputs for more efficient lookups when the entire side input fits into memory.
* Beam YAML now supports the jinja templating syntax.
  Template variables can be passed with the (json-formatted) `--jinja_variables` flag.
* DataFrame API now supports pandas 2.1.x and adds 12 more string functions for Series.([#31185](https://github.com/apache/beam/pull/31185)).
* Added BigQuery handler for enrichment transform (Python) ([#31295](https://github.com/apache/beam/pull/31295))
* Disable soft delete policy when creating the default bucket for a project (Java) ([#31324](https://github.com/apache/beam/pull/31324)).
* Go SDK Prism Runner
  * Pre-built Prism binaries are now part of the release and are available via the Github release page. ([#29697](https://github.com/apache/beam/issues/29697)).
  * ProcessingTime is now handled synthetically with TestStream pipelines and Non-TestStream pipelines, for fast test pipeline execution by default. ([#30083](https://github.com/apache/beam/issues/30083)).
    * Prism does NOT yet support "real time" execution for this release.

## Breaking Changes

* X behavior was changed ([#X](https://github.com/apache/beam/issues/X)).
* Java's View.asList() side inputs are now optimized for iterating rather than
  indexing when in the global window.
  This new implementation still supports all (immutable) List methods as before,
  but some of the random access methods like get() and size() will be slower.
  To use the old implementation one can use View.asList().withRandomAccess().
* SchemaTransforms implemented with TypedSchemaTransformProvider now produce a
  configuration Schema with snake_case naming convention
  ([#31374](https://github.com/apache/beam/pull/31374)). This will make the following
  cases problematic:
  * Running a pre-2.57.0 remote SDK pipeline containing a 2.57.0+ Java SchemaTransform,
    and vice versa:
  * Running a 2.57.0+ remote SDK pipeline containing a pre-2.57.0 Java SchemaTransform
  * All direct uses of Python's [SchemaAwareExternalTransform](https://github.com/apache/beam/blob/a998107a1f5c3050821eef6a5ad5843d8adb8aec/sdks/python/apache_beam/transforms/external.py#L381)
    should be updated to use new snake_case parameter names.
* Upgraded Jackson Databind to 2.15.4 (Java) ([#26743](https://github.com/apache/beam/issues/26743)).
  jackson-2.15 has known breaking changes. An important one is it imposed a buffer limit for parser.
  If your custom PTransform/DoFn are affected, refer to [#31580](https://github.com/apache/beam/pull/31580) for mitigation.

## Deprecations

* X behavior is deprecated and will be removed in X versions ([#X](https://github.com/apache/beam/issues/X)).

## Bugfixes

* Fixed X (Java/Python) ([#X](https://github.com/apache/beam/issues/X)).

## Security Fixes
* Fixed (CVE-YYYY-NNNN)[https://www.cve.org/CVERecord?id=CVE-YYYY-NNNN] (Java/Python/Go) ([#X](https://github.com/apache/beam/issues/X)).

## Known Issues

* ([#X](https://github.com/apache/beam/issues/X)).

# [2.56.0] - 2024-05-01

## Highlights

* Added FlinkRunner for Flink 1.17, removed support for Flink 1.12 and 1.13. Previous version of Pipeline running on Flink 1.16 and below can be upgraded to 1.17, if the Pipeline is first updated to Beam 2.56.0 with the same Flink version. After Pipeline runs with Beam 2.56.0, it should be possible to upgrade to FlinkRunner with Flink 1.17. ([#29939](https://github.com/apache/beam/issues/29939))
* New Managed I/O Java API ([#30830](https://github.com/apache/beam/pull/30830)).
* New Ordered Processing PTransform added for processing order-sensitive stateful data ([#30735](https://github.com/apache/beam/pull/30735)).

## I/Os

* Upgraded Avro version to 1.11.3, kafka-avro-serializer and kafka-schema-registry-client versions to 7.6.0 (Java) ([#30638](https://github.com/apache/beam/pull/30638)).
  The newer Avro package is known to have breaking changes. If you are affected, you can keep pinned to older Avro versions which are also tested with Beam.
* Iceberg read/write support is available through the new Managed I/O Java API ([#30830](https://github.com/apache/beam/pull/30830)).

## New Features / Improvements

* Added ability to control the exact number of models loaded across processes by RunInference. This may be useful for pipelines with tight memory constraints ([#31052](https://github.com/apache/beam/pull/31052))
* Profiling of Cythonized code has been disabled by default. This might improve performance for some Python pipelines ([#30938](https://github.com/apache/beam/pull/30938)).
* Bigtable enrichment handler now accepts a custom function to build a composite row key. (Python) ([#30974](https://github.com/apache/beam/issues/30975)).

## Breaking Changes

* Default consumer polling timeout for KafkaIO.Read was increased from 1 second to 2 seconds. Use KafkaIO.read().withConsumerPollingTimeout(Duration duration) to configure this timeout value when necessary ([#30870](https://github.com/apache/beam/issues/30870)).
* Python Dataflow users no longer need to manually specify --streaming for pipelines using unbounded sources such as ReadFromPubSub.

## Bugfixes

* Fixed locking issue when shutting down inactive bundle processors. Symptoms of this issue include slowness or stuckness in long-running jobs (Python) ([#30679](https://github.com/apache/beam/pull/30679)).
* Fixed logging issue that caused silecing the pip output when installing of dependencies provided in `--requirements_file` (Python).
* Fixed pipeline stuckness issue by disallowing versions of grpcio that can cause the stuckness (Python) ([#30867](https://github.com/apache/beam/issues/30867)).

## Known Issues

* The beam interactive runner does not correctly run on flink ([#31168](https://github.com/apache/beam/issues/31168)).
* When using the Flink runner from Python, 1.17 is not supported and 1.12/13 do not work correctly. Support for 1.17 will be added in 2.57.0, and the ability to choose 1.12/13 will be cleaned up and fully removed in 2.57.0 as well ([#31168](https://github.com/apache/beam/issues/31168)).
* In rare case, streaming pipelines' coders may throw and surface a NullPointerException ([#32566](https://github.com/apache/beam/pull/32566)).

# [2.55.1] - 2024-04-08

## Bugfixes

* Fixed issue that broke WriteToJson in languages other than Java (X-lang) ([#30776](https://github.com/apache/beam/issues/30776)).

# [2.55.0] - 2024-03-25

## Highlights

* The Python SDK will now include automatically generated wrappers for external Java transforms! ([#29834](https://github.com/apache/beam/pull/29834))

## I/Os

* Added support for handling bad records to BigQueryIO ([#30081](https://github.com/apache/beam/pull/30081)).
  * Full Support for Storage Read and Write APIs
  * Partial Support for File Loads (Failures writing to files supported, failures loading files to BQ unsupported)
  * No Support for Extract or Streaming Inserts
* Added support for handling bad records to PubSubIO ([#30372](https://github.com/apache/beam/pull/30372)).
  * Support is not available for handling schema mismatches, and enabling error handling for writing to pubsub topics with schemas is not recommended
* `--enableBundling` pipeline option for BigQueryIO DIRECT_READ is replaced by `--enableStorageReadApiV2`. Both were considered experimental and may subject to change (Java) ([#26354](https://github.com/apache/beam/issues/26354)).

## New Features / Improvements

* Allow writing clustered and not time partitioned BigQuery tables (Java) ([#30094](https://github.com/apache/beam/pull/30094)).
* Redis cache support added to RequestResponseIO and Enrichment transform (Python) ([#30307](https://github.com/apache/beam/pull/30307))
* Merged sdks/java/fn-execution and runners/core-construction-java into the main SDK. These artifacts were never meant for users, but noting
  that they no longer exist. These are steps to bring portability into the core SDK alongside all other core functionality.
* Added Vertex AI Feature Store handler for Enrichment transform (Python) ([#30388](https://github.com/apache/beam/pull/30388))

## Breaking Changes

* Arrow version was bumped to 15.0.0 from 5.0.0 ([#30181](https://github.com/apache/beam/pull/30181)).
* Go SDK users who build custom worker containers may run into issues with the move to distroless containers as a base (see Security Fixes).
  * The issue stems from distroless containers lacking additional tools, which current custom container processes may rely on.
  * See https://beam.apache.org/documentation/runtime/environments/#from-scratch-go for instructions on building and using a custom container.
* Python SDK has changed the default value for the `--max_cache_memory_usage_mb` pipeline option from 100 to 0. This option was first introduced in 2.52.0 SDK. This change restores the behavior of 2.51.0 SDK, which does not use the state cache. If your pipeline uses iterable side inputs views, consider increasing the cache size by setting the option manually. ([#30360](https://github.com/apache/beam/issues/30360)).

## Bugfixes

* Fixed SpannerIO.readChangeStream to support propagating credentials from pipeline options
  to the getDialect calls for authenticating with Spanner (Java) ([#30361](https://github.com/apache/beam/pull/30361)).
* Reduced the number of HTTP requests in GCSIO function calls (Python) ([#30205](https://github.com/apache/beam/pull/30205))

## Security Fixes

* Go SDK base container image moved to distroless/base-nossl-debian12, reducing vulnerable container surface to kernel and glibc ([#30011](https://github.com/apache/beam/pull/30011)).

## Known Issues

* In Python pipelines, when shutting down inactive bundle processors, shutdown logic can overaggressively hold the lock, blocking acceptance of new work. Symptoms of this issue include slowness or stuckness in long-running jobs. Fixed in 2.56.0 ([#30679](https://github.com/apache/beam/pull/30679)).
* WriteToJson broken in languages other than Java (X-lang) ([#30776](https://github.com/apache/beam/issues/30776)).
* Python pipelines might occasionally become stuck due to a regression in grpcio ([#30867](https://github.com/apache/beam/issues/30867)). The issue manifests frequently with Bigtable IO connector, but might also affect other GCP connectors. Fixed in 2.56.0.

# [2.54.0] - 2024-02-14

## Highlights

* [Enrichment Transform](https://s.apache.org/enrichment-transform) along with GCP BigTable handler added to Python SDK ([#30001](https://github.com/apache/beam/pull/30001)).
* Beam Java Batch pipelines run on Google Cloud Dataflow will default to the Portable (Runner V2)[https://cloud.google.com/dataflow/docs/runner-v2] starting with this version. (All other languages are already on Runner V2.)
    * This change is still rolling out to the Dataflow service, see (Runner V2 documentation)[https://cloud.google.com/dataflow/docs/runner-v2] for how to enable or disable it intentionally.

## I/Os

* Added support for writing to BigQuery dynamic destinations with Python's Storage Write API ([#30045](https://github.com/apache/beam/pull/30045))
* Adding support for Tuples DataType in ClickHouse (Java) ([#29715](https://github.com/apache/beam/pull/29715)).
* Added support for handling bad records to FileIO, TextIO, AvroIO ([#29670](https://github.com/apache/beam/pull/29670)).
* Added support for handling bad records to BigtableIO ([#29885](https://github.com/apache/beam/pull/29885)).

## New Features / Improvements

* [Enrichment Transform](https://s.apache.org/enrichment-transform) along with GCP BigTable handler added to Python SDK ([#30001](https://github.com/apache/beam/pull/30001)).

## Breaking Changes

* N/A

## Deprecations

* N/A

## Bugfixes

* Fixed a memory leak affecting some Go SDK since 2.46.0. ([#28142](https://github.com/apache/beam/pull/28142))

## Security Fixes

* N/A

## Known Issues

* Some Python pipelines that run with 2.52.0-2.54.0 SDKs and use large materialized side inputs might be affected by a performance regression. To restore the prior behavior on these SDK versions, supply the `--max_cache_memory_usage_mb=0` pipeline option. ([#30360](https://github.com/apache/beam/issues/30360)).
* Python pipelines that run with 2.53.0-2.54.0 SDKs and perform file operations on GCS might be affected by excess HTTP requests. This could lead to a performance regression or a permission issue. ([#28398](https://github.com/apache/beam/issues/28398))
* In Python pipelines, when shutting down inactive bundle processors, shutdown logic can overaggressively hold the lock, blocking acceptance of new work. Symptoms of this issue include slowness or stuckness in long-running jobs. Fixed in 2.56.0 ([#30679](https://github.com/apache/beam/pull/30679)).

# [2.53.0] - 2024-01-04

## Highlights

* Python streaming users that use 2.47.0 and newer versions of Beam should update to version 2.53.0, which fixes a known issue: ([#27330](https://github.com/apache/beam/issues/27330)).

## I/Os

* TextIO now supports skipping multiple header lines (Java) ([#17990](https://github.com/apache/beam/issues/17990)).
* Python GCSIO is now implemented with GCP GCS Client instead of apitools ([#25676](https://github.com/apache/beam/issues/25676))
* Added support for handling bad records to KafkaIO (Java) ([#29546](https://github.com/apache/beam/pull/29546))
* Add support for generating text embeddings in MLTransform for Vertex AI and Hugging Face Hub models.([#29564](https://github.com/apache/beam/pull/29564))
* NATS IO connector added (Go) ([#29000](https://github.com/apache/beam/issues/29000)).
* Adding support for LowCardinality (Java) ([#29533](https://github.com/apache/beam/pull/29533)).

## New Features / Improvements

* The Python SDK now type checks `collections.abc.Collections` types properly. Some type hints that were erroneously allowed by the SDK may now fail. ([#29272](https://github.com/apache/beam/pull/29272))
* Running multi-language pipelines locally no longer requires Docker.
  Instead, the same (generally auto-started) subprocess used to perform the
  expansion can also be used as the cross-language worker.
* Framework for adding Error Handlers to composite transforms added in Java ([#29164](https://github.com/apache/beam/pull/29164)).
* Python 3.11 images now include google-cloud-profiler ([#29561](https://github.com/apache/beam/pull/29651)).

## Deprecations

* Euphoria DSL is deprecated and will be removed in a future release (not before 2.56.0) ([#29451](https://github.com/apache/beam/issues/29451))

## Bugfixes

* (Python) Fixed sporadic crashes in streaming pipelines that affected some users of 2.47.0 and newer SDKs ([#27330](https://github.com/apache/beam/issues/27330)).
* (Python) Fixed a bug that caused MLTransform to drop identical elements in the output PCollection ([#29600](https://github.com/apache/beam/issues/29600)).

## Security Fixes

* Upgraded to go 1.21.5 to build, fixing [CVE-2023-45285](https://security-tracker.debian.org/tracker/CVE-2023-45285) and [CVE-2023-39326](https://security-tracker.debian.org/tracker/CVE-2023-39326)

## Known Issues

* Potential race condition causing NPE in DataflowExecutionStateSampler in Dataflow Java Streaming pipelines ([#29987](https://github.com/apache/beam/issues/29987)).
* Some Python pipelines that run with 2.52.0-2.54.0 SDKs and use large materialized side inputs might be affected by a performance regression. To restore the prior behavior on these SDK versions, supply the `--max_cache_memory_usage_mb=0` pipeline option. ([#30360](https://github.com/apache/beam/issues/30360)).
* Python pipelines that run with 2.53.0-2.54.0 SDKs and perform file operations on GCS might be affected by excess HTTP requests. This could lead to a performance regression or a permission issue. ([#28398](https://github.com/apache/beam/issues/28398))
* In Python pipelines, when shutting down inactive bundle processors, shutdown logic can overaggressively hold the lock, blocking acceptance of new work. Symptoms of this issue include slowness or stuckness in long-running jobs. Fixed in 2.56.0 ([#30679](https://github.com/apache/beam/pull/30679)).

# [2.52.0] - 2023-11-17

## Highlights

* Previously deprecated Avro-dependent code (Beam Release 2.46.0) has been finally removed from Java SDK "core" package.
Please, use `beam-sdks-java-extensions-avro` instead. This will allow to easily update Avro version in user code without
potential breaking changes in Beam "core" since the Beam Avro extension already supports the latest Avro versions and
should handle this. ([#25252](https://github.com/apache/beam/issues/25252)).
* Publishing Java 21 SDK container images now supported as part of Apache Beam release process. ([#28120](https://github.com/apache/beam/issues/28120))
  * Direct Runner and Dataflow Runner support running pipelines on Java21 (experimental until tests fully setup). For other runners (Flink, Spark, Samza, etc) support status depend on runner projects.

## New Features / Improvements

* Add `UseDataStreamForBatch` pipeline option to the Flink runner. When it is set to true, Flink runner will run batch
  jobs using the DataStream API. By default the option is set to false, so the batch jobs are still executed
  using the DataSet API.
* `upload_graph` as one of the Experiments options for DataflowRunner is no longer required when the graph is larger than 10MB for Java SDK ([PR#28621](https://github.com/apache/beam/pull/28621)).
* Introduced a pipeline option `--max_cache_memory_usage_mb` to configure state and side input cache size. The cache has been enabled to a default of 100 MB. Use `--max_cache_memory_usage_mb=X` to provide cache size for the user state API and side inputs. ([#28770](https://github.com/apache/beam/issues/28770)).
* Beam YAML stable release. Beam pipelines can now be written using YAML and leverage the Beam YAML framework which includes a preliminary set of IO's and turnkey transforms. More information can be found in the YAML root folder and in the [README](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/yaml/README.md).


## Breaking Changes

* `org.apache.beam.sdk.io.CountingSource.CounterMark` uses custom `CounterMarkCoder` as a default coder since all Avro-dependent
classes finally moved to `extensions/avro`. In case if it's still required to use `AvroCoder` for `CounterMark`, then,
as a workaround, a copy of "old" `CountingSource` class should be placed into a project code and used directly
([#25252](https://github.com/apache/beam/issues/25252)).
* Renamed `host` to `firestoreHost` in `FirestoreOptions` to avoid potential conflict of command line arguments (Java) ([#29201](https://github.com/apache/beam/pull/29201)).

## Bugfixes

* Fixed "Desired bundle size 0 bytes must be greater than 0" in Java SDK's BigtableIO.BigtableSource when you have more cores than bytes to read (Java) [#28793](https://github.com/apache/beam/issues/28793).
* `watch_file_pattern` arg of the [RunInference](https://github.com/apache/beam/blob/104c10b3ee536a9a3ea52b4dbf62d86b669da5d9/sdks/python/apache_beam/ml/inference/base.py#L997) arg had no effect prior to 2.52.0. To use the behavior of arg `watch_file_pattern` prior to 2.52.0, follow the documentation at https://beam.apache.org/documentation/ml/side-input-updates/ and use `WatchFilePattern` PTransform as a SideInput. ([#28948](https://github.com/apache/beam/pulls/28948))
* `MLTransform` doesn't output artifacts such as min, max and quantiles. Instead, `MLTransform` will add a feature to output these artifacts as human readable format - [#29017](https://github.com/apache/beam/issues/29017). For now, to use the artifacts such as min and max that were produced by the eariler `MLTransform`, use `read_artifact_location` of `MLTransform`, which reads artifacts that were produced earlier in a different `MLTransform` ([#29016](https://github.com/apache/beam/pull/29016/))
* Fixed a memory leak, which affected some long-running Python pipelines: [#28246](https://github.com/apache/beam/issues/28246).

## Security Fixes
* Fixed [CVE-2023-39325](https://www.cve.org/CVERecord?id=CVE-2023-39325) (Java/Python/Go) ([#29118](https://github.com/apache/beam/issues/29118)).
* Mitigated [CVE-2023-47248](https://nvd.nist.gov/vuln/detail/CVE-2023-47248)  (Python) [#29392](https://github.com/apache/beam/issues/29392).

## Known issues

* MLTransform drops the identical elements in the output PCollection. For any duplicate elements, a single element will be emitted downstream. ([#29600](https://github.com/apache/beam/issues/29600)).
* Some Python pipelines that run with 2.52.0-2.54.0 SDKs and use large materialized side inputs might be affected by a performance regression. To restore the prior behavior on these SDK versions, supply the `--max_cache_memory_usage_mb=0` pipeline option. (Python) ([#30360](https://github.com/apache/beam/issues/30360)).
* Users who lauch Python pipelines in an environment without internet access and use the `--setup_file` pipeline option might experience an increase in pipeline submission time. This has been fixed in 2.56.0 ([#31070](https://github.com/apache/beam/pull/31070)).

# [2.51.0] - 2023-10-03

## New Features / Improvements

* In Python, [RunInference](https://beam.apache.org/documentation/sdks/python-machine-learning/#why-use-the-runinference-api) now supports loading many models in the same transform using a [KeyedModelHandler](https://beam.apache.org/documentation/sdks/python-machine-learning/#use-a-keyed-modelhandler) ([#27628](https://github.com/apache/beam/issues/27628)).
* In Python, the [VertexAIModelHandlerJSON](https://beam.apache.org/releases/pydoc/current/apache_beam.ml.inference.vertex_ai_inference.html#apache_beam.ml.inference.vertex_ai_inference.VertexAIModelHandlerJSON) now supports passing in inference_args. These will be passed through to the Vertex endpoint as parameters.
* Added support to run `mypy` on user pipelines ([#27906](https://github.com/apache/beam/issues/27906))
* Python SDK worker start-up logs and crash logs are now captured by a buffer and logged at appropriate levels via Beam logging API. Dataflow Runner users might observe that most `worker-startup` log content is now captured by the `worker` logger. Users who relied on `print()` statements for logging might notice that some logs don't flush before pipeline succeeds - we strongly advise to use `logging` package instead of `print()` statements for logging. ([#28317](https://github.com/apache/beam/pull/28317))


## Breaking Changes

* Removed fastjson library dependency for Beam SQL. Table property is changed to be based on jackson ObjectNode (Java) ([#24154](https://github.com/apache/beam/issues/24154)).
* Removed TensorFlow from Beam Python container images [PR](https://github.com/apache/beam/pull/28424). If you have been negatively affected by this change, please comment on [#20605](https://github.com/apache/beam/issues/20605).
* Removed the parameter `t reflect.Type` from `parquetio.Write`. The element type is derived from the input PCollection (Go) ([#28490](https://github.com/apache/beam/issues/28490))
* Refactor BeamSqlSeekableTable.setUp adding a parameter joinSubsetType. [#28283](https://github.com/apache/beam/issues/28283)


## Bugfixes

* Fixed exception chaining issue in GCS connector (Python) ([#26769](https://github.com/apache/beam/issues/26769#issuecomment-1700422615)).
* Fixed streaming inserts exception handling, GoogleAPICallErrors are now retried according to retry strategy and routed to failed rows where appropriate rather than causing a pipeline error (Python) ([#21080](https://github.com/apache/beam/issues/21080)).
* Fixed a bug in Python SDK's cross-language Bigtable sink that mishandled records that don't have an explicit timestamp set: [#28632](https://github.com/apache/beam/issues/28632).


## Security Fixes
* Python containers updated, fixing [CVE-2021-30474](https://nvd.nist.gov/vuln/detail/CVE-2021-30474), [CVE-2021-30475](https://nvd.nist.gov/vuln/detail/CVE-2021-30475), [CVE-2021-30473](https://nvd.nist.gov/vuln/detail/CVE-2021-30473), [CVE-2020-36133](https://nvd.nist.gov/vuln/detail/CVE-2020-36133), [CVE-2020-36131](https://nvd.nist.gov/vuln/detail/CVE-2020-36131), [CVE-2020-36130](https://nvd.nist.gov/vuln/detail/CVE-2020-36130), and [CVE-2020-36135](https://nvd.nist.gov/vuln/detail/CVE-2020-36135)
* Used go 1.21.1 to build, fixing [CVE-2023-39320](https://security-tracker.debian.org/tracker/CVE-2023-39320)

## Known Issues

* Long-running Python pipelines might experience a memory leak: [#28246](https://github.com/apache/beam/issues/28246).
* Python pipelines using BigQuery Storage Read API might need to pin `fastavro`
  dependency to 1.8.3 or earlier on some runners that don't use Beam Docker containers: [#28811](https://github.com/apache/beam/issues/28811)
* MLTransform drops the identical elements in the output PCollection. For any duplicate elements, a single element will be emitted downstream. ([#29600](https://github.com/apache/beam/issues/29600)).


# [2.50.0] - 2023-08-30

## Highlights

* Spark 3.2.2 is used as default version for Spark runner ([#23804](https://github.com/apache/beam/issues/23804)).
* The Go SDK has a new default local runner, called Prism ([#24789](https://github.com/apache/beam/issues/24789)).
* All Beam released container images are now [multi-arch images](https://cloud.google.com/kubernetes-engine/docs/how-to/build-multi-arch-for-arm#what_is_a_multi-arch_image) that support both x86 and ARM CPU architectures.

## I/Os

* Java KafkaIO now supports picking up topics via topicPattern ([#26948](https://github.com/apache/beam/pull/26948))
* Support for read from Cosmos DB Core SQL API ([#23604](https://github.com/apache/beam/issues/23604))
* Upgraded to HBase 2.5.5 for HBaseIO. (Java) ([#27711](https://github.com/apache/beam/issues/19554))
* Added support for GoogleAdsIO source (Java) ([#27681](https://github.com/apache/beam/pull/27681)).

## New Features / Improvements

* The Go SDK now requires Go 1.20 to build. ([#27558](https://github.com/apache/beam/issues/27558))
* The Go SDK has a new default local runner, Prism. ([#24789](https://github.com/apache/beam/issues/24789)).
  * Prism is a portable runner that executes each transform independantly, ensuring coders.
  * At this point it supercedes the Go direct runner in functionality. The Go direct runner is now deprecated.
  * See https://github.com/apache/beam/blob/master/sdks/go/pkg/beam/runners/prism/README.md for the goals and features of Prism.
* Hugging Face Model Handler for RunInference added to Python SDK. ([#26632](https://github.com/apache/beam/pull/26632))
* Hugging Face Pipelines support for RunInference added to Python SDK. ([#27399](https://github.com/apache/beam/pull/27399))
* Vertex AI Model Handler for RunInference now supports private endpoints ([#27696](https://github.com/apache/beam/pull/27696))
* MLTransform transform added with support for common ML pre/postprocessing operations ([#26795](https://github.com/apache/beam/pull/26795))
* Upgraded the Kryo extension for the Java SDK to Kryo 5.5.0. This brings in bug fixes, performance improvements, and serialization of Java 14 records. ([#27635](https://github.com/apache/beam/issues/27635))
* All Beam released container images are now [multi-arch images](https://cloud.google.com/kubernetes-engine/docs/how-to/build-multi-arch-for-arm#what_is_a_multi-arch_image) that support both x86 and ARM CPU architectures. ([#27674](https://github.com/apache/beam/issues/27674)). The multi-arch container images include:
  * All versions of Go, Python, Java and Typescript SDK containers.
  * All versions of Flink job server containers.
  * Java and Python expansion service containers.
  * Transform service controller container.
  * Spark3 job server container.
* Added support for batched writes to AWS SQS for improved throughput (Java, AWS 2).([#21429](https://github.com/apache/beam/issues/21429))

## Breaking Changes

* Python SDK: Legacy runner support removed from Dataflow, all pipelines must use runner v2.
* Python SDK: Dataflow Runner will no longer stage Beam SDK from PyPI in the `--staging_location` at pipeline submission. Custom container images that are not based on Beam's default image must include Apache Beam installation.([#26996](https://github.com/apache/beam/issues/26996))

## Deprecations

* The Go Direct Runner is now Deprecated. It remains available to reduce migration churn.
  * Tests can be set back to the direct runner by overriding TestMain: `func TestMain(m *testing.M) { ptest.MainWithDefault(m, "direct") }`
  * It's recommended to fix issues seen in tests using Prism, as they can also happen on any portable runner.
  * Use the generic register package for your pipeline DoFns to ensure pipelines function on portable runners, like prism.
  * Do not rely on closures or using package globals for DoFn configuration. They don't function on portable runners.

## Bugfixes

* Fixed DirectRunner bug in Python SDK where GroupByKey gets empty PCollection and fails when pipeline option `direct_num_workers!=1`.([#27373](https://github.com/apache/beam/pull/27373))
* Fixed BigQuery I/O bug when estimating size on queries that utilize row-level security ([#27474](https://github.com/apache/beam/pull/27474))

## Known Issues

* Long-running Python pipelines might experience a memory leak: [#28246](https://github.com/apache/beam/issues/28246).
* Python Pipelines using BigQuery IO or `orjson` dependency might experience segmentation faults or get stuck: [#28318](https://github.com/apache/beam/issues/28318).
* Beam Python containers rely on a version of Debian/aom that has several security vulnerabilities: [CVE-2021-30474](https://nvd.nist.gov/vuln/detail/CVE-2021-30474), [CVE-2021-30475](https://nvd.nist.gov/vuln/detail/CVE-2021-30475), [CVE-2021-30473](https://nvd.nist.gov/vuln/detail/CVE-2021-30473), [CVE-2020-36133](https://nvd.nist.gov/vuln/detail/CVE-2020-36133), [CVE-2020-36131](https://nvd.nist.gov/vuln/detail/CVE-2020-36131), [CVE-2020-36130](https://nvd.nist.gov/vuln/detail/CVE-2020-36130), and [CVE-2020-36135](https://nvd.nist.gov/vuln/detail/CVE-2020-36135)
* Python SDK's cross-language Bigtable sink mishandles records that don't have an explicit timestamp set: [#28632](https://github.com/apache/beam/issues/28632). To avoid this issue, set explicit timestamps for all records before writing to Bigtable.
* Python SDK worker start-up logs, particularly PIP dependency installations, that are not logged at warning or higher are suppressed. This suppression is reverted in 2.51.0.
* MLTransform drops the identical elements in the output PCollection. For any duplicate elements, a single element will be emitted downstream. ([#29600](https://github.com/apache/beam/issues/29600)).

# [2.49.0] - 2023-07-17


## I/Os

* Support for Bigtable Change Streams added in Java `BigtableIO.ReadChangeStream` ([#27183](https://github.com/apache/beam/issues/27183))

## New Features / Improvements

* Allow prebuilding large images when using `--prebuild_sdk_container_engine=cloud_build`, like images depending on `tensorflow` or `torch` ([#27023](https://github.com/apache/beam/pull/27023)).
* Disabled `pip` cache when installing packages on the workers. This reduces the size of prebuilt Python container images ([#27035](https://github.com/apache/beam/pull/27035)).
* Select dedicated avro datum reader and writer (Java) ([#18874](https://github.com/apache/beam/issues/18874)).
* Timer API for the Go SDK (Go) ([#22737](https://github.com/apache/beam/issues/22737)).

## Deprecations

* Removed Python 3.7 support. ([#26447](https://github.com/apache/beam/issues/26447))

## Bugfixes

* Fixed KinesisIO `NullPointerException` when a progress check is made before the reader is started (IO) ([#23868](https://github.com/apache/beam/issues/23868))

## Known Issues

* Long-running Python pipelines might experience a memory leak: [#28246](https://github.com/apache/beam/issues/28246).


# [2.48.0] - 2023-05-31

## Highlights

* "Experimental" annotation cleanup: the annotation and concept have been removed from Beam to avoid
  the misperception of code as "not ready". Any proposed breaking changes will be subject to
  case-by-case pro/con decision making (and generally avoided) rather than using the "Experimental"
  to allow them.

## I/Os

* Added rename for GCS and copy for local filesystem (Go) ([#25779](https://github.com/apache/beam/issues/26064)).
* Added support for enhanced fan-out in KinesisIO.Read (Java) ([#19967](https://github.com/apache/beam/issues/19967)).
  * This change is not compatible with Flink savepoints created by Beam 2.46.0 applications which had KinesisIO sources.
* Added textio.ReadWithFilename transform (Go) ([#25812](https://github.com/apache/beam/issues/25812)).
* Added fileio.MatchContinuously transform (Go) ([#26186](https://github.com/apache/beam/issues/26186)).

## New Features / Improvements

* Allow passing service name for google-cloud-profiler (Python) ([#26280](https://github.com/apache/beam/issues/26280)).
* Dead letter queue support added to RunInference in Python ([#24209](https://github.com/apache/beam/issues/24209)).
* Support added for defining pre/postprocessing operations on the RunInference transform ([#26308](https://github.com/apache/beam/issues/26308))
* Adds a Docker Compose based transform service that can be used to discover and use portable Beam transforms ([#26023](https://github.com/apache/beam/pull/26023)).

## Breaking Changes

* Passing a tag into MultiProcessShared is now required in the Python SDK ([#26168](https://github.com/apache/beam/issues/26168)).
* CloudDebuggerOptions is removed (deprecated in Beam v2.47.0) for Dataflow runner as the Google Cloud Debugger service is [shutting down](https://cloud.google.com/debugger/docs/deprecations). (Java) ([#25959](https://github.com/apache/beam/issues/25959)).
* AWS 2 client providers (deprecated in Beam [v2.38.0](#2380---2022-04-20)) are finally removed ([#26681](https://github.com/apache/beam/issues/26681)).
* AWS 2 SnsIO.writeAsync (deprecated in Beam v2.37.0 due to risk of data loss) was finally removed ([#26710](https://github.com/apache/beam/issues/26710)).
* AWS 2 coders (deprecated in Beam v2.43.0 when adding Schema support for AWS Sdk Pojos) are finally removed ([#23315](https://github.com/apache/beam/issues/23315)).

## Deprecations


## Bugfixes

* Fixed Java bootloader failing with Too Long Args due to long classpaths, with a pathing jar. (Java) ([#25582](https://github.com/apache/beam/issues/25582)).

## Known Issues

* PubsubIO writes will throw *SizeLimitExceededException* for any message above 100 bytes, when used in batch (bounded) mode. (Java) ([#27000](https://github.com/apache/beam/issues/27000)).
* Long-running Python pipelines might experience a memory leak: [#28246](https://github.com/apache/beam/issues/28246).
* Python SDK's cross-language Bigtable sink mishandles records that don't have an explicit timestamp set: [#28632](https://github.com/apache/beam/issues/28632). To avoid this issue, set explicit timestamps for all records before writing to Bigtable.


# [2.47.0] - 2023-05-10

## Highlights

* Apache Beam adds Python 3.11 support ([#23848](https://github.com/apache/beam/issues/23848)).

## I/Os

* BigQuery Storage Write API is now available in Python SDK via cross-language ([#21961](https://github.com/apache/beam/issues/21961)).
* Added HbaseIO support for writing RowMutations (ordered by rowkey) to Hbase (Java) ([#25830](https://github.com/apache/beam/issues/25830)).
* Added fileio transforms MatchFiles, MatchAll and ReadMatches (Go) ([#25779](https://github.com/apache/beam/issues/25779)).
* Add integration test for JmsIO + fix issue with multiple connections (Java) ([#25887](https://github.com/apache/beam/issues/25887)).

## New Features / Improvements

* The Flink runner now supports Flink 1.16.x ([#25046](https://github.com/apache/beam/issues/25046)).
* Schema'd PTransforms can now be directly applied to Beam dataframes just like PCollections.
  (Note that when doing multiple operations, it may be more efficient to explicitly chain the operations
  like `df | (Transform1 | Transform2 | ...)` to avoid excessive conversions.)
* The Go SDK adds new transforms periodic.Impulse and periodic.Sequence that extends support
  for slowly updating side input patterns. ([#23106](https://github.com/apache/beam/issues/23106))
* Several Google client libraries in Python SDK dependency chain were updated to latest available major versions. ([#24599](https://github.com/apache/beam/pull/24599))

## Breaking Changes

* If a main session fails to load, the pipeline will now fail at worker startup. ([#25401](https://github.com/apache/beam/issues/25401)).
* Python pipeline options will now ignore unparsed command line flags prefixed with a single dash. ([#25943](https://github.com/apache/beam/issues/25943)).
* The SmallestPerKey combiner now requires keyword-only arguments for specifying optional parameters, such as `key` and `reverse`. ([#25888](https://github.com/apache/beam/issues/25888)).

## Deprecations

* Cloud Debugger support and its pipeline options are deprecated and will be removed in the next Beam version,
  in response to the Google Cloud Debugger service [turning down](https://cloud.google.com/debugger/docs/deprecations). (Java) ([#25959](https://github.com/apache/beam/issues/25959)).

## Bugfixes

* BigQuery sink in STORAGE_WRITE_API mode in batch pipelines could result in data consistency issues during the handling of other unrelated transient errors for Beam SDKs 2.35.0 - 2.46.0 (inclusive). For more details see: https://github.com/apache/beam/issues/26521

## Known Issues

* The google-cloud-profiler dependency was accidentally removed from Beam's Python Docker
  Image [#26998](https://github.com/apache/beam/issues/26698). [Dataflow Docker images](https://cloud.google.com/dataflow/docs/concepts/sdk-worker-dependencies) still preinstall this dependency.
* Long-running Python pipelines might experience a memory leak: [#28246](https://github.com/apache/beam/issues/28246).

# [2.46.0] - 2023-03-10

## Highlights

* Java SDK containers migrated to [Eclipse Temurin](https://hub.docker.com/_/eclipse-temurin)
  as a base. This change migrates away from the deprecated [OpenJDK](https://hub.docker.com/_/openjdk)
  container. Eclipse Temurin is currently based upon Ubuntu 22.04 while the OpenJDK
  container was based upon Debian 11.
* RunInference PTransform will accept model paths as SideInputs in Python SDK. ([#24042](https://github.com/apache/beam/issues/24042))
* RunInference supports ONNX runtime in Python SDK ([#22972](https://github.com/apache/beam/issues/22972))
* Tensorflow Model Handler for RunInference in Python SDK ([#25366](https://github.com/apache/beam/issues/25366))
* Java SDK modules migrated to use `:sdks:java:extensions:avro` ([#24748](https://github.com/apache/beam/issues/24748))

## I/Os

* Added in JmsIO a retry policy for failed publications (Java) ([#24971](https://github.com/apache/beam/issues/24971)).
* Support for `LZMA` compression/decompression of text files added to the Python SDK ([#25316](https://github.com/apache/beam/issues/25316))
* Added ReadFrom/WriteTo Csv/Json as top-level transforms to the Python SDK.

## New Features / Improvements

* Add UDF metrics support for Samza portable mode.
* Option for SparkRunner to avoid the need of SDF output to fit in memory ([#23852](https://github.com/apache/beam/issues/23852)).
  This helps e.g. with ParquetIO reads. Turn the feature on by adding experiment `use_bounded_concurrent_output_for_sdf`.
* Add `WatchFilePattern` transform, which can be used as a side input to the RunInference PTransfrom to watch for model updates using a file pattern. ([#24042](https://github.com/apache/beam/issues/24042))
* Add support for loading TorchScript models with `PytorchModelHandler`. The TorchScript model path can be
  passed to PytorchModelHandler using `torch_script_model_path=<path_to_model>`. ([#25321](https://github.com/apache/beam/pull/25321))
* The Go SDK now requires Go 1.19 to build. ([#25545](https://github.com/apache/beam/pull/25545))
* The Go SDK now has an initial native Go implementation of a portable Beam Runner called Prism. ([#24789](https://github.com/apache/beam/pull/24789))
  * For more details and current state see https://github.com/apache/beam/tree/master/sdks/go/pkg/beam/runners/prism.

## Breaking Changes

* The deprecated SparkRunner for Spark 2 (see [2.41.0](#2410---2022-08-23)) was removed ([#25263](https://github.com/apache/beam/pull/25263)).
* Python's BatchElements performs more aggressive batching in some cases,
  capping at 10 second rather than 1 second batches by default and excluding
  fixed cost in this computation to better handle cases where the fixed cost
  is larger than a single second. To get the old behavior, one can pass
  `target_batch_duration_secs_including_fixed_cost=1` to BatchElements.
* Dataflow runner enables sibling SDK protocol for Python pipelines using custom containers on Beam 2.46.0 and newer SDKs.
  If your Python pipeline starts to stall after you switch to 2.46.0 and you use a custom container, please verify
  that your custom container does not include artifacts from older Beam SDK releases. In particular, check in your `Dockerfile`
  that the Beam container entrypoint and/or Beam base image version match the Beam SDK version used at job submission.

## Deprecations

* Avro related classes are deprecated in module `beam-sdks-java-core` and will be eventually removed. Please, migrate to a new module `beam-sdks-java-extensions-avro` instead by importing the classes from `org.apache.beam.sdk.extensions.avro` package.
  For the sake of migration simplicity, the relative package path and the whole class hierarchy of Avro related classes in new module is preserved the same as it was before.
  For example, import `org.apache.beam.sdk.extensions.avro.coders.AvroCoder` class instead of`org.apache.beam.sdk.coders.AvroCoder`. ([#24749](https://github.com/apache/beam/issues/24749)).

## Bugfixes


# [2.45.0] - 2023-02-15

## I/Os

* Support for X source added (Java/Python) ([#X](https://github.com/apache/beam/issues/X)).
* MongoDB IO connector added (Go) ([#24575](https://github.com/apache/beam/issues/24575)).

## New Features / Improvements

* RunInference Wrapper with Sklearn Model Handler support added in Go SDK ([#24497](https://github.com/apache/beam/issues/23382)).
* Adding override of allowed TLS algorithms (Java), now maintaining the disabled/legacy algorithms
  present in 2.43.0 (up to 1.8.0_342, 11.0.16, 17.0.2 for respective Java versions). This is accompanied
  by an explicit re-enabling of TLSv1 and TLSv1.1 for Java 8 and Java 11.
* Add UDF metrics support for Samza portable mode.

## Breaking Changes

* Portable Java pipelines, Go pipelines, Python streaming pipelines, and portable Python batch
  pipelines on Dataflow are required to use Runner V2. The `disable_runner_v2`,
  `disable_runner_v2_until_2023`, `disable_prime_runner_v2` experiments will raise an error during
  pipeline construction. You can no longer specify the Dataflow worker jar override. Note that
  non-portable Java jobs and non-portable Python batch jobs are not impacted. ([#24515](https://github.com/apache/beam/issues/24515)).
* Beam now requires `pyarrow>=3` and `pandas>=1.4.3` since older versions are not compatible with `numpy==1.24.0`.

## Bugfixes

* Avoids Cassandra syntax error when user-defined query has no where clause in it (Java) ([#24829](https://github.com/apache/beam/issues/24829)).
* Fixed JDBC connection failures (Java) during handshake due to deprecated TLSv1(.1) protocol for the JDK. ([#24623](https://github.com/apache/beam/issues/24623))
* Fixed Python BigQuery Batch Load write may truncate valid data when deposition sets to WRITE_TRUNCATE and incoming data is large (Python) ([#24623](https://github.com/apache/beam/issues/24535)).

# [2.44.0] - 2023-01-12

## I/Os

* Support for Bigtable sink (Write and WriteBatch) added (Go) ([#23324](https://github.com/apache/beam/issues/23324)).
* S3 implementation of the Beam filesystem (Go) ([#23991](https://github.com/apache/beam/issues/23991)).
* Support for SingleStoreDB source and sink added (Java) ([#22617](https://github.com/apache/beam/issues/22617)).
* Added support for DefaultAzureCredential authentication in Azure Filesystem (Python) ([#24210](https://github.com/apache/beam/issues/24210)).

## New Features / Improvements

* Beam now provides a portable "runner" that can render pipeline graphs with
  graphviz.  See `python -m apache_beam.runners.render --help` for more details.
* Local packages can now be used as dependencies in the requirements.txt file, rather
  than requiring them to be passed separately via the `--extra_package` option
  (Python) ([#23684](https://github.com/apache/beam/pull/23684)).
* Pipeline Resource Hints now supported via `--resource_hints` flag (Go) ([#23990](https://github.com/apache/beam/pull/23990)).
* Make Python SDK containers reusable on portable runners by installing dependencies to temporary venvs ([BEAM-12792](https://issues.apache.org/jira/browse/BEAM-12792), [#16658](https://github.com/apache/beam/pull/16658)).
* RunInference model handlers now support the specification of a custom inference function in Python ([#22572](https://github.com/apache/beam/issues/22572)).
* Support for `map_windows` urn added to Go SDK ([#24307](https://github.apache/beam/pull/24307)).

## Breaking Changes

* `ParquetIO.withSplit` was removed since splittable reading has been the default behavior since 2.35.0. The effect of
  this change is to drop support for non-splittable reading (Java)([#23832](https://github.com/apache/beam/issues/23832)).
* `beam-sdks-java-extensions-google-cloud-platform-core` is no longer a
  dependency of the Java SDK Harness. Some users of a portable runner (such as Dataflow Runner v2)
  may have an undeclared dependency on this package (for example using GCS with
  TextIO) and will now need to declare the dependency.
* `beam-sdks-java-core` is no longer a dependency of the Java SDK Harness. Users of a portable
  runner (such as Dataflow Runner v2) will need to provide this package and its dependencies.
* Slices now use the Beam Iterable Coder. This enables cross language use, but breaks pipeline updates
  if a Slice type is used as a PCollection element or State API element. (Go)[#24339](https://github.com/apache/beam/issues/24339)
* If you activated a virtual environment in your custom container image, this environment might no longer be activated, since a new environment will be created (see the note about [BEAM-12792](https://issues.apache.org/jira/browse/BEAM-12792) above).
  To work around, install dependencies into the default (global) python environment. When using poetry you may need to use `poetry config virtualenvs.create false` before installing deps, see an example in: [#25085](https://github.com/apache/beam/issues/25085).
  If you were negatively impacted by this change and cannot find a workaround, feel free to chime in on [#16658](https://github.com/apache/beam/pull/16658).
  To disable this behavior, you could upgrade to Beam 2.48.0 and set an environment variable
  `ENV RUN_PYTHON_SDK_IN_DEFAULT_ENVIRONMENT=1` in your Dockerfile.

## Deprecations

* X behavior is deprecated and will be removed in X versions ([#X](https://github.com/apache/beam/issues/X)).

## Bugfixes

* Fixed X (Java/Python) ([#X](https://github.com/apache/beam/issues/X)).
* Fixed JmsIO acknowledgment issue (Java) ([#20814](https://github.com/apache/beam/issues/20814))
* Fixed Beam SQL CalciteUtils (Java) and Cross-language JdbcIO (Python) did not support JDBC CHAR/VARCHAR, BINARY/VARBINARY logical types ([#23747](https://github.com/apache/beam/issues/23747), [#23526](https://github.com/apache/beam/issues/23526)).
* Ensure iterated and emitted types are used with the generic register package are registered with the type and schema registries.(Go) ([#23889](https://github.com/apache/beam/pull/23889))


# [2.43.0] - 2022-11-17

## Highlights

* Python 3.10 support in Apache Beam ([#21458](https://github.com/apache/beam/issues/21458)).
* An initial implementation of a runner that allows us to run Beam pipelines on Dask. Try it out and give us feedback! (Python) ([#18962](https://github.com/apache/beam/issues/18962)).

## I/Os

* Decreased TextSource CPU utilization by 2.3x (Java) ([#23193](https://github.com/apache/beam/issues/23193)).
* Fixed bug when using SpannerIO with RuntimeValueProvider options (Java) ([#22146](https://github.com/apache/beam/issues/22146)).
* Fixed issue for unicode rendering on WriteToBigQuery ([#22312](https://github.com/apache/beam/issues/22312))
* Remove obsolete variants of BigQuery Read and Write, always using Beam-native variant
  ([#23564](https://github.com/apache/beam/issues/23564) and [#23559](https://github.com/apache/beam/issues/23559)).
* Bumped google-cloud-spanner dependency version to 3.x for Python SDK ([#21198](https://github.com/apache/beam/issues/21198)).

## New Features / Improvements

* Dataframe wrapper added in Go SDK via Cross-Language (with automatic expansion service). (Go) ([#23384](https://github.com/apache/beam/issues/23384)).
* Name all Java threads to aid in debugging ([#23049](https://github.com/apache/beam/issues/23049)).
* An initial implementation of a runner that allows us to run Beam pipelines on Dask. (Python) ([#18962](https://github.com/apache/beam/issues/18962)).
* Allow configuring GCP OAuth scopes via pipeline options. This unblocks usages of Beam IOs that require additional scopes.
  For example, this feature makes it possible to access Google Drive backed tables in BigQuery ([#23290](https://github.com/apache/beam/issues/23290)).
* An example for using Python RunInference from Java ([#23290](https://github.com/apache/beam/pull/23619)).
* Data can now be read from BigQuery and directly plumbed into a DeferredDataframe in the Dataframe API. Users no longer have to re-specify the schema in this case ([#22907](https://github.com/apache/beam/pull/22907)).

## Breaking Changes

* CoGroupByKey transform in Python SDK has changed the output typehint. The typehint component representing grouped values changed from List to Iterable,
  which more accurately reflects the nature of the arbitrarily large output collection. [#21556](https://github.com/apache/beam/issues/21556) Beam users may see an error on transforms downstream from CoGroupByKey. Users must change methods expecting a List to expect an Iterable going forward. See [document](https://docs.google.com/document/d/1RIzm8-g-0CyVsPb6yasjwokJQFoKHG4NjRUcKHKINu0) for information and fixes.
* The PortableRunner for Spark assumes Spark 3 as default Spark major version unless configured otherwise using `--spark_version`.
  Spark 2 support is deprecated and will be removed soon ([#23728](https://github.com/apache/beam/issues/23728)).

## Bugfixes

* Fixed Python cross-language JDBC IO Connector cannot read or write rows containing Numeric/Decimal type values ([#19817](https://github.com/apache/beam/issues/19817)).

# [2.42.0] - 2022-10-17

## Highlights

* Added support for stateful DoFns to the Go SDK.
* Added support for [Batched
  DoFns](https://beam.apache.org/documentation/programming-guide/#batched-dofns)
  to the Python SDK.

## New Features / Improvements

* Added support for Zstd compression to the Python SDK.
* Added support for Google Cloud Profiler to the Go SDK.
* Added support for stateful DoFns to the Go SDK.

## Breaking Changes

* The Go SDK's Row Coder now uses a different single-precision float encoding for float32 types to match Java's behavior ([#22629](https://github.com/apache/beam/issues/22629)).

## Bugfixes

* Fixed Python cross-language JDBC IO Connector cannot read or write rows containing Timestamp type values [#19817](https://github.com/apache/beam/issues/19817).
* Fixed `AfterProcessingTime` behavior in Python's `DirectRunner` to match Java ([#23071](https://github.com/apache/beam/issues/23071))

## Known Issues

* Go SDK doesn't yet support Slowly Changing Side Input pattern ([#23106](https://github.com/apache/beam/issues/23106))

# [2.41.0] - 2022-08-23

## I/Os

* Projection Pushdown optimizer is now on by default for streaming, matching the behavior of batch pipelines since 2.38.0. If you encounter a bug with the optimizer, please file an issue and disable the optimizer using pipeline option `--experiments=disable_projection_pushdown`.

## New Features / Improvements

* Previously available in Java sdk, Python sdk now also supports logging level overrides per module. ([#18222](https://github.com/apache/beam/issues/18222)).
* Added support for accessing GCP PubSub Message ordering keys (Java) ([BEAM-13592](https://issues.apache.org/jira/browse/BEAM-13592))

## Breaking Changes

* Projection Pushdown optimizer may break Dataflow upgrade compatibility for optimized pipelines when it removes unused fields. If you need to upgrade and encounter a compatibility issue, disable the optimizer using pipeline option `--experiments=disable_projection_pushdown`.

## Deprecations

* Support for Spark 2.4.x is deprecated and will be dropped with the release of Beam 2.44.0 or soon after (Spark runner) ([#22094](https://github.com/apache/beam/issues/22094)).
* The modules [amazon-web-services](https://github.com/apache/beam/tree/master/sdks/java/io/amazon-web-services) and
  [kinesis](https://github.com/apache/beam/tree/master/sdks/java/io/kinesis) for AWS Java SDK v1 are deprecated
  in favor of [amazon-web-services2](https://github.com/apache/beam/tree/master/sdks/java/io/amazon-web-services2)
  and will be eventually removed after a few Beam releases (Java) ([#21249](https://github.com/apache/beam/issues/21249)).

## Bugfixes

* Fixed a condition where retrying queries would yield an incorrect cursor in the Java SDK Firestore Connector ([#22089](https://github.com/apache/beam/issues/22089)).
* Fixed plumbing allowed lateness in Go SDK. It was ignoring the user set value earlier and always used to set to 0. ([#22474](https://github.com/apache/beam/issues/22474)).


# [2.40.0] - 2022-06-25

## Highlights

* Added [RunInference](https://s.apache.org/inference-sklearn-pytorch) API, a framework agnostic transform for inference. With this release, PyTorch and Scikit-learn are supported by the transform.
    See also example at apache_beam/examples/inference/pytorch_image_classification.py

## I/Os

* Upgraded to Hive 3.1.3 for HCatalogIO. Users can still provide their own version of Hive. (Java) ([Issue-19554](https://github.com/apache/beam/issues/19554)).

## New Features / Improvements

* Go SDK users can now use generic registration functions to optimize their DoFn execution. ([BEAM-14347](https://issues.apache.org/jira/browse/BEAM-14347))
* Go SDK users may now write self-checkpointing Splittable DoFns to read from streaming sources. ([BEAM-11104](https://issues.apache.org/jira/browse/BEAM-11104))
* Go SDK textio Reads have been moved to Splittable DoFns exclusively. ([BEAM-14489](https://issues.apache.org/jira/browse/BEAM-14489))
* Pipeline drain support added for Go SDK has now been tested. ([BEAM-11106](https://issues.apache.org/jira/browse/BEAM-11106))
* Go SDK users can now see heap usage, sideinput cache stats, and active process bundle stats in Worker Status. ([BEAM-13829](https://issues.apache.org/jira/browse/BEAM-13829))

## Breaking Changes

* The Go Sdk now requires a minimum version of 1.18 in order to support generics ([BEAM-14347](https://issues.apache.org/jira/browse/BEAM-14347)).
* synthetic.SourceConfig field types have changed to int64 from int for better compatibility with Flink's use of Logical types in Schemas (Go) ([BEAM-14173](https://issues.apache.org/jira/browse/BEAM-14173))
* Default coder updated to compress sources used with `BoundedSourceAsSDFWrapperFn` and `UnboundedSourceAsSDFWrapper`.

## Bugfixes
* Fixed Java expansion service to allow specific files to stage ([BEAM-14160](https://issues.apache.org/jira/browse/BEAM-14160)).
* Fixed Elasticsearch connection when using both ssl and username/password (Java) ([BEAM-14000](https://issues.apache.org/jira/browse/BEAM-14000))

# [2.39.0] - 2022-05-25

## Highlights

* Watermark estimation is now supported in the Go SDK ([BEAM-11105](https://issues.apache.org/jira/browse/BEAM-11105)).
* Support for impersonation credentials added to dataflow runner in the Java and Python SDK ([BEAM-14014](https://issues.apache.org/jira/browse/BEAM-14014)).
* Implemented Apache PulsarIO ([BEAM-8218](https://issues.apache.org/jira/browse/BEAM-8218)).

## I/Os

* JmsIO gains the ability to map any kind of input to any subclass of `javax.jms.Message` (Java) ([BEAM-16308](https://issues.apache.org/jira/browse/BEAM-16308)).
* JmsIO introduces the ability to write to dynamic topics (Java) ([BEAM-16308](https://issues.apache.org/jira/browse/BEAM-16308)).
  * A `topicNameMapper` must be set to extract the topic name from the input value.
  * A `valueMapper` must be set to convert the input value to JMS message.
* Reduce number of threads spawned by BigqueryIO StreamingInserts (
  [BEAM-14283](https://issues.apache.org/jira/browse/BEAM-14283)).
* Implemented Apache PulsarIO ([BEAM-8218](https://issues.apache.org/jira/browse/BEAM-8218)).


## New Features / Improvements

* Support for flink scala 2.12, because most of the libraries support version 2.12 onwards. ([beam-14386](https://issues.apache.org/jira/browse/BEAM-14386))
* 'Manage Clusters' JupyterLab extension added for users to configure usage of Dataproc clusters managed by Interactive Beam (Python) ([BEAM-14130](https://issues.apache.org/jira/browse/BEAM-14130)).
* Pipeline drain support added for Go SDK ([BEAM-11106](https://issues.apache.org/jira/browse/BEAM-11106)). **Note: this feature is not yet fully validated and should be treated as experimental in this release.**
* `DataFrame.unstack()`, `DataFrame.pivot() ` and  `Series.unstack()`
  implemented for DataFrame API ([BEAM-13948](https://issues.apache.org/jira/browse/BEAM-13948), [BEAM-13966](https://issues.apache.org/jira/browse/BEAM-13966)).
* Support for impersonation credentials added to dataflow runner in the Java and Python SDK ([BEAM-14014](https://issues.apache.org/jira/browse/BEAM-14014)).
* Implemented Jupyterlab extension for managing Dataproc clusters ([BEAM-14130](https://issues.apache.org/jira/browse/BEAM-14130)).
* ExternalPythonTransform API added for easily invoking Python transforms from
  Java ([BEAM-14143](https://issues.apache.org/jira/browse/BEAM-14143)).
* Added Add support for Elasticsearch 8.x ([BEAM-14003](https://issues.apache.org/jira/browse/BEAM-14003)).
* Shard aware Kinesis record aggregation (AWS Sdk v2), ([BEAM-14104](https://issues.apache.org/jira/browse/BEAM-14104)).
* Upgrade to ZetaSQL 2022.04.1 ([BEAM-14348](https://issues.apache.org/jira/browse/BEAM-14348)).
* Fixed ReadFromBigQuery cannot be used with the interactive runner ([BEAM-14112](https://issues.apache.org/jira/browse/BEAM-14112)).


## Breaking Changes

* Unused functions `ShallowCloneParDoPayload()`, `ShallowCloneSideInput()`, and `ShallowCloneFunctionSpec()` have been removed from the Go SDK's pipelinex package ([BEAM-13739](https://issues.apache.org/jira/browse/BEAM-13739)).
* JmsIO requires an explicit `valueMapper` to be set ([BEAM-16308](https://issues.apache.org/jira/browse/BEAM-16308)). You can use the `TextMessageMapper` to convert `String` inputs to JMS `TestMessage`s:
```java
  JmsIO.<String>write()
        .withConnectionFactory(jmsConnectionFactory)
        .withValueMapper(new TextMessageMapper());
```
* Coders in Python are expected to inherit from Coder. ([BEAM-14351](https://issues.apache.org/jira/browse/BEAM-14351)).
* New abstract method `metadata()` added to io.filesystem.FileSystem in the
  Python SDK. ([BEAM-14314](https://issues.apache.org/jira/browse/BEAM-14314))

## Deprecations

* Flink 1.11 is no longer supported ([BEAM-14139](https://issues.apache.org/jira/browse/BEAM-14139)).
* Python 3.6 is no longer supported ([BEAM-13657](https://issues.apache.org/jira/browse/BEAM-13657)).

## Bugfixes

* Fixed Java Spanner IO NPE when ProjectID not specified in template executions (Java) ([BEAM-14405](https://issues.apache.org/jira/browse/BEAM-14405)).
* Fixed potential NPE in BigQueryServicesImpl.getErrorInfo (Java) ([BEAM-14133](https://issues.apache.org/jira/browse/BEAM-14133)).


# [2.38.0] - 2022-04-20

## I/Os
* Introduce projection pushdown optimizer to the Java SDK ([BEAM-12976](https://issues.apache.org/jira/browse/BEAM-12976)). The optimizer currently only works on the [BigQuery Storage API](https://beam.apache.org/documentation/io/built-in/google-bigquery/#storage-api), but more I/Os will be added in future releases. If you encounter a bug with the optimizer, please file a JIRA and disable the optimizer using pipeline option `--experiments=disable_projection_pushdown`.
* A new IO for Neo4j graph databases was added. ([BEAM-1857](https://issues.apache.org/jira/browse/BEAM-1857))  It has the ability to update nodes and relationships using UNWIND statements and to read data using cypher statements with parameters.
* `amazon-web-services2` has reached feature parity and is finally recommended over the earlier `amazon-web-services` and `kinesis` modules (Java). These will be deprecated in one of the next releases ([BEAM-13174](https://issues.apache.org/jira/browse/BEAM-13174)).
  * Long outstanding write support for `Kinesis` was added ([BEAM-13175](https://issues.apache.org/jira/browse/BEAM-13175)).
  * Configuration was simplified and made consistent across all IOs, including the usage of `AwsOptions` ([BEAM-13563](https://issues.apache.org/jira/browse/BEAM-13563), [BEAM-13663](https://issues.apache.org/jira/browse/BEAM-13663), [BEAM-13587](https://issues.apache.org/jira/browse/BEAM-13587)).
  * Additionally, there's a long list of recent improvements and fixes to
    `S3` Filesystem ([BEAM-13245](https://issues.apache.org/jira/browse/BEAM-13245), [BEAM-13246](https://issues.apache.org/jira/browse/BEAM-13246), [BEAM-13441](https://issues.apache.org/jira/browse/BEAM-13441), [BEAM-13445](https://issues.apache.org/jira/browse/BEAM-13445), [BEAM-14011](https://issues.apache.org/jira/browse/BEAM-14011)),
    `DynamoDB` IO ([BEAM-13209](https://issues.apache.org/jira/browse/BEAM-13009), [BEAM-13209](https://issues.apache.org/jira/browse/BEAM-13209)),
    `SQS` IO ([BEAM-13631](https://issues.apache.org/jira/browse/BEAM-13631), [BEAM-13510](https://issues.apache.org/jira/browse/BEAM-13510)) and others.

## New Features / Improvements

* Pipeline dependencies supplied through `--requirements_file` will now be staged to the runner using binary distributions (wheels) of the PyPI packages for linux_x86_64 platform ([BEAM-4032](https://issues.apache.org/jira/browse/BEAM-4032)). To restore the behavior to use source distributions, set pipeline option `--requirements_cache_only_sources`. To skip staging the packages at submission time, set pipeline option `--requirements_cache=skip` (Python).
* The Flink runner now supports Flink 1.14.x ([BEAM-13106](https://issues.apache.org/jira/browse/BEAM-13106)).
* Interactive Beam now supports remotely executing Flink pipelines on Dataproc (Python) ([BEAM-14071](https://issues.apache.org/jira/browse/BEAM-14071)).

## Breaking Changes

* (Python) Previously `DoFn.infer_output_types` was expected to return `Iterable[element_type]` where `element_type` is the PCollection elemnt type. It is now expected to return `element_type`. Take care if you have overriden `infer_output_type` in a `DoFn` (this is not common). See [BEAM-13860](https://issues.apache.org/jira/browse/BEAM-13860).
* (`amazon-web-services2`) The types of `awsRegion` / `endpoint` in `AwsOptions` changed from String to `Region` / `URI` ([BEAM-13563](https://issues.apache.org/jira/browse/BEAM-13563)).

## Deprecations

* Beam 2.38.0 will be the last minor release to support Flink 1.11.
* (`amazon-web-services2`) Client providers (`withXYZClientProvider()`) as well as IO specific `RetryConfiguration`s are deprecated, instead use `withClientConfiguration()` or `AwsOptions` to configure AWS IOs / clients.
  Custom implementations of client providers shall be replaced with a respective `ClientBuilderFactory` and configured through `AwsOptions` ([BEAM-13563](https://issues.apache.org/jira/browse/BEAM-13563)).

## Bugfixes

* Fix S3 copy for large objects (Java) ([BEAM-14011](https://issues.apache.org/jira/browse/BEAM-14011))
* Fix quadratic behavior of pipeline canonicalization (Go) ([BEAM-14128](https://issues.apache.org/jira/browse/BEAM-14128))
  * This caused unnecessarily long pre-processing times before job submission for large complex pipelines.
* Fix `pyarrow` version parsing (Python)([BEAM-14235](https://issues.apache.org/jira/browse/BEAM-14235))

## Known Issues

* Some pipelines that use Java SpannerIO may raise a NPE when the project ID is not specified ([BEAM-14405](https://issues.apache.org/jira/browse/BEAM-14405))

# [2.37.0] - 2022-03-04

## Highlights
* Java 17 support for Dataflow ([BEAM-12240](https://issues.apache.org/jira/browse/BEAM-12240)).
  * Users using Dataflow Runner V2 may see issues with state cache due to inaccurate object sizes ([BEAM-13695](https://issues.apache.org/jira/browse/BEAM-13695)).
  * ZetaSql is currently unsupported ([issue](https://github.com/google/zetasql/issues/89)).
* Python 3.9 support in Apache Beam ([BEAM-12000](https://issues.apache.org/jira/browse/BEAM-12000)).

## I/Os

* Go SDK now has wrappers for the following Cross Language Transforms from Java, along with automatic expansion service startup for each.
    *  JDBCIO ([BEAM-13293](https://issues.apache.org/jira/browse/BEAM-13293)).
    *  Debezium ([BEAM-13761](https://issues.apache.org/jira/browse/BEAM-13761)).
    *  BeamSQL ([BEAM-13683](https://issues.apache.org/jira/browse/BEAM-13683)).
    *  BiqQuery ([BEAM-13732](https://issues.apache.org/jira/browse/BEAM-13732)).
    *  KafkaIO now also has automatic expansion service startup. ([BEAM-13821](https://issues.apache.org/jira/browse/BEAM-13821)).

## New Features / Improvements

* DataFrame API now supports pandas 1.4.x ([BEAM-13605](https://issues.apache.org/jira/browse/BEAM-13605)).
* Go SDK DoFns can now observe trigger panes directly ([BEAM-13757](https://issues.apache.org/jira/browse/BEAM-13757)).
* Added option to specify a caching directory in Interactive Beam (Python) ([BEAM-13685](https://issues.apache.org/jira/browse/BEAM-13685)).
* Added support for caching batch pipelines to GCS in Interactive Beam (Python) ([BEAM-13734](https://issues.apache.org/jira/browse/BEAM-13734)).

## Breaking Changes

## Deprecations

## Bugfixes

## Known Issues

* On rare occations, Python Datastore source may swallow some exceptions. Users are adviced to upgrade to Beam 2.38.0 or later ([BEAM-14282](https://issues.apache.org/jira/browse/BEAM-14282))
* On rare occations, Python GCS source may swallow some exceptions. Users are adviced to upgrade to Beam 2.38.0 or later ([BEAM-14282](https://issues.apache.org/jira/browse/BEAM-14282))

# [2.36.0] - 2022-02-07

## I/Os

* Support for stopReadTime on KafkaIO SDF (Java).([BEAM-13171](https://issues.apache.org/jira/browse/BEAM-13171)).
* Added ability to register URI schemes to use the S3 protocol via FileIO using amazon-web-services2 (amazon-web-services already had this ability). ([BEAM-12435](https://issues.apache.org/jira/brows/BEAM-12435), [BEAM-13245](https://issues.apache.org/jira/brows/BEAM-13245)).

## New Features / Improvements

* Added support for cloudpickle as a pickling library for Python SDK ([BEAM-8123](https://issues.apache.org/jira/browse/BEAM-8123)). To use cloudpickle, set pipeline option: --pickler_lib=cloudpickle
* Added option to specify triggering frequency when streaming to BigQuery (Python) ([BEAM-12865](https://issues.apache.org/jira/browse/BEAM-12865)).
* Added option to enable caching uploaded artifacts across job runs for Python Dataflow jobs ([BEAM-13459](https://issues.apache.org/jira/browse/BEAM-13459)).  To enable, set pipeline option: --enable_artifact_caching, this will be enabled by default in a future release.

## Breaking Changes

* Updated the jedis from 3.x to 4.x to Java RedisIO. If you are using RedisIO and using jedis directly, please refer to [this page](https://github.com/redis/jedis/blob/v4.0.0/docs/3to4.md) to update it. ([BEAM-12092](https://issues.apache.org/jira/browse/BEAM-12092)).
* Datatype of timestamp fields in `SqsMessage` for AWS IOs for SDK v2 was changed from `String` to `long`, visibility of all fields was fixed from `package private` to `public` [BEAM-13638](https://issues.apache.org/jira/browse/BEAM-13638).

## Bugfixes

* Properly check output timestamps on elements output from DoFns, timers, and onWindowExpiration in Java [BEAM-12931](https://issues.apache.org/jira/browse/BEAM-12931).
* Fixed a bug with DeferredDataFrame.xs when used with a non-tuple key
  ([BEAM-13421](https://issues.apache.org/jira/browse/BEAM-13421])).

# Known Issues

* Users may encounter an unexpected java.lang.ArithmeticException when outputting a timestamp
  for an element further than allowedSkew from an allowed DoFN skew set to a value more than
  Integer.MAX_VALUE.
* On rare occations, Python Datastore source may swallow some exceptions. Users are adviced to upgrade to Beam 2.38.0 or later ([BEAM-14282](https://issues.apache.org/jira/browse/BEAM-14282))
* On rare occations, Python GCS source may swallow some exceptions. Users are adviced to upgrade to Beam 2.38.0 or later ([BEAM-14282](https://issues.apache.org/jira/browse/BEAM-14282))
* On rare occations, Java SpannerIO source may swallow some exceptions. Users are adviced to upgrade to Beam 2.37.0 or later ([BEAM-14005](https://issues.apache.org/jira/browse/BEAM-14005))

# [2.35.0] - 2021-12-29

## Highlights

* MultiMap side inputs are now supported by the Go SDK ([BEAM-3293](https://issues.apache.org/jira/browse/BEAM-3293)).
* Side inputs are supported within Splittable DoFns for Dataflow Runner V1 and Dataflow Runner V2. ([BEAM-12522](https://issues.apache.org/jira/browse/BEAM-12522)).
* Upgrades Log4j version used in test suites (Apache Beam testing environment only, not for end user consumption) to 2.17.0([BEAM-13434](https://issues.apache.org/jira/browse/BEAM-13434)).
    Note that Apache Beam versions do not depend on the Log4j 2 dependency (log4j-core) impacted by [CVE-2021-44228](https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2021-44228).
    However we urge users to update direct and indirect dependencies (if any) on Log4j 2 to the latest version by updating their build configuration and redeploying impacted pipelines.

## I/Os

* We changed the data type for ranges in `JdbcIO.readWithPartitions` from `int` to `long` ([BEAM-13149](https://issues.apache.org/jira/browse/BEAM-13149)).
    This is a relatively minor breaking change, which we're implementing to improve the usability of the transform without increasing cruft.
    This transform is relatively new, so we may implement other breaking changes in the future to improve its usability.
* Side inputs are supported within Splittable DoFns for Dataflow Runner V1 and Dataflow Runner V2. ([BEAM-12522](https://issues.apache.org/jira/browse/BEAM-12522)).

## New Features / Improvements

* Added custom delimiters to Python TextIO reads ([BEAM-12730](https://issues.apache.org/jira/browse/BEAM-12730)).
* Added escapechar parameter to Python TextIO reads ([BEAM-13189](https://issues.apache.org/jira/browse/BEAM-13189)).
* Splittable reading is enabled by default while reading data with ParquetIO ([BEAM-12070](https://issues.apache.org/jira/browse/BEAM-12070)).
* DoFn Execution Time metrics added to Go ([BEAM-13001](https://issues.apache.org/jira/browse/BEAM-13001)).
* Cross-bundle side input caching is now available in the Go SDK for runners that support the feature by setting the EnableSideInputCache hook ([BEAM-11097](https://issues.apache.org/jira/browse/BEAM-11097)).
* Upgraded the GCP Libraries BOM version to 24.0.0 and associated dependencies ([BEAM-11205](
  https://issues.apache.org/jira/browse/BEAM-11205)). For Google Cloud client library versions set by this BOM,
  see [this table](https://storage.googleapis.com/cloud-opensource-java-dashboard/com.google.cloud/libraries-bom/24.0.0/artifact_details.html).
* Removed avro-python3 dependency in AvroIO. Fastavro has already been our Avro library of choice on Python 3. Boolean use_fastavro is left for api compatibility, but will have no effect.([BEAM-13016](https://github.com/apache/beam/pull/15900)).
* MultiMap side inputs are now supported by the Go SDK ([BEAM-3293](https://issues.apache.org/jira/browse/BEAM-3293)).
* Remote packages can now be downloaded from locations supported by apache_beam.io.filesystems. The files will be downloaded on Stager and uploaded to staging location. For more information, see [BEAM-11275](https://issues.apache.org/jira/browse/BEAM-11275)

## Breaking Changes

* A new URN convention was adopted for cross-language transforms and existing URNs were updated. This may break advanced use-cases, for example, if a custom expansion service is used to connect diffrent Beam Java and Python versions. ([BEAM-12047](https://issues.apache.org/jira/browse/BEAM-12047)).
* The upgrade to Calcite 1.28.0 introduces a breaking change in the SUBSTRING function in SqlTransform, when used with the Calcite dialect ([BEAM-13099](https://issues.apache.org/jira/browse/BEAM-13099), [CALCITE-4427](https://issues.apache.org/jira/browse/CALCITE-4427)).
* ListShards (with DescribeStreamSummary) is used instead of DescribeStream to list shards in Kinesis streams (AWS SDK v2). Due to this change, as mentioned in [AWS documentation](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_ListShards.html), for fine-grained IAM policies it is required to update them to allow calls to ListShards and DescribeStreamSummary APIs. For more information, see [Controlling Access to Amazon Kinesis Data Streams](https://docs.aws.amazon.com/streams/latest/dev/controlling-access.html) ([BEAM-13233](https://issues.apache.org/jira/browse/BEAM-13233)).

## Deprecations

* Non-splittable reading is deprecated while reading data with ParquetIO ([BEAM-12070](https://issues.apache.org/jira/browse/BEAM-12070)).

## Bugfixes

* Properly map main input windows to side input windows by default (Go)
  ([BEAM-11087](https://issues.apache.org/jira/browse/BEAM-11087)).
* Fixed data loss when writing to DynamoDB without setting deduplication key names (Java)
  ([BEAM-13009](https://issues.apache.org/jira/browse/BEAM-13009)).
* Go SDK Examples now have types and functions registered. (Go) ([BEAM-5378](https://issues.apache.org/jira/browse/BEAM-5378))

## Known Issues

* Users of beam-sdks-java-io-hcatalog (and beam-sdks-java-extensions-sql-hcatalog) must take care to override the transitive log4j dependency when they add a hive dependency ([BEAM-13499](https://issues.apache.org/jira/browse/BEAM-13499)).
* On rare occations, Python Datastore source may swallow some exceptions. Users are adviced to upgrade to Beam 2.38.0 or later ([BEAM-14282](https://issues.apache.org/jira/browse/BEAM-14282))
* On rare occations, Python GCS source may swallow some exceptions. Users are adviced to upgrade to Beam 2.38.0 or later ([BEAM-14282](https://issues.apache.org/jira/browse/BEAM-14282))
* On rare occations, Java SpannerIO source may swallow some exceptions. Users are adviced to upgrade to Beam 2.37.0 or later ([BEAM-14005](https://issues.apache.org/jira/browse/BEAM-14005))

# [2.34.0] - 2021-11-11

## Highlights

* The Beam Java API for Calcite SqlTransform is no longer experimental ([BEAM-12680](https://issues.apache.org/jira/browse/BEAM-12680)).
* Python's ParDo (Map, FlatMap, etc.) transforms now suport a `with_exception_handling` option for easily ignoring bad records and implementing the dead letter pattern.

## I/Os

* `ReadFromBigQuery` and `ReadAllFromBigQuery` now run queries with BATCH priority by default. The `query_priority` parameter is introduced to the same transforms to allow configuring the query priority (Python) ([BEAM-12913](https://issues.apache.org/jira/browse/BEAM-12913)).
* [EXPERIMENTAL] Support for [BigQuery Storage Read API](https://cloud.google.com/bigquery/docs/reference/storage) added to `ReadFromBigQuery`. The newly introduced `method` parameter can be set as `DIRECT_READ` to use the Storage Read API. The default is `EXPORT` which invokes a BigQuery export request. (Python) ([BEAM-10917](https://issues.apache.org/jira/browse/BEAM-10917)).
* [EXPERIMENTAL] Added `use_native_datetime` parameter to `ReadFromBigQuery` to configure the return type of [DATETIME](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#datetime_type) fields when using `ReadFromBigQuery`. This parameter can *only* be used when `method = DIRECT_READ`(Python) ([BEAM-10917](https://issues.apache.org/jira/browse/BEAM-10917)).
* [EXPERIMENTAL] Added support for writing to [Redis Streams](https://redis.io/topics/streams-intro) as a sink in RedisIO ([BEAM-13159](https://issues.apache.org/jira/browse/BEAM-13159))

## New Features / Improvements

* Upgraded to Calcite 1.26.0 ([BEAM-9379](https://issues.apache.org/jira/browse/BEAM-9379)).
* Added a new `dataframe` extra to the Python SDK that tracks `pandas` versions
  we've verified compatibility with. We now recommend installing Beam with `pip
  install apache-beam[dataframe]` when you intend to use the DataFrame API
  ([BEAM-12906](https://issues.apache.org/jira/browse/BEAM-12906)).
* Added an [example](https://github.com/cometta/python-apache-beam-spark) of deploying Python Apache Beam job with Spark Cluster

## Breaking Changes

* SQL Rows are no longer flattened ([BEAM-5505](https://issues.apache.org/jira/browse/BEAM-5505)).
* [Go SDK] beam.TryCrossLanguage's signature now matches beam.CrossLanguage. Like other Try functions it returns an error instead of panicking. ([BEAM-9918](https://issues.apache.org/jira/browse/BEAM-9918)).
* [BEAM-12925](https://jira.apache.org/jira/browse/BEAM-12925) was fixed. It used to silently pass incorrect null data read from JdbcIO. Pipelines affected by this will now start throwing failures instead of silently passing incorrect data.

## Bugfixes

* Fixed error while writing multiple DeferredFrames to csv (Python) ([BEAM-12701](https://issues.apache.org/jira/browse/BEAM-12701)).
* Fixed error when importing the DataFrame API with pandas 1.0.x installed ([BEAM-12945](https://issues.apache.org/jira/browse/BEAM-12945)).
* Fixed top.SmallestPerKey implementation in the Go SDK ([BEAM-12946](https://issues.apache.org/jira/browse/BEAM-12946)).

## Known Issues

* On rare occations, Python Datastore source may swallow some exceptions. Users are adviced to upgrade to Beam 2.38.0 or later ([BEAM-14282](https://issues.apache.org/jira/browse/BEAM-14282))
* On rare occations, Python GCS source may swallow some exceptions. Users are adviced to upgrade to Beam 2.38.0 or later ([BEAM-14282](https://issues.apache.org/jira/browse/BEAM-14282))
* On rare occations, Java SpannerIO source may swallow some exceptions. Users are adviced to upgrade to Beam 2.37.0 or later ([BEAM-14005](https://issues.apache.org/jira/browse/BEAM-14005))

# [2.33.0] - 2021-10-07

## Highlights

* Go SDK is no longer experimental, and is officially part of the Beam release process.
  * Matching Go SDK containers are published on release.
  * Batch usage is well supported, and tested on Flink, Spark, and the Python Portable Runner.
    * SDK Tests are also run against Google Cloud Dataflow, but this doesn't indicate reciprical support.
  * The SDK supports Splittable DoFns, Cross Language transforms, and most Beam Model basics.
  * Go Modules are now used for dependency management.
    * This is a breaking change, see Breaking Changes for resolution.
    * Easier path to contribute to the Go SDK, no need to set up a GO\_PATH.
    * Minimum Go version is now Go v1.16
  * See the announcement blogpost for full information once published.

<!--
## I/Os

* Support for X source added (Java/Python) ([BEAM-X](https://issues.apache.org/jira/browse/BEAM-X)).
-->

## New Features / Improvements

* Projection pushdown in SchemaIO ([BEAM-12609](https://issues.apache.org/jira/browse/BEAM-12609)).
* Upgrade Flink runner to Flink versions 1.13.2, 1.12.5 and 1.11.4 ([BEAM-10955](https://issues.apache.org/jira/browse/BEAM-10955)).

## Breaking Changes

* Go SDK pipelines require new import paths to use this release due to migration to Go Modules.
  * `go.mod` files will need to change to require `github.com/apache/beam/sdks/v2`.
  * Code depending on beam imports need to include v2 on the module path.
    * Fix by'v2' to the import paths, turning  `.../sdks/go/...` to `.../sdks/v2/go/...`
  * No other code change should be required to use v2.33.0 of the Go SDK.
* Since release 2.30.0, "The AvroCoder changes for BEAM-2303 \[changed\] the reader/writer from the Avro ReflectDatum* classes to the SpecificDatum* classes" (Java). This default behavior change has been reverted in this release. Use the `useReflectApi` setting to control it ([BEAM-12628](https://issues.apache.org/jira/browse/BEAM-12628)).

## Deprecations

* Python GBK will stop supporting unbounded PCollections that have global windowing and a default trigger in Beam 2.34. This can be overriden with `--allow_unsafe_triggers`. ([BEAM-9487](https://issues.apache.org/jira/browse/BEAM-9487)).
* Python GBK will start requiring safe triggers or the `--allow_unsafe_triggers` flag starting with Beam 2.34. ([BEAM-9487](https://issues.apache.org/jira/browse/BEAM-9487)).

## Bug fixes

* Workaround to not delete orphaned files to avoid missing events when using Python WriteToFiles in streaming pipeline ([BEAM-12950](https://issues.apache.org/jira/browse/BEAM-12950)))

## Known Issues

* Spark 2.x users will need to update Spark's Jackson runtime dependencies (`spark.jackson.version`) to at least version 2.9.2, due to Beam updating its dependencies.
* Go SDK jobs may produce "Failed to deduce Step from MonitoringInfo" messages following successful job execution. The messages are benign and don't indicate job failure. These are due to not yet handling PCollection metrics.
* On rare occations, Python GCS source may swallow some exceptions. Users are adviced to upgrade to Beam 2.38.0 or later ([BEAM-14282](https://issues.apache.org/jira/browse/BEAM-14282))

# [2.32.0] - 2021-08-25

## Highlights
* The [Beam DataFrame
  API](https://beam.apache.org/documentation/dsls/dataframes/overview/) is no
  longer experimental! We've spent the time since the [2.26.0 preview
  announcement](https://beam.apache.org/blog/dataframe-api-preview-available/)
  implementing the most frequently used pandas operations
  ([BEAM-9547](https://issues.apache.org/jira/browse/BEAM-9547)), improving
  [documentation](https://beam.apache.org/releases/pydoc/current/apache_beam.dataframe.html)
  and [error messages](https://issues.apache.org/jira/browse/BEAM-12028),
  adding
  [examples](https://github.com/apache/beam/tree/master/sdks/python/apache_beam/examples/dataframe),
  integrating DataFrames with [interactive
  Beam](https://beam.apache.org/releases/pydoc/current/apache_beam.runners.interactive.interactive_beam.html),
  and of course finding and fixing
  [bugs](https://issues.apache.org/jira/issues/?jql=project%3DBEAM%20AND%20issuetype%3DBug%20AND%20status%3DResolved%20AND%20component%3Ddsl-dataframe).
  Leaving experimental just means that we now have high confidence in the API
  and recommend its use for production workloads. We will continue to improve
  the API, guided by your
  [feedback](https://beam.apache.org/community/contact-us/).


## I/Os

* New experimental Firestore connector in Java SDK, providing sources and sinks to Google Cloud Firestore ([BEAM-8376](https://issues.apache.org/jira/browse/BEAM-8376)).
* Added ability to use JdbcIO.Write.withResults without statement and preparedStatementSetter. ([BEAM-12511](https://issues.apache.org/jira/browse/BEAM-12511))
- Added ability to register URI schemes to use the S3 protocol via FileIO. ([BEAM-12435](https://issues.apache.org/jira/browse/BEAM-12435)).
* Respect number of shards set in SnowflakeWrite batch mode. ([BEAM-12715](https://issues.apache.org/jira/browse/BEAM-12715))
* Java SDK: Update Google Cloud Healthcare IO connectors from using v1beta1 to using the GA version.

## New Features / Improvements

* Add support to convert Beam Schema to Avro Schema for JDBC LogicalTypes:
  `VARCHAR`, `NVARCHAR`, `LONGVARCHAR`, `LONGNVARCHAR`, `DATE`, `TIME`
  (Java)([BEAM-12385](https://issues.apache.org/jira/browse/BEAM-12385)).
* Reading from JDBC source by partitions (Java) ([BEAM-12456](https://issues.apache.org/jira/browse/BEAM-12456)).
* PubsubIO can now write to a dead-letter topic after a parsing error (Java)([BEAM-12474](https://issues.apache.org/jira/browse/BEAM-12474)).
* New append-only option for Elasticsearch sink (Java) [BEAM-12601](https://issues.apache.org/jira/browse/BEAM-12601)
* DatastoreIO: Write and delete operations now follow automatic gradual ramp-up, in line with best practices (Java/Python) ([BEAM-12260](https://issues.apache.org/jira/browse/BEAM-12260), [BEAM-12272](https://issues.apache.org/jira/browse/BEAM-12272)).

## Breaking Changes

* ListShards (with DescribeStreamSummary) is used instead of DescribeStream to list shards in Kinesis streams. Due to this change, as mentioned in [AWS documentation](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_ListShards.html), for fine-grained IAM policies it is required to update them to allow calls to ListShards and DescribeStreamSummary APIs. For more information, see [Controlling Access to Amazon Kinesis Data Streams](https://docs.aws.amazon.com/streams/latest/dev/controlling-access.html) ([BEAM-12225](https://issues.apache.org/jira/browse/BEAM-12225)).

## Deprecations

* Python GBK will stop supporting unbounded PCollections that have global windowing and a default trigger in Beam 2.33. This can be overriden with `--allow_unsafe_triggers`. ([BEAM-9487](https://issues.apache.org/jira/browse/BEAM-9487)).
* Python GBK will start requiring safe triggers or the `--allow_unsafe_triggers` flag starting with Beam 2.33. ([BEAM-9487](https://issues.apache.org/jira/browse/BEAM-9487)).

## Bugfixes

* Fixed race condition in RabbitMqIO causing duplicate acks (Java) ([BEAM-6516](https://issues.apache.org/jira/browse/BEAM-6516)))

## Known Issues
* On rare occations, Python GCS source may swallow some exceptions. Users are adviced to upgrade to Beam 2.38.0 or later ([BEAM-14282](https://issues.apache.org/jira/browse/BEAM-14282))

# [2.31.0] - 2021-07-08

## I/Os

* Fixed bug in ReadFromBigQuery when a RuntimeValueProvider is used as value of table argument (Python) ([BEAM-12514](https://issues.apache.org/jira/browse/BEAM-12514)).

## New Features / Improvements

* `CREATE FUNCTION` DDL statement added to Calcite SQL syntax. `JAR` and `AGGREGATE` are now reserved keywords. ([BEAM-12339](https://issues.apache.org/jira/browse/BEAM-12339)).
* Flink 1.13 is now supported by the Flink runner ([BEAM-12277](https://issues.apache.org/jira/browse/BEAM-12277)).
* Python `TriggerFn` has a new `may_lose_data` method to signal potential data loss. Default behavior assumes safe (necessary for backwards compatibility). See Deprecations for potential impact of overriding this. ([BEAM-9487](https://issues.apache.org/jira/browse/BEAM-9487)).

## Breaking Changes

* Python Row objects are now sensitive to field order. So `Row(x=3, y=4)` is no
  longer considered equal to `Row(y=4, x=3)` (BEAM-11929).
* Kafka Beam SQL tables now ascribe meaning to the LOCATION field; previously
  it was ignored if provided.
* `TopCombineFn` disallow `compare` as its argument (Python) ([BEAM-7372](https://issues.apache.org/jira/browse/BEAM-7372)).
* Drop support for Flink 1.10 ([BEAM-12281](https://issues.apache.org/jira/browse/BEAM-12281)).

## Deprecations

* Python GBK will stop supporting unbounded PCollections that have global windowing and a default trigger in Beam 2.33. This can be overriden with `--allow_unsafe_triggers`. ([BEAM-9487](https://issues.apache.org/jira/browse/BEAM-9487)).
* Python GBK will start requiring safe triggers or the `--allow_unsafe_triggers` flag starting with Beam 2.33. ([BEAM-9487](https://issues.apache.org/jira/browse/BEAM-9487)).

# [2.30.0] - 2021-06-09

## I/Os

* Allow splitting apart document serialization and IO for ElasticsearchIO
* Support Bulk API request size optimization through addition of ElasticsearchIO.Write.withStatefulBatches

## New Features / Improvements

* Added capability to declare resource hints in Java and Python SDKs ([BEAM-2085](https://issues.apache.org/jira/browse/BEAM-2085)).
* Added Spanner IO Performance tests for read and write. (Python) ([BEAM-10029](https://issues.apache.org/jira/browse/BEAM-10029)).
* Added support for accessing GCP PubSub Message ordering keys, message IDs and message publish timestamp (Python) ([BEAM-7819](https://issues.apache.org/jira/browse/BEAM-7819)).
* DataFrame API: Added support for collecting DataFrame objects in interactive Beam ([BEAM-11855](https://issues.apache.org/jira/browse/BEAM-11855))
* DataFrame API: Added [apache_beam.examples.dataframe](https://github.com/apache/beam/tree/master/sdks/python/apache_beam/examples/dataframe) module ([BEAM-12024](https://issues.apache.org/jira/browse/BEAM-12024))
* Upgraded the GCP Libraries BOM version to 20.0.0 ([BEAM-11205](https://issues.apache.org/jira/browse/BEAM-11205)).
  For Google Cloud client library versions set by this BOM, see [this table](https://storage.googleapis.com/cloud-opensource-java-dashboard/com.google.cloud/libraries-bom/20.0.0/artifact_details.html).

## Breaking Changes

* Drop support for Flink 1.8 and 1.9 ([BEAM-11948](https://issues.apache.org/jira/browse/BEAM-11948)).
* MongoDbIO: Read.withFilter() and Read.withProjection() are removed since they are deprecated since
  Beam 2.12.0 ([BEAM-12217](https://issues.apache.org/jira/browse/BEAM-12217)).
* RedisIO.readAll() was removed since it was deprecated since Beam 2.13.0. Please use
  RedisIO.readKeyPatterns() for the equivalent functionality.
  ([BEAM-12214](https://issues.apache.org/jira/browse/BEAM-12214)).
* MqttIO.create() with clientId constructor removed because it was deprecated since Beam
  2.13.0 ([BEAM-12216](https://issues.apache.org/jira/browse/BEAM-12216)).

# [2.29.0] - 2021-04-29

## Highlights

* Spark Classic and Portable runners officially support Spark 3 ([BEAM-7093](https://issues.apache.org/jira/browse/BEAM-7093)).
* Official Java 11 support for most runners (Dataflow, Flink, Spark) ([BEAM-2530](https://issues.apache.org/jira/browse/BEAM-2530)).
* DataFrame API now supports GroupBy.apply ([BEAM-11628](https://issues.apache.org/jira/browse/BEAM-11628)).

## I/Os

* Added support for S3 filesystem on AWS SDK V2 (Java) ([BEAM-7637](https://issues.apache.org/jira/browse/BEAM-7637))

## New Features / Improvements

* DataFrame API now supports pandas 1.2.x ([BEAM-11531](https://issues.apache.org/jira/browse/BEAM-11531)).
* Multiple DataFrame API bugfixes ([BEAM-12071](https://issues.apache.org/jira/browse/BEAM-12071), [BEAM-11929](https://issues.apache.org/jira/browse/BEAM-11929))

## Breaking Changes

* Deterministic coding enforced for GroupByKey and Stateful DoFns.  Previously non-deterministic coding was allowed, resulting in keys not properly being grouped in some cases. ([BEAM-11719](https://issues.apache.org/jira/browse/BEAM-11719))
  To restore the old behavior, one can register `FakeDeterministicFastPrimitivesCoder` with
  `beam.coders.registry.register_fallback_coder(beam.coders.coders.FakeDeterministicFastPrimitivesCoder())`
  or use the `allow_non_deterministic_key_coders` pipeline option.

## Deprecations

* Support for Flink 1.8 and 1.9 will be removed in the next release (2.30.0) ([BEAM-11948](https://issues.apache.org/jira/browse/BEAM-11948)).

# [2.28.0] - 2021-02-22

## Highlights
* Many improvements related to Parquet support ([BEAM-11460](https://issues.apache.org/jira/browse/BEAM-11460), [BEAM-8202](https://issues.apache.org/jira/browse/BEAM-8202), and [BEAM-11526](https://issues.apache.org/jira/browse/BEAM-11526))
* Hash Functions in BeamSQL ([BEAM-10074](https://issues.apache.org/jira/browse/BEAM-10074))
* Hash functions in ZetaSQL ([BEAM-11624](https://issues.apache.org/jira/browse/BEAM-11624))
* Create ApproximateDistinct using HLL Impl ([BEAM-10324](https://issues.apache.org/jira/browse/BEAM-10324))

## I/Os

* SpannerIO supports using BigDecimal for Numeric fields ([BEAM-11643](https://issues.apache.org/jira/browse/BEAM-11643))
* Add Beam schema support to ParquetIO ([BEAM-11526](https://issues.apache.org/jira/browse/BEAM-11526))
* Support ParquetTable Writer ([BEAM-8202](https://issues.apache.org/jira/browse/BEAM-8202))
* GCP BigQuery sink (streaming inserts) uses runner determined sharding ([BEAM-11408](https://issues.apache.org/jira/browse/BEAM-11408))
* PubSub support types: TIMESTAMP, DATE, TIME, DATETIME ([BEAM-11533](https://issues.apache.org/jira/browse/BEAM-11533))

## New Features / Improvements

* ParquetIO add methods _readGenericRecords_ and _readFilesGenericRecords_ can read files with an unknown schema. See [PR-13554](https://github.com/apache/beam/pull/13554) and ([BEAM-11460](https://issues.apache.org/jira/browse/BEAM-11460))
* Added support for thrift in KafkaTableProvider ([BEAM-11482](https://issues.apache.org/jira/browse/BEAM-11482))
* Added support for HadoopFormatIO to skip key/value clone ([BEAM-11457](https://issues.apache.org/jira/browse/BEAM-11457))
* Support Conversion to GenericRecords in Convert.to transform ([BEAM-11571](https://issues.apache.org/jira/browse/BEAM-11571)).
* Support writes for Parquet Tables in Beam SQL ([BEAM-8202](https://issues.apache.org/jira/browse/BEAM-8202)).
* Support reading Parquet files with unknown schema ([BEAM-11460](https://issues.apache.org/jira/browse/BEAM-11460))
* Support user configurable Hadoop Configuration flags for ParquetIO ([BEAM-11527](https://issues.apache.org/jira/browse/BEAM-11527))
* Expose commit_offset_in_finalize and timestamp_policy to ReadFromKafka ([BEAM-11677](https://issues.apache.org/jira/browse/BEAM-11677))
* S3 options does not provided to boto3 client while using FlinkRunner and Beam worker pool container ([BEAM-11799](https://issues.apache.org/jira/browse/BEAM-11799))
* HDFS not deduplicating identical configuration paths ([BEAM-11329](https://issues.apache.org/jira/browse/BEAM-11329))
* Hash Functions in BeamSQL ([BEAM-10074](https://issues.apache.org/jira/browse/BEAM-10074))
* Create ApproximateDistinct using HLL Impl ([BEAM-10324](https://issues.apache.org/jira/browse/BEAM-10324))
* Add Beam schema support to ParquetIO ([BEAM-11526](https://issues.apache.org/jira/browse/BEAM-11526))
* Add a Deque Encoder ([BEAM-11538](https://issues.apache.org/jira/browse/BEAM-11538))
* Hash functions in ZetaSQL ([BEAM-11624](https://issues.apache.org/jira/browse/BEAM-11624))
* Refactor ParquetTableProvider ([](https://issues.apache.org/jira/browse/))
* Add JVM properties to JavaJobServer ([BEAM-8344](https://issues.apache.org/jira/browse/BEAM-8344))
* Single source of truth for supported Flink versions ([](https://issues.apache.org/jira/browse/))
* Use metric for Python BigQuery streaming insert API latency logging ([BEAM-11018](https://issues.apache.org/jira/browse/BEAM-11018))
* Use metric for Java BigQuery streaming insert API latency logging ([BEAM-11032](https://issues.apache.org/jira/browse/BEAM-11032))
* Upgrade Flink runner to Flink versions 1.12.1 and 1.11.3 ([BEAM-11697](https://issues.apache.org/jira/browse/BEAM-11697))
* Upgrade Beam base image to use Tensorflow 2.4.1 ([BEAM-11762](https://issues.apache.org/jira/browse/BEAM-11762))
* Create Beam GCP BOM ([BEAM-11665](https://issues.apache.org/jira/browse/BEAM-11665))

## Breaking Changes

* The Java artifacts "beam-sdks-java-io-kinesis", "beam-sdks-java-io-google-cloud-platform", and
  "beam-sdks-java-extensions-sql-zetasql" declare Guava 30.1-jre dependency (It was 25.1-jre in Beam 2.27.0).
  This new Guava version may introduce dependency conflicts if your project or dependencies rely
  on removed APIs. If affected, ensure to use an appropriate Guava version via `dependencyManagement` in Maven and
  `force` in Gradle.


# [2.27.0] - 2021-01-08

## I/Os
* ReadFromMongoDB can now be used with MongoDB Atlas (Python) ([BEAM-11266](https://issues.apache.org/jira/browse/BEAM-11266).)
* ReadFromMongoDB/WriteToMongoDB will mask password in display_data (Python) ([BEAM-11444](https://issues.apache.org/jira/browse/BEAM-11444).)
* Support for X source added (Java/Python) ([BEAM-X](https://issues.apache.org/jira/browse/BEAM-X)).
* There is a new transform `ReadAllFromBigQuery` that can receive multiple requests to read data from BigQuery at pipeline runtime. See [PR 13170](https://github.com/apache/beam/pull/13170), and [BEAM-9650](https://issues.apache.org/jira/browse/BEAM-9650).

## New Features / Improvements

* Beam modules that depend on Hadoop are now tested for compatibility with Hadoop 3 ([BEAM-8569](https://issues.apache.org/jira/browse/BEAM-8569)). (Hive/HCatalog pending)
* Publishing Java 11 SDK container images now supported as part of Apache Beam release process. ([BEAM-8106](https://issues.apache.org/jira/browse/BEAM-8106))
* Added Cloud Bigtable Provider extension to Beam SQL ([BEAM-11173](https://issues.apache.org/jira/browse/BEAM-11173), [BEAM-11373](https://issues.apache.org/jira/browse/BEAM-11373))
* Added a schema provider for thrift data ([BEAM-11338](https://issues.apache.org/jira/browse/BEAM-11338))
* Added combiner packing pipeline optimization to Dataflow runner. ([BEAM-10641](https://issues.apache.org/jira/browse/BEAM-10641))
* Support for the Deque structure by adding a coder ([BEAM-11538](https://issues.apache.org/jira/browse/BEAM-11538))

## Breaking Changes

* HBaseIO hbase-shaded-client dependency should be now provided by the users ([BEAM-9278](https://issues.apache.org/jira/browse/BEAM-9278)).
* `--region` flag in amazon-web-services2 was replaced by `--awsRegion` ([BEAM-11331](https://issues.apache.org/jira/projects/BEAM/issues/BEAM-11331)).

# [2.26.0] - 2020-12-11

## Highlights

* Splittable DoFn is now the default for executing the Read transform for Java based runners (Spark with bounded pipelines) in addition to existing runners from the 2.25.0 release (Direct, Flink, Jet, Samza, Twister2). The expected output of the Read transform is unchanged. Users can opt-out using `--experiments=use_deprecated_read`. The Apache Beam community is looking for feedback for this change as the community is planning to make this change permanent with no opt-out. If you run into an issue requiring the opt-out, please send an e-mail to [user@beam.apache.org](mailto:user@beam.apache.org) specifically referencing BEAM-10670 in the subject line and why you needed to opt-out. (Java) ([BEAM-10670](https://issues.apache.org/jira/browse/BEAM-10670))

## I/Os

* Java BigQuery streaming inserts now have timeouts enabled by default. Pass `--HTTPWriteTimeout=0` to revert to the old behavior. ([BEAM-6103](https://issues.apache.org/jira/browse/BEAM-6103))
* Added support for Contextual Text IO (Java), a version of text IO that provides metadata about the records ([BEAM-10124](https://issues.apache.org/jira/browse/BEAM-10124)). Support for this IO is currently experimental. Specifically, **there are no update-compatibility guarantees** for streaming jobs with this IO between current future verisons of Apache Beam SDK.

## New Features / Improvements
* Added support for avro payload format in Beam SQL Kafka Table ([BEAM-10885](https://issues.apache.org/jira/browse/BEAM-10885))
* Added support for json payload format in Beam SQL Kafka Table ([BEAM-10893](https://issues.apache.org/jira/browse/BEAM-10893))
* Added support for protobuf payload format in Beam SQL Kafka Table ([BEAM-10892](https://issues.apache.org/jira/browse/BEAM-10892))
* Added support for avro payload format in Beam SQL Pubsub Table ([BEAM-5504](https://issues.apache.org/jira/browse/BEAM-5504))
* Added option to disable unnecessary copying between operators in Flink Runner (Java) ([BEAM-11146](https://issues.apache.org/jira/browse/BEAM-11146))
* Added CombineFn.setup and CombineFn.teardown to Python SDK. These methods let you initialize the CombineFn's state before any of the other methods of the CombineFn is executed and clean that state up later on. If you are using Dataflow, you need to enable Dataflow Runner V2 by passing `--experiments=use_runner_v2` before using this feature. ([BEAM-3736](https://issues.apache.org/jira/browse/BEAM-3736))
* Added support for NestedValueProvider for the Python SDK ([BEAM-10856](https://issues.apache.org/jira/browse/BEAM-10856)).

## Breaking Changes

* BigQuery's DATETIME type now maps to Beam logical type org.apache.beam.sdk.schemas.logicaltypes.SqlTypes.DATETIME
* Pandas 1.x is now required for dataframe operations.

## Known Issues

* Non-idempotent combiners built via `CombineFn.from_callable()` or `CombineFn.maybe_from_callable()` can lead to incorrect behavior. ([BEAM-11522](https://issues.apache.org/jira/browse/BEAM-11522)).


# [2.25.0] - 2020-10-23

## Highlights

* Splittable DoFn is now the default for executing the Read transform for Java based runners (Direct, Flink, Jet, Samza, Twister2). The expected output of the Read transform is unchanged. Users can opt-out using `--experiments=use_deprecated_read`. The Apache Beam community is looking for feedback for this change as the community is planning to make this change permanent with no opt-out. If you run into an issue requiring the opt-out, please send an e-mail to [user@beam.apache.org](mailto:user@beam.apache.org) specifically referencing BEAM-10670 in the subject line and why you needed to opt-out. (Java) ([BEAM-10670](https://issues.apache.org/jira/browse/BEAM-10670))

## I/Os

* Added cross-language support to Java's KinesisIO, now available in the Python module `apache_beam.io.kinesis` ([BEAM-10138](https://issues.apache.org/jira/browse/BEAM-10138), [BEAM-10137](https://issues.apache.org/jira/browse/BEAM-10137)).
* Update Snowflake JDBC dependency for SnowflakeIO ([BEAM-10864](https://issues.apache.org/jira/browse/BEAM-10864))
* Added cross-language support to Java's SnowflakeIO.Write, now available in the Python module `apache_beam.io.snowflake` ([BEAM-9898](https://issues.apache.org/jira/browse/BEAM-9898)).
* Added delete function to Java's `ElasticsearchIO#Write`. Now, Java's ElasticsearchIO can be used to selectively delete documents using `withIsDeleteFn` function ([BEAM-5757](https://issues.apache.org/jira/browse/BEAM-5757)).
* Java SDK: Added new IO connector for InfluxDB - InfluxDbIO ([BEAM-2546](https://issues.apache.org/jira/browse/BEAM-2546)).
* Config options added for Python's S3IO ([BEAM-9094](https://issues.apache.org/jira/browse/BEAM-9094))

## New Features / Improvements

* Support for repeatable fields in JSON decoder for `ReadFromBigQuery` added. (Python) ([BEAM-10524](https://issues.apache.org/jira/browse/BEAM-10524))
* Added an opt-in, performance-driven runtime type checking system for the Python SDK ([BEAM-10549](https://issues.apache.org/jira/browse/BEAM-10549)).
    More details will be in an upcoming [blog post](https://beam.apache.org/blog/python-performance-runtime-type-checking/index.html).
* Added support for Python 3 type annotations on PTransforms using typed PCollections ([BEAM-10258](https://issues.apache.org/jira/browse/BEAM-10258)).
    More details will be in an upcoming [blog post](https://beam.apache.org/blog/python-improved-annotations/index.html).
* Improved the Interactive Beam API where recording streaming jobs now start a long running background recording job. Running ib.show() or ib.collect() samples from the recording ([BEAM-10603](https://issues.apache.org/jira/browse/BEAM-10603)).
* In Interactive Beam, ib.show() and ib.collect() now have "n" and "duration" as parameters. These mean read only up to "n" elements and up to "duration" seconds of data read from the recording ([BEAM-10603](https://issues.apache.org/jira/browse/BEAM-10603)).
* Initial preview of [Dataframes](https://s.apache.org/simpler-python-pipelines-2020#slide=id.g905ac9257b_1_21) support.
    See also example at apache_beam/examples/wordcount_dataframe.py
* Fixed support for type hints on `@ptransform_fn` decorators in the Python SDK.
  ([BEAM-4091](https://issues.apache.org/jira/browse/BEAM-4091))
  This has not enabled by default to preserve backwards compatibility; use the
  `--type_check_additional=ptransform_fn` flag to enable. It may be enabled by
  default in future versions of Beam.

## Breaking Changes

* Python 2 and Python 3.5 support dropped ([BEAM-10644](https://issues.apache.org/jira/browse/BEAM-10644), [BEAM-9372](https://issues.apache.org/jira/browse/BEAM-9372)).
* Pandas 1.x allowed.  Older version of Pandas may still be used, but may not be as well tested.

## Deprecations

* Python transform ReadFromSnowflake has been moved from `apache_beam.io.external.snowflake` to `apache_beam.io.snowflake`. The previous path will be removed in the future versions.

## Known Issues

* Dataflow streaming timers once against not strictly time ordered when set earlier mid-bundle, as the fix for  [BEAM-8543](https://issues.apache.org/jira/browse/BEAM-8543) introduced more severe bugs and has been rolled back.
* Default compressor change breaks dataflow python streaming job update compatibility. Please use python SDK version <= 2.23.0 or > 2.25.0 if job update is critical.([BEAM-11113](https://issues.apache.org/jira/browse/BEAM-11113))


# [2.24.0] - 2020-09-18

## Highlights

* Apache Beam 2.24.0 is the last release with Python 2 and Python 3.5
  support.

## I/Os

* New overloads for BigtableIO.Read.withKeyRange() and BigtableIO.Read.withRowFilter()
  methods that take ValueProvider as a parameter (Java) ([BEAM-10283](https://issues.apache.org/jira/browse/BEAM-10283)).
* The WriteToBigQuery transform (Python) in Dataflow Batch no longer relies on BigQuerySink by default. It relies on
  a new, fully-featured transform based on file loads into BigQuery. To revert the behavior to the old implementation,
  you may use `--experiments=use_legacy_bq_sink`.
* Add cross-language support to Java's JdbcIO, now available in the Python module `apache_beam.io.jdbc` ([BEAM-10135](https://issues.apache.org/jira/browse/BEAM-10135), [BEAM-10136](https://issues.apache.org/jira/browse/BEAM-10136)).
* Add support of AWS SDK v2 for KinesisIO.Read (Java) ([BEAM-9702](https://issues.apache.org/jira/browse/BEAM-9702)).
* Add streaming support to SnowflakeIO in Java SDK ([BEAM-9896](https://issues.apache.org/jira/browse/BEAM-9896))
* Support reading and writing to Google Healthcare DICOM APIs in Python SDK ([BEAM-10601](https://issues.apache.org/jira/browse/BEAM-10601))
* Add dispositions for SnowflakeIO.write ([BEAM-10343](https://issues.apache.org/jira/browse/BEAM-10343))
* Add cross-language support to SnowflakeIO.Read now available in the Python module `apache_beam.io.external.snowflake` ([BEAM-9897](https://issues.apache.org/jira/browse/BEAM-9897)).

## New Features / Improvements

* Shared library for simplifying management of large shared objects added to Python SDK. An example use case is sharing a large TF model object across threads ([BEAM-10417](https://issues.apache.org/jira/browse/BEAM-10417)).
* Dataflow streaming timers are not strictly time ordered when set earlier mid-bundle ([BEAM-8543](https://issues.apache.org/jira/browse/BEAM-8543)).
* OnTimerContext should not create a new one when processing each element/timer in FnApiDoFnRunner ([BEAM-9839](https://issues.apache.org/jira/browse/BEAM-9839))
* Key should be available in @OnTimer methods (Spark Runner) ([BEAM-9850](https://issues.apache.org/jira/browse/BEAM-9850))

## Breaking Changes

* WriteToBigQuery transforms now require a GCS location to be provided through either
  custom_gcs_temp_location in the constructor of WriteToBigQuery or the fallback option
  --temp_location, or pass method="STREAMING_INSERTS" to WriteToBigQuery ([BEAM-6928](https://issues.apache.org/jira/browse/BEAM-6928)).
* Python SDK now understands `typing.FrozenSet` type hints, which are not interchangeable with `typing.Set`. You may need to update your pipelines if type checking fails. ([BEAM-10197](https://issues.apache.org/jira/browse/BEAM-10197))

## Known issues

* When a timer fires but is reset prior to being executed, a watermark hold may be leaked, causing a stuck pipeline [BEAM-10991](https://issues.apache.org/jira/browse/BEAM-10991).
* Default compressor change breaks dataflow python streaming job update compatibility. Please use python SDK version <= 2.23.0 or > 2.25.0 if job update is critical.([BEAM-11113](https://issues.apache.org/jira/browse/BEAM-11113))

# [2.23.0] - 2020-06-29

## Highlights

* Twister2 Runner ([BEAM-7304](https://issues.apache.org/jira/browse/BEAM-7304)).
* Python 3.8 support ([BEAM-8494](https://issues.apache.org/jira/browse/BEAM-8494)).

## I/Os

* Support for reading from Snowflake added (Java) ([BEAM-9722](https://issues.apache.org/jira/browse/BEAM-9722)).
* Support for writing to Splunk added (Java) ([BEAM-8596](https://issues.apache.org/jira/browse/BEAM-8596)).
* Support for assume role added (Java) ([BEAM-10335](https://issues.apache.org/jira/browse/BEAM-10335)).
* A new transform to read from BigQuery has been added: `apache_beam.io.gcp.bigquery.ReadFromBigQuery`. This transform
  is experimental. It reads data from BigQuery by exporting data to Avro files, and reading those files. It also supports
  reading data by exporting to JSON files. This has small differences in behavior for Time and Date-related fields. See
  Pydoc for more information.

## New Features / Improvements

* Update Snowflake JDBC dependency and add application=beam to connection URL ([BEAM-10383](https://issues.apache.org/jira/browse/BEAM-10383)).

## Breaking Changes

* `RowJson.RowJsonDeserializer`, `JsonToRow`, and `PubsubJsonTableProvider` now accept "implicit
  nulls" by default when deserializing JSON (Java) ([BEAM-10220](https://issues.apache.org/jira/browse/BEAM-10220)).
  Previously nulls could only be represented with explicit null values, as in
  `{"foo": "bar", "baz": null}`, whereas an implicit null like `{"foo": "bar"}` would raise an
  exception. Now both JSON strings will yield the same result by default. This behavior can be
  overridden with `RowJson.RowJsonDeserializer#withNullBehavior`.
* Fixed a bug in `GroupIntoBatches` experimental transform in Python to actually group batches by key.
  This changes the output type for this transform ([BEAM-6696](https://issues.apache.org/jira/browse/BEAM-6696)).

## Deprecations

* Remove Gearpump runner. ([BEAM-9999](https://issues.apache.org/jira/browse/BEAM-9999))
* Remove Apex runner. ([BEAM-9999](https://issues.apache.org/jira/browse/BEAM-9999))
* RedisIO.readAll() is deprecated and will be removed in 2 versions, users must use RedisIO.readKeyPatterns() as a replacement ([BEAM-9747](https://issues.apache.org/jira/browse/BEAM-9747)).

## Known Issues

* Fixed X (Java/Python) ([BEAM-X](https://issues.apache.org/jira/browse/BEAM-X)).

# [2.22.0] - 2020-06-08

## Highlights

## I/Os

* Basic Kafka read/write support for DataflowRunner (Python) ([BEAM-8019](https://issues.apache.org/jira/browse/BEAM-8019)).
* Sources and sinks for Google Healthcare APIs (Java)([BEAM-9468](https://issues.apache.org/jira/browse/BEAM-9468)).
* Support for writing to Snowflake added (Java) ([BEAM-9894](https://issues.apache.org/jira/browse/BEAM-9894)).

## New Features / Improvements

* `--workerCacheMB` flag is supported in Dataflow streaming pipeline ([BEAM-9964](https://issues.apache.org/jira/browse/BEAM-9964))
* `--direct_num_workers=0` is supported for FnApi runner. It will set the number of threads/subprocesses to number of cores of the machine executing the pipeline ([BEAM-9443](https://issues.apache.org/jira/browse/BEAM-9443)).
* Python SDK now has experimental support for SqlTransform ([BEAM-8603](https://issues.apache.org/jira/browse/BEAM-8603)).
* Add OnWindowExpiration method to Stateful DoFn ([BEAM-1589](https://issues.apache.org/jira/browse/BEAM-1589)).
* Added PTransforms for Google Cloud DLP (Data Loss Prevention) services integration ([BEAM-9723](https://issues.apache.org/jira/browse/BEAM-9723)):
    * Inspection of data,
    * Deidentification of data,
    * Reidentification of data.
* Add a more complete I/O support matrix in the documentation site ([BEAM-9916](https://issues.apache.org/jira/browse/BEAM-9916)).
* Upgrade Sphinx to 3.0.3 for building PyDoc.
* Added a PTransform for image annotation using Google Cloud AI image processing service
([BEAM-9646](https://issues.apache.org/jira/browse/BEAM-9646))
* Dataflow streaming timers are not strictly time ordered when set earlier mid-bundle ([BEAM-8543](https://issues.apache.org/jira/browse/BEAM-8543)).

## Breaking Changes

* The Python SDK now requires `--job_endpoint` to be set when using `--runner=PortableRunner` ([BEAM-9860](https://issues.apache.org/jira/browse/BEAM-9860)). Users seeking the old default behavior should set `--runner=FlinkRunner` instead.

## Deprecations

## Known Issues


# [2.21.0] - 2020-05-27

## Highlights

## I/Os
* Python: Deprecated module `apache_beam.io.gcp.datastore.v1` has been removed
as the client it uses is out of date and does not support Python 3
([BEAM-9529](https://issues.apache.org/jira/browse/BEAM-9529)).
Please migrate your code to use
[apache_beam.io.gcp.datastore.**v1new**](https://beam.apache.org/releases/pydoc/current/apache_beam.io.gcp.datastore.v1new.datastoreio.html).
See the updated
[datastore_wordcount](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/cookbook/datastore_wordcount.py)
for example usage.
* Python SDK: Added integration tests and updated batch write functionality for Google Cloud Spanner transform ([BEAM-8949](https://issues.apache.org/jira/browse/BEAM-8949)).

## New Features / Improvements
* Python SDK will now use Python 3 type annotations as pipeline type hints.
([#10717](https://github.com/apache/beam/pull/10717))

    If you suspect that this feature is causing your pipeline to fail, calling
    `apache_beam.typehints.disable_type_annotations()` before pipeline creation
    will disable is completely, and decorating specific functions (such as
    `process()`) with `@apache_beam.typehints.no_annotations` will disable it
    for that function.

    More details will be in
    [Ensuring Python Type Safety](https://beam.apache.org/documentation/sdks/python-type-safety/)
    and an upcoming
    [blog post](https://beam.apache.org/blog/python-typing/index.html).

* Java SDK: Introducing the concept of options in Beam Schemas. These options add extra
context to fields and schemas. This replaces the current Beam metadata that is present
in a FieldType only, options are available in fields and row schemas. Schema options are
fully typed and can contain complex rows. *Remark: Schema aware is still experimental.*
([BEAM-9035](https://issues.apache.org/jira/browse/BEAM-9035))
* Java SDK: The protobuf extension is fully schema aware and also includes protobuf option
conversion to beam schema options. *Remark: Schema aware is still experimental.*
([BEAM-9044](https://issues.apache.org/jira/browse/BEAM-9044))
* Added ability to write to BigQuery via Avro file loads (Python) ([BEAM-8841](https://issues.apache.org/jira/browse/BEAM-8841))

    By default, file loads will be done using JSON, but it is possible to
    specify the temp_file_format parameter to perform file exports with AVRO.
    AVRO-based file loads work by exporting Python types into Avro types, so
    to switch to Avro-based loads, you will need to change your data types
    from Json-compatible types (string-type dates and timestamp, long numeric
    values as strings) into Python native types that are written to Avro
    (Python's date, datetime types, decimal, etc). For more information
    see https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-avro#avro_conversions.
* Added integration of Java SDK with Google Cloud AI VideoIntelligence service
([BEAM-9147](https://issues.apache.org/jira/browse/BEAM-9147))
* Added integration of Java SDK with Google Cloud AI natural language processing API
([BEAM-9634](https://issues.apache.org/jira/browse/BEAM-9634))
* `docker-pull-licenses` tag was introduced. Licenses/notices of third party dependencies will be added to the docker images when `docker-pull-licenses` was set.
  The files are added to `/opt/apache/beam/third_party_licenses/`.
  By default, no licenses/notices are added to the docker images. ([BEAM-9136](https://issues.apache.org/jira/browse/BEAM-9136))


## Breaking Changes

* Dataflow runner now requires the `--region` option to be set, unless a default value is set in the environment ([BEAM-9199](https://issues.apache.org/jira/browse/BEAM-9199)). See [here](https://cloud.google.com/dataflow/docs/concepts/regional-endpoints) for more details.
* HBaseIO.ReadAll now requires a PCollection of HBaseIO.Read objects instead of HBaseQuery objects ([BEAM-9279](https://issues.apache.org/jira/browse/BEAM-9279)).
* ProcessContext.updateWatermark has been removed in favor of using a WatermarkEstimator ([BEAM-9430](https://issues.apache.org/jira/browse/BEAM-9430)).
* Coder inference for PCollection of Row objects has been disabled ([BEAM-9569](https://issues.apache.org/jira/browse/BEAM-9569)).
* Go SDK docker images are no longer released until further notice.

## Deprecations
* Java SDK: Beam Schema FieldType.getMetadata is now deprecated and is replaced by the Beam
Schema Options, it will be removed in version `2.23.0`. ([BEAM-9704](https://issues.apache.org/jira/browse/BEAM-9704))
* The `--zone` option in the Dataflow runner is now deprecated. Please use `--worker_zone` instead. ([BEAM-9716](https://issues.apache.org/jira/browse/BEAM-9716))

## Known Issues


# [2.20.0] - 2020-04-15

## Highlights


## I/Os

* Java SDK: Adds support for Thrift encoded data via ThriftIO. ([BEAM-8561](https://issues.apache.org/jira/browse/BEAM-8561))
* Java SDK: KafkaIO supports schema resolution using Confluent Schema Registry. ([BEAM-7310](https://issues.apache.org/jira/browse/BEAM-7310))
* Java SDK: Add Google Cloud Healthcare IO connectors: HL7v2IO and FhirIO ([BEAM-9468](https://issues.apache.org/jira/browse/BEAM-9468))
* Python SDK: Support for Google Cloud Spanner. This is an experimental module for reading and writing data from Google Cloud Spanner ([BEAM-7246](https://issues.apache.org/jira/browse/BEAM-7246)).
* Python SDK: Adds support for standard HDFS URLs (with server name). ([#10223](https://github.com/apache/beam/pull/10223)).


## New Features / Improvements

* New AnnotateVideo & AnnotateVideoWithContext PTransform's that integrates GCP Video Intelligence functionality. (Python) ([BEAM-9146](https://issues.apache.org/jira/browse/BEAM-9146))
* New AnnotateImage & AnnotateImageWithContext PTransform's for element-wise & batch image annotation using Google Cloud Vision API. (Python) ([BEAM-9247](https://issues.apache.org/jira/browse/BEAM-9247))
* Added a PTransform for inspection and deidentification of text using Google Cloud DLP. (Python) ([BEAM-9258](https://issues.apache.org/jira/browse/BEAM-9258))
* New AnnotateText PTransform that integrates Google Cloud Natural Language functionality (Python) ([BEAM-9248](https://issues.apache.org/jira/browse/BEAM-9248))
* _ReadFromBigQuery_ now supports value providers for the query string (Python) ([BEAM-9305](https://issues.apache.org/jira/browse/BEAM-9305))
* Direct runner for FnApi supports further parallelism (Python) ([BEAM-9228](https://issues.apache.org/jira/browse/BEAM-9228))
* Support for _@RequiresTimeSortedInput_ in Flink and Spark (Java) ([BEAM-8550](https://issues.apache.org/jira/browse/BEAM-8550))

## Breaking Changes

* ReadFromPubSub(topic=<topic>) in Python previously created a subscription under the same project as the topic. Now it will create the subscription under the project specified in pipeline_options. If the project is not specified in pipeline_options, then it will create the subscription under the same project as the topic. ([BEAM-3453](https://issues.apache.org/jira/browse/BEAM-3453)).
* SpannerAccessor in Java is now package-private to reduce API surface. `SpannerConfig.connectToSpanner` has been moved to `SpannerAccessor.create`. ([BEAM-9310](https://issues.apache.org/jira/browse/BEAM-9310)).
* ParquetIO hadoop dependency should be now provided by the users ([BEAM-8616](https://issues.apache.org/jira/browse/BEAM-8616)).
* Docker images will be deployed to
  [apache/beam](https://hub.docker.com/search?q=apache%2Fbeam&type=image) repositories from 2.20. They
 used to be deployed to
 [apachebeam](https://hub.docker.com/search?q=apachebeam&type=image) repository.
 ([BEAM-9063](https://issues.apache.org/jira/browse/BEAM-9093))
* PCollections now have tags inferred from the result type (e.g. the keys of a dict or index of a tuple).  Users may expect the old implementation which gave PCollection output ids a monotonically increasing id. To go back to the old implementation, use the `force_generated_pcollection_output_ids` experiment.

## Deprecations

## Bugfixes

* Fixed numpy operators in ApproximateQuantiles (Python) ([BEAM-9579](https://issues.apache.org/jira/browse/BEAM-9579)).
* Fixed exception when running in IPython notebook (Python) ([BEAM-X9277](https://issues.apache.org/jira/browse/BEAM-9277)).
* Fixed Flink uberjar job termination bug. ([BEAM-9225](https://issues.apache.org/jira/browse/BEAM-9225))
* Fixed SyntaxError in process worker startup ([BEAM-9503](https://issues.apache.org/jira/browse/BEAM-9503))
* Key should be available in @OnTimer methods (Java) ([BEAM-1819](https://issues.apache.org/jira/browse/BEAM-1819)).

## Known Issues

* ([BEAM-X](https://issues.apache.org/jira/browse/BEAM-X)).
* ([BEAM-9322](https://issues.apache.org/jira/browse/BEAM-9322)).
* Python SDK `pre_optimize=all` experiment may cause error ([BEAM-9445](https://issues.apache.org/jira/browse/BEAM-9445))

# [2.19.0] - 2020-01-31

- For versions 2.19.0 and older release notes are available on [Apache Beam Blog](https://beam.apache.org/blog/).
