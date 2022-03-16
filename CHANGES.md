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

* New highly anticipated feature X added to Python SDK ([BEAM-X](https://issues.apache.org/jira/browse/BEAM-X)).
* New highly anticipated feature Y added to Java SDK ([BEAM-Y](https://issues.apache.org/jira/browse/BEAM-Y)).

## I/Os

* Support for X source added (Java/Python) ([BEAM-X](https://issues.apache.org/jira/browse/BEAM-X)).

## New Features / Improvements

* X feature added (Java/Python) ([BEAM-X](https://issues.apache.org/jira/browse/BEAM-X)).

## Breaking Changes

* X behavior was changed ([BEAM-X](https://issues.apache.org/jira/browse/BEAM-X)).

## Deprecations

* X behavior is deprecated and will be removed in X versions ([BEAM-X](https://issues.apache.org/jira/browse/BEAM-X)).

## Bugfixes

* Fixed X (Java/Python) ([BEAM-X](https://issues.apache.org/jira/browse/BEAM-X)).

## Known Issues

* ([BEAM-X](https://issues.apache.org/jira/browse/BEAM-X)).
-->
# [2.38.0] - Unreleased

## Highlights

* New highly anticipated feature X added to Python SDK ([BEAM-X](https://issues.apache.org/jira/browse/BEAM-X)).
* New highly anticipated feature Y added to Java SDK ([BEAM-Y](https://issues.apache.org/jira/browse/BEAM-Y)).

## I/Os

* Support for X source added (Java/Python) ([BEAM-X](https://issues.apache.org/jira/browse/BEAM-X)).
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

## Breaking Changes

* (Python) Previously `DoFn.infer_output_types` was expected to return `Iterable[element_type]` where `element_type` is the PCollection elemnt type. It is now expected to return `element_type`. Take care if you have overriden `infer_output_type` in a `DoFn` (this is not common). See [BEAM-13860](https://issues.apache.org/jira/browse/BEAM-13860).
* (`amazon-web-services2`) The types of `awsRegion` / `endpoint` in `AwsOptions` changed from String to `Region` / `URI` ([BEAM-13563](https://issues.apache.org/jira/browse/BEAM-13563)).

## Deprecations

* Beam 2.38.0 will be the last minor release to support Flink 1.11.
* (`amazon-web-services2`) Client providers (`withXYZClientProvider()`) as well as IO specific `RetryConfiguration`s are deprecated, instead use `withClientConfiguration()` or `AwsOptions` to configure AWS IOs / clients.
  Custom implementations of client providers shall be replaced with a respective `ClientBuilderFactory` and configured through `AwsOptions` ([BEAM-13563](https://issues.apache.org/jira/browse/BEAM-13563)).
* X behavior is deprecated and will be removed in X versions ([BEAM-X](https://issues.apache.org/jira/browse/BEAM-X)).

## Bugfixes

* Fix S3 copy for large objects (Java) ([BEAM-14011](https://issues.apache.org/jira/browse/BEAM-14011))

## Known Issues

* ([BEAM-X](https://issues.apache.org/jira/browse/BEAM-X)).

# [2.37.0] - 2022-03-04

## Highlights
* Java 17 support for Dataflow ([BEAM-12240](https://issues.apache.org/jira/browse/BEAM-12240)).
  * Users using Dataflow Runner V2 may see issues with state cache due to inaccurate object sizes ([BEAM-13695](https://issues.apache.org/jira/browse/BEAM-13695)).
  * ZetaSql is currently unsupported ([issue](https://github.com/google/zetasql/issues/89)).
* Python 3.9 support in Apache Beam ([BEAM-12000](https://issues.apache.org/jira/browse/BEAM-12000)).
  * Dataflow support for Python 3.9 is expected to be available with 2.37.0,
    but may not be fully available yet when the release is announced ([BEAM-13864](https://issues.apache.org/jira/browse/BEAM-13864)).
  * Users of Dataflow Runner V2 can run Python 3.9 pipelines with 2.37.0 release right away.

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

## Breaking Changes

## Deprecations

## Bugfixes

## Known Issues

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
* Multiple DataFrame API bugfixes ([BEAM-12071](https://issues.apache/jira/browse/BEAM-12071), [BEAM-11929](https://issues.apache/jira/browse/BEAM-11929))

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
