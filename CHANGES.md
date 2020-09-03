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

## Known Issues

* Fixed X (Java/Python) ([BEAM-X](https://issues.apache.org/jira/browse/BEAM-X)).
-->

# [2.25.0] - Unreleased

## Highlights

* Splittable DoFn is opt-out for Java based runners (Direct, Twister2) using `--experiments=use_deprecated_read`. For all other runners, users can opt-in using `--experiments=use_sdf_read`. (Java) ([BEAM-10670](https://issues.apache.org/jira/browse/BEAM-10135))
* New highly anticipated feature X added to Python SDK ([BEAM-X](https://issues.apache.org/jira/browse/BEAM-X)).
* New highly anticipated feature Y added to Java SDK ([BEAM-Y](https://issues.apache.org/jira/browse/BEAM-Y)).

## I/Os

* Added cross-language support to Java's KinesisIO, now available in the Python module `apache_beam.io.kinesis` ([BEAM-10138](https://issues.apache.org/jira/browse/BEAM-10138), [BEAM-10137](https://issues.apache.org/jira/browse/BEAM-10137)).
* Added cross-language support to Java's SnowflakeIO.Write, now available in the Python module `apache_beam.io.external.snowflake` ([BEAM-9898](https://issues.apache.org/jira/browse/BEAM-9898)).

## New Features / Improvements

* Support for repeatable fields in JSON decoder for `ReadFromBigQuery` added. (Python) ([BEAM-10524](https://issues.apache.org/jira/browse/BEAM-10524))
* Added an opt-in, performance-driven runtime type checking system for the Python SDK ([BEAM-10549](https://issues.apache.org/jira/browse/BEAM-10549)).
    More details will be in an upcoming [blog post](https://beam.apache.org/blog/python-performance-runtime-type-checking/index.html).
* Added support for Python 3 type annotations on PTransforms using typed PCollections ([BEAM-10258](https://issues.apache.org/jira/browse/BEAM-10258)).
    More details will be in an upcoming [blog post](https://beam.apache.org/blog/python-improved-annotations/index.html).
* X feature added (Java/Python) ([BEAM-X](https://issues.apache.org/jira/browse/BEAM-X)).

## Breaking Changes

* X behavior was changed ([BEAM-X](https://issues.apache.org/jira/browse/BEAM-X)).

## Deprecations

* X behavior is deprecated and will be removed in X versions ([BEAM-X](https://issues.apache.org/jira/browse/BEAM-X)).

## Known Issues

* Fixed X (Java/Python) ([BEAM-X](https://issues.apache.org/jira/browse/BEAM-X)).

# [2.24.0] - Unreleased

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
* Support for X source added (Java/Python) ([BEAM-X](https://issues.apache.org/jira/browse/BEAM-X)).
* Add streaming support to SnowflakeIO in Java SDK ([BEAM-9896](https://issues.apache.org/jira/browse/BEAM-9896  ))
* Support reading and writing to Google Healthcare DICOM APIs in Python SDK ([BEAM-10601](https://issues.apache.org/jira/browse/BEAM-10601))

## New Features / Improvements

* Shared library for simplifying management of large shared objects added to Python SDK. Example use case is sharing a large TF model object across threads ([BEAM-10417](https://issues.apache.org/jira/browse/BEAM-10417)).
* X feature added (Java/Python) ([BEAM-X](https://issues.apache.org/jira/browse/BEAM-X)).
* Dataflow streaming timers are not strictly time ordered when set earlier mid-bundle ([BEAM-8543](https://issues.apache.org/jira/browse/BEAM-8543)).
* OnTimerContext should not create a new one when processing each element/timer in FnApiDoFnRunner ([BEAM-9839](https://issues.apache.org/jira/browse/BEAM-9839))
* Key should be available in @OnTimer methods (Spark Runner) ([BEAM-9850](https://issues.apache.org/jira/browse/BEAM-9850))

## Breaking Changes

* X behavior was changed ([BEAM-X](https://issues.apache.org/jira/browse/BEAM-X)).

## Deprecations

* X behavior is deprecated and will be removed in X versions ([BEAM-X](https://issues.apache.org/jira/browse/BEAM-X)).

## Known Issues

* Fixed X (Java/Python) ([BEAM-X](https://issues.apache.org/jira/browse/BEAM-X)).

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
* Add dispositions for SnowflakeIO.write ([BEAM-10343](https://issues.apache.org/jira/browse/BEAM-10343))
* Add cross-language support to SnowflakeIO.Read([BEAM-9897](https://issues.apache.org/jira/browse/BEAM-9897)).

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

* Java SDK: Introducing the concept of options in Beam Schema’s. These options add extra
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
