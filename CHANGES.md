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
* New highly anticipated feature Y added to JavaSDK ([BEAM-Y](https://issues.apache.org/jira/browse/BEAM-Y)).

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

# [2.22.0] - Unreleased

## Highlights

* New highly anticipated feature X added to Python SDK ([BEAM-X](https://issues.apache.org/jira/browse/BEAM-X)).
* New highly anticipated feature Y added to JavaSDK ([BEAM-Y](https://issues.apache.org/jira/browse/BEAM-Y)).

## I/Os

* Support for X source added (Java/Python) ([BEAM-X](https://issues.apache.org/jira/browse/BEAM-X)).

## New Features / Improvements

* --direct_num_workers=0 is supported for FnApi runner. It will set the number of threads/subprocesses to number of cores of the machine executing the pipeline([BEAM-9443](https://issues.apache.org/jira/browse/BEAM-9443)).

## Breaking Changes

* X behavior was changed ([BEAM-X](https://issues.apache.org/jira/browse/BEAM-X)).

## Deprecations

* X behavior is deprecated and will be removed in X versions ([BEAM-X](https://issues.apache.org/jira/browse/BEAM-X)).

## Known Issues

* Fixed X (Java/Python) ([BEAM-X](https://issues.apache.org/jira/browse/BEAM-X)).

# [2.21.0] - Unreleased

## Highlights

## I/Os

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
    [blog post](https://beam.apache.org/blog/python/typing/2020/03/06/python-typing.html).

## Breaking Changes

* HBaseIO.ReadAll now requires a PCollection of HBaseIO.Read objects instead of HBaseQuery objects ([BEAM-9279](https://issues.apache.org/jira/browse/BEAM-9279)).
* ProcessContext.updateWatermark has been removed in favor of using a WatermarkEstimator ([BEAM-9430](https://issues.apache.org/jira/browse/BEAM-9430)).

## Deprecations

## Known Issues


# [2.20.0] - Unreleased

## Highlights


## I/Os

* Java SDK: Adds support for Thrift encoded data via ThriftIO. ([BEAM-8561](https://issues.apache.org/jira/browse/BEAM-8561))
* Java SDK: KafkaIO supports schema resolution using Confluent Schema Registry. ([BEAM-7310](https://issues.apache.org/jira/browse/BEAM-7310))
* Python SDK: Support for Google Cloud Spanner. This is an experimental module for reading and writing data from Google Cloud Spanner ([BEAM-7246](https://issues.apache.org/jira/browse/BEAM-7246)).
* Python SDK: Adds support for standard HDFS URLs (with server name). ([#10223](https://github.com/apache/beam/pull/10223)).


## New Features / Improvements

* New AnnotateVideo & AnnotateVideoWithContext PTransform's that integrates GCP Video Intelligence functionality. (Python) ([BEAM-9146](https://issues.apache.org/jira/browse/BEAM-9146))
* New AnnotateImage & AnnotateImageWithContext PTransform's for element-wise & batch image annotation using Google Cloud Vision API. (Python) ([BEAM-9247](https://issues.apache.org/jira/browse/BEAM-9247))
* Added a PTransform for inspection and deidentification of text using Google Cloud DLP. (Python) ([BEAM-9258](https://issues.apache.org/jira/browse/BEAM-9258))
* New AnnotateText PTransform that integrates Google Cloud Natural Language functionality (Python) ([BEAM-9248](https://issues.apache.org/jira/browse/BEAM-9248))
* _ReadFromBigQuery_ now supports value providers for the query string (Python) ([BEAM-9305](https://issues.apache.org/jira/browse/BEAM-9305))
* Added ability to write to BigQuery via Avro file loads (Python) ([BEAM-8841](https://issues.apache.org/jira/browse/BEAM-8841))
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

## Deprecations

## Bugfixes

* Fixed numpy operators in ApproximateQuantiles (Python) ([BEAM-9579](https://issues.apache.org/jira/browse/BEAM-9579)).
* Fixed exception when running in IPython notebook (Python) ([BEAM-X9277](https://issues.apache.org/jira/browse/BEAM-9277)).
* Fixed 1833 (Python) ([BEAM-1833](https://issues.apache.org/jira/browse/BEAM-1833))
* Fixed Flink uberjar job termination bug. ([BEAM-9225](https://issues.apache.org/jira/browse/BEAM-9225))
* Fixed SyntaxError in process worker startup ([BEAM-9503](https://issues.apache.org/jira/browse/BEAM-9503))

## Known Issues

* ([BEAM-X](https://issues.apache.org/jira/browse/BEAM-X)).
* ([BEAM-9322](https://issues.apache.org/jira/browse/BEAM-9322)).
* Python SDK `pre_optimize=all` experiment may cause error ([BEAM-9445](https://issues.apache.org/jira/browse/BEAM-9445))

# [2.19.0] - 2020-01-31

- For versions 2.19.0 and older release notes are available on [Apache Beam Blog](https://beam.apache.org/blog/).
