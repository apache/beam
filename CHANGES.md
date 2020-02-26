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

# Current version (not yet released; still in development)

## Highlights

 * New highly anticipated feature X added to Python SDK ([BEAM-X](https://issues.apache.org/jira/browse/BEAM-X)).
 * New highly anticipated feature Y added to JavaSDK ([BEAM-Y](https://issues.apache.org/jira/browse/BEAM-Y)).

### I/Os

* Java SDK: Adds support for Thrift encoded data via ThriftIO. ([BEAM-8561](https://issues.apache.org/jira/browse/BEAM-8561))
* Java SDK: KafkaIO supports schema resolution using Confluent Schema Registry. ([BEAM-7310](https://issues.apache.org/jira/browse/BEAM-7310))
* Python SDK: Support for Google Cloud Spanner. This is an experimental module for reading and writing data from Google Cloud Spanner ([BEAM-7246](https://issues.apache.org/jira/browse/BEAM-7246)).
* Python SDK: Adds support for standard HDFS URLs (with server name). ([#10223](https://github.com/apache/beam/pull/10223)).
* Support for X source added (Java/Python) ([BEAM-X](https://issues.apache.org/jira/browse/BEAM-X)).

### New Features / Improvements

* X feature added (Java/Python) ([BEAM-X](https://issues.apache.org/jira/browse/BEAM-X)).
* New AnnotateVideo & AnnotateVideoWithContext PTransform's that integrates GCP Video Intelligence functionality. (Python) ([BEAM-9146](https://issues.apache.org/jira/browse/BEAM-9146))
* New AnnotateImage & AnnotateImageWithContext PTransform's for element-wise & batch image annotation using Google Cloud Vision API. (Python) ([BEAM-9247](https://issues.apache.org/jira/browse/BEAM-9247))
* Added a PTransform for inspection and deidentification of text using Google Cloud DLP. (Python) ([BEAM-9258](https://issues.apache.org/jira/browse/BEAM-9258))
* New AnnotateText PTransform that integrates Google Cloud Natural Language functionality (Python) ([BEAM-9248](https://issues.apache.org/jira/browse/BEAM-9248))

### Breaking Changes

* ReadFromPubSub(topic=<topic>) in Python previously created a subscription under the same project as the topic. Now it will create the subscription under the project specified in pipeline_options. If the project is not specified in pipeline_options, then it will create the subscription under the same project as the topic. ([BEAM-3453](https://issues.apache.org/jira/browse/BEAM-3453)).
* SpannerAccessor in Java is now package-private to reduce API surface. `SpannerConfig.connectToSpanner` has been moved to `SpannerAccessor.create`. ([BEAM-9310](https://issues.apache.org/jira/browse/BEAM-9310)).
* PCollections will now have their tags correctly propagated through the Pipeline. Users may expect the old implementation which gave PCollection output ids a monotonically increasing id. To go back to the old implementation, use the "force_generated_pcollection_output_ids" experiment. The default is the new implementation (force_generated_pcollection_output_ids=False).
* ParquetIO hadoop dependency should be now provided by the users ([BEAM-8616](https://issues.apache.org/jira/browse/BEAM-8616)).

### Deprecations

* X behavior is deprecated and will be removed in X versions ([BEAM-X](https://issues.apache.org/jira/browse/BEAM-X)).

### Bugfixes

* Fixed X (Java/Python) ([BEAM-X](https://issues.apache.org/jira/browse/BEAM-X)).
* Fixed exception when running in IPython notebook (Python) ([BEAM-X9277](https://issues.apache.org/jira/browse/BEAM-9277)).
* Fixed 1833 (Python) ([BEAM-1833](https://issues.apache.org/jira/browse/BEAM-1833))

### Known Issues

* ([BEAM-X](https://issues.apache.org/jira/browse/BEAM-X)).
* ([BEAM-9322](https://issues.apache.org/jira/browse/BEAM-9322)).

# [2.19.0] - 2020-01-31

- For versions 2.19.0 and older release notes are available on [Apache Beam Blog](https://beam.apache.org/blog/).
