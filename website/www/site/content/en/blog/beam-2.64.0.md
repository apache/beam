---
title:  "Apache Beam 2.64.0"
date:   2025-03-31 10:30:00 -0500
categories:
  - blog
  - release
authors:
  - liferoad
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

We are happy to present the new 2.64.0 release of Beam.
This release includes both improvements and new functionality.
See the [download page](/get-started/downloads/{$DOWNLOAD_ANCHOR}) for this release.

<!--more-->

For more information on changes in 2.64.0, check out the [detailed release notes](https://github.com/apache/beam/milestone/28).
## Highlights

* Managed API for [Java](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/managed/Managed.html) and [Python](https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.managed.html#module-apache_beam.transforms.managed) supports [key I/O connectors](https://beam.apache.org/documentation/io/connectors/) Iceberg, Kafka, and BigQuery.

## I/Os

* [Java] Use API compatible with both com.google.cloud.bigdataoss:util 2.x and 3.x in BatchLoads ([#34105](https://github.com/apache/beam/pull/34105))
* [IcebergIO] Added new CDC source for batch and streaming, available as `Managed.ICEBERG_CDC` ([#33504](https://github.com/apache/beam/pull/33504))
* [IcebergIO] Address edge case where bundle retry following a successful data commit results in data duplication ([#34264](https://github.com/apache/beam/pull/34264))

## New Features / Improvements

* [Python] Support custom coders in Reshuffle ([#29908](https://github.com/apache/beam/issues/29908), [#33356](https://github.com/apache/beam/issues/33356)).
* [Java] Upgrade SLF4J to 2.0.16. Update default Spark version to 3.5.0. ([#33574](https://github.com/apache/beam/pull/33574))
* [Java] Support for `--add-modules` JVM option is added through a new pipeline option `JdkAddRootModules`. This allows extending the module graph with optional modules such as SDK incubator modules. Sample usage: `<pipeline invocation> --jdkAddRootModules=jdk.incubator.vector` ([#30281](https://github.com/apache/beam/issues/30281)).
* Managed API for [Java](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/managed/Managed.html) and [Python](https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.managed.html#module-apache_beam.transforms.managed) supports [key I/O connectors](https://beam.apache.org/documentation/io/connectors/) Iceberg, Kafka, and BigQuery.
* Prism now supports event time triggers for most common cases. ([#31438](https://github.com/apache/beam/issues/31438))
  * Prism does not yet support triggered side inputs, or triggers on merging windows (such as session windows).

## Breaking Changes

* [Python] Reshuffle now correctly respects user-specified type hints, fixing a previous bug where it might use FastPrimitivesCoder wrongly. This change could break pipelines with incorrect type hints in Reshuffle. If you have issues after upgrading, temporarily set update_compatibility_version to a previous Beam version to use the old behavior. The recommended solution is to fix the type hints in your code. ([#33932](https://github.com/apache/beam/pull/33932))
* [Java] SparkReceiver 2 has been moved to SparkReceiver 3 that supports Spark 3.x. ([#33574](https://github.com/apache/beam/pull/33574))
* [Python] Correct parsing of `collections.abc.Sequence` type hints was added, which can lead to pipelines failing type hint checks that were previously passing erroneously. These issues will be most commonly seen trying to consume a PCollection with a `Sequence` type hint after a GroupByKey or a CoGroupByKey. ([#33999](https://github.com/apache/beam/pull/33999)).

## Bugfixes

* (Python) Fixed occasional pipeline stuckness that was affecting Python 3.11 users ([#33966](https://github.com/apache/beam/issues/33966)).
* (Java) Fixed TIME field encodings for BigQuery Storage API writes on GenericRecords ([#34059](https://github.com/apache/beam/pull/34059)).
* (Java) Fixed a race condition in JdbcIO which could cause hangs trying to acquire a connection ([#34058](https://github.com/apache/beam/pull/34058)).
* (Java) Fix BigQuery Storage Write compatibility with Avro 1.8 ([#34281](https://github.com/apache/beam/pull/34281)).
* Fixed checkpoint recovery and streaming behavior in Spark Classic and Portable runner's Flatten transform by replacing queueStream with SingleEmitInputDStream ([#34080](https://github.com/apache/beam/pull/34080), [#18144](https://github.com/apache/beam/issues/18144), [#20426](https://github.com/apache/beam/issues/20426))
* (Java) Fixed Read caching of UnboundedReader objects to effectively cache across multiple DoFns and avoid checkpointing unstarted reader. [#34146](https://github.com/apache/beam/pull/34146) [#33901](https://github.com/apache/beam/pull/33901)

## Known Issues

* (Java) Current version of protobuf has a [bug](https://github.com/protocolbuffers/protobuf/issues/20599) leading to incompatibilities with clients using older versions of Protobuf ([example issue](https://github.com/GoogleCloudPlatform/DataflowTemplates/issues/2191)). This issue has been seen in SpannerIO in particular. Tracked in [#34452](https://github.com/GoogleCloudPlatform/DataflowTemplates/issues/34452).
* (Java) When constructing `SpannerConfig` for `SpannerIO`, calling `withHost` with a null or empty host will now result in a Null Pointer Exception (`java.lang.NullPointerException: Cannot invoke "java.lang.CharSequence.length()" because "this.text" is null`). See https://github.com/GoogleCloudPlatform/DataflowTemplates/issues/34489 for context.

## List of Contributors

According to git shortlog, the following people contributed to the 2.64.0 release. Thank you to all contributors!

Ahmed Abualsaud

akashorabek

Arun Pandian

Bentsi Leviav

Chamikara Jayalath

Charles Nguyen

Claire McGinty

claudevdm

Damon

Danny McCormick

darshan-sj

Derrick Williams

fozzie15

Hai Joey Tran

Jack McCluskey

Jozef Vilcek

jrmccluskey

Kenneth Knowles

Liam Miller-Cushon

liferoad

Luv Agarwal

martin trieu

Matar

Matthew Suozzo

Michel Davit

Minbo Bae

Mohamed Awnallah

Naireen Hussain

Pablo Rodriguez Defino

Radosław Stankiewicz

Rakesh Kumar

Reuven Lax

Robert Bradshaw

Robert Burke

Rohit

Rohit Sinha

Sam Whittle

Saumil Patel

Shunping Huang

So-shi Nakachi

Steven van Rossum

Suvrat Acharya

Svetak Sundhar

synenka

Talat UYARER

tvalentyn

twosom

utkarshparekh

Vitaly Terentyev

XQ Hu

Yi Hu

Zilin Du
