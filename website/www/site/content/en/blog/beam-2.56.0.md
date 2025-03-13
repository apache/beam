---
title:  "Apache Beam 2.56.0"
date:   2024-05-01 10:00:00 -0400
categories:
  - blog
  - release
authors:
  - damccorm
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

We are happy to present the new 2.56.0 release of Beam.
This release includes both improvements and new functionality.
See the [download page](/get-started/downloads/#2550-2023-03-25) for this release.

<!--more-->

For more information on changes in 2.56.0, check out the [detailed release notes](https://github.com/apache/beam/milestone/20).

## Highlights

* Added FlinkRunner for Flink 1.17, removed support for Flink 1.12 and 1.13. Previous version of Pipeline running on Flink 1.16 and below can be upgraded to 1.17, if the Pipeline is first updated to Beam 2.56.0 with the same Flink version. After Pipeline runs with Beam 2.56.0, it should be possible to upgrade to FlinkRunner with Flink 1.17. ([#29939](https://github.com/apache/beam/issues/29939))
* New Managed I/O Java API ([#30830](https://github.com/apache/beam/pull/30830)).
* New Ordered Processing PTransform added for processing order-sensitive stateful data ([#30735](https://github.com/apache/beam/pull/30735)).

## I/Os

* Upgraded Avro version to 1.11.3, kafka-avro-serializer and kafka-schema-registry-client versions to 7.6.0 (Java) ([#30638](https://github.com/apache/beam/pull/30638)).
  The newer Avro package is known to have breaking changes. If you are affected, you can keep pinned to older Avro versions which are also tested with Beam.
* Iceberg read/write support is available through the new Managed I/O Java API ([#30830](https://github.com/apache/beam/pull/30830)).

## New Features / Improvements

* Profiling of Cythonized code has been disabled by default. This might improve performance for some Python pipelines ([#30938](https://github.com/apache/beam/pull/30938)).
* Bigtable enrichment handler now accepts a custom function to build a composite row key. (Python) ([#30974](https://github.com/apache/beam/issues/30975)).

## Breaking Changes

* Default consumer polling timeout for KafkaIO.Read was increased from 1 second to 2 seconds. Use KafkaIO.read().withConsumerPollingTimeout(Duration duration) to configure this timeout value when necessary ([#30870](https://github.com/apache/beam/issues/30870)).
* Python Dataflow users no longer need to manually specify --streaming for pipelines using unbounded sources such as ReadFromPubSub.

## Bugfixes

* Fixed locking issue when shutting down inactive bundle processors. Symptoms of this issue include slowness or stuckness in long-running jobs (Python) ([#30679](https://github.com/apache/beam/pull/30679)).
* Fixed logging issue that caused silecing the pip output when installing of dependencies provided in `--requirements_file` (Python).

## Known Issues

* Python pipelines that run with 2.53.0-2.58.0 SDKs and read data from GCS might be affected by a data corruption issue ([#32169](https://github.com/apache/beam/issues/32169)). The issue will be fixed in 2.59.0 ([#32135](https://github.com/apache/beam/pull/32135)). To work around this, update the google-cloud-storage package to version 2.18.2 or newer.

For the most up to date list of known issues, see https://github.com/apache/beam/blob/master/CHANGES.md

## List of Contributors

According to git shortlog, the following people contributed to the 2.56.0 release. Thank you to all contributors!

Abacn

Ahmed Abualsaud

Andrei Gurau

Andrey Devyatkin

Aravind Pedapudi

Arun Pandian

Arvind Ram

Bartosz Zablocki

Brachi Packter

Byron Ellis

Chamikara Jayalath

Clement DAL PALU

Damon

Danny McCormick

Daria Bezkorovaina

Dip Patel

Evan Burrell

Hai Joey Tran

Jack McCluskey

Jan Lukavský

JayajP

Jeff Kinard

Julien Tournay

Kenneth Knowles

Luís Bianchin

Maciej Szwaja

Melody Shen

Oleh Borysevych

Pablo Estrada

Rebecca Szper

Ritesh Ghorse

Robert Bradshaw

Sam Whittle

Sergei Lilichenko

Shahar Epstein

Shunping Huang

Svetak Sundhar

Timothy Itodo

Veronica Wasson

Vitaly Terentyev

Vlado Djerek

Yi Hu

akashorabek

bzablocki

clmccart

damccorm

dependabot[bot]

dmitryor

github-actions[bot]

liferoad

martin trieu

tvalentyn

xianhualiu
