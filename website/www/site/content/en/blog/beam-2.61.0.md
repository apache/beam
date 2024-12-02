---
title:  "Apache Beam 2.61.0"
date:   2024-11-25 15:00:00 -0500
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

We are happy to present the new 2.61.0 release of Beam.
This release includes both improvements and new functionality.
See the [download page](/get-started/downloads/#2610-2024-11-25) for this release.

<!--more-->

For more information on changes in 2.61.0, check out the [detailed release notes](https://github.com/apache/beam/milestone/25).

## Highlights

* [Python] Introduce Managed Transforms API ([#31495](https://github.com/apache/beam/pull/31495))
* Flink 1.19 support added ([#32648](https://github.com/apache/beam/pull/32648))

## I/Os

* [Managed Iceberg] Support creating tables if needed ([#32686](https://github.com/apache/beam/pull/32686))
* [Managed Iceberg] Now available in Python SDK ([#31495](https://github.com/apache/beam/pull/31495))
* [Managed Iceberg] Add support for TIMESTAMP, TIME, and DATE types ([#32688](https://github.com/apache/beam/pull/32688))
* BigQuery CDC writes are now available in Python SDK, only supported when using StorageWrite API at least once mode ([#32527](https://github.com/apache/beam/issues/32527))
* [Managed Iceberg] Allow updating table partition specs during pipeline runtime ([#32879](https://github.com/apache/beam/pull/32879))
* Added BigQueryIO as a Managed IO ([#31486](https://github.com/apache/beam/pull/31486))
* Support for writing to [Solace messages queues](https://solace.com/) (`SolaceIO.Write`) added (Java) ([#31905](https://github.com/apache/beam/issues/31905)).

## New Features / Improvements

* Added support for read with metadata in MqttIO (Java) ([#32195](https://github.com/apache/beam/issues/32195))
* Added support for processing events which use a global sequence to "ordered" extension (Java) [#32540](https://github.com/apache/beam/pull/32540)
* Add new meta-transform FlattenWith and Tee that allow one to introduce branching
  without breaking the linear/chaining style of pipeline construction.
* Use Prism as a fallback to the Python Portable runner when running a pipeline with the Python Direct runner [#32876](https://github.com/apache/beam/pull/32876)

## Deprecations

* Removed support for Flink 1.15 and 1.16
* Removed support for Python 3.8

## Bugfixes

* (Java) Fixed tearDown not invoked when DoFn throws on Portable Runners ([#18592](https://github.com/apache/beam/issues/18592), [#31381](https://github.com/apache/beam/issues/31381)).
* (Java) Fixed protobuf error with MapState.remove() in Dataflow Streaming Java Legacy Runner without Streaming Engine ([#32892](https://github.com/apache/beam/issues/32892)).
* Adding flag to support conditionally disabling auto-commit in JdbcIO ReadFn ([#31111](https://github.com/apache/beam/issues/31111))

## Known Issues

N/A

For the most up to date list of known issues, see https://github.com/apache/beam/blob/master/CHANGES.md

## List of Contributors

According to git shortlog, the following people contributed to the 2.60.0 release. Thank you to all contributors!

Ahmed Abualsaud, Ahmet Altay, Arun Pandian, Ayush Pandey, Chamikara Jayalath, Chris Ashcraft, Christoph Grotz, DKPHUONG, Damon, Danny Mccormick, Dmitry Ulyumdzhiev, Ferran Fernández Garrido, Hai Joey Tran, Hyeonho Kim, Idan Attias, Israel Herraiz, Jack McCluskey, Jan Lukavský, Jeff Kinard, Jeremy Edwards, Joey Tran, Kenneth Knowles, Maciej Szwaja, Manit Gupta, Mattie Fu, Michel Davit, Minbo Bae, Mohamed Awnallah, Naireen Hussain, Rebecca Szper, Reeba Qureshi, Reuven Lax, Robert Bradshaw, Robert Burke, S. Veyrié, Sam Whittle, Sergei Lilichenko, Shunping Huang, Steven van Rossum, Tan Le, Thiago Nunes, Vitaly Terentyev, Vlado Djerek, Yi Hu, claudevdm, fozzie15, johnjcasey, kushmiD, liferoad, martin trieu, pablo rodriguez defino, razvanculea, s21lee, tvalentyn, twosom
