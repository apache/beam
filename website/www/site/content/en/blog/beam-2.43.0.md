---
title:  "Apache Beam 2.43.0"
date:   2022-11-17 09:00:00 -0700
categories:
- blog
- release
authors:
- chamikara
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

We are happy to present the new 2.43.0 release of Beam.
This release includes both improvements and new functionality.
See the [download page](/get-started/downloads/#2430-2022-11-17) for this release.

<!--more-->

For more information on changes in 2.43.0, check out the [detailed release notes](https://github.com/apache/beam/milestone/5?closed=1).

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

## Breaking Changes

* CoGroupByKey transform in Python SDK has changed the output typehint. The typehint component representing grouped values changed from List to Iterable,
  which more accurately reflects the nature of the arbitrarily large output collection. [#21556](https://github.com/apache/beam/issues/21556) Beam users may see an error on transforms downstream from CoGroupByKey. Users must change methods expecting a List to expect an Iterable going forward. See [document](https://docs.google.com/document/d/1RIzm8-g-0CyVsPb6yasjwokJQFoKHG4NjRUcKHKINu0) for information and fixes.
* The PortableRunner for Spark assumes Spark 3 as default Spark major version unless configured otherwise using `--spark_version`.
  Spark 2 support is deprecated and will be removed soon ([#23728](https://github.com/apache/beam/issues/23728)).

## Bugfixes

* Fixed Python cross-language JDBC IO Connector cannot read or write rows containing Numeric/Decimal type values ([#19817](https://github.com/apache/beam/issues/19817)).

## List of Contributors

According to git shortlog, the following people contributed to the 2.43.0 release. Thank you to all contributors!

Ahmed Abualsaud
AlexZMLyu
Alexey Romanenko
Anand Inguva
Andrew Pilloud
Andy Ye
Arnout Engelen
Benjamin Gonzalez
Bharath Kumarasubramanian
BjornPrime
Brian Hulette
Bruno Volpato
Chamikara Jayalath
Colin Versteeg
Damon
Daniel Smilkov
Daniela Martín
Danny McCormick
Darkhan Nausharipov
David Huntsperger
Denis Pyshev
Dmitry Repin
Evan Galpin
Evgeny Antyshev
Fernando Morales
Geddy05
Harshit Mehrotra
Iñigo San Jose Visiers
Ismaël Mejía
Israel Herraiz
Jan Lukavský
Juta Staes
Kanishk Karanawat
Kenneth Knowles
KevinGG
Kiley Sok
Liam Miller-Cushon
Luke Cwik
Mc
Melissa Pashniak
Moritz Mack
Ning Kang
Pablo Estrada
Philippe Moussalli
Pranav Bhandari
Rebecca Szper
Reuven Lax
Ritesh Ghorse
Robert Bradshaw
Robert Burke
Ryan Thompson
Ryohei Nagao
Sam Rohde
Sam Whittle
Sanil Jain
Seunghwan Hong
Shane Hansen
Shubham Krishna
Shunsuke Otani
Steve Niemitz
Steven van Rossum
Svetak Sundhar
Thiago Nunes
Toran Sahu
Veronica Wasson
Vitaly Terentyev
Vladislav Chunikhin
Xinyu Liu
Yi Hu
Yixiao Shen
alexeyinkin
arne-alex
azhurkevich
bulat safiullin
bullet03
coldWater
dpcollins-google
egalpin
johnjcasey
liferoad
rvballada
shaojwu
tvalentyn
