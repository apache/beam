---
title:  "Apache Beam 2.42.0"
date:   2022-10-17 9:00:00 -0700
categories:
- blog
- release
authors:
- lostluck
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

We are happy to present the new 2.42.0 release of Beam.
This release includes both improvements and new functionality.
See the [download page](/get-started/downloads/#2420-2022-10-17) for this release.

<!--more-->

For more information on changes in 2.42.0, check out the [detailed release notes](https://github.com/apache/beam/milestone/4?closed=1).

## Highlights

* Added support for stateful DoFns to the Go SDK.
* Added support for [Batched
  DoFns](/documentation/programming-guide/#batched-dofns)
  to the Python SDK.

## New Features / Improvements

* Added support for Zstd compression to the Python SDK.
* Added support for Google Cloud Profiler to the Go SDK.
* Added support for stateful DoFns to the Go SDK.

## Breaking Changes

* The Go SDK's Row Coder now uses a different single-precision float encoding for float32 types to match Java's behavior ([#22629](https://github.com/apache/beam/issues/22629)).

## Bugfixes

* Fixed Python cross-language JDBC IO Connector cannot read or write rows containing Timestamp type values [19817](https://github.com/apache/beam/issues/19817).

### Known Issues

* Go SDK doesn't yet support Slowly Changing Side Input pattern ([#23106](https://github.com/apache/beam/issues/23106))
* See a full list of open [issues that affect](https://github.com/apache/beam/milestone/4) this version.

## List of Contributors

According to git shortlog, the following people contributed to the 2.42.0 release. Thank you to all contributors!

Abirdcfly
Ahmed Abualsaud
Alexander Zhuravlev
Alexey Inkin
Alexey Romanenko
Anand Inguva
Andrej Galad
Andrew Pilloud
Andy Ye
Balázs Németh
Brian Hulette
Bruno Volpato
bulat safiullin
bullet03
Chamikara Jayalath
ChangyuLi28
Clément Guillaume
Damon
Danny McCormick
Darkhan Nausharipov
David Huntsperger
dpcollins-google
Evgeny Antyshev
grufino
Heejong Lee
Ismaël Mejía
Jack McCluskey
johnjcasey
Jonathan Shen
Kenneth Knowles
Ke Wu
Kiley Sok
Liam Miller-Cushon
liferoad
Lucas Nogueira
Luke Cwik
MakarkinSAkvelon
Manit Gupta
masahitojp
Michael Hu
Michel Davit
Moritz Mack
Naireen Hussain
nancyxu123
Nikhil Nadig
oborysevych
Pablo Estrada
Pranav Bhandari
Rajat Bhatta
Rebecca Szper
Reuven Lax
Ritesh Ghorse
Robert Bradshaw
Robert Burke
Ryan Thompson
Sam Whittle
Sergey Pronin
Shivam
Shunsuke Otani
Shunya Ueta
Steven Niemitz
Stuart
Svetak Sundhar
Valentyn Tymofieiev
Vitaly Terentyev
Vlad
Vladislav Chunikhin
Yichi Zhang
Yi Hu
Yixiao Shen
