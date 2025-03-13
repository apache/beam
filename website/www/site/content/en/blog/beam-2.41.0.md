---
title:  "Apache Beam 2.41.0"
date:   2022-08-23 9:00:00 -0700
categories:
- blog
- release
authors:
- kileysok
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

We are happy to present the new 2.41.0 release of Beam.
This release includes both improvements and new functionality.
See the [download page](/get-started/downloads/#2410-2022-08-23) for this release.

<!--more-->

For more information on changes in 2.41.0, check out the [detailed release notes](https://github.com/apache/beam/milestone/3?closed=1).

## I/Os

* Projection Pushdown optimizer is now on by default for streaming, matching the behavior of batch pipelines since 2.38.0. If you encounter a bug with the optimizer, please file an issue and disable the optimizer using pipeline option `--experiments=disable_projection_pushdown`.

## New Features / Improvements

* Previously available in Java sdk, Python sdk now also supports logging level overrides per module. ([#18222](https://github.com/apache/beam/issues/18222)).

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

### Known Issues

* See a full list of open [issues that affect](https://github.com/apache/beam/milestone/3) this version.

## List of Contributors

According to git shortlog, the following people contributed to the 2.41.0 release. Thank you to all contributors!

Ahmed Abualsaud
Ahmet Altay
akashorabek
Alexey Inkin
Alexey Romanenko
Anand Inguva
andoni-guzman
Andrew Pilloud
Andrey
Andy Ye
Balázs Németh
Benjamin Gonzalez
BjornPrime
Brian Hulette
bulat safiullin
bullet03
Byron Ellis
Chamikara Jayalath
Damon Douglas
Daniel Oliveira
Daniel Thevessen
Danny McCormick
David Huntsperger
Dheeraj Gharde
Etienne Chauchot
Evan Galpin
Fernando Morales
Heejong Lee
Jack McCluskey
johnjcasey
Kenneth Knowles
Ke Wu
Kiley Sok
Liam Miller-Cushon
Lucas Nogueira
Luke Cwik
MakarkinSAkvelon
Manu Zhang
Minbo Bae
Moritz Mack
Naireen Hussain
Ning Kang
Oleh Borysevych
Pablo Estrada
pablo rodriguez defino
Pranav Bhandari
Rebecca Szper
Red Daly
Reuven Lax
Ritesh Ghorse
Robert Bradshaw
Robert Burke
Ryan Thompson
Sam Whittle
Steven Niemitz
Valentyn Tymofieiev
Vincent Marquez
Vitaly Terentyev
Vlad
Vladislav Chunikhin
Yichi Zhang
Yi Hu
yirutang
Yixiao Shen
Yu Feng
