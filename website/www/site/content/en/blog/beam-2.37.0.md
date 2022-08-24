---
title:  "Apache Beam 2.37.0"
date:   2022-03-04 08:30:00 -0800
categories:
  - blog
  - release
authors:
  - bhulette
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

We are happy to present the new 2.37.0 release of Beam.
This release includes both improvements and new functionality.
See the [download page](/get-started/downloads/#2370-2022-03-04) for this release.

<!--more-->

For more information on changes in 2.37.0 check out the [detailed release notes](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12319527&version=12351168).

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

### Known Issues

* See a full list of open [issues that affect](https://issues.apache.org/jira/issues/?jql=project%20%3D%20BEAM%20AND%20affectedVersion%20%3D%202.37.0%20ORDER%20BY%20priority%20DESC%2C%20updated%20DESC) this version.

## List of Contributors

According to git shortlog, the following people contributed to the 2.37.0 release. Thank you to all contributors!

Aizhamal Nurmamat kyzy
Alexander
Alexander Chermenin
Alexandr Zhuravlev
Alexey Romanenko
Anand Inguva
andoni-guzman
andreukus
Andy Ye
Artur Khanin
Aydar Farrakhov
Aydar Zainutdinov
AydarZaynutdinov
Benjamin Gonzalez
Brian Hulette
Chamikara Jayalath
Daniel Oliveira
Danny McCormick
daria-malkova
daria.malkova
darshan-sj
David Huntsperger
dprieto91
emily
Etienne Chauchot
Fernando Morales
Heejong Lee
Ismaël Mejía
Jack McCluskey
Jan Lukavský
johnjcasey
Kamil Breguła
kellen
Kenneth Knowles
kileys
Kyle Weaver
Luke Cwik
Marcin Kuthan
Marco Robles
Matt Rudary
Miguel Hernandez
Milena Bukal
Moritz Mack
Mostafa Aghajani
Ning Kang
Pablo Estrada
Pavel Avilov
Reuven Lax
Ritesh Ghorse
Robert Bradshaw
Robert Burke
Sam Whittle
Sandy Chapman
Sergey Kalinin
Thiago Nunes
thorbjorn444
Tim Robertson
Tomo Suzuki
Valentyn Tymofieiev
Victor
Victor Chen
Vitaly Ivanov
Yichi Zhang
