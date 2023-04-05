---
title:  "Apache Beam 2.36.0"
date:   2022-02-07 10:11:00 -0800
categories:
  - blog
  - release
authors:
  - emilymye
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

We are happy to present the new 2.36.0 release of Apache Beam.
This release includes both improvements and new functionality.
See the [download page](/get-started/downloads/#2360-2022-02-07) for this release.

<!--more-->

For more information on changes in 2.36.0, check out the [detailed release
notes](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12319527&version=12350407).

## I/Os

* Support for stopReadTime on KafkaIO SDF (Java).([BEAM-13171](https://issues.apache.org/jira/browse/BEAM-13171)).

## New Features / Improvements

* 💻 Support for ARM64 / Mac M1 out of the box. ([BEAM-11703](https://issues.apache.org/jira/browse/BEAM-11703)).
* Added support for cloudpickle as a pickling library for Python SDK ([BEAM-8123](https://issues.apache.org/jira/browse/BEAM-8123)). To use cloudpickle, set pipeline option: --pickle_library=cloudpickle
* Added option to specify triggering frequency when streaming to BigQuery (Python) ([BEAM-12865](https://issues.apache.org/jira/browse/BEAM-12865)).
* Added option to enable caching uploaded artifacts across job runs for Python Dataflow jobs ([BEAM-13459](https://issues.apache.org/jira/browse/BEAM-13459)).  To enable, set pipeline option: --enable_artifact_caching, this will be enabled by default in a future release.

## Breaking Changes

* Updated the jedis from 3.x to 4.x to Java RedisIO. If you are using RedisIO and using jedis directly, please refer to [this page](https://github.com/redis/jedis/blob/v4.0.0/docs/3to4.md) to update it. ([BEAM-12092](https://issues.apache.org/jira/browse/BEAM-12092)).
* Datatype of timestamp fields in `SqsMessage` for AWS IOs for SDK v2 was changed from `String` to `long`, visibility of all fields was fixed from `package private` to `public` [BEAM-13638](https://issues.apache.org/jira/browse/BEAM-13638).
* Properly check output timestamps on elements output from DoFns, timers, and onWindowExpiration in Java [BEAM-12931](https://issues.apache.org/jira/browse/BEAM-12931).
* Fixed a bug with DeferredDataFrame.xs when used with a non-tuple key
  ([BEAM-13421](https://issues.apache.org/jira/browse/BEAM-13421])).
* Beam Python now requires `google-cloud-pubsub>=2.1.0`. The API surface for `apache_beam.io.gcp.pubsub` has not changed, but code that uses the PubSub client directly may need to be updated.

## Known Issues

* Users may encounter an unexpected java.lang.ArithmeticException when outputting a timestamp
  for an element further than allowedSkew from an allowed DoFN skew set to a value more than
  Integer.MAX_VALUE.
* S3 object metadata retrieval broken in Python SDK ([BEAM-13980](https://issues.apache.org/jira/browse/BEAM-13980))
* See a full list of open [issues that affect](https://issues.apache.org/jira/issues/?jql=project%20%3D%20BEAM%20AND%20affectedVersion%20%3D%202.36.0%20ORDER%20BY%20priority%20DESC%2C%20updated%20DESC) this version.


## List of Contributors

According to git shortlog, the following people contributed to the 2.36.0 release. Thank you to all contributors!

Ada Wong
Ahmet Altay
Alexander
Alexander Dahl
Alexandr Zhuravlev
Alexey Romanenko
AlikRodriguez
Anand Inguva
Andrew Pilloud
Andy Ye
Arkadiusz Gasiński
Artur Khanin
Arun Pandian
Aydar Farrakhov
Aydar Zainutdinov
AydarZaynutdinov
Benjamin Gonzalez
Brian Hulette
Chamikara Jayalath
Daniel Collins
Daniel Oliveira
Daniel Thevessen
Daniela Martín
David Hinkes
David Huntsperger
Emily Ye
Etienne Chauchot
Evan Galpin
Heejong Lee
Ilya
Ilya Kozyrev
In-Ho Yi
Jack McCluskey
Janek Bevendorff
Jarek Potiuk
Ke Wu
KevinGG
Kyle Hersey
Kyle Weaver
Luís Bianchin
Luke Cwik
Masato Nakamura
Matthias Baetens
Mehdi Drissi
Melissa Pashniak
Michel Davit
Miguel Hernandez
MiguelAnzoWizeline
Milena Bukal
Moritz Mack
Mostafa Aghajani
Nathan J Mehl
Niel Markwick
Ning Kang
Pablo Estrada
Pavel Avilov
Quentin Sommer
Reuben van Ammers
Reuven Lax
Ritesh Ghorse
Robert Bradshaw
Robert Burke
Ryan Thompson
Sam Whittle
Sayat
Sergei Lebedev
Sergey Kalinin
Steve Niemitz
Talat Uyarer
Thiago Nunes
Tianyang Hu
Tim Robertson
Valentyn Tymofieiev
Vitaly Ivanov
Yichi Zhang
Yiru Tang
Yu Feng
Yu ISHIKAWA
Zachary Houfek
blais
daria-malkova
daria.malkova
darshan-sj
dpcollins-google
emily
ewianda
johnjcasey
kileys
lam206
laraschmidt
mosche
msbukal@google.com
tvalentyn
