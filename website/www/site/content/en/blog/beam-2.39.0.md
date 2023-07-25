---
title:  "Apache Beam 2.39.0"
date:   2022-05-25 9:00:00 -0700
categories:
  - blog
  - release
authors:
  - yichi
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

We are happy to present the new 2.39.0 release of Beam.
This release includes both improvements and new functionality.
See the [download page](/get-started/downloads/#2390-2022-05-25) for this
release.

<!--more-->

For more information on changes in 2.39.0 check out the [detailed release notes](https://issues.apache.org/jira/secure/ConfigureReleaseNote.jspa?projectId=12319527&version=12351170).

## I/Os

* JmsIO gains the ability to map any kind of input to any subclass of `javax.jms.Message` (Java) ([BEAM-16308](https://issues.apache.org/jira/browse/BEAM-16308)).
* JmsIO introduces the ability to write to dynamic topics (Java) ([BEAM-16308](https://issues.apache.org/jira/browse/BEAM-16308)).
  * A `topicNameMapper` must be set to extract the topic name from the input value.
  * A `valueMapper` must be set to convert the input value to JMS message.
* Reduce number of threads spawned by BigqueryIO StreamingInserts (
  [BEAM-14283](https://issues.apache.org/jira/browse/BEAM-14283)).
* Implemented Apache PulsarIO ([BEAM-8218](https://issues.apache.org/jira/browse/BEAM-8218)).


## New Features / Improvements

* Support for flink scala 2.12, because most of the libraries support version 2.12 onwards. ([beam-14386](https://issues.apache.org/jira/browse/BEAM-14386))
* 'Manage Clusters' JupyterLab extension added for users to configure usage of Dataproc clusters managed by Interactive Beam (Python) ([BEAM-14130](https://issues.apache.org/jira/browse/BEAM-14130)).
* Pipeline drain support added for Go SDK ([BEAM-11106](https://issues.apache.org/jira/browse/BEAM-11106)). **Note: this feature is not yet fully validated and should be treated as experimental in this release.**
* `DataFrame.unstack()`, `DataFrame.pivot() ` and  `Series.unstack()`
  implemented for DataFrame API ([BEAM-13948](https://issues.apache.org/jira/browse/BEAM-13948), [BEAM-13966](https://issues.apache.org/jira/browse/BEAM-13966)).
* Support for impersonation credentials added to dataflow runner in the Java and Python SDK ([BEAM-14014](https://issues.apache.org/jira/browse/BEAM-14014)).
* Implemented Jupyterlab extension for managing Dataproc clusters ([BEAM-14130](https://issues.apache.org/jira/browse/BEAM-14130)).
* ExternalPythonTransform API added for easily invoking Python transforms from
  Java ([BEAM-14143](https://issues.apache.org/jira/browse/BEAM-14143)).
* Added Add support for Elasticsearch 8.x ([BEAM-14003](https://issues.apache.org/jira/browse/BEAM-14003)).
* Shard aware Kinesis record aggregation (AWS Sdk v2), ([BEAM-14104](https://issues.apache.org/jira/browse/BEAM-14104)).
* Upgrade to ZetaSQL 2022.04.1 ([BEAM-14348](https://issues.apache.org/jira/browse/BEAM-14348)).
* Fixed ReadFromBigQuery cannot be used with the interactive runner ([BEAM-14112](https://issues.apache.org/jira/browse/BEAM-14112)).


## Breaking Changes

* Unused functions `ShallowCloneParDoPayload()`, `ShallowCloneSideInput()`, and `ShallowCloneFunctionSpec()` have been removed from the Go SDK's pipelinex package ([BEAM-13739](https://issues.apache.org/jira/browse/BEAM-13739)).
* JmsIO requires an explicit `valueMapper` to be set ([BEAM-16308](https://issues.apache.org/jira/browse/BEAM-16308)). You can use the `TextMessageMapper` to convert `String` inputs to JMS `TestMessage`s:
```java
  JmsIO.<String>write()
        .withConnectionFactory(jmsConnectionFactory)
        .withValueMapper(new TextMessageMapper());
```
* Coders in Python are expected to inherit from Coder. ([BEAM-14351](https://issues.apache.org/jira/browse/BEAM-14351)).
* New abstract method `metadata()` added to io.filesystem.FileSystem in the
  Python SDK. ([BEAM-14314](https://issues.apache.org/jira/browse/BEAM-14314))

## Deprecations

* Flink 1.11 is no longer supported ([BEAM-14139](https://issues.apache.org/jira/browse/BEAM-14139)).
* Python 3.6 is no longer supported ([BEAM-13657](https://issues.apache.org/jira/browse/BEAM-13657)).

## Bugfixes

* Fixed Java Spanner IO NPE when ProjectID not specified in template executions (Java) ([BEAM-14405](https://issues.apache.org/jira/browse/BEAM-14405)).
* Fixed potential NPE in BigQueryServicesImpl.getErrorInfo (Java) ([BEAM-14133](https://issues.apache.org/jira/browse/BEAM-14133)).


## Known Issues
* See a full list of open [issues that affect](https://issues.apache.org/jira/browse/BEAM-14412?jql=project%20%3D%20BEAM%20AND%20affectedVersion%20%3D%202.39.0%20ORDER%20BY%20priority%20DESC%2C%20updated%20DESC) this version.

## List of Contributors

According to git shortlog, the following people contributed to the 2.39.0 release. Thank you to all contributors!

Ahmed Abualsaud,
Ahmet Altay,
Aizhamal Nurmamat kyzy,
Alexander Zhuravlev,
Alexey Romanenko,
Anand Inguva,
Andrei Gurau,
Andrew Pilloud,
Andy Ye,
Arun Pandian,
Arwin Tio,
Aydar Farrakhov,
Aydar Zainutdinov,
AydarZaynutdinov,
Balázs Németh,
Benjamin Gonzalez,
Brian Hulette,
Buqian Zheng,
Chamikara Jayalath,
Chun Yang,
Daniel Oliveira,
Daniela Martín,
Danny McCormick,
David Huntsperger,
Deepak Nagaraj,
Denise Case,
Esun Kim,
Etienne Chauchot,
Evan Galpin,
Hector Miuler Malpica Gallegos,
Heejong Lee,
Hengfeng Li,
Ilango Rajagopal,
Ilion Beyst,
Israel Herraiz,
Jack McCluskey,
Kamil Bregula,
Kamil Breguła,
Ke Wu,
Kenneth Knowles,
KevinGG,
Kiley,
Kiley Sok,
Kyle Weaver,
Liam Miller-Cushon,
Luke Cwik,
Marco Robles,
Matt Casters,
Michael Li,
MiguelAnzoWizeline,
Milan Patel,
Minbo Bae,
Moritz Mack,
Nick Caballero,
Niel Markwick,
Ning Kang,
Oskar Firlej,
Pablo Estrada,
Pavel Avilov,
Reuven Lax,
Reza Rokni,
Ritesh Ghorse,
Robert Bradshaw,
Robert Burke,
Ryan Thompson,
Sam Whittle,
Steven Niemitz,
Thiago Nunes,
Tomo Suzuki,
Valentyn Tymofieiev,
Victor,
Yi Hu,
Yichi Zhang,
Yiru Tang,
ahmedabu98,
andoni-guzman,
brachipa,
bulat safiullin,
bullet03,
dannymartinm,
daria.malkova,
dpcollins-google,
egalpin,
emily,
fbeevikm,
johnjcasey,
kileys,
msbukal@google.com,
nguyennk92,
pablo rodriguez defino,
rszper,
rvballada,
sachinag,
tvalentyn,
vachan-shetty,
yirutang
