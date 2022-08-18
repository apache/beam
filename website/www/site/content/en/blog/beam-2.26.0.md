---
title:  "Apache Beam 2.26.0"
date:   2020-12-11 12:00:00 -0800
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
We are happy to present the new 2.26.0 release of Apache Beam. This release includes both improvements and new functionality.
See the [download page](/get-started/downloads/#2260-2020-12-11) for this release.<!--more-->
For more information on changes in 2.26.0, check out the
[detailed release notes](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12319527&version=12348833).

## Highlights

* Splittable DoFn is now the default for executing the Read transform for Java based runners (Spark with bounded pipelines) in addition to existing runners from the 2.25.0 release (Direct, Flink, Jet, Samza, Twister2). The expected output of the Read transform is unchanged. Users can opt-out using `--experiments=use_deprecated_read`. The Apache Beam community is looking for feedback for this change as the community is planning to make this change permanent with no opt-out. If you run into an issue requiring the opt-out, please send an e-mail to [user@beam.apache.org](mailto:user@beam.apache.org) specifically referencing BEAM-10670 in the subject line and why you needed to opt-out. (Java) ([BEAM-10670](https://issues.apache.org/jira/browse/BEAM-10670))

## I/Os

* Java BigQuery streaming inserts now have timeouts enabled by default. Pass `--HTTPWriteTimeout=0` to revert to the old behavior. ([BEAM-6103](https://issues.apache.org/jira/browse/BEAM-6103))
* Added support for Contextual Text IO (Java), a version of text IO that provides metadata about the records ([BEAM-10124](https://issues.apache.org/jira/browse/BEAM-10124)). Support for this IO is currently experimental. Specifically, **there are no update-compatibility guarantees** for streaming jobs with this IO between current future verisons of Apache Beam SDK.

## New Features / Improvements
* Added support for avro payload format in Beam SQL Kafka Table ([BEAM-10885](https://issues.apache.org/jira/browse/BEAM-10885))
* Added support for json payload format in Beam SQL Kafka Table ([BEAM-10893](https://issues.apache.org/jira/browse/BEAM-10893))
* Added support for protobuf payload format in Beam SQL Kafka Table ([BEAM-10892](https://issues.apache.org/jira/browse/BEAM-10892))
* Added support for avro payload format in Beam SQL Pubsub Table ([BEAM-5504](https://issues.apache.org/jira/browse/BEAM-5504))
* Added option to disable unnecessary copying between operators in Flink Runner (Java) ([BEAM-11146](https://issues.apache.org/jira/browse/BEAM-11146))
* Added CombineFn.setup and CombineFn.teardown to Python SDK. These methods let you initialize the CombineFn's state before any of the other methods of the CombineFn is executed and clean that state up later on. If you are using Dataflow, you need to enable Dataflow Runner V2 by passing `--experiments=use_runner_v2` before using this feature. ([BEAM-3736](https://issues.apache.org/jira/browse/BEAM-3736))

## Breaking Changes

* BigQuery's DATETIME type now maps to Beam logical type org.apache.beam.sdk.schemas.logicaltypes.SqlTypes.DATETIME
* Pandas 1.x is now required for dataframe operations.

## List of Contributors

According to git shortlog, the following people contributed to the 2.26.0 release. Thank you to all contributors!

Abhishek Yadav, AbhiY98, Ahmet Altay, Alan Myrvold, Alex Amato, Alexey Romanenko,
Andrew Pilloud, Ankur Goenka, Boyuan Zhang, Brian Hulette, Chad Dombrova,
Chamikara Jayalath, Curtis "Fjord" Hawthorne, Damon Douglas, dandy10, Daniel Oliveira,
David Cavazos, dennis, Derrick Qin, dpcollins-google, Dylan Hercher, emily, Esun Kim,
Gleb Kanterov, Heejong Lee, Ismaël Mejía, Jan Lukavský, Jean-Baptiste Onofré, Jing,
Jozef Vilcek, Justin White, Kamil Wasilewski, Kenneth Knowles, kileys, Kyle Weaver,
lostluck, Luke Cwik, Mark, Maximilian Michels, Milan Cermak, Mohammad Hossein Sekhavat,
Nelson Osacky, Neville Li, Ning Kang, pabloem, Pablo Estrada, pawelpasterz,
Pawel Pasterz, Piotr Szuberski, PoojaChandak, purbanow, rarokni, Ravi Magham,
Reuben van Ammers, Reuven Lax, Reza Rokni, Robert Bradshaw, Robert Burke,
Romain Manni-Bucau, Rui Wang, rworley-monster, Sam Rohde, Sam Whittle, shollyman,
Simone Primarosa, Siyuan Chen, Steve Niemitz, Steven van Rossum, sychen, Teodor Spæren,
Tim Clemons, Tim Robertson, Tobiasz Kędzierski, tszerszen, Tudor Marian, tvalentyn,
Tyson Hamilton, Udi Meiri, Vasu Gupta, xasm83, Yichi Zhang, yichuan66, Yifan Mai,
yoshiki.obata, Yueyang Qiu, yukihira1992
