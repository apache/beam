---
title:  "Apache Beam 2.27.0"
date:   2021-01-07 12:00:00 -0800
categories:
  - blog
  - release
authors:
  - pabloem
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
We are happy to present the new 2.27.0 release of Apache Beam. This release includes both improvements and new functionality.
See the [download page](/get-started/downloads/#2270-2020-12-22) for this release.
For more information on changes in 2.27.0, check out the
[detailed release notes](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12319527&version=12349380).

## Highlights

* Java 11 Containers are now published with all Beam releases.
* There is a new transform `ReadAllFromBigQuery` that can receive multiple requests to read data from BigQuery at pipeline runtime. See [PR 13170](https://github.com/apache/beam/pull/13170), and [BEAM-9650](https://issues.apache.org/jira/browse/BEAM-9650).


## I/Os
* ReadFromMongoDB can now be used with MongoDB Atlas (Python) ([BEAM-11266](https://issues.apache.org/jira/browse/BEAM-11266).)
* ReadFromMongoDB/WriteToMongoDB will mask password in display_data (Python) ([BEAM-11444](https://issues.apache.org/jira/browse/BEAM-11444).)
* There is a new transform `ReadAllFromBigQuery` that can receive multiple requests to read data from BigQuery at pipeline runtime. See [PR 13170](https://github.com/apache/beam/pull/13170), and [BEAM-9650](https://issues.apache.org/jira/browse/BEAM-9650).

## New Features / Improvements

* Beam modules that depend on Hadoop are now tested for compatibility with Hadoop 3 ([BEAM-8569](https://issues.apache.org/jira/browse/BEAM-8569)). (Hive/HCatalog pending)
* Publishing Java 11 SDK container images now supported as part of Apache Beam release process. ([BEAM-8106](https://issues.apache.org/jira/browse/BEAM-8106))
* Added Cloud Bigtable Provider extension to Beam SQL ([BEAM-11173](https://issues.apache.org/jira/browse/BEAM-11173), [BEAM-11373](https://issues.apache.org/jira/browse/BEAM-11373))
* Added a schema provider for thrift data ([BEAM-11338](https://issues.apache.org/jira/browse/BEAM-11338))
* Added combiner packing pipeline optimization to Dataflow runner. ([BEAM-10641](https://issues.apache.org/jira/browse/BEAM-10641))
* Added an example to ingest data from Apache Kafka to Google Pub/Sub. ([BEAM-11065](https://issues.apache.org/jira/browse/BEAM-11065))

## Breaking Changes

* HBaseIO hbase-shaded-client dependency should be now provided by the users ([BEAM-9278](https://issues.apache.org/jira/browse/BEAM-9278)).
* `--region` flag in amazon-web-services2 was replaced by `--awsRegion` ([BEAM-11331](https://issues.apache.org/jira/projects/BEAM/issues/BEAM-11331)).


## List of Contributors

According to git shortlog, the following people contributed to the 2.27.0 release. Thank you to all contributors!

Ahmet Altay, Alan Myrvold, Alex Amato, Alexey Romanenko, Aliraza Nagamia, Allen Pradeep Xavier,
Andrew Pilloud, andreyKaparulin, Ashwin Ramaswami, Boyuan Zhang, Brent Worden, Brian Hulette,
Carlos Marin, Chamikara Jayalath, Costi Ciudatu, Damon Douglas, Daniel Collins,
Daniel Oliveira, David Huntsperger, David Lu, David Moravek, David Wrede,
dennis, Dennis Yung, dpcollins-google, Emily Ye, emkornfield,
Esun Kim, Etienne Chauchot, Eugene Nikolaiev, Frank Zhao, Haizhou Zhao,
Hector Acosta, Heejong Lee, Ilya, Iñigo San Jose Visiers, InigoSJ,
Ismaël Mejía, janeliulwq, Jan Lukavský, Kamil Wasilewski, Kenneth Jung,
Kenneth Knowles, Ke Wu, kileys, Kyle Weaver, lostluck,
Matt Casters, Maximilian Michels, Michal Walenia, Mike Dewar, nehsyc,
Nelson Osacky, Niels Basjes, Ning Kang, Pablo Estrada, palmere-google,
Pawel Pasterz, Piotr Szuberski, purbanow, Reuven Lax, rHermes,
Robert Bradshaw, Robert Burke, Rui Wang, Sam Rohde, Sam Whittle,
Siyuan Chen, Tim Robertson, Tobiasz Kędzierski, tszerszen,
Valentyn Tymofieiev, Tyson Hamilton, Udi Meiri, vachan-shetty, Xinyu Liu,
Yichi Zhang, Yifan Mai, yoshiki.obata, Yueyang Qiu
