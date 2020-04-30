---
title:  "Apache Beam 2.16.0"
date:   2019-10-07 00:00:01 -0800
categories:
  - blog
aliases:
  - /blog/2019/10/07/beam-2.16.0.html
authors:
  - markliu

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

We are happy to present the new 2.16.0 release of Beam. This release includes both improvements and new functionality.
See the [download page](/get-started/downloads/#2160-2019-10-07) for this release.<!--more-->
For more information on changes in 2.16.0, check out the
[detailed release notes](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12319527&version=12345494).

## Highlights

 * Customizable Docker container images released and supported by Beam portable runners on Python 2.7, 3.5, 3.6, 3.7. ([BEAM-7907](https://issues.apache.org/jira/browse/BEAM-7907))
 * Integration improvements for Python Streaming on Dataflow including service features like autoscaling, drain, update, streaming engine and counter updates.


### New Features / Improvements

* A new count distinct transform based on BigQuery compatible HyperLogLog++ implementation. ([BEAM-7013](https://issues.apache.org/jira/browse/BEAM-7013))
* Element counters in the Web UI graph representations for transforms for Python streaming jobs in Google Cloud Dataflow. ([BEAM-7045](https://issues.apache.org/jira/browse/BEAM-7045))
* Add SetState in Python sdk. ([BEAM-7741](https://issues.apache.org/jira/browse/BEAM-7741))
* Add hot key detection to Dataflow Runner. ([BEAM-7820](https://issues.apache.org/jira/browse/BEAM-7820))
* Add ability to get the list of submitted jobs from gRPC JobService. ([BEAM-7927](https://issues.apache.org/jira/browse/BEAM-7927))
* Portable Flink pipelines can now be bundled into executable jars. ([BEAM-7966](https://issues.apache.org/jira/browse/BEAM-7966), [BEAM-7967](https://issues.apache.org/jira/browse/BEAM-7967))
* SQL join selection should be done in planner, not in expansion to PTransform. ([BEAM-6114](https://issues.apache.org/jira/browse/BEAM-6114))
* A Python Sink for BigQuery with File Loads in Streaming. ([BEAM-6611](https://issues.apache.org/jira/browse/BEAM-6611))
* Python BigQuery sink should be able to handle 15TB load job quota. ([BEAM-7588](https://issues.apache.org/jira/browse/BEAM-7588))
* Spark portable runner: reuse SDK harness. ([BEAM-7600](https://issues.apache.org/jira/browse/BEAM-7600))
* BigQuery File Loads to work well with load job size limits. ([BEAM-7742](https://issues.apache.org/jira/browse/BEAM-7742))
* External environment with containerized worker pool. ([BEAM-7980](https://issues.apache.org/jira/browse/BEAM-7980))
* Use OffsetRange as restriction for OffsetRestrictionTracker. ([BEAM-8014](https://issues.apache.org/jira/browse/BEAM-8014))
* Get logs for SDK worker Docker containers. ([BEAM-8015](https://issues.apache.org/jira/browse/BEAM-8015))
* PCollection boundedness is tracked and propagated in python sdk. ([BEAM-8088](https://issues.apache.org/jira/browse/BEAM-8088))


### Dependency Changes

* Upgrade "com.amazonaws:amazon-kinesis-producer" to version 0.13.1. ([BEAM-7894](https://issues.apache.org/jira/browse/BEAM-7894))
* Upgrade to joda time 2.10.3 to get updated TZDB. ([BEAM-8161](https://issues.apache.org/jira/browse/BEAM-8161))
* Upgrade Jackson to version 2.9.10. ([BEAM-8299](https://issues.apache.org/jira/browse/BEAM-8299))
* Upgrade grpcio minimum required version to 1.12.1. ([BEAM-7986](https://issues.apache.org/jira/browse/BEAM-7986))
* Upgrade funcsigs minimum required version to 1.0.2 in Python2. ([BEAM-7060](https://issues.apache.org/jira/browse/BEAM-7060))
* Upgrade google-cloud-pubsub maximum required version to 1.0.0. ([BEAM-5539](https://issues.apache.org/jira/browse/BEAM-5539))
* Upgrade google-cloud-bigtable maximum required version to 1.0.0. ([BEAM-5539](https://issues.apache.org/jira/browse/BEAM-5539))
* Upgrade dill version to 0.3.0. ([BEAM-8324](https://issues.apache.org/jira/browse/BEAM-8324))


### Bugfixes

* Various bug fixes and performance improvements.


### Known Issues

* Given that Python 2 will reach EOL on Jan 1 2020, Python 2 users of Beam will now receive a warning that new releases of Apache Beam will soon support Python 3 only.
* Filesystems not properly registered using FileIO.write in FlinkRunner. ([BEAM-8303](https://issues.apache.org/jira/browse/BEAM-8303))
* Performance regression in Java DirectRunner in streaming mode. ([BEAM-8363](https://issues.apache.org/jira/browse/BEAM-8363))
* Can't install the Python SDK on macOS 10.15. ([BEAM-8368](https://issues.apache.org/jira/browse/BEAM-8368))


## List of Contributors

 According to git shortlog, the following people contributed to the 2.16.0 release. Thank you to all contributors!

Ahmet Altay, Alex Van Boxel, Alexey Romanenko, Alexey Strokach, Alireza Samadian,
Andre-Philippe Paquet, Andrew Pilloud, Ankur Goenka, Anton Kedin, Aryan Naraghi,
B M VISHWAS, Bartok Jozsef, Bill Neubauer, Boyuan Zhang, Brian Hulette, Bruno Volpato,
Chad Dombrova, Chamikara Jayalath, Charith Ellawala, Charles Chen, Claire McGinty,
Cyrus Maden, Daniel Oliveira, Dante, David Cavazos, David Moravek, David Yan,
Dominic Mitchell, Elias Djurfeldt, Enrico Canzonieri, Etienne Chauchot, Gleb Kanterov,
Hai Lu, Hannah Jiang, Heejong Lee, Ian Lance Taylor, Ismaël Mejía, Jack Whelpton,
James Wen, Jan Lukavský, Jean-Baptiste Onofré, Jofre, Kai Jiang, Kamil Wasilewski,
Kasia Kucharczyk, Kenneth Jung, Kenneth Knowles, Kirill Kozlov, Kohki YAMAGIWA,
Kyle Weaver, Kyle Winkelman, Ludovic Post, Luis Enrique Ortíz Ramirez, Luke Cwik,
Mark Liu, Maximilian Michels, Michal Walenia, Mike Kaplinskiy, Mikhail Gryzykhin,
NING KANG, Oliver Henlich, Pablo Estrada, Rakesh Kumar, Renat Nasyrov, Reuven Lax,
Robert Bradshaw, Robert Burke, Rui Wang, Ruoyun Huang, Ryan Skraba, Sahith Nallapareddy,
Salman Raza, Sam Rohde, Saul Chavez, Shoaib, Shoaib Zafar, Slava Chernyak, Tanay Tummalapalli,
Thinh Ha, Thomas Weise, Tianzi Cai, Tim van der Lippe, Tomer Zeltzer, Tudor Marian,
Udi Meiri, Valentyn Tymofieiev, Yichi Zhang, Yifan Zou, Yueyang Qiu, gxercavins,
jesusrv1103, lostluck, matt-darwin, mrociorg, ostrokach, parahul, rahul8383, rosetn,
sunjincheng121, the1plummie, ttanay, tvalentyn, venn001, yoshiki.obata, Łukasz Gajowy
