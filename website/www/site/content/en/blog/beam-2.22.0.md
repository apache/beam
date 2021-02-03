---
title:  "Apache Beam 2.22.0"
date:   2020-06-08 00:00:01 -0800
categories:
  - blog
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
We are happy to present the new 2.22.0 release of Beam. This release includes both improvements and new functionality.
See the [download page](/get-started/downloads/#2220-2020-06-08) for this release.
For more information on changes in 2.22.0, check out the
[detailed release notes](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12319527&version=12347144).

## I/Os

* Basic Kafka read/write support for DataflowRunner (Python) ([BEAM-8019](https://issues.apache.org/jira/browse/BEAM-8019)).
* Sources and sinks for Google Healthcare APIs (Java)([BEAM-9468](https://issues.apache.org/jira/browse/BEAM-9468)).

## New Features / Improvements

* `--workerCacheMB` flag is supported in Dataflow streaming pipeline ([BEAM-9964](https://issues.apache.org/jira/browse/BEAM-9964))
* `--direct_num_workers=0` is supported for FnApi runner. It will set the number of threads/subprocesses to number of cores of the machine executing the pipeline ([BEAM-9443](https://issues.apache.org/jira/browse/BEAM-9443)).
* Python SDK now has experimental support for SqlTransform ([BEAM-8603](https://issues.apache.org/jira/browse/BEAM-8603)).
* Add OnWindowExpiration method to Stateful DoFn ([BEAM-1589](https://issues.apache.org/jira/browse/BEAM-1589)).
* Added PTransforms for Google Cloud DLP (Data Loss Prevention) services integration ([BEAM-9723](https://issues.apache.org/jira/browse/BEAM-9723)):
    * Inspection of data,
    * Deidentification of data,
    * Reidentification of data.
* Add a more complete I/O support matrix in the documentation site ([BEAM-9916](https://issues.apache.org/jira/browse/BEAM-9916)).
* Upgrade Sphinx to 3.0.3 for building PyDoc.
* Added a PTransform for image annotation using Google Cloud AI image processing service
([BEAM-9646](https://issues.apache.org/jira/browse/BEAM-9646))

## Breaking Changes

* The Python SDK now requires `--job_endpoint` to be set when using `--runner=PortableRunner` ([BEAM-9860](https://issues.apache.org/jira/browse/BEAM-9860)). Users seeking the old default behavior should set `--runner=FlinkRunner` instead.

## List of Contributors

According to git shortlog, the following people contributed to the 2.22.0 release. Thank you to all contributors!

Ahmet Altay, aiyangar, Ajo Thomas, Akshay-Iyangar, Alan Pryor, Alexey Romanenko, Allen Pradeep Xavier, amaliujia, Andrew Pilloud, Ankur Goenka, Ashwin Ramaswami, bntnam, Borzoo Esmailloo, Boyuan Zhang, Brian Hulette, Chamikara Jayalath, Colm O hEigeartaigh, Craig Chambers, Damon Douglas, Daniel Oliveira, David Cavazos, David Moravek, Esun Kim, Etienne Chauchot, Filipe Regadas, Graeme Morgan, Hannah Jiang, Hannah-Jiang, Harch Vardhan, Heejong Lee, Henry Suryawirawan, Ismaël Mejía, Israel Herraiz, Jacob Ferriero, Jan Lukavský, John Mora, Kamil Wasilewski, Kenneth Jung, Kenneth Knowles, kevingg, Kyle Weaver, Kyoungha Min, Kyungwon Jo, Luke Cwik, Mark Liu, Matthias Baetens, Maximilian Michels, Michal Walenia, Mikhail Gryzykhin, Nam Bui, Niel Markwick, Ning Kang, Omar Ismail, omarismail94, Pablo Estrada, paul fisher, pawelpasterz, Pawel Pasterz, Piotr Szuberski, Rahul Patwari, rarokni, Rehman, Rehman Murad Ali, Reuven Lax, Robert Bradshaw, Robert Burke, Rui Wang, Ruoyun Huang, Sam Rohde, Sam Whittle, Sebastian Graca, Shoaib Zafar, Sruthi Sree Kumar, Stephen O'Kennedy, Steve Koonce, Steve Niemitz, Steven van Rossum, Tesio, Thomas Weise, tobiaslieber-cognitedata, Tomo Suzuki, Tudor Marian, tvalentyn, Tyson Hamilton, Udi Meiri, Valentyn Tymofieiev, Vasu Nori, xuelianhan, Yichi Zhang, Yifan Zou, yoshiki.obata, Yueyang Qiu, Zhuo Peng
