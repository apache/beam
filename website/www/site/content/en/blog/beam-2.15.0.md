---
title:  "Apache Beam 2.15.0"
date:   2019-08-22 00:00:01 -0800
categories:
  - blog
aliases:
  - /blog/2019/08/22/beam-2.15.0.html
authors:
        - yifanzou

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

We are happy to present the new 2.15.0 release of Beam. This release includes both improvements and new functionality.
See the [download page](/get-started/downloads/#2150-2019-08-22) for this release.<!--more-->
For more information on changes in 2.15.0, check out the
[detailed release notes](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12319527&version=12345489).

## Highlights

 * Vendored Guava was upgraded to version 26.0.
 * Support multi-process execution on the FnApiRunner for Python. ([BEAM-3645](https://issues.apache.org/jira/browse/BEAM-3645))


### I/Os

* Add AvroIO.sink for IndexedRecord (FileIO compatible). ([BEAM-6480](https://issues.apache.org/jira/browse/BEAM-6480))
* Add support for writing to BigQuery clustered tables. ([BEAM-5191](https://issues.apache.org/jira/browse/BEAM-5191))

### New Features / Improvements

* Support ParquetTable in SQL. ([BEAM-7728](https://issues.apache.org/jira/browse/BEAM-7728))
* Add hot key detection to Dataflow Runner. ([BEAM-7820](https://issues.apache.org/jira/browse/BEAM-7820))
* Support schemas in the JDBC sink. ([BEAM-6675](https://issues.apache.org/jira/browse/BEAM-6675))
* Report GCS throttling time to Dataflow autoscaler for better autoscaling. ([BEAM-7667](https://issues.apache.org/jira/browse/BEAM-7667))
* Support transform_name_mapping option in Python SDK for `--update` use. ([BEAM-7761](https://issues.apache.org/jira/browse/BEAM-7761))
* Dependency: Upgrade Jackson databind to version 2.9.9.3 ([BEAM-7880](https://issues.apache.org/jira/browse/BEAM-7880))

### Bugfixes

* Various bug fixes and performance improvements.


### Known Issues

* [BEAM-7616](https://issues.apache.org/jira/browse/BEAM-7616) urlopen calls may get stuck. (Regression from 2.14.0)
* [BEAM-8111](https://issues.apache.org/jira/browse/BEAM-8111) SchemaCoder fails on Dataflow, preventing the use of SqlTransform and schema-aware transforms. (Regression from 2.14.0)
* ([BEAM-8368](https://issues.apache.org/jira/browse/BEAM-8368)) Can't install the Python SDK on macOS 10.15.


### Breaking Changes
* `--region` flag will be a required flag in the future for Dataflow. A warning is added to warn for this future change. ([BEAM-7833](https://issues.apache.org/jira/browse/BEAM-7833))



## List of Contributors

 According to git shortlog, the following people contributed to the 2.15.0 release. Thank you to all contributors!

 Ahmet Altay, Alexey Romanenko, Alex Goos, Alireza Samadian, Andrew Pilloud, Ankur Goenka,
Anton Kedin, Aryan Naraghi, Bartok Jozsef, bmv126, B M VISHWAS, Boyuan Zhang,
Brian Hulette, brucearctor, Cade Markegard, Cam Mach, Chad Dombrova,
Chaim Turkel, Chamikara Jayalath, Charith Ellawala, Claire McGinty, Craig Chambers,
Daniel Oliveira, David Cavazos, David Moravek, Dominic Mitchell, Dustin Rhodes,
Etienne Chauchot, Filipe Regadas, Gleb Kanterov, Gunnar Schulze, Hannah Jiang,
Heejong Lee, Henry Suryawirawan, Ismaël Mejía, Ivo Galic, Jan Lukavský,
Jawad, Juta, Juta Staes, Kai Jiang, Kamil Wasilewski, Kasia Kucharczyk,
Kenneth Jung, Kenneth Knowles, Kyle Weaver, Lily Li, Logan HAUSPIE, lostluck,
Łukasz Gajowy, Luke Cwik, Mark Liu, Matt Helm, Maximilian Michels,
Michael Luckey, Mikhail Gryzykhin, Neville Li, Nicholas Rucci, pabloem,
Pablo Estrada, Paul King, Paul Suganthan, Raheel Khan, Rakesh Kumar,
Reza Rokni, Robert Bradshaw, Robert Burke, rosetn, Rui Wang, Ryan Skraba, RyanSkraba,
Sahith Nallapareddy, Sam Rohde, Sam Whittle, Steve Niemitz, Tanay Tummalapalli, Thomas Weise,
Tianyang Hu, ttanay, tvalentyn, Udi Meiri, Valentyn Tymofieiev, Wout Scheepers,
yanzhi, Yekut, Yichi Zhang, Yifan Zou, yoshiki.obata, Yueyang Qiu, Yunqing Zhou
