---
title:  "Apache Beam 2.30.0"
date:   2021-06-09 9:00:00 -0700
categories:
  - blog
  - release
authors:
  - heejong
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

We are happy to present the new 2.30.0 release of Beam.
This release includes both improvements and new functionality.
See the [download page](/get-started/downloads/#2300-2021-06-09) for this release.

<!--more-->

For more information on changes in 2.30.0, check out the [detailed release notes](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12319527&version=12349978).

## Highlights

* Legacy Read transform (non-SDF based Read) is used by default for non-FnAPI opensource runners. Use `use_sdf_read` experimental flag to re-enable SDF based Read transforms ([BEAM-10670](https://issues.apache.org/jira/browse/BEAM-10670))
* Upgraded vendored gRPC dependency to 1.36.0 ([BEAM-11227](https://issues.apache.org/jira/browse/BEAM-11227))


### I/Os

* Fixed the issue that WriteToBigQuery with batch file loads does not respect schema update options when there are multiple load jobs ([BEAM-11277](https://issues.apache.org/jira/browse/BEAM-11277))
* Fixed the issue that the job didn't properly retry since BigQuery sink swallows HttpErrors when performing streaming inserts ([BEAM-12362](https://issues.apache.org/jira/browse/BEAM-12362))


### New Features / Improvements

* Added capability to declare resource hints in Java and Python SDKs ([BEAM-2085](https://issues.apache.org/jira/browse/BEAM-2085))
* Added Spanner IO Performance tests for read and write in Python SDK ([BEAM-10029](https://issues.apache.org/jira/browse/BEAM-10029))
* Added support for accessing GCP PubSub Message ordering keys, message IDs and message publish timestamp in Python SDK ([BEAM-7819](https://issues.apache.org/jira/browse/BEAM-7819))
* DataFrame API: Added support for collecting DataFrame objects in interactive Beam ([BEAM-11855](https://issues.apache.org/jira/browse/BEAM-11855))
* DataFrame API: Added [apache_beam.examples.dataframe](https://github.com/apache/beam/tree/master/sdks/python/apache_beam/examples/dataframe) module ([BEAM-12024](https://issues.apache.org/jira/browse/BEAM-12024))
* Upgraded the GCP Libraries BOM version to 20.0.0 ([BEAM-11205](https://issues.apache.org/jira/browse/BEAM-11205)). For Google Cloud client library versions set by this BOM, see [this table](https://storage.googleapis.com/cloud-opensource-java-dashboard/com.google.cloud/libraries-bom/20.0.0/artifact_details.html)
* Added `sdkContainerImage` flag to (eventually) replace `workerHarnessContainerImage` ([BEAM-12212](https://issues.apache.org/jira/browse/BEAM-12212))
* Added support for Dataflow update when schemas are used ([BEAM-12198](https://issues.apache.org/jira/browse/BEAM-12198))
* Fixed the issue that `ZipFiles.zipDirectory` leaks native JVM memory ([BEAM-12220](https://issues.apache.org/jira/browse/BEAM-12220))
* Fixed the issue that `Reshuffle.withNumBuckets` creates `(N*2)-1` buckets ([BEAM-12361](https://issues.apache.org/jira/browse/BEAM-12361))

### Breaking Changes

* Drop support for Flink 1.8 and 1.9 ([BEAM-11948](https://issues.apache.org/jira/browse/BEAM-11948))
* MongoDbIO: Read.withFilter() and Read.withProjection() are removed since they are deprecated since Beam 2.12.0 ([BEAM-12217](https://issues.apache.org/jira/browse/BEAM-12217))
* RedisIO.readAll() was removed since it was deprecated since Beam 2.13.0. Please use RedisIO.readKeyPatterns() for the equivalent functionality ([BEAM-12214](https://issues.apache.org/jira/browse/BEAM-12214))
* MqttIO.create() with clientId constructor removed because it was deprecated since Beam 2.13.0 ([BEAM-12216](https://issues.apache.org/jira/browse/BEAM-12216))

### Known Issues

* See a full list of open [issues that affect](https://issues.apache.org/jira/issues/?jql=project%20%3D%20BEAM%20AND%20affectedVersion%20%3D%202.30.0%20ORDER%20BY%20priority%20DESC%2C%20updated%20DESC) this version.

## List of Contributors

According to `git shortlog`, the following people contributed to the 2.30.0 release. Thank you to all contributors!

Ahmet Altay, Alex Amato, Alexey Romanenko, Anant Damle, Andreas Bergmeier, Andrew Pilloud, Ankur Goenka,
Anup D, Artur Khanin, Benjamin Gonzalez, Bipin Upadhyaya, Boyuan Zhang, Brian Hulette, Bulat Shakirzyanov,
Chamikara Jayalath, Chun Yang, Daniel Kulp, Daniel Oliveira, David Cavazos, Elliotte Rusty Harold, Emily Ye,
Eric Roshan-Eisner, Evan Galpin, Fabien Caylus, Fernando Morales, Heejong Lee, Iñigo San Jose Visiers,
Isidro Martínez, Ismaël Mejía, Ke Wu, Kenneth Knowles, KevinGG, Kyle Weaver, Ludovic Post, MATTHEW Ouyang (LCL),
Mackenzie Clark, Masato Nakamura, Matthias Baetens, Max, Nicholas Azar, Ning Kang, Pablo Estrada, Patrick McCaffrey,
Quentin Sommer, Reuven Lax, Robert Bradshaw, Robert Burke, Rui Wang, Sam Rohde, Sam Whittle, Shoaib Zafar,
Siyuan Chen, Sruthi Sree Kumar, Steve Niemitz, Sylvain Veyrié, Tomo Suzuki, Udi Meiri, Valentyn Tymofieiev,
Vitaly Terentyev, Wenbing, Xinyu Liu, Yichi Zhang, Yifan Mai, Yueyang Qiu, Yunqing Zhou, ajo thomas, brucearctor,
dmkozh, dpcollins-google, emily, jordan-moore, kileys, lostluck, masahitojp, roger-mike, sychen, tvalentyn,
vachan-shetty, yoshiki.obata

