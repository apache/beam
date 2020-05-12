---
title:  "Apache Beam 2.12.0"
date:   2019-04-25 00:00:01 -0800
categories:
  - blog
aliases:
  - /blog/2019/04/25/beam-2.12.0.html
authors:
        - apilloud

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

We are happy to present the new 2.12.0 release of Beam. This release includes both improvements and new functionality.
See the [download page](/get-started/downloads/#2120-2019-04-25) for this release.<!--more-->
For more information on changes in 2.12.0, check out the
[detailed release notes](https://jira.apache.org/jira/secure/ReleaseNote.jspa?projectId=12319527&version=12344944).

## Highlights

### I/Os

* Add support for a BigQuery custom sink for Python SDK.
* Add support to specify a query in CassandraIO for Java SDK.
* Add experimental support for cross-language transforms,
  please see [BEAM-6730](https://issues.apache.org/jira/browse/BEAM-6730)
* Add support in the Flink Runner for exactly-once Writes with KafkaIO

### New Features / Improvements

* Enable Bundle Finalization in Python SDK for portable runners.
* Add support to the Java SDK harness to merge windows.
* Add Kafka Sink EOS support on Flink runner.
* Added a dead letter queue to Python streaming BigQuery sink.
* Add Experimental Python 3.6 and 3.7 workloads enabled.
  Beam 2.12 supports starting Dataflow pipelines under Python 3.6, 3.7, however 3.5 remains the only recommended minor version for Dataflow runner. In addition to announced 2.11 limitations, Beam typehint annotations are currently not supported on Python >= 3.6.


### Bugfixes

* Various bug fixes and performance improvements.

## List of Contributors

According to git shortlog, the following people contributed
to the 2.12.0 release. Thank you to all contributors!

Ahmed El.Hussaini, Ahmet Altay, Alan Myrvold, Alex Amato, Alexander Savchenko,
Alexey Romanenko, Andrew Brampton, Andrew Pilloud, Ankit Jhalaria,
Ankur Goenka, Anton Kedin, Boyuan Zhang, Brian Hulette, Chamikara Jayalath,
Charles Chen, Colm O hEigeartaigh, Craig Chambers, Dan Duong, Daniel Mescheder,
Daniel Oliveira, David Moravek, David Rieber, David Yan, Eric Roshan-Eisner,
Etienne Chauchot, Gleb Kanterov, Heejong Lee, Ho Tien Vu, Ismaël Mejía,
Jan Lukavský, Jean-Baptiste Onofré, Jeff Klukas, Juta, Kasia Kucharczyk,
Kengo Seki, Kenneth Jung, Kenneth Knowles, kevin, Kyle Weaver, Kyle Winkelman,
Łukasz Gajowy, Mark Liu, Mathieu Blanchard, Max Charas, Maximilian Michels,
Melissa Pashniak, Michael Luckey, Michal Walenia, Mike Kaplinskiy,
Mikhail Gryzykhin, Niel Markwick, Pablo Estrada, Radoslaw Stankiewicz,
Reuven Lax, Robbe Sneyders, Robert Bradshaw, Robert Burke, Rui Wang,
Ruoyun Huang, Ryan Williams, Slava Chernyak, Shahar Frank, Sunil Pedapudi,
Thomas Weise, Tim Robertson, Tanay Tummalapalli, Udi Meiri,
Valentyn Tymofieiev, Xinyu Liu, Yifan Zou, Yueyang Qiu
