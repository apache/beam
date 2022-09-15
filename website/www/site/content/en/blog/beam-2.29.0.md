---
title:  "Apache Beam 2.29.0"
date:   2021-04-29 9:00:00 -0700
categories:
  - blog
  - release
authors:
  - klk
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

We are happy to present the new 2.29.0 release of Beam.
This release includes both improvements and new functionality.
See the [download page](/get-started/downloads/#2290-2021-04-15) for this release.

<!--more-->

For more information on changes in 2.29.0, check out the [detailed release notes](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12319527&version=12349629).

## Highlights

* Spark Classic and Portable runners officially support Spark 3 ([BEAM-7093](https://issues.apache.org/jira/browse/BEAM-7093)).
* Official Java 11 support for most runners (Dataflow, Flink, Spark) ([BEAM-2530](https://issues.apache.org/jira/browse/BEAM-2530)).
* DataFrame API now supports GroupBy.apply ([BEAM-11628](https://issues.apache.org/jira/browse/BEAM-11628)).

### I/Os

* Added support for S3 filesystem on AWS SDK V2 (Java) ([BEAM-7637](https://issues.apache.org/jira/browse/BEAM-7637))
* GCP BigQuery sink (file loads) uses runner determined sharding for unbounded data ([BEAM-11772](https://issues.apache.org/jira/browse/BEAM-11772))
* KafkaIO now recognizes the `partition` property in writing records ([BEAM-11806](https://issues.apache.org/jira/browse/BEAM-11806))
* Support for Hadoop configuration on ParquetIO ([BEAM-11913](https://issues.apache.org/jira/browse/BEAM-11913))

### New Features / Improvements

* DataFrame API now supports pandas 1.2.x ([BEAM-11531](https://issues.apache.org/jira/browse/BEAM-11531)).
* Multiple DataFrame API bugfixes ([BEAM-12071](https://issues.apache.org/jira/browse/BEAM-12071), [BEAM-11929](https://issues.apache.org/jira/browse/BEAM-11929))
* DDL supported in SQL transforms ([BEAM-11850](https://issues.apache.org/jira/browse/BEAM-11850))
* Upgrade Flink runner to Flink version 1.12.2 ([BEAM-11941](https://issues.apache.org/jira/browse/BEAM-11941))

### Breaking Changes

* Deterministic coding enforced for GroupByKey and Stateful DoFns.  Previously non-deterministic coding was allowed, resulting in keys not properly being grouped in some cases. ([BEAM-11719](https://issues.apache.org/jira/browse/BEAM-11719))
  To restore the old behavior, one can register `FakeDeterministicFastPrimitivesCoder` with
  `beam.coders.registry.register_fallback_coder(beam.coders.coders.FakeDeterministicFastPrimitivesCoder())`
  or use the `allow_non_deterministic_key_coders` pipeline option.

### Deprecations

* Support for Flink 1.8 and 1.9 will be removed in the next release (2.30.0) ([BEAM-11948](https://issues.apache.org/jira/browse/BEAM-11948)).

### Known Issues

* See a full list of open [issues that affect](https://issues.apache.org/jira/issues/?jql=project%20%3D%20BEAM%20AND%20affectedVersion%20%3D%202.29.0%20ORDER%20BY%20priority%20DESC%2C%20updated%20DESC) this version.

## List of Contributors

According to `git shortlog`, the following people contributed to the 2.29.0 release. Thank you to all contributors!

Ahmet Altay, Alan Myrvold, Alex Amato, Alexander Chermenin, Alexey Romanenko,
Allen Pradeep Xavier, Amy Wu, Anant Damle, Andreas Bergmeier, Andrei Balici,
Andrew Pilloud, Andy Xu, Ankur Goenka, Bashir Sadjad, Benjamin Gonzalez, Boyuan
Zhang, Brian Hulette, Chamikara Jayalath, Chinmoy Mandayam, Chuck Yang,
dandy10, Daniel Collins, Daniel Oliveira, David Cavazos, David Huntsperger,
David Moravek, Dmytro Kozhevin, Emily Ye, Esun Kim, Evgeniy Belousov, Filip
Popić, Fokko Driesprong, Gris Cuevas, Heejong Lee, Ihor Indyk, Ismaël Mejía,
Jakub-Sadowski, Jan Lukavský, John Edmonds, Juan Sandoval, 谷口恵輔, Kenneth
Jung, Kenneth Knowles, KevinGG, Kiley Sok, Kyle Weaver, MabelYC, Mackenzie
Clark, Masato Nakamura, Milena Bukal, Miltos, Minbo Bae, Miraç Vuslat Başaran,
mynameborat, Nahian-Al Hasan, Nam Bui, Niel Markwick, Niels Basjes, Ning Kang,
Nir Gazit, Pablo Estrada, Ramazan Yapparov, Raphael Sanamyan, Reuven Lax, Rion
Williams, Robert Bradshaw, Robert Burke, Rui Wang, Sam Rohde, Sam Whittle,
Shehzaad Nakhoda, Shehzaad Nakhoda, Siyuan Chen, Sonam Ramchand, Steve Niemitz,
sychen, Sylvain Veyrié, Tim Robertson, Tobias Kaymak, Tomasz Szerszeń, Tomasz
Szerszeń, Tomo Suzuki, Tyson Hamilton, Udi Meiri, Valentyn Tymofieiev, Yichi
Zhang, Yifan Mai, Yixing Zhang, Yoshiki Obata
