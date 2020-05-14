---
title:  "Apache Beam 2.17.0"
date:   2020-01-06 00:00:01 -0800
categories:
  - blog
aliases:
  - /blog/2020/01/06/beam-2.17.0.html
authors:
  - ardagan

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

We are happy to present the new 2.17.0 release of Beam. This release includes both improvements and new functionality.
Users of the MongoDbIO connector are encouraged to upgrade to this release to address a [security vulnerability](/security/CVE-2020-1929/).

See the [download page](/get-started/downloads/#2170-2020-01-06) for this release.<!--more-->
For more information on changes in 2.17.0, check out the
[detailed release notes](https://issues.apache.org/jira/secure/ReleaseNote.jspa?version=12345970&projectId=12319527).

## Highlights
* [BEAM-7962](https://issues.apache.org/jira/browse/BEAM-7962) - Drop support for Flink 1.5 and 1.6
* [BEAM-7635](https://issues.apache.org/jira/browse/BEAM-7635) - Migrate SnsIO to AWS SDK for Java 2
* Improved usability for portable Flink Runner
    * [BEAM-8183](https://issues.apache.org/jira/browse/BEAM-8183) - Optionally bundle multiple pipelines into a single Flink jar.
    * [BEAM-8372](https://issues.apache.org/jira/browse/BEAM-8372) - Allow submission of Flink UberJar directly to flink cluster.
    * [BEAM-8471](https://issues.apache.org/jira/browse/BEAM-8471) - Flink native job submission for portable pipelines.
    * [BEAM-8312](https://issues.apache.org/jira/browse/BEAM-8312) - Flink portable pipeline jars do not need to stage artifacts remotely.

### New Features / Improvements
* [BEAM-7730](https://issues.apache.org/jira/browse/BEAM-7730) - Add Flink 1.9 build target and Make FlinkRunner compatible with Flink 1.9.
* [BEAM-7990](https://issues.apache.org/jira/browse/BEAM-7990) - Add ability to read parquet files into PCollection of pyarrow.Table.
* [BEAM-8355](https://issues.apache.org/jira/browse/BEAM-8355) - Make BooleanCoder a standard coder.
* [BEAM-8394](https://issues.apache.org/jira/browse/BEAM-8394) - Add withDataSourceConfiguration() method in JdbcIO.ReadRows class.
* [BEAM-5428](https://issues.apache.org/jira/browse/BEAM-5428) - Implement cross-bundle state caching.
* [BEAM-5967](https://issues.apache.org/jira/browse/BEAM-5967) - Add handling of DynamicMessage in ProtoCoder.
* [BEAM-7473](https://issues.apache.org/jira/browse/BEAM-7473) - Update RestrictionTracker within Python to not be required to be thread safe.
* [BEAM-7920](https://issues.apache.org/jira/browse/BEAM-7920) - Added AvroTableProvider to Beam SQL.
* [BEAM-8098](https://issues.apache.org/jira/browse/BEAM-8098) - Improve documentation on BigQueryIO.
* [BEAM-8100](https://issues.apache.org/jira/browse/BEAM-8100) - Add exception handling to Json transforms in Java SDK.
* [BEAM-8306](https://issues.apache.org/jira/browse/BEAM-8306) - Improve estimation of data byte size reading from source in ElasticsearchIO.
* [BEAM-8351](https://issues.apache.org/jira/browse/BEAM-8351) - Support passing in arbitrary KV pairs to sdk worker via external environment config.
* [BEAM-8396](https://issues.apache.org/jira/browse/BEAM-8396) - Default to LOOPBACK mode for local flink (spark, ...) runner.
* [BEAM-8410](https://issues.apache.org/jira/browse/BEAM-8410) - JdbcIO should support setConnectionInitSqls in its DataSource.
* [BEAM-8609](https://issues.apache.org/jira/browse/BEAM-8609) - Add HllCount to Java transform catalog.
* [BEAM-8861](https://issues.apache.org/jira/browse/BEAM-8861) - Disallow self-signed certificates by default in ElasticsearchIO.

### Dependency Changes
* [BEAM-8285](https://issues.apache.org/jira/browse/BEAM-8285) - Upgrade ZetaSQL to 2019.09.1.
* [BEAM-8392](https://issues.apache.org/jira/browse/BEAM-8392) - Upgrade pyarrow version bounds: 0.15.1<= to <0.16.0.
* [BEAM-5895](https://issues.apache.org/jira/browse/BEAM-5895) - Upgrade com.rabbitmq:amqp-client to 5.7.3.
* [BEAM-6896](https://issues.apache.org/jira/browse/BEAM-6896) - Upgrade PyYAML version bounds: 3.12<= to <6.0.0.


### Bugfixes
* [BEAM-8819] - AvroCoder for SpecificRecords is not serialized correctly since 2.13.0
* Various bug fixes and performance improvements.

### Known Issues

* [BEAM-8989](https://issues.apache.org/jira/browse/BEAM-8989) Apache Nemo
  runner broken due to backwards incompatible change since 2.16.0.

## List of Contributors

 According to git shortlog, the following people contributed to the 2.17.0 release. Thank you to all contributors!

Ahmet Altay, Alan Myrvold, Alexey Romanenko, Andre-Philippe Paquet, Andrew
Pilloud, angulartist, Ankit Jhalaria, Ankur Goenka, Anton Kedin, Aryan Naraghi,
Aurélien Geron, B M VISHWAS, Bartok Jozsef, Boyuan Zhang, Brian Hulette, Cerny
Ondrej, Chad Dombrova, Chamikara Jayalath, ChethanU, cmach, Colm O hEigeartaigh,
Cyrus Maden, Daniel Oliveira, Daniel Robert, Dante, David Cavazos, David
Moravek, David Yan, Enrico Canzonieri, Etienne Chauchot, gxercavins, Hai Lu,
Hannah Jiang, Ian Lance Taylor, Ismaël Mejía, Israel Herraiz, James Wen, Jan
Lukavský, Jean-Baptiste Onofré, Jeff Klukas, jesusrv1103, Jofre, Kai Jiang,
Kamil Wasilewski, Kasia Kucharczyk, Kenneth Knowles, Kirill Kozlov,
kirillkozlov, Kohki YAMAGIWA, Kyle Weaver, Leonardo Alves Miguel, lloigor,
lostluck, Luis Enrique Ortíz Ramirez, Luke Cwik, Mark Liu, Maximilian Michels,
Michal Walenia, Mikhail Gryzykhin, mrociorg, Nicolas Delsaux, Ning Kang, NING
KANG, Pablo Estrada, pabloem, Piotr Szczepanik, rahul8383, Rakesh Kumar, Renat
Nasyrov, Reuven Lax, Robert Bradshaw, Robert Burke, Rui Wang, Ruslan Altynnikov,
Ryan Skraba, Salman Raza, Saul Chavez, Sebastian Jambor, sunjincheng121, Tatu
Saloranta, tchiarato, Thomas Weise, Tomo Suzuki, Tudor Marian, tvalentyn, Udi
Meiri, Valentyn Tymofieiev, Viola Lyu, Vishwas, Yichi Zhang, Yifan Zou, Yueyang
Qiu, Łukasz Gajowy

