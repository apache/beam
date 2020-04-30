---
title:  "Apache Beam 2.10.0"
date:   2019-02-15 00:00:01 -0800
categories:
  - blog
aliases:
  - /blog/2019/02/15/beam-2.10.0.html
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

We are happy to present the new 2.10.0 release of Beam. This release includes both improvements and new functionality.
See the [download page](/get-started/downloads/#2100-2019-02-01) for this release.<!--more-->
For more information on changes in 2.10.0, check out the
[detailed release notes](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12319527&version=12344540).

## Highlights

### Dependency Upgrades/Changes

* FlinkRunner: Flink 1.5.x/1.6.x/1.7.x
* Java: AutoValue 1.6.3
* Java: Jackson 2.9.8
* Java: google_cloud_bigdataoss 1.9.13
* Java: Apache Commons Codec: 1.10
* Python: avro>=1.8.1,<2.0.0; python_version < "3.0"
* Python: avro-python3>=1.8.1,<2.0.0; python_version >= "3.0"
* Python: bigdataoss_gcsio 1.9.12
* Python: dill>=0.2.9,<0.2.10
* Python: gcsio 1.9.13
* Python: google-cloud-pubsub 0.39.0
* Python: pytz>=2018.3
* Python: pyyaml>=3.12,<4.0.0
* MongoDbIO: mongo client 3.9.1

### I/Os

* ParquetIO for Python SDK
* HadoopOutputFormatIO: Add batching support
* HadoopOutputFormatIO: Add streaming support
* MongoDbIO: Add projections
* MongoDbIO: Add support for server with self signed SSL
* MongoDbIO add ordered option (inserts documents even if errors)
* KafkaIO: Add support to write to multiple topics
* KafkaIO: add writing support with ProducerRecord
* CassandraIO: Add ability to delete data
* JdbcIO: Add ValueProvider support for Statement in JdbcIO.write(), so it can be templatized

### New Features / Improvements

* FlinkRunner: support Flink config directory
* FlinkRunner: master url now supports IPv6 addresses
* FlinkRunner: portable runner savepoint / upgrade support
* FlinkRunner: can be built against different Flink versions
* FlinkRunner: Send metrics to Flink in portable runner
* Java: Migrate to vendored gRPC (no conflicts with user gRPC, smaller jars)
* Java: Migrate to vendored Guava (no conflicts with user Guava, smaller jars)
* SQL: support joining unbounded to bounded sources via side input (and is no longer sensitive to left vs right join)
* SQL: support table macro
* Schemas: support for Avro, with automatic schema registration
* Schemas: Automatic schema registration for AutoValue classes

### Bugfixes

* Watch PTransform fixed (affects FileIO)
* FlinkRunner: no longer fails if GroupByKey contains null values (streaming mode only)
* FlinkRunner: no longer prepares to-be-staged file too late
* FlinkRunner: sets number of shards for writes with runner determined sharding
* FlinkRunner: prevents CheckpointMarks from not getting acknowledged
* Schemas: Generated row object for POJOs, Avros, and JavaBeans should work if the wrapped class is package private
* Schemas: Nested collection types in schemas no longer cause NullPointerException when converting to a POJO
* BigQueryIO: now handles quotaExceeded errors properly
* BigQueryIO: now handles triggering correctly in certain very large load jobs
* FileIO and other file-based IOs: Beam LocalFilesystem now matches glob patterns in windows
* SQL: joins no longer moves timestamps to the end of the window
* SQL: was missing some transitive dependencies
* SQL: JDBC driver no longer breaks interactions with other JDBC sources
* pyarrow supported on Windows Python 2

### Deprecations

* Deprecate HadoopInputFormatIO

## List of Contributors

According to git shortlog, the following people contributed
to the 2.10.0 release. Thank you to all contributors!

Ahmet Altay, Alan Myrvold, Alex Amato, Alexey Romanenko, Anton Kedin, Rui Wang, 
Andrew Brampton Andrew Pilloud, Ankur Goenka, Antonio D'souza, Bingfeng Shu,
Boyuan Zhang, brucearctor, Cade Markegard, Chaim Turkel, Chamikara Jayalath, 
Charles Chen, Colm O hEigeartaigh, Cory, Craig Chambers, Cristian, Daniel
Mills, Daniel Oliveira, David Cavazos, David Hrbacek, David Moravek, Dawid
Wysakowicz, djhworld, Dustin Rhodes, Etienne Chauchot, Fabien Rousseau, Garrett
Jones, Gleb Kanterov, Heejong Lee, Ismaël Mejía, Jason Kuster, Jean-Baptiste
Onofré, Jeff Klukas, Joar Wandborg, Jozef Vilcek, Kadir Cetinkaya, Kasia
Kucharczyk, Kengo Seki, Kenneth Knowles, lcaggio, Lukasz Cwik, Łukasz Gajowy,
Manu Zhang, marek.simunek, Mark Daoust, Mark Liu, Maximilian Michels, Melissa
Pashniak, Michael Luckey, Mikhail Gryzykhin, mlotstein, morokosi, Niel
Markwick, Pablo Estrada, Prem Kumar Karunakaran, Reuven Lax, robbe, Robbe
Sneyders, Robert Bradshaw, Robert Burke, Ruoyun Huang, Ryan Williams, Sam
Whittle, Scott Wegner, Slava Chernyak, Theodore Siu, Thomas Weise, Udi Meiri,
vaclav.plajt@gmail.com, Valentyn Tymofieiev, Won Wook SONG, Wout Scheepers,
Xinyu Liu, Yueyang Qiu, Zhuo Peng

