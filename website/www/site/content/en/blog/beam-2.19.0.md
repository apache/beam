---
title:  "Apache Beam 2.19.0"
date:   2020-02-04 00:00:01 -0800
categories:
  - blog
aliases:
  - /blog/2020/02/04/beam-2.19.0.html
authors:
  - boyuanzz

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

We are happy to present the new 2.19.0 release of Beam. This release includes both improvements and new functionality.
See the [download page](/get-started/downloads/#2190-2020-02-04) for this release.<!--more-->
For more information on changes in 2.19.0, check out the
[detailed release notes](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12319527&version=12346582).

## Highlights
 * Multiple improvements made into Python SDK harness: 
 [BEAM-8624](https://issues.apache.org/jira/browse/BEAM-8624), 
 [BEAM-8623](https://issues.apache.org/jira/browse/BEAM-8623), 
 [BEAM-7949](https://issues.apache.org/jira/browse/BEAM-7949), 
 [BEAM-8935](https://issues.apache.org/jira/browse/BEAM-8935), 
 [BEAM-8816](https://issues.apache.org/jira/browse/BEAM-8816)

### I/Os
* [BEAM-1440](https://issues.apache.org/jira/browse/BEAM-1440) Create a BigQuery source (that implements iobase.BoundedSource) for Python SDK
* [BEAM-2572](https://issues.apache.org/jira/browse/BEAM-2572) Implement an S3 filesystem for Python SDK
* [BEAM-5192](https://issues.apache.org/jira/browse/BEAM-5192) Support Elasticsearch 7.x
* [BEAM-8745](https://issues.apache.org/jira/browse/BEAM-8745) More fine-grained controls for the size of a BigQuery Load job
* [BEAM-8801](https://issues.apache.org/jira/browse/BEAM-8801) PubsubMessageToRow should not check useFlatSchema() in processElement
* [BEAM-8953](https://issues.apache.org/jira/browse/BEAM-8953) Extend ParquetIO.Read/ReadFiles.Builder to support Avro GenericData model
* [BEAM-8946](https://issues.apache.org/jira/browse/BEAM-8946) Report collection size from MongoDBIOIT
* [BEAM-8978](https://issues.apache.org/jira/browse/BEAM-8978) Report saved data size from HadoopFormatIOIT

### New Features / Improvements
* [BEAM-6008](https://issues.apache.org/jira/browse/BEAM-6008) Improve error reporting in Java/Python PortableRunner
* [BEAM-8296](https://issues.apache.org/jira/browse/BEAM-8296) Containerize the Spark job server
* [BEAM-8746](https://issues.apache.org/jira/browse/BEAM-8746) Allow the local job service to work from inside docker
* [BEAM-8837](https://issues.apache.org/jira/browse/BEAM-8837) PCollectionVisualizationTest: possible bug
* [BEAM-8139](https://issues.apache.org/jira/browse/BEAM-8139) Execute portable Spark application jar
* [BEAM-9019](https://issues.apache.org/jira/browse/BEAM-9019) Improve Spark Encoders (wrappers of beam coders)
* [BEAM-9053](https://issues.apache.org/jira/browse/BEAM-9053) Improve error message when unable to get the correct filesystem for specified path in Python SDK) Improve error message when unable to get the correct filesystem for specified path in Python SDK
* [BEAM-9055](https://issues.apache.org/jira/browse/BEAM-9055) Unify the config names of Fn Data API across languages

### SQL
* [BEAM-5690](https://issues.apache.org/jira/browse/BEAM-5690) Issue with GroupByKey in BeamSql using SparkRunner
* [BEAM-8993](https://issues.apache.org/jira/browse/BEAM-8993) [SQL] MongoDb should use predicate push-down
* [BEAM-8844](https://issues.apache.org/jira/browse/BEAM-8844) [SQL] Create performance tests for BigQueryTable
* [BEAM-9023](https://issues.apache.org/jira/browse/BEAM-9023) Upgrade to ZetaSQL 2019.12.1

### Breaking Changes
* [BEAM-8989](https://issues.apache.org/jira/browse/BEAM-8989) Backwards incompatible change in ParDo.getSideInputs (caught by failure when running Apache Nemo quickstart)
* [BEAM-8402](https://issues.apache.org/jira/browse/BEAM-8402) Backwards incompatible change related to how Environments are represented in Python `DirectRunner`.
* [BEAM-9218](https://issues.apache.org/jira/browse/BEAM-9218) Template staging broken on Beam 2.18.0

### Dependency Changes
* [BEAM-8696](https://issues.apache.org/jira/browse/BEAM-8696) Beam Dependency Update Request: com.google.protobuf:protobuf-java
* [BEAM-8701](https://issues.apache.org/jira/browse/BEAM-8701) Beam Dependency Update Request: commons-io:commons-io
* [BEAM-8716](https://issues.apache.org/jira/browse/BEAM-8716) Beam Dependency Update Request: org.apache.commons:commons-csv
* [BEAM-8717](https://issues.apache.org/jira/browse/BEAM-8717) Beam Dependency Update Request: org.apache.commons:commons-lang3
* [BEAM-8749](https://issues.apache.org/jira/browse/BEAM-8749) Beam Dependency Update Request: com.datastax.cassandra:cassandra-driver-mapping
* [BEAM-5546](https://issues.apache.org/jira/browse/BEAM-5546) Beam Dependency Update Request: commons-codec:commons-codec

### Bugfixes
* [BEAM-9123](https://issues.apache.org/jira/browse/BEAM-9123) HadoopResourceId returns wrong directory name
* [BEAM-8962](https://issues.apache.org/jira/browse/BEAM-8962) FlinkMetricContainer causes churn in the JobManager and lets the web frontend malfunction
* [BEAM-5495](https://issues.apache.org/jira/browse/BEAM-5495) PipelineResources algorithm is not working in most environments
* [BEAM-8025](https://issues.apache.org/jira/browse/BEAM-8025) Cassandra IO classMethod test is flaky
* [BEAM-8577](https://issues.apache.org/jira/browse/BEAM-8577) FileSystems may have not be initialized during ResourceId deserialization
* [BEAM-8582](https://issues.apache.org/jira/browse/BEAM-8582) Python SDK emits duplicate records for Default and AfterWatermark triggers
* [BEAM-8943](https://issues.apache.org/jira/browse/BEAM-8943) SDK harness servers don't shut down properly when SDK harness environment cleanup fails
* [BEAM-8995](https://issues.apache.org/jira/browse/BEAM-8995) apache_beam.io.gcp.bigquery_read_it_test failing on Py3.5 PC with: TypeError: the JSON object must be str, not 'bytes'
* [BEAM-8999](https://issues.apache.org/jira/browse/BEAM-8999) PGBKCVOperation does not respect timestamp combiners
* [BEAM-9050](https://issues.apache.org/jira/browse/BEAM-9050) Beam pickler doesn't pickle classes that have __module__ set to None.
* 
* Various bug fixes and performance improvements.

## List of Contributors

According to git shortlog, the following people contributed to the 2.19.0 release. Thank you to all contributors!

Ahmet Altay, Alex Amato, Alexey Romanenko, Andrew Pilloud, Ankur Goenka, Anton Kedin, Boyuan Zhang, Brian Hulette, Brian Martin, Chamikara Jayalath, Charles Chen, Craig Chambers, Daniel Oliveira, David Moravek, David Rieber, Dustin Rhodes, Etienne Chauchot, Gleb Kanterov, Hai Lu, Heejong Lee, Ismaël Mejía, Jan Lukavský, Jason Kuster, Jean-Baptiste Onofré, Jeff Klukas, João Cabrita, J Ross Thomson, Juan Rael, Juta, Kasia Kucharczyk, Kengo Seki, Kenneth Jung, Kenneth Knowles, Kyle Weaver, Kyle Winkelman, Lukas Drbal, Łukasz Gajowy, Marek Simunek, Mark Liu, Maximilian Michels, Melissa Pashniak, Michael Luckey, Michal Walenia, Mike Pedersen, Mikhail Gryzykhin, Niel Markwick, Pablo Estrada, Pascal Gula, Reuven Lax, Rob, Robbe Sneyders, Robert Bradshaw, Robert Burke, Rui Wang, Ruoyun Huang, Ryan Williams, Sam Rohde, Sam Whittle, Scott Wegner, Thomas Weise, Tianyang Hu, ttanay, tvalentyn, Tyler Akidau, Udi Meiri, Valentyn Tymofieiev, Xinyu Liu, XuMingmin
