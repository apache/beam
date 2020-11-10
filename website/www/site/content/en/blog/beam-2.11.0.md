---
title:  "Apache Beam 2.11.0"
date:   2019-03-05 00:00:01 -0800
categories:
  - blog
aliases:
  - /blog/2019/03/05/beam-2.11.0.html
authors:
        - altay

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

We are happy to present the new 2.11.0 release of Beam. This release includes both improvements and new functionality.
See the [download page](/get-started/downloads/#2110-2019-02-26) for this release.<!--more-->
For more information on changes in 2.11.0, check out the
[detailed release notes](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12319527&version=12344775).

## Highlights

### Dependency Upgrades/Changes

* Java: antlr: 4.7
* Java: antlr_runtime: 4.7
* Java: bigdataoss_gcsio: 1.9.16
* Java: bigdataoss_util: 1.9.16
* Java: bigtable_client_core: 1.8.0
* Java: cassandra-driver-core: 3.6.0
* Java: cassandra-driver-mapping: 3.6.0
* Java: commons-compress: 1.18
* Java: gax_grpc: 1.38.0
* Java: google_api_common: 1.7.0
* Java: google_api_services_dataflow: v1b3-rev20190126-1.27.0
* Java: google_cloud_bigquery_storage: 0.79.0-alpha
* Java: google_cloud_bigquery_storage_proto: 0.44.0
* Java: google_auth_library_credentials: 0.12.0
* Java: google_auth_library_oauth2_http: 0.12.0
* Java: google_cloud_core: 1.61.0
* Java: google_cloud_core_grpc: 1.61.0
* Java: google_cloud_spanner: 1.6.0
* Java: grpc_all: 1.17.1
* Java: grpc_auth: 1.17.1
* Java: grpc_core: 1.17.1
* Java: grpc_google_cloud_pubsub_v1: 1.17.1
* Java: grpc_protobuf: 1.17.1
* Java: grpc_protobuf_lite: 1.17.1
* Java: grpc_netty: 1.17.1
* Java: grpc_stub: 1.17.1
* Java: netty_handler: 4.1.30.Final
* Java: netty_tcnative_boringssl_static: 2.0.17.Final
* Java: netty_transport_native_epoll: 4.1.30.Final
* Java: proto_google_cloud_spanner_admin_database_v1: 1.6.0
* Java: zstd_jni: 1.3.8-3
* Python: futures>=3.2.0,<4.0.0; python_version < "3.0"
* Python: pyvcf>=0.6.8,<0.7.0; python_version < "3.0"
* Python: google-apitools>=0.5.26,<0.5.27
* Python: google-cloud-core==0.28.1
* Python: google-cloud-bigtable==0.31.1

### I/Os

* Portable Flink runner support for running cross-language transforms.
* Add Cloud KMS support to GCS copies.
* Add parameters for offsetConsumer in KafkaIO.read().
* Allow setting compression codec in ParquetIO write.
* Add kms_key to BigQuery transforms, pass to Dataflow.

### New Features / Improvements

* Python 3 (experimental) suppport for DirectRunner and DataflowRunner.
* Add ZStandard compression support for Java SDK.
* Python: Add CombineFn.compact, similar to Java.
* SparkRunner: GroupByKey optimized for non-merging windows.
* SparkRunner: Add bundleSize parameter to control splitting of Spark sources.
* FlinkRunner: Portable runner savepoint / upgrade support.

### Bugfixes

* Various bug fixes and performance improvements.

### Deprecations

* Deprecate MongoDb `withKeepAlive` because it is deprecated in the Mongo driver.

## List of Contributors

According to git shortlog, the following people contributed
to the 2.11.0 release. Thank you to all contributors!

Ahmet Altay, Alex Amato. Alexey Romanenko, Andrew Pilloud, Ankur Goenka, Anton Kedin,
Boyuan Zhang, Brian Hulette, Brian Martin, Chamikara Jayalath, Charles Chen, Craig Chambers,
Daniel Oliveira, David Moravek, David Rieber, Dustin Rhodes, Etienne Chauchot, Gleb Kanterov,
Hai Lu, Heejong Lee, Ismaël Mejía, J Ross Thomson, Jan Lukavsky, Jason Kuster, Jean-Baptiste Onofré,
Jeff Klukas, João Cabrita, Juan Rael, Juta Staes, Kasia Kucharczyk, Kengo Seki, Kenneth Jung,
Kenneth Knowles, Kyle Weaver, Kyle Winkelman, Lukas Drbal, Marek Simunek, Mark Liu,
Maximilian Michels, Melissa Pashniak, Michael Luckey, Michal Walenia, Mike Pedersen,
Mikhail Gryzykhin, Niel Markwick, Pablo Estrada, Pascal Gula, Reuven Lax, Robbe Sneyders,
Robert Bradshaw, Robert Burke, Rui Wang, Ruoyun Huang, Ryan Williams, Sam Rohde, Sam Whittle,
Scott Wegner, Tanay Tummalapalli, Thomas Weise, Tianyang Hu, Tyler Akidau, Udi Meiri,
Valentyn Tymofieiev, Xinyu Liu, Xu Mingmin, Łukasz Gajowy.


