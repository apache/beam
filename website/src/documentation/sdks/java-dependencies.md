---
layout: section
title: "Java SDK dependencies"
section_menu: section-menu/sdks.html
permalink: /documentation/sdks/java-dependencies/
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

# Beam SDK for Java dependencies

The Beam SDKs depend on common third-party components which then
import additional dependencies. Version collisions can result in unexpected
behavior in the service. If you are using any of these packages in your code, be
aware that some libraries are not forward-compatible and you may need to pin to
the listed versions that will be in scope during execution.

## 2.8.0

Beam SDK for Java 2.8.0 has the following compile and runtime dependencies.
<table class="table-bordered table-striped">
  <tr><th>GroupId</th><th>ArtifactId</th><th>Version</th></tr>
  <tr><td>activemq-amqp</td><td>org.apache.activemq</td><td>5.13.1</td></tr>
  <tr><td>activemq-broker</td><td>org.apache.activemq</td><td>5.13.1</td></tr>
  <tr><td>activemq-client</td><td>org.apache.activemq</td><td>5.13.1</td></tr>
  <tr><td>activemq-jaas</td><td>org.apache.activemq</td><td>5.13.1</td></tr>
  <tr><td>activemq-junit</td><td>org.apache.activemq.tooling</td><td>5.13.1</td></tr>
  <tr><td>activemq-kahadb-store</td><td>org.apache.activemq</td><td>5.13.1</td></tr>
  <tr><td>activemq-mqtt</td><td>org.apache.activemq</td><td>5.13.1</td></tr>
  <tr><td>apex-common</td><td>org.apache.apex</td><td>3.7.0</td></tr>
  <tr><td>apex-engine</td><td>org.apache.apex</td><td>3.7.0</td></tr>
  <tr><td>api-common</td><td>com.google.api</td><td>1.6.0</td></tr>
  <tr><td>args4j</td><td>args4j</td><td>2.33</td></tr>
  <tr><td>avro</td><td>org.apache.avro</td><td>1.8.2</td></tr>
  <tr><td>bigtable-client-core</td><td>com.google.cloud.bigtable</td><td>1.4.0</td></tr>
  <tr><td>byte-buddy</td><td>net.bytebuddy</td><td>1.8.11</td></tr>
  <tr><td>commons-compress</td><td>org.apache.commons</td><td>1.16.1</td></tr>
  <tr><td>commons-csv</td><td>org.apache.commons</td><td>1.4</td></tr>
  <tr><td>commons-io</td><td>commons-io</td><td>1.3.2</td></tr>
  <tr><td>commons-io</td><td>commons-io</td><td>2.5</td></tr>
  <tr><td>commons-lang3</td><td>org.apache.commons</td><td>3.6</td></tr>
  <tr><td>commons-math3</td><td>org.apache.commons</td><td>3.6.1</td></tr>
  <tr><td>datastore-v1-proto-client</td><td>com.google.cloud.datastore</td><td>1.6.0</td></tr>
  <tr><td>error_prone_annotations</td><td>com.google.errorprone</td><td>2.0.15</td></tr>
  <tr><td>gax-grpc</td><td>com.google.api</td><td>1.29.0</td></tr>
  <tr><td>gcsio</td><td>com.google.cloud.bigdataoss</td><td>1.9.0</td></tr>
  <tr><td>google-api-client-jackson2</td><td>com.google.api-client</td><td>1.23.0</td></tr>
  <tr><td>google-api-client-java6</td><td>com.google.api-client</td><td>1.23.0</td></tr>
  <tr><td>google-api-client</td><td>com.google.api-client</td><td>1.23.0</td></tr>
  <tr><td>google-api-services-bigquery</td><td>com.google.apis</td><td>v2-rev402-1.23.0</td></tr>
  <tr><td>google-api-services-clouddebugger</td><td>com.google.apis</td><td>v2-rev253-1.23.0</td></tr>
  <tr><td>google-api-services-cloudresourcemanager</td><td>com.google.apis</td><td>v1-rev502-1.23.0</td></tr>
  <tr><td>google-api-services-dataflow</td><td>com.google.apis</td><td>v1b3-rev257-1.23.0</td></tr>
  <tr><td>google-api-services-pubsub</td><td>com.google.apis</td><td>v1-rev399-1.23.0</td></tr>
  <tr><td>google-api-services-storage</td><td>com.google.apis</td><td>v1-rev136-1.23.0</td></tr>
  <tr><td>google-auth-library-credentials</td><td>com.google.auth</td><td>0.10.0</td></tr>
  <tr><td>google-auth-library-oauth2-http</td><td>com.google.auth</td><td>0.10.0</td></tr>
  <tr><td>google-cloud-core-grpc</td><td>com.google.cloud</td><td>1.36.0</td></tr>
  <tr><td>google-cloud-core</td><td>com.google.cloud</td><td>1.36.0</td></tr>
  <tr><td>google-cloud-dataflow-java-proto-library-all</td><td>com.google.cloud.dataflow</td><td>0.5.160304</td></tr>
  <tr><td>google-cloud-spanner</td><td>com.google.cloud</td><td>0.54.0-beta</td></tr>
  <tr><td>google-http-client-jackson2</td><td>com.google.http-client</td><td>1.23.0</td></tr>
  <tr><td>google-http-client-jackson</td><td>com.google.http-client</td><td>1.23.0</td></tr>
  <tr><td>google-http-client-protobuf</td><td>com.google.http-client</td><td>1.23.0</td></tr>
  <tr><td>google-http-client</td><td>com.google.http-client</td><td>1.23.0</td></tr>
  <tr><td>google-oauth-client-java6</td><td>com.google.oauth-client</td><td>1.23.0</td></tr>
  <tr><td>google-oauth-client</td><td>com.google.oauth-client</td><td>1.23.0</td></tr>
  <tr><td>grpc-all</td><td>io.grpc</td><td>1.13.1</td></tr>
  <tr><td>grpc-auth</td><td>io.grpc</td><td>1.13.1</td></tr>
  <tr><td>grpc-core</td><td>io.grpc</td><td>1.13.1</td></tr>
  <tr><td>grpc-google-cloud-bigtable-v2</td><td>com.google.api.grpc</td><td>0.19.0</td></tr>
  <tr><td>grpc-google-cloud-pubsub-v1</td><td>com.google.api.grpc</td><td>1.18.0</td></tr>
  <tr><td>grpc-netty</td><td>io.grpc</td><td>1.13.1</td></tr>
  <tr><td>grpc-protobuf-lite</td><td>io.grpc</td><td>1.13.1</td></tr>
  <tr><td>grpc-protobuf</td><td>io.grpc</td><td>1.13.1</td></tr>
  <tr><td>grpc-stub</td><td>io.grpc</td><td>1.13.1</td></tr>
  <tr><td>guava</td><td>com.google.guava</td><td>20.0</td></tr>
  <tr><td>guava-testlib</td><td>com.google.guava</td><td>20.0</td></tr>
  <tr><td>hadoop-client</td><td>org.apache.hadoop</td><td>2.7.3</td></tr>
  <tr><td>hadoop-common</td><td>org.apache.hadoop</td><td>2.7.3</td></tr>
  <tr><td>hadoop-hdfs</td><td>org.apache.hadoop</td><td>2.7.3</td></tr>
  <tr><td>hadoop-hdfs</td><td>org.apache.hadoop</td><td>2.7.3</td></tr>
  <tr><td>hadoop-mapreduce-client-core</td><td>org.apache.hadoop</td><td>2.7.3</td></tr>
  <tr><td>hadoop-minicluster</td><td>org.apache.hadoop</td><td>2.7.3</td></tr>
  <tr><td>hamcrest-core</td><td>org.hamcrest</td><td>1.3</td></tr>
  <tr><td>hamcrest-library</td><td>org.hamcrest</td><td>1.3</td></tr>
  <tr><td>jackson-annotations</td><td>com.fasterxml.jackson.core</td><td>2.9.5</td></tr>
  <tr><td>jackson-core</td><td>com.fasterxml.jackson.core</td><td>2.9.5</td></tr>
  <tr><td>jackson-databind</td><td>com.fasterxml.jackson.core</td><td>2.9.5</td></tr>
  <tr><td>jackson-dataformat-cbor</td><td>com.fasterxml.jackson.dataformat</td><td>2.9.5</td></tr>
  <tr><td>jackson-dataformat-yaml</td><td>com.fasterxml.jackson.dataformat</td><td>2.9.5</td></tr>
  <tr><td>jackson-datatype-joda</td><td>com.fasterxml.jackson.datatype</td><td>2.9.5</td></tr>
  <tr><td>jackson-module-scala_2.11</td><td>com.fasterxml.jackson.module</td><td>2.9.5</td></tr>
  <tr><td>jaxb-api</td><td>javax.xml.bind</td><td>2.2.12</td></tr>
  <tr><td>joda-time</td><td>joda-time</td><td>2.4</td></tr>
  <tr><td>junit-quickcheck-core</td><td>com.pholser</td><td>0.8</td></tr>
  <tr><td>junit</td><td>junit</td><td>4.12</td></tr>
  <tr><td>kafka_2.11</td><td>org.apache.kafka</td><td>1.0.0</td></tr>
  <tr><td>kafka-clients</td><td>org.apache.kafka</td><td>1.0.0</td></tr>
  <tr><td>malhar-library</td><td>org.apache.apex</td><td>3.4.0</td></tr>
  <tr><td>mockito-core</td><td>org.mockito</td><td>1.10.19</td></tr>
  <tr><td>netty-handler</td><td>io.netty</td><td>4.1.25.Final</td></tr>
  <tr><td>netty-tcnative-boringssl-static</td><td>io.netty</td><td>2.0.8.Final</td></tr>
  <tr><td>netty-transport-native-epoll</td><td>io.netty</td><td>4.1.25.Final</td></tr>
  <tr><td>postgresql</td><td>org.postgresql</td><td>42.2.2</td></tr>
  <tr><td>powermock-mockito-release-full</td><td>org.powermock</td><td>1.6.4</td></tr>
  <tr><td>protobuf-java</td><td>com.google.protobuf</td><td>3.6.0</td></tr>
  <tr><td>protobuf-java-util</td><td>com.google.protobuf</td><td>3.6.0</td></tr>
  <tr><td>proto-google-cloud-datastore-v1</td><td>com.google.api.grpc</td><td>0.19.0</td></tr>
  <tr><td>proto-google-cloud-pubsub-v1</td><td>com.google.api.grpc</td><td>1.18.0</td></tr>
  <tr><td>proto-google-cloud-spanner-admin-database-v1</td><td>com.google.api.grpc</td><td>0.19.0</td></tr>
  <tr><td>proto-google-common-protos</td><td>com.google.api.grpc</td><td>1.12.0</td></tr>
  <tr><td>slf4j-api</td><td>org.slf4j</td><td>1.7.25</td></tr>
  <tr><td>slf4j-jdk14</td><td>org.slf4j</td><td>1.7.25</td></tr>
  <tr><td>slf4j-log4j12</td><td>org.slf4j</td><td>1.7.25</td></tr>
  <tr><td>slf4j-simple</td><td>org.slf4j</td><td>1.7.25</td></tr>
  <tr><td>snappy-java</td><td>org.xerial.snappy</td><td>1.1.4</td></tr>
  <tr><td>spark-core_2.11</td><td>org.apache.spark</td><td>2.3.1</td></tr>
  <tr><td>spark-network-common_2.11</td><td>org.apache.spark</td><td>2.3.1</td></tr>
  <tr><td>spark-streaming_2.11</td><td>org.apache.spark</td><td>2.3.1</td></tr>
  <tr><td>stax2-api</td><td>org.codehaus.woodstox</td><td>3.1.4</td></tr>
  <tr><td>util</td><td>com.google.cloud.bigdataoss</td><td>1.9.0</td></tr>
  <tr><td>woodstox-core-asl</td><td>org.codehaus.woodstox</td><td>4.4.1</td></tr>
</table>


## Previous releases

<details><summary markdown="span">2.7.0</summary>
<p>Beam SDK for Java 2.7.0 has the following compile and runtime dependencies.</p>
<table class="table-bordered table-striped">
  <tr><th>GroupId</th><th>ArtifactId</th><th>Version</th></tr>
  <tr><td>activemq-amqp</td><td>org.apache.activemq</td><td>5.13.1</td></tr>
  <tr><td>activemq-broker</td><td>org.apache.activemq</td><td>5.13.1</td></tr>
  <tr><td>activemq-client</td><td>org.apache.activemq</td><td>5.13.1</td></tr>
  <tr><td>activemq-jaas</td><td>org.apache.activemq</td><td>5.13.1</td></tr>
  <tr><td>activemq-junit</td><td>org.apache.activemq.tooling</td><td>5.13.1</td></tr>
  <tr><td>activemq-kahadb-store</td><td>org.apache.activemq</td><td>5.13.1</td></tr>
  <tr><td>activemq-mqtt</td><td>org.apache.activemq</td><td>5.13.1</td></tr>
  <tr><td>apex-common</td><td>org.apache.apex</td><td>3.7.0</td></tr>
  <tr><td>apex-engine</td><td>org.apache.apex</td><td>3.7.0</td></tr>
  <tr><td>api-common</td><td>com.google.api</td><td>1.6.0</td></tr>
  <tr><td>args4j</td><td>args4j</td><td>2.33</td></tr>
  <tr><td>avro</td><td>org.apache.avro</td><td>1.8.2</td></tr>
  <tr><td>bigtable-client-core</td><td>com.google.cloud.bigtable</td><td>1.4.0</td></tr>
  <tr><td>byte-buddy</td><td>net.bytebuddy</td><td>1.8.11</td></tr>
  <tr><td>commons-compress</td><td>org.apache.commons</td><td>1.16.1</td></tr>
  <tr><td>commons-csv</td><td>org.apache.commons</td><td>1.4</td></tr>
  <tr><td>commons-io</td><td>commons-io</td><td>1.3.2</td></tr>
  <tr><td>commons-io</td><td>commons-io</td><td>2.5</td></tr>
  <tr><td>commons-lang3</td><td>org.apache.commons</td><td>3.6</td></tr>
  <tr><td>commons-math3</td><td>org.apache.commons</td><td>3.6.1</td></tr>
  <tr><td>datastore-v1-proto-client</td><td>com.google.cloud.datastore</td><td>1.6.0</td></tr>
  <tr><td>error_prone_annotations</td><td>com.google.errorprone</td><td>2.0.15</td></tr>
  <tr><td>gax-grpc</td><td>com.google.api</td><td>1.29.0</td></tr>
  <tr><td>gcsio</td><td>com.google.cloud.bigdataoss</td><td>1.9.0</td></tr>
  <tr><td>google-api-client-jackson2</td><td>com.google.api-client</td><td>1.23.0</td></tr>
  <tr><td>google-api-client-java6</td><td>com.google.api-client</td><td>1.23.0</td></tr>
  <tr><td>google-api-client</td><td>com.google.api-client</td><td>1.23.0</td></tr>
  <tr><td>google-api-services-bigquery</td><td>com.google.apis</td><td>v2-rev402-1.23.0</td></tr>
  <tr><td>google-api-services-clouddebugger</td><td>com.google.apis</td><td>v2-rev253-1.23.0</td></tr>
  <tr><td>google-api-services-cloudresourcemanager</td><td>com.google.apis</td><td>v1-rev502-1.23.0</td></tr>
  <tr><td>google-api-services-dataflow</td><td>com.google.apis</td><td>v1b3-rev257-1.23.0</td></tr>
  <tr><td>google-api-services-pubsub</td><td>com.google.apis</td><td>v1-rev399-1.23.0</td></tr>
  <tr><td>google-api-services-storage</td><td>com.google.apis</td><td>v1-rev136-1.23.0</td></tr>
  <tr><td>google-auth-library-credentials</td><td>com.google.auth</td><td>0.10.0</td></tr>
  <tr><td>google-auth-library-oauth2-http</td><td>com.google.auth</td><td>0.10.0</td></tr>
  <tr><td>google-cloud-core-grpc</td><td>com.google.cloud</td><td>1.36.0</td></tr>
  <tr><td>google-cloud-core</td><td>com.google.cloud</td><td>1.36.0</td></tr>
  <tr><td>google-cloud-dataflow-java-proto-library-all</td><td>com.google.cloud.dataflow</td><td>0.5.160304</td></tr>
  <tr><td>google-cloud-spanner</td><td>com.google.cloud</td><td>0.54.0-beta</td></tr>
  <tr><td>google-http-client-jackson2</td><td>com.google.http-client</td><td>1.23.0</td></tr>
  <tr><td>google-http-client-jackson</td><td>com.google.http-client</td><td>1.23.0</td></tr>
  <tr><td>google-http-client-protobuf</td><td>com.google.http-client</td><td>1.23.0</td></tr>
  <tr><td>google-http-client</td><td>com.google.http-client</td><td>1.23.0</td></tr>
  <tr><td>google-oauth-client-java6</td><td>com.google.oauth-client</td><td>1.23.0</td></tr>
  <tr><td>google-oauth-client</td><td>com.google.oauth-client</td><td>1.23.0</td></tr>
  <tr><td>grpc-all</td><td>io.grpc</td><td>1.13.1</td></tr>
  <tr><td>grpc-auth</td><td>io.grpc</td><td>1.13.1</td></tr>
  <tr><td>grpc-core</td><td>io.grpc</td><td>1.13.1</td></tr>
  <tr><td>grpc-google-cloud-bigtable-v2</td><td>com.google.api.grpc</td><td>0.19.0</td></tr>
  <tr><td>grpc-google-cloud-pubsub-v1</td><td>com.google.api.grpc</td><td>1.18.0</td></tr>
  <tr><td>grpc-netty</td><td>io.grpc</td><td>1.13.1</td></tr>
  <tr><td>grpc-protobuf-lite</td><td>io.grpc</td><td>1.13.1</td></tr>
  <tr><td>grpc-protobuf</td><td>io.grpc</td><td>1.13.1</td></tr>
  <tr><td>grpc-stub</td><td>io.grpc</td><td>1.13.1</td></tr>
  <tr><td>guava</td><td>com.google.guava</td><td>20.0</td></tr>
  <tr><td>guava-testlib</td><td>com.google.guava</td><td>20.0</td></tr>
  <tr><td>hadoop-client</td><td>org.apache.hadoop</td><td>2.7.3</td></tr>
  <tr><td>hadoop-common</td><td>org.apache.hadoop</td><td>2.7.3</td></tr>
  <tr><td>hadoop-hdfs</td><td>org.apache.hadoop</td><td>2.7.3</td></tr>
  <tr><td>hadoop-hdfs</td><td>org.apache.hadoop</td><td>2.7.3</td></tr>
  <tr><td>hadoop-mapreduce-client-core</td><td>org.apache.hadoop</td><td>2.7.3</td></tr>
  <tr><td>hadoop-minicluster</td><td>org.apache.hadoop</td><td>2.7.3</td></tr>
  <tr><td>hamcrest-core</td><td>org.hamcrest</td><td>1.3</td></tr>
  <tr><td>hamcrest-library</td><td>org.hamcrest</td><td>1.3</td></tr>
  <tr><td>jackson-annotations</td><td>com.fasterxml.jackson.core</td><td>2.9.5</td></tr>
  <tr><td>jackson-core</td><td>com.fasterxml.jackson.core</td><td>2.9.5</td></tr>
  <tr><td>jackson-databind</td><td>com.fasterxml.jackson.core</td><td>2.9.5</td></tr>
  <tr><td>jackson-dataformat-cbor</td><td>com.fasterxml.jackson.dataformat</td><td>2.9.5</td></tr>
  <tr><td>jackson-dataformat-yaml</td><td>com.fasterxml.jackson.dataformat</td><td>2.9.5</td></tr>
  <tr><td>jackson-datatype-joda</td><td>com.fasterxml.jackson.datatype</td><td>2.9.5</td></tr>
  <tr><td>jackson-module-scala_2.11</td><td>com.fasterxml.jackson.module</td><td>2.9.5</td></tr>
  <tr><td>jaxb-api</td><td>javax.xml.bind</td><td>2.2.12</td></tr>
  <tr><td>joda-time</td><td>joda-time</td><td>2.4</td></tr>
  <tr><td>junit-quickcheck-core</td><td>com.pholser</td><td>0.8</td></tr>
  <tr><td>junit</td><td>junit</td><td>4.12</td></tr>
  <tr><td>kafka_2.11</td><td>org.apache.kafka</td><td>1.0.0</td></tr>
  <tr><td>kafka-clients</td><td>org.apache.kafka</td><td>1.0.0</td></tr>
  <tr><td>malhar-library</td><td>org.apache.apex</td><td>3.4.0</td></tr>
  <tr><td>mockito-core</td><td>org.mockito</td><td>1.10.19</td></tr>
  <tr><td>netty-handler</td><td>io.netty</td><td>4.1.25.Final</td></tr>
  <tr><td>netty-tcnative-boringssl-static</td><td>io.netty</td><td>2.0.8.Final</td></tr>
  <tr><td>netty-transport-native-epoll</td><td>io.netty</td><td>4.1.25.Final</td></tr>
  <tr><td>postgresql</td><td>org.postgresql</td><td>42.2.2</td></tr>
  <tr><td>powermock-mockito-release-full</td><td>org.powermock</td><td>1.6.4</td></tr>
  <tr><td>protobuf-java</td><td>com.google.protobuf</td><td>3.6.0</td></tr>
  <tr><td>protobuf-java-util</td><td>com.google.protobuf</td><td>3.6.0</td></tr>
  <tr><td>proto-google-cloud-datastore-v1</td><td>com.google.api.grpc</td><td>0.19.0</td></tr>
  <tr><td>proto-google-cloud-pubsub-v1</td><td>com.google.api.grpc</td><td>1.18.0</td></tr>
  <tr><td>proto-google-cloud-spanner-admin-database-v1</td><td>com.google.api.grpc</td><td>0.19.0</td></tr>
  <tr><td>proto-google-common-protos</td><td>com.google.api.grpc</td><td>1.12.0</td></tr>
  <tr><td>slf4j-api</td><td>org.slf4j</td><td>1.7.25</td></tr>
  <tr><td>slf4j-jdk14</td><td>org.slf4j</td><td>1.7.25</td></tr>
  <tr><td>slf4j-log4j12</td><td>org.slf4j</td><td>1.7.25</td></tr>
  <tr><td>slf4j-simple</td><td>org.slf4j</td><td>1.7.25</td></tr>
  <tr><td>snappy-java</td><td>org.xerial.snappy</td><td>1.1.4</td></tr>
  <tr><td>spark-core_2.11</td><td>org.apache.spark</td><td>2.3.1</td></tr>
  <tr><td>spark-network-common_2.11</td><td>org.apache.spark</td><td>2.3.1</td></tr>
  <tr><td>spark-streaming_2.11</td><td>org.apache.spark</td><td>2.3.1</td></tr>
  <tr><td>stax2-api</td><td>org.codehaus.woodstox</td><td>3.1.4</td></tr>
  <tr><td>util</td><td>com.google.cloud.bigdataoss</td><td>1.9.0</td></tr>
  <tr><td>woodstox-core-asl</td><td>org.codehaus.woodstox</td><td>4.4.1</td></tr>
</table>
</details>

