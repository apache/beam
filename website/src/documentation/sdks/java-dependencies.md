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

<p>To see the compile and runtime dependencies for your Beam SDK version, expand
the relevant section below.</p>

<details><summary markdown="span"><b>2.9.0</b></summary>

<p>Beam SDK for Java 2.9.0 has the following compile and runtime dependencies.</p>

<table class="table-bordered table-striped">
  <tr><th>GroupId</th><th>ArtifactId</th><th>Version</th></tr>
  <tr><td>org.apache.activemq</td><td>activemq-amqp</td><td>5.13.1</td></tr>
  <tr><td>org.apache.activemq</td><td>activemq-broker</td><td>5.13.1</td></tr>
  <tr><td>org.apache.activemq</td><td>activemq-client</td><td>5.13.1</td></tr>
  <tr><td>org.apache.activemq</td><td>activemq-jaas</td><td>5.13.1</td></tr>
  <tr><td>org.apache.activemq.tooling</td><td>activemq-junit</td><td>5.13.1</td></tr>
  <tr><td>org.apache.activemq</td><td>activemq-kahadb-store</td><td>5.13.1</td></tr>
  <tr><td>org.apache.activemq</td><td>activemq-mqtt</td><td>5.13.1</td></tr>
  <tr><td>org.apache.apex</td><td>apex-common</td><td>3.7.0</td></tr>
  <tr><td>org.apache.apex</td><td>apex-engine</td><td>3.7.0</td></tr>
  <tr><td>args4j</td><td>args4j</td><td>2.33</td></tr>
  <tr><td>org.apache.avro</td><td>avro</td><td>1.8.2</td></tr>
  <tr><td>com.google.cloud.bigdataoss</td><td>gcsio</td><td>1.9.0</td></tr>
  <tr><td>com.google.cloud.bigdataoss</td><td>util</td><td>1.9.0</td></tr>
  <tr><td>com.google.cloud.bigtable</td><td>bigtable-client-core</td><td>1.4.0</td></tr>
  <tr><td>net.bytebuddy</td><td>byte-buddy</td><td>1.9.3</td></tr>
  <tr><td>org.apache.commons</td><td>commons-compress</td><td>1.16.1</td></tr>
  <tr><td>org.apache.commons</td><td>commons-csv</td><td>1.4</td></tr>
  <tr><td>commons-io</td><td>commons-io</td><td>1.3.2</td></tr>
  <tr><td>commons-io</td><td>commons-io</td><td>2.5</td></tr>
  <tr><td>org.apache.commons</td><td>commons-lang3</td><td>3.6</td></tr>
  <tr><td>org.apache.commons</td><td>commons-math3</td><td>3.6.1</td></tr>
  <tr><td>com.google.cloud.datastore</td><td>datastore-v1-proto-client</td><td>1.6.0</td></tr>
  <tr><td>com.google.errorprone</td><td>error_prone_annotations</td><td>2.0.15</td></tr>
  <tr><td>com.google.api</td><td>gax-grpc</td><td>1.29.0</td></tr>
  <tr><td>com.google.api-client</td><td>google-api-client</td><td>1.27.0</td></tr>
  <tr><td>com.google.api-client</td><td>google-api-client-jackson2</td><td>1.27.0</td></tr>
  <tr><td>com.google.api-client</td><td>google-api-client-java6</td><td>1.27.0</td></tr>
  <tr><td>com.google.apis</td><td>google-api-services-bigquery</td><td>v2-rev20181104-1.27.0</td></tr>
  <tr><td>com.google.apis</td><td>google-api-services-clouddebugger</td><td>v2-rev20180801-1.27.0</td></tr>
  <tr><td>com.google.apis</td><td>google-api-services-cloudresourcemanager</td><td>v1-rev20181015-1.27.0</td></tr>
  <tr><td>com.google.apis</td><td>google-api-services-dataflow</td><td>v1b3-rev20181107-1.27.0</td></tr>
  <tr><td>com.google.apis</td><td>google-api-services-pubsub</td><td>v1-rev20181105-1.27.0</td></tr>
  <tr><td>com.google.apis</td><td>google-api-services-storage</td><td>v1-rev20181013-1.27.0</td></tr>
  <tr><td>com.google.auth</td><td>google-auth-library-credentials</td><td>0.10.0</td></tr>
  <tr><td>com.google.auth</td><td>google-auth-library-oauth2-http</td><td>0.10.0</td></tr>
  <tr><td>com.google.cloud</td><td>google-cloud-bigquery</td><td>1.27.0</td></tr>
  <tr><td>com.google.cloud</td><td>google-cloud-core</td><td>1.36.0</td></tr>
  <tr><td>com.google.cloud</td><td>google-cloud-core-grpc</td><td>1.36.0</td></tr>
  <tr><td>com.google.cloud.dataflow</td><td>google-cloud-dataflow-java-proto-library-all</td><td>0.5.160304</td></tr>
  <tr><td>com.google.cloud</td><td>google-cloud-spanner</td><td>0.54.0-beta</td></tr>
  <tr><td>com.google.http-client</td><td>google-http-client</td><td>1.27.0</td></tr>
  <tr><td>com.google.http-client</td><td>google-http-client-jackson</td><td>1.27.0</td></tr>
  <tr><td>com.google.http-client</td><td>google-http-client-jackson2</td><td>1.27.0</td></tr>
  <tr><td>com.google.http-client</td><td>google-http-client-protobuf</td><td>1.27.0</td></tr>
  <tr><td>com.google.oauth-client</td><td>google-oauth-client</td><td>1.27.0</td></tr>
  <tr><td>com.google.oauth-client</td><td>google-oauth-client-java6</td><td>1.27.0</td></tr>
  <tr><td>io.grpc</td><td>grpc-all</td><td>1.13.1</td></tr>
  <tr><td>io.grpc</td><td>grpc-auth</td><td>1.13.1</td></tr>
  <tr><td>io.grpc</td><td>grpc-core</td><td>1.13.1</td></tr>
  <tr><td>com.google.api.grpc</td><td>grpc-google-cloud-pubsub-v1</td><td>1.18.0</td></tr>
  <tr><td>io.grpc</td><td>grpc-netty</td><td>1.13.1</td></tr>
  <tr><td>io.grpc</td><td>grpc-protobuf-lite</td><td>1.13.1</td></tr>
  <tr><td>io.grpc</td><td>grpc-protobuf</td><td>1.13.1</td></tr>
  <tr><td>io.grpc</td><td>grpc-stub</td><td>1.13.1</td></tr>
  <tr><td>com.google.guava</td><td>guava</td><td>20.0</td></tr>
  <tr><td>com.google.guava</td><td>guava-testlib</td><td>20.0</td></tr>
  <tr><td>org.apache.hadoop</td><td>hadoop-client</td><td>2.7.3</td></tr>
  <tr><td>org.apache.hadoop</td><td>hadoop-common</td><td>2.7.3</td></tr>
  <tr><td>org.apache.hadoop</td><td>hadoop-hdfs</td><td>2.7.3</td></tr>
  <tr><td>org.apache.hadoop</td><td>hadoop-mapreduce-client-core</td><td>2.7.3</td></tr>
  <tr><td>org.apache.hadoop</td><td>hadoop-minicluster</td><td>2.7.3</td></tr>
  <tr><td>org.hamcrest</td><td>hamcrest-core</td><td>1.3</td></tr>
  <tr><td>org.hamcrest</td><td>hamcrest-library</td><td>1.3</td></tr>
  <tr><td>com.fasterxml.jackson.core</td><td>jackson-annotations</td><td>2.9.5</td></tr>
  <tr><td>com.fasterxml.jackson.core</td><td>jackson-core</td><td>2.9.5</td></tr>
  <tr><td>com.fasterxml.jackson.core</td><td>jackson-databind</td><td>2.9.5</td></tr>
  <tr><td>com.fasterxml.jackson.dataformat</td><td>jackson-dataformat-cbor</td><td>2.9.5</td></tr>
  <tr><td>com.fasterxml.jackson.dataformat</td><td>jackson-dataformat-yaml</td><td>2.9.5</td></tr>
  <tr><td>com.fasterxml.jackson.datatype</td><td>jackson-datatype-joda</td><td>2.9.5</td></tr>
  <tr><td>com.fasterxml.jackson.module</td><td>jackson-module-scala_2.11</td><td>2.9.5</td></tr>
  <tr><td>javax.xml.bind</td><td>jaxb-api</td><td>2.2.12</td></tr>
  <tr><td>joda-time</td><td>joda-time</td><td>2.4</td></tr>
  <tr><td>junit</td><td>junit</td><td>4.12</td></tr>
  <tr><td>org.apache.kafka</td><td>kafka_2.11</td><td>1.0.0</td></tr>
  <tr><td>org.apache.kafka</td><td>kafka-clients</td><td>1.0.0</td></tr>
  <tr><td>org.apache.apex</td><td>malhar-library</td><td>3.4.0</td></tr>
  <tr><td>org.mockito</td><td>mockito-core</td><td>1.10.19</td></tr>
  <tr><td>io.netty</td><td>netty-handler</td><td>4.1.25.Final</td></tr>
  <tr><td>io.netty</td><td>netty-tcnative-boringssl-static</td><td>2.0.8.Final</td></tr>
  <tr><td>io.netty</td><td>netty-transport-native-epoll</td><td>4.1.25.Final</td></tr>
  <tr><td>org.postgresql</td><td>postgresql</td><td>42.2.2</td></tr>
  <tr><td>org.powermock</td><td>powermock-mockito-release-full</td><td>1.6.4</td></tr>
  <tr><td>com.google.protobuf</td><td>protobuf-java</td><td>3.6.0</td></tr>
  <tr><td>com.google.protobuf</td><td>protobuf-java-util</td><td>3.6.0</td></tr>
  <tr><td>com.google.api.grpc</td><td>proto-google-cloud-pubsub-v1</td><td>1.18.0</td></tr>
  <tr><td>com.google.api.grpc</td><td>proto-google-cloud-spanner-admin-database-v1</td><td>0.19.0</td></tr>
  <tr><td>com.google.api.grpc</td><td>proto-google-common-protos</td><td>1.12.0</td></tr>
  <tr><td>org.slf4j</td><td>slf4j-api</td><td>1.7.25</td></tr>
  <tr><td>org.slf4j</td><td>slf4j-jdk14</td><td>1.7.25</td></tr>
  <tr><td>org.slf4j</td><td>slf4j-log4j12</td><td>1.7.25</td></tr>
  <tr><td>org.slf4j</td><td>slf4j-simple</td><td>1.7.25</td></tr>
  <tr><td>org.xerial.snappy</td><td>snappy-java</td><td>1.1.4</td></tr>
  <tr><td>org.apache.spark</td><td>spark-core_2.11</td><td>2.3.2</td></tr>
  <tr><td>org.apache.spark</td><td>spark-network-common_2.11</td><td>2.3.2</td></tr>
  <tr><td>org.apache.spark</td><td>spark-streaming_2.11</td><td>2.3.2</td></tr>
  <tr><td>org.codehaus.woodstox</td><td>stax2-api</td><td>3.1.4</td></tr>
  <tr><td>org.codehaus.woodstox</td><td>woodstox-core-asl</td><td>4.4.1</td></tr>
  <tr><td>com.pholser</td><td>junit-quickcheck-core</td><td>0.8</td></tr>
</table>

</details>

<details><summary markdown="span"><b>2.8.0</b></summary>

<p>Beam SDK for Java 2.8.0 has the following compile and runtime dependencies.</p>

<table class="table-bordered table-striped">
  <tr><th>GroupId</th><th>ArtifactId</th><th>Version</th></tr>
  <tr><td>org.apache.activemq</td><td>activemq-amqp</td><td>5.13.1</td></tr>
  <tr><td>org.apache.activemq</td><td>activemq-broker</td><td>5.13.1</td></tr>
  <tr><td>org.apache.activemq</td><td>activemq-client</td><td>5.13.1</td></tr>
  <tr><td>org.apache.activemq</td><td>activemq-jaas</td><td>5.13.1</td></tr>
  <tr><td>org.apache.activemq.tooling</td><td>activemq-junit</td><td>5.13.1</td></tr>
  <tr><td>org.apache.activemq</td><td>activemq-kahadb-store</td><td>5.13.1</td></tr>
  <tr><td>org.apache.activemq</td><td>activemq-mqtt</td><td>5.13.1</td></tr>
  <tr><td>org.apache.apex</td><td>apex-common</td><td>3.7.0</td></tr>
  <tr><td>org.apache.apex</td><td>apex-engine</td><td>3.7.0</td></tr>
  <tr><td>com.google.api</td><td>api-common</td><td>1.6.0</td></tr>
  <tr><td>args4j</td><td>args4j</td><td>2.33</td></tr>
  <tr><td>org.apache.avro</td><td>avro</td><td>1.8.2</td></tr>
  <tr><td>com.google.cloud.bigtable</td><td>bigtable-client-core</td><td>1.4.0</td></tr>
  <tr><td>net.bytebuddy</td><td>byte-buddy</td><td>1.8.11</td></tr>
  <tr><td>org.apache.commons</td><td>commons-compress</td><td>1.16.1</td></tr>
  <tr><td>org.apache.commons</td><td>commons-csv</td><td>1.4</td></tr>
  <tr><td>commons-io</td><td>commons-io</td><td>1.3.2</td></tr>
  <tr><td>commons-io</td><td>commons-io</td><td>2.5</td></tr>
  <tr><td>org.apache.commons</td><td>commons-lang3</td><td>3.6</td></tr>
  <tr><td>org.apache.commons</td><td>commons-math3</td><td>3.6.1</td></tr>
  <tr><td>com.google.cloud.datastore</td><td>datastore-v1-proto-client</td><td>1.6.0</td></tr>
  <tr><td>com.google.errorprone</td><td>error_prone_annotations</td><td>2.0.15</td></tr>
  <tr><td>com.google.api</td><td>gax-grpc</td><td>1.29.0</td></tr>
  <tr><td>com.google.cloud.bigdataoss</td><td>gcsio</td><td>1.9.0</td></tr>
  <tr><td>com.google.api-client</td><td>google-api-client-jackson2</td><td>1.24.1</td></tr>
  <tr><td>com.google.api-client</td><td>google-api-client-java6</td><td>1.24.1</td></tr>
  <tr><td>com.google.api-client</td><td>google-api-client</td><td>1.24.1</td></tr>
  <tr><td>com.google.apis</td><td>google-api-services-bigquery</td><td>v2-rev402-1.24.1</td></tr>
  <tr><td>com.google.apis</td><td>google-api-services-clouddebugger</td><td>v2-rev253-1.24.1</td></tr>
  <tr><td>com.google.apis</td><td>google-api-services-cloudresourcemanager</td><td>v1-rev502-1.24.1</td></tr>
  <tr><td>com.google.apis</td><td>google-api-services-dataflow</td><td>v1b3-rev257-1.24.1</td></tr>
  <tr><td>com.google.apis</td><td>google-api-services-pubsub</td><td>v1-rev399-1.24.1</td></tr>
  <tr><td>com.google.apis</td><td>google-api-services-storage</td><td>v1-rev136-1.24.1</td></tr>
  <tr><td>com.google.auth</td><td>google-auth-library-credentials</td><td>0.10.0</td></tr>
  <tr><td>com.google.auth</td><td>google-auth-library-oauth2-http</td><td>0.10.0</td></tr>
  <tr><td>com.google.cloud</td><td>google-cloud-core-grpc</td><td>1.36.0</td></tr>
  <tr><td>com.google.cloud</td><td>google-cloud-core</td><td>1.36.0</td></tr>
  <tr><td>com.google.cloud.dataflow</td><td>google-cloud-dataflow-java-proto-library-all</td><td>0.5.160304</td></tr>
  <tr><td>com.google.cloud</td><td>google-cloud-spanner</td><td>0.54.0-beta</td></tr>
  <tr><td>com.google.http-client</td><td>google-http-client-jackson2</td><td>1.24.1</td></tr>
  <tr><td>com.google.http-client</td><td>google-http-client-jackson</td><td>1.24.1</td></tr>
  <tr><td>com.google.http-client</td><td>google-http-client-protobuf</td><td>1.24.1</td></tr>
  <tr><td>com.google.http-client</td><td>google-http-client</td><td>1.24.1</td></tr>
  <tr><td>com.google.oauth-client</td><td>google-oauth-client-java6</td><td>1.24.1</td></tr>
  <tr><td>com.google.oauth-client</td><td>google-oauth-client</td><td>1.24.1</td></tr>
  <tr><td>io.grpc</td><td>grpc-all</td><td>1.13.1</td></tr>
  <tr><td>io.grpc</td><td>grpc-auth</td><td>1.13.1</td></tr>
  <tr><td>io.grpc</td><td>grpc-core</td><td>1.13.1</td></tr>
  <tr><td>com.google.api.grpc</td><td>grpc-google-cloud-bigtable-v2</td><td>0.19.0</td></tr>
  <tr><td>com.google.api.grpc</td><td>grpc-google-cloud-pubsub-v1</td><td>1.18.0</td></tr>
  <tr><td>io.grpc</td><td>grpc-netty</td><td>1.13.1</td></tr>
  <tr><td>io.grpc</td><td>grpc-protobuf-lite</td><td>1.13.1</td></tr>
  <tr><td>io.grpc</td><td>grpc-protobuf</td><td>1.13.1</td></tr>
  <tr><td>io.grpc</td><td>grpc-stub</td><td>1.13.1</td></tr>
  <tr><td>com.google.guava</td><td>guava</td><td>20.0</td></tr>
  <tr><td>com.google.guava</td><td>guava-testlib</td><td>20.0</td></tr>
  <tr><td>org.apache.hadoop</td><td>hadoop-client</td><td>2.7.3</td></tr>
  <tr><td>org.apache.hadoop</td><td>hadoop-common</td><td>2.7.3</td></tr>
  <tr><td>org.apache.hadoop</td><td>hadoop-hdfs</td><td>2.7.3</td></tr>
  <tr><td>org.apache.hadoop</td><td>hadoop-hdfs</td><td>2.7.3</td></tr>
  <tr><td>org.apache.hadoop</td><td>hadoop-mapreduce-client-core</td><td>2.7.3</td></tr>
  <tr><td>org.apache.hadoop</td><td>hadoop-minicluster</td><td>2.7.3</td></tr>
  <tr><td>org.hamcrest</td><td>hamcrest-core</td><td>1.3</td></tr>
  <tr><td>org.hamcrest</td><td>hamcrest-library</td><td>1.3</td></tr>
  <tr><td>com.fasterxml.jackson.core</td><td>jackson-annotations</td><td>2.9.5</td></tr>
  <tr><td>com.fasterxml.jackson.core</td><td>jackson-core</td><td>2.9.5</td></tr>
  <tr><td>com.fasterxml.jackson.core</td><td>jackson-databind</td><td>2.9.5</td></tr>
  <tr><td>com.fasterxml.jackson.dataformat</td><td>jackson-dataformat-cbor</td><td>2.9.5</td></tr>
  <tr><td>com.fasterxml.jackson.dataformat</td><td>jackson-dataformat-yaml</td><td>2.9.5</td></tr>
  <tr><td>com.fasterxml.jackson.datatype</td><td>jackson-datatype-joda</td><td>2.9.5</td></tr>
  <tr><td>com.fasterxml.jackson.module</td><td>jackson-module-scala_2.11</td><td>2.9.5</td></tr>
  <tr><td>javax.xml.bind</td><td>jaxb-api</td><td>2.2.12</td></tr>
  <tr><td>joda-time</td><td>joda-time</td><td>2.4</td></tr>
  <tr><td>com.pholser</td><td>junit-quickcheck-core</td><td>0.8</td></tr>
  <tr><td>junit</td><td>junit</td><td>4.12</td></tr>
  <tr><td>org.apache.kafka</td><td>kafka_2.11</td><td>1.0.0</td></tr>
  <tr><td>org.apache.kafka</td><td>kafka-clients</td><td>1.0.0</td></tr>
  <tr><td>org.apache.apex</td><td>malhar-library</td><td>3.4.0</td></tr>
  <tr><td>org.mockito</td><td>mockito-core</td><td>1.10.19</td></tr>
  <tr><td>io.netty</td><td>netty-handler</td><td>4.1.25.Final</td></tr>
  <tr><td>io.netty</td><td>netty-tcnative-boringssl-static</td><td>2.0.8.Final</td></tr>
  <tr><td>io.netty</td><td>netty-transport-native-epoll</td><td>4.1.25.Final</td></tr>
  <tr><td>org.postgresql</td><td>postgresql</td><td>42.2.2</td></tr>
  <tr><td>org.powermock</td><td>powermock-mockito-release-full</td><td>1.6.4</td></tr>
  <tr><td>com.google.protobuf</td><td>protobuf-java</td><td>3.6.0</td></tr>
  <tr><td>com.google.protobuf</td><td>protobuf-java-util</td><td>3.6.0</td></tr>
  <tr><td>com.google.api.grpc</td><td>proto-google-cloud-datastore-v1</td><td>0.19.0</td></tr>
  <tr><td>com.google.api.grpc</td><td>proto-google-cloud-pubsub-v1</td><td>1.18.0</td></tr>
  <tr><td>com.google.api.grpc</td><td>proto-google-cloud-spanner-admin-database-v1</td><td>0.19.0</td></tr>
  <tr><td>com.google.api.grpc</td><td>proto-google-common-protos</td><td>1.12.0</td></tr>
  <tr><td>org.slf4j</td><td>slf4j-api</td><td>1.7.25</td></tr>
  <tr><td>org.slf4j</td><td>slf4j-jdk14</td><td>1.7.25</td></tr>
  <tr><td>org.slf4j</td><td>slf4j-log4j12</td><td>1.7.25</td></tr>
  <tr><td>org.slf4j</td><td>slf4j-simple</td><td>1.7.25</td></tr>
  <tr><td>org.xerial.snappy</td><td>snappy-java</td><td>1.1.4</td></tr>
  <tr><td>org.apache.spark</td><td>spark-core_2.11</td><td>2.3.1</td></tr>
  <tr><td>org.apache.spark</td><td>spark-network-common_2.11</td><td>2.3.1</td></tr>
  <tr><td>org.apache.spark</td><td>spark-streaming_2.11</td><td>2.3.1</td></tr>
  <tr><td>org.codehaus.woodstox</td><td>stax2-api</td><td>3.1.4</td></tr>
  <tr><td>com.google.cloud.bigdataoss</td><td>util</td><td>1.9.0</td></tr>
  <tr><td>org.codehaus.woodstox</td><td>woodstox-core-asl</td><td>4.4.1</td></tr>
</table>

</details>

<details><summary markdown="span"><b>2.7.0</b></summary>

<p>Beam SDK for Java 2.7.0 has the following compile and runtime dependencies.</p>

<table class="table-bordered table-striped">
  <tr><th>GroupId</th><th>ArtifactId</th><th>Version</th></tr>
  <tr><td>org.apache.activemq</td><td>activemq-amqp</td><td>5.13.1</td></tr>
  <tr><td>org.apache.activemq</td><td>activemq-broker</td><td>5.13.1</td></tr>
  <tr><td>org.apache.activemq</td><td>activemq-client</td><td>5.13.1</td></tr>
  <tr><td>org.apache.activemq</td><td>activemq-jaas</td><td>5.13.1</td></tr>
  <tr><td>org.apache.activemq.tooling</td><td>activemq-junit</td><td>5.13.1</td></tr>
  <tr><td>org.apache.activemq</td><td>activemq-kahadb-store</td><td>5.13.1</td></tr>
  <tr><td>org.apache.activemq</td><td>activemq-mqtt</td><td>5.13.1</td></tr>
  <tr><td>org.apache.apex</td><td>apex-common</td><td>3.7.0</td></tr>
  <tr><td>org.apache.apex</td><td>apex-engine</td><td>3.7.0</td></tr>
  <tr><td>com.google.api</td><td>api-common</td><td>1.6.0</td></tr>
  <tr><td>args4j</td><td>args4j</td><td>2.33</td></tr>
  <tr><td>org.apache.avro</td><td>avro</td><td>1.8.2</td></tr>
  <tr><td>com.google.cloud.bigtable</td><td>bigtable-client-core</td><td>1.4.0</td></tr>
  <tr><td>net.bytebuddy</td><td>byte-buddy</td><td>1.8.11</td></tr>
  <tr><td>org.apache.commons</td><td>commons-compress</td><td>1.16.1</td></tr>
  <tr><td>org.apache.commons</td><td>commons-csv</td><td>1.4</td></tr>
  <tr><td>commons-io</td><td>commons-io</td><td>1.3.2</td></tr>
  <tr><td>commons-io</td><td>commons-io</td><td>2.5</td></tr>
  <tr><td>org.apache.commons</td><td>commons-lang3</td><td>3.6</td></tr>
  <tr><td>org.apache.commons</td><td>commons-math3</td><td>3.6.1</td></tr>
  <tr><td>com.google.cloud.datastore</td><td>datastore-v1-proto-client</td><td>1.6.0</td></tr>
  <tr><td>com.google.errorprone</td><td>error_prone_annotations</td><td>2.0.15</td></tr>
  <tr><td>com.google.api</td><td>gax-grpc</td><td>1.29.0</td></tr>
  <tr><td>com.google.cloud.bigdataoss</td><td>gcsio</td><td>1.9.0</td></tr>
  <tr><td>com.google.api-client</td><td>google-api-client-jackson2</td><td>1.23.0</td></tr>
  <tr><td>com.google.api-client</td><td>google-api-client-java6</td><td>1.23.0</td></tr>
  <tr><td>com.google.api-client</td><td>google-api-client</td><td>1.23.0</td></tr>
  <tr><td>com.google.apis</td><td>google-api-services-bigquery</td><td>v2-rev374-1.23.0</td></tr>
  <tr><td>com.google.apis</td><td>google-api-services-clouddebugger</td><td>v2-rev233-1.23.0</td></tr>
  <tr><td>com.google.apis</td><td>google-api-services-cloudresourcemanager</td><td>v1-rev477-1.23.0</td></tr>
  <tr><td>com.google.apis</td><td>google-api-services-dataflow</td><td>v1b3-rev221-1.23.0</td></tr>
  <tr><td>com.google.apis</td><td>google-api-services-pubsub</td><td>v1-rev382-1.23.0</td></tr>
  <tr><td>com.google.apis</td><td>google-api-services-storage</td><td>v1-rev124-1.23.0</td></tr>
  <tr><td>com.google.auth</td><td>google-auth-library-credentials</td><td>0.10.0</td></tr>
  <tr><td>com.google.auth</td><td>google-auth-library-oauth2-http</td><td>0.10.0</td></tr>
  <tr><td>com.google.cloud</td><td>google-cloud-core-grpc</td><td>1.36.0</td></tr>
  <tr><td>com.google.cloud</td><td>google-cloud-core</td><td>1.36.0</td></tr>
  <tr><td>com.google.cloud.dataflow</td><td>google-cloud-dataflow-java-proto-library-all</td><td>0.5.160304</td></tr>
  <tr><td>com.google.cloud</td><td>google-cloud-spanner</td><td>0.54.0-beta</td></tr>
  <tr><td>com.google.http-client</td><td>google-http-client-jackson2</td><td>1.23.0</td></tr>
  <tr><td>com.google.http-client</td><td>google-http-client-jackson</td><td>1.23.0</td></tr>
  <tr><td>com.google.http-client</td><td>google-http-client-protobuf</td><td>1.23.0</td></tr>
  <tr><td>com.google.http-client</td><td>google-http-client</td><td>1.23.0</td></tr>
  <tr><td>com.google.oauth-client</td><td>google-oauth-client-java6</td><td>1.23.0</td></tr>
  <tr><td>com.google.oauth-client</td><td>google-oauth-client</td><td>1.23.0</td></tr>
  <tr><td>io.grpc</td><td>grpc-all</td><td>1.13.1</td></tr>
  <tr><td>io.grpc</td><td>grpc-auth</td><td>1.13.1</td></tr>
  <tr><td>io.grpc</td><td>grpc-core</td><td>1.13.1</td></tr>
  <tr><td>com.google.api.grpc</td><td>grpc-google-cloud-bigtable-v2</td><td>0.19.0</td></tr>
  <tr><td>com.google.api.grpc</td><td>grpc-google-cloud-pubsub-v1</td><td>1.18.0</td></tr>
  <tr><td>io.grpc</td><td>grpc-netty</td><td>1.13.1</td></tr>
  <tr><td>io.grpc</td><td>grpc-protobuf-lite</td><td>1.13.1</td></tr>
  <tr><td>io.grpc</td><td>grpc-protobuf</td><td>1.13.1</td></tr>
  <tr><td>io.grpc</td><td>grpc-stub</td><td>1.13.1</td></tr>
  <tr><td>com.google.guava</td><td>guava</td><td>20.0</td></tr>
  <tr><td>com.google.guava</td><td>guava-testlib</td><td>20.0</td></tr>
  <tr><td>org.apache.hadoop</td><td>hadoop-client</td><td>2.7.3</td></tr>
  <tr><td>org.apache.hadoop</td><td>hadoop-common</td><td>2.7.3</td></tr>
  <tr><td>org.apache.hadoop</td><td>hadoop-hdfs</td><td>2.7.3</td></tr>
  <tr><td>org.apache.hadoop</td><td>hadoop-hdfs</td><td>2.7.3</td></tr>
  <tr><td>org.apache.hadoop</td><td>hadoop-mapreduce-client-core</td><td>2.7.3</td></tr>
  <tr><td>org.apache.hadoop</td><td>hadoop-minicluster</td><td>2.7.3</td></tr>
  <tr><td>org.hamcrest</td><td>hamcrest-core</td><td>1.3</td></tr>
  <tr><td>org.hamcrest</td><td>hamcrest-library</td><td>1.3</td></tr>
  <tr><td>com.fasterxml.jackson.core</td><td>jackson-annotations</td><td>2.9.5</td></tr>
  <tr><td>com.fasterxml.jackson.core</td><td>jackson-core</td><td>2.9.5</td></tr>
  <tr><td>com.fasterxml.jackson.core</td><td>jackson-databind</td><td>2.9.5</td></tr>
  <tr><td>com.fasterxml.jackson.dataformat</td><td>jackson-dataformat-cbor</td><td>2.9.5</td></tr>
  <tr><td>com.fasterxml.jackson.dataformat</td><td>jackson-dataformat-yaml</td><td>2.9.5</td></tr>
  <tr><td>com.fasterxml.jackson.datatype</td><td>jackson-datatype-joda</td><td>2.9.5</td></tr>
  <tr><td>com.fasterxml.jackson.module</td><td>jackson-module-scala_2.11</td><td>2.9.5</td></tr>
  <tr><td>javax.xml.bind</td><td>jaxb-api</td><td>2.2.12</td></tr>
  <tr><td>joda-time</td><td>joda-time</td><td>2.4</td></tr>
  <tr><td>com.pholser</td><td>junit-quickcheck-core</td><td>0.8</td></tr>
  <tr><td>junit</td><td>junit</td><td>4.12</td></tr>
  <tr><td>org.apache.kafka</td><td>kafka_2.11</td><td>1.0.0</td></tr>
  <tr><td>org.apache.kafka</td><td>kafka-clients</td><td>1.0.0</td></tr>
  <tr><td>org.apache.apex</td><td>malhar-library</td><td>3.4.0</td></tr>
  <tr><td>org.mockito</td><td>mockito-core</td><td>1.10.19</td></tr>
  <tr><td>io.netty</td><td>netty-handler</td><td>4.1.25.Final</td></tr>
  <tr><td>io.netty</td><td>netty-tcnative-boringssl-static</td><td>2.0.8.Final</td></tr>
  <tr><td>io.netty</td><td>netty-transport-native-epoll</td><td>4.1.25.Final</td></tr>
  <tr><td>org.postgresql</td><td>postgresql</td><td>42.2.2</td></tr>
  <tr><td>org.powermock</td><td>powermock-mockito-release-full</td><td>1.6.4</td></tr>
  <tr><td>com.google.protobuf</td><td>protobuf-java</td><td>3.6.0</td></tr>
  <tr><td>com.google.protobuf</td><td>protobuf-java-util</td><td>3.6.0</td></tr>
  <tr><td>com.google.api.grpc</td><td>proto-google-cloud-datastore-v1</td><td>0.19.0</td></tr>
  <tr><td>com.google.api.grpc</td><td>proto-google-cloud-pubsub-v1</td><td>1.18.0</td></tr>
  <tr><td>com.google.api.grpc</td><td>proto-google-cloud-spanner-admin-database-v1</td><td>0.19.0</td></tr>
  <tr><td>com.google.api.grpc</td><td>proto-google-common-protos</td><td>1.12.0</td></tr>
  <tr><td>org.slf4j</td><td>slf4j-api</td><td>1.7.25</td></tr>
  <tr><td>org.slf4j</td><td>slf4j-jdk14</td><td>1.7.25</td></tr>
  <tr><td>org.slf4j</td><td>slf4j-log4j12</td><td>1.7.25</td></tr>
  <tr><td>org.slf4j</td><td>slf4j-simple</td><td>1.7.25</td></tr>
  <tr><td>org.xerial.snappy</td><td>snappy-java</td><td>1.1.4</td></tr>
  <tr><td>org.apache.spark</td><td>spark-core_2.11</td><td>2.3.1</td></tr>
  <tr><td>org.apache.spark</td><td>spark-network-common_2.11</td><td>2.3.1</td></tr>
  <tr><td>org.apache.spark</td><td>spark-streaming_2.11</td><td>2.3.1</td></tr>
  <tr><td>org.codehaus.woodstox</td><td>stax2-api</td><td>3.1.4</td></tr>
  <tr><td>com.google.cloud.bigdataoss</td><td>util</td><td>1.9.0</td></tr>
  <tr><td>org.codehaus.woodstox</td><td>woodstox-core-asl</td><td>4.4.1</td></tr>
</table>

</details>

