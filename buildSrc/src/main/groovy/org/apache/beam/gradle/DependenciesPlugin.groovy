/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * License); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.beam.gradle

import org.gradle.api.Plugin
import org.gradle.api.Project

class DependenciesPlugin implements Plugin<Project> {
  // These versions are defined here because they represent
  // a dependency version which should match across multiple
  // Maven artifacts.
  static def activemq_version = "5.14.5"
  static def autovalue_version = "1.7.4"
  static def aws_java_sdk_version = "1.11.718"
  static def aws_java_sdk2_version = "2.13.54"
  static def cassandra_driver_version = "3.10.2"
  static def checkerframework_version = "3.10.0"
  static def classgraph_version = "4.8.65"
  static def google_clients_version = "1.30.10"
  static def google_cloud_bigdataoss_version = "2.1.6"
  static def google_cloud_pubsublite_version = "0.7.0"
  static def google_code_gson_version = "2.8.6"
  static def google_oauth_clients_version = "1.31.0"
  // Try to keep grpc_version consistent with gRPC version in google_cloud_platform_libraries_bom
  static def grpc_version = "1.35.0"
  static def guava_version = "30.1-jre"
  static def hadoop_version = "2.10.1"
  static def hamcrest_version = "2.1"
  static def influxdb_version = "2.19"
  static def httpclient_version = "4.5.10"
  static def httpcore_version = "4.4.12"
  static def jackson_version = "2.12.1"
  static def jaxb_api_version = "2.3.3"
  static def jsr305_version = "3.0.2"
  static def kafka_version = "2.4.1"
  static def nemo_version = "0.1"
  static def netty_version = "4.1.52.Final"
  static def postgres_version = "42.2.16"
  static def powermock_version = "2.0.9"
  static def protobuf_version = "3.12.0"
  static def quickcheck_version = "0.8"
  static def slf4j_version = "1.7.30"
  static def spark_version = "2.4.7"
  static def spotbugs_version = "4.0.6"
  static def testcontainers_version = "1.15.1"

  @Override
  void apply(Project project) {
    // Define and export a map dependencies shared across multiple sub-projects.
    //
    // Example usage:
    // configuration {
    //   compile library.java.avro
    //   testCompile library.java.junit
    // }


    // A map of maps containing common libraries used per language. To use:
    // dependencies {
    //   compile library.java.slf4j_api
    // }
    project.ext.library = [
      java  : [
        activemq_amqp                               : "org.apache.activemq:activemq-amqp:$activemq_version",
        activemq_broker                             : "org.apache.activemq:activemq-broker:$activemq_version",
        activemq_client                             : "org.apache.activemq:activemq-client:$activemq_version",
        activemq_jaas                               : "org.apache.activemq:activemq-jaas:$activemq_version",
        activemq_junit                              : "org.apache.activemq.tooling:activemq-junit:$activemq_version",
        activemq_kahadb_store                       : "org.apache.activemq:activemq-kahadb-store:$activemq_version",
        activemq_mqtt                               : "org.apache.activemq:activemq-mqtt:$activemq_version",
        antlr                                       : "org.antlr:antlr4:4.7",
        antlr_runtime                               : "org.antlr:antlr4-runtime:4.7",
        args4j                                      : "args4j:args4j:2.33",
        auto_value_annotations                      : "com.google.auto.value:auto-value-annotations:$autovalue_version",
        avro                                        : "org.apache.avro:avro:1.8.2",
        avro_tests                                  : "org.apache.avro:avro:1.8.2:tests",
        aws_java_sdk_cloudwatch                     : "com.amazonaws:aws-java-sdk-cloudwatch:$aws_java_sdk_version",
        aws_java_sdk_core                           : "com.amazonaws:aws-java-sdk-core:$aws_java_sdk_version",
        aws_java_sdk_dynamodb                       : "com.amazonaws:aws-java-sdk-dynamodb:$aws_java_sdk_version",
        aws_java_sdk_kinesis                        : "com.amazonaws:aws-java-sdk-kinesis:$aws_java_sdk_version",
        aws_java_sdk_s3                             : "com.amazonaws:aws-java-sdk-s3:$aws_java_sdk_version",
        aws_java_sdk_sns                            : "com.amazonaws:aws-java-sdk-sns:$aws_java_sdk_version",
        aws_java_sdk_sqs                            : "com.amazonaws:aws-java-sdk-sqs:$aws_java_sdk_version",
        aws_java_sdk_sts                            : "com.amazonaws:aws-java-sdk-sts:$aws_java_sdk_version",
        aws_java_sdk2_apache_client                 : "software.amazon.awssdk:apache-client:$aws_java_sdk2_version",
        aws_java_sdk2_auth                          : "software.amazon.awssdk:auth:$aws_java_sdk2_version",
        aws_java_sdk2_cloudwatch                    : "software.amazon.awssdk:cloudwatch:$aws_java_sdk2_version",
        aws_java_sdk2_dynamodb                      : "software.amazon.awssdk:dynamodb:$aws_java_sdk2_version",
        aws_java_sdk2_kinesis                       : "software.amazon.awssdk:kinesis:$aws_java_sdk2_version",
        aws_java_sdk2_sdk_core                      : "software.amazon.awssdk:sdk-core:$aws_java_sdk2_version",
        aws_java_sdk2_sns                           : "software.amazon.awssdk:sns:$aws_java_sdk2_version",
        aws_java_sdk2_sqs                           : "software.amazon.awssdk:sqs:$aws_java_sdk2_version",
        bigdataoss_gcsio                            : "com.google.cloud.bigdataoss:gcsio:$google_cloud_bigdataoss_version",
        bigdataoss_util                             : "com.google.cloud.bigdataoss:util:$google_cloud_bigdataoss_version",
        cassandra_driver_core                       : "com.datastax.cassandra:cassandra-driver-core:$cassandra_driver_version",
        cassandra_driver_mapping                    : "com.datastax.cassandra:cassandra-driver-mapping:$cassandra_driver_version",
        classgraph                                  : "io.github.classgraph:classgraph:$classgraph_version",
        commons_codec                               : "commons-codec:commons-codec:1.14",
        commons_compress                            : "org.apache.commons:commons-compress:1.20",
        commons_csv                                 : "org.apache.commons:commons-csv:1.8",
        commons_io                                  : "commons-io:commons-io:2.6",
        commons_lang3                               : "org.apache.commons:commons-lang3:3.9",
        commons_math3                               : "org.apache.commons:commons-math3:3.6.1",
        error_prone_annotations                     : "com.google.errorprone:error_prone_annotations:2.3.1",
        gax                                         : "com.google.api:gax", // google_cloud_platform_libraries_bom sets version
        gax_grpc                                    : "com.google.api:gax-grpc", // google_cloud_platform_libraries_bom sets version
        google_api_client                           : "com.google.api-client:google-api-client:$google_clients_version",
        google_api_client_jackson2                  : "com.google.api-client:google-api-client-jackson2:$google_clients_version",
        google_api_client_java6                     : "com.google.api-client:google-api-client-java6:$google_clients_version",
        google_api_common                           : "com.google.api:api-common", // google_cloud_platform_libraries_bom sets version
        google_api_services_bigquery                : "com.google.apis:google-api-services-bigquery:v2-rev20201030-$google_clients_version",
        google_api_services_clouddebugger           : "com.google.apis:google-api-services-clouddebugger:v2-rev20200501-$google_clients_version",
        google_api_services_cloudresourcemanager    : "com.google.apis:google-api-services-cloudresourcemanager:v1-rev20200720-$google_clients_version",
        google_api_services_dataflow                : "com.google.apis:google-api-services-dataflow:v1b3-rev20200713-$google_clients_version",
        google_api_services_healthcare              : "com.google.apis:google-api-services-healthcare:v1beta1-rev20200713-$google_clients_version",
        google_api_services_pubsub                  : "com.google.apis:google-api-services-pubsub:v1-rev20200713-$google_clients_version",
        google_api_services_storage                 : "com.google.apis:google-api-services-storage:v1-rev20200611-$google_clients_version",
        google_auth_library_credentials             : "com.google.auth:google-auth-library-credentials", // google_cloud_platform_libraries_bom sets version
        google_auth_library_oauth2_http             : "com.google.auth:google-auth-library-oauth2-http", // google_cloud_platform_libraries_bom sets version
        google_cloud_bigquery                       : "com.google.cloud:google-cloud-bigquery", // google_cloud_platform_libraries_bom sets version
        google_cloud_bigquery_storage               : "com.google.cloud:google-cloud-bigquerystorage:1.8.5",
        google_cloud_bigtable_client_core           : "com.google.cloud.bigtable:bigtable-client-core:1.16.0",
        google_cloud_bigtable_emulator              : "com.google.cloud:google-cloud-bigtable-emulator:0.125.2",
        google_cloud_core                           : "com.google.cloud:google-cloud-core", // google_cloud_platform_libraries_bom sets version
        google_cloud_core_grpc                      : "com.google.cloud:google-cloud-core-grpc", // google_cloud_platform_libraries_bom sets version
        google_cloud_datacatalog_v1beta1            : "com.google.cloud:google-cloud-datacatalog", // google_cloud_platform_libraries_bom sets version
        google_cloud_dataflow_java_proto_library_all: "com.google.cloud.dataflow:google-cloud-dataflow-java-proto-library-all:0.5.160304",
        google_cloud_datastore_v1_proto_client      : "com.google.cloud.datastore:datastore-v1-proto-client:1.6.3",
        google_cloud_pubsub                         : "com.google.cloud:google-cloud-pubsub", // google_cloud_platform_libraries_bom sets version
        google_cloud_pubsublite                     : "com.google.cloud:google-cloud-pubsublite:$google_cloud_pubsublite_version",
        // The GCP Libraries BOM dashboard shows the versions set by the BOM:
        // https://storage.googleapis.com/cloud-opensource-java-dashboard/com.google.cloud/libraries-bom/16.3.0/artifact_details.html
        // Update libraries-bom version on sdks/java/container/license_scripts/dep_urls_java.yaml
        google_cloud_platform_libraries_bom         : "com.google.cloud:libraries-bom:16.3.0",
        google_cloud_spanner                        : "com.google.cloud:google-cloud-spanner", // google_cloud_platform_libraries_bom sets version
        google_code_gson                            : "com.google.code.gson:gson:$google_code_gson_version",
        // google-http-client's version is explicitly declared for sdks/java/maven-archetypes/examples
        // This version should be in line with the one in com.google.cloud:libraries-bom.
        google_http_client                          : "com.google.http-client:google-http-client", // google_cloud_platform_libraries_bom sets version
        google_http_client_apache_v2                : "com.google.http-client:google-http-client-apache-v2", // google_cloud_platform_libraries_bom sets version
        google_http_client_jackson                  : "com.google.http-client:google-http-client-jackson:1.29.2",
        google_http_client_jackson2                 : "com.google.http-client:google-http-client-jackson2", // google_cloud_platform_libraries_bom sets version
        google_http_client_protobuf                 : "com.google.http-client:google-http-client-protobuf", // google_cloud_platform_libraries_bom sets version
        google_oauth_client                         : "com.google.oauth-client:google-oauth-client:$google_oauth_clients_version",
        google_oauth_client_java6                   : "com.google.oauth-client:google-oauth-client-java6:$google_oauth_clients_version",
        // Don't use grpc_all, it can cause issues in Bazel builds. Reference the gRPC libraries you need individually instead.
        grpc_alts                                   : "io.grpc:grpc-alts", // google_cloud_platform_libraries_bom sets version
        grpc_api                                    : "io.grpc:grpc-api", // google_cloud_platform_libraries_bom sets version
        grpc_auth                                   : "io.grpc:grpc-auth", // google_cloud_platform_libraries_bom sets version
        grpc_context                                : "io.grpc:grpc-context", // google_cloud_platform_libraries_bom sets version
        grpc_core                                   : "io.grpc:grpc-core", // google_cloud_platform_libraries_bom sets version
        grpc_google_cloud_pubsub_v1                 : "com.google.api.grpc:grpc-google-cloud-pubsub-v1", // google_cloud_platform_libraries_bom sets version
        grpc_google_cloud_pubsublite_v1             : "com.google.api.grpc:grpc-google-cloud-pubsublite-v1:$google_cloud_pubsublite_version",
        grpc_google_common_protos                   : "com.google.api.grpc:grpc-google-common-protos", // google_cloud_platform_libraries_bom sets version
        grpc_grpclb                                 : "io.grpc:grpc-grpclb", // google_cloud_platform_libraries_bom sets version
        grpc_protobuf                               : "io.grpc:grpc-protobuf", // google_cloud_platform_libraries_bom sets version
        grpc_protobuf_lite                          : "io.grpc:grpc-protobuf-lite:$grpc_version",
        grpc_netty                                  : "io.grpc:grpc-netty", // google_cloud_platform_libraries_bom sets version
        grpc_netty_shaded                           : "io.grpc:grpc-netty-shaded", // google_cloud_platform_libraries_bom sets version
        grpc_stub                                   : "io.grpc:grpc-stub", // google_cloud_platform_libraries_bom sets version
        guava                                       : "com.google.guava:guava:$guava_version",
        guava_testlib                               : "com.google.guava:guava-testlib:$guava_version",
        hadoop_client                               : "org.apache.hadoop:hadoop-client:$hadoop_version",
        hadoop_common                               : "org.apache.hadoop:hadoop-common:$hadoop_version",
        hadoop_mapreduce_client_core                : "org.apache.hadoop:hadoop-mapreduce-client-core:$hadoop_version",
        hadoop_minicluster                          : "org.apache.hadoop:hadoop-minicluster:$hadoop_version",
        hadoop_hdfs                                 : "org.apache.hadoop:hadoop-hdfs:$hadoop_version",
        hadoop_hdfs_tests                           : "org.apache.hadoop:hadoop-hdfs:$hadoop_version:tests",
        hamcrest                                    : "org.hamcrest:hamcrest:$hamcrest_version",
        hamcrest_core                               : "org.hamcrest:hamcrest-core:$hamcrest_version",
        hamcrest_library                            : "org.hamcrest:hamcrest-library:$hamcrest_version",
        http_client                                 : "org.apache.httpcomponents:httpclient:$httpclient_version",
        http_core                                   : "org.apache.httpcomponents:httpcore:$httpcore_version",
        influxdb_library                            : "org.influxdb:influxdb-java:$influxdb_version",
        jackson_annotations                         : "com.fasterxml.jackson.core:jackson-annotations:$jackson_version",
        jackson_jaxb_annotations                    : "com.fasterxml.jackson.module:jackson-module-jaxb-annotations:$jackson_version",
        jackson_core                                : "com.fasterxml.jackson.core:jackson-core:$jackson_version",
        jackson_databind                            : "com.fasterxml.jackson.core:jackson-databind:$jackson_version",
        jackson_dataformat_cbor                     : "com.fasterxml.jackson.dataformat:jackson-dataformat-cbor:$jackson_version",
        jackson_dataformat_csv                      : "com.fasterxml.jackson.dataformat:jackson-dataformat-csv:$jackson_version",
        jackson_dataformat_xml                      : "com.fasterxml.jackson.dataformat:jackson-dataformat-xml:$jackson_version",
        jackson_dataformat_yaml                     : "com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:$jackson_version",
        jackson_datatype_joda                       : "com.fasterxml.jackson.datatype:jackson-datatype-joda:$jackson_version",
        jackson_module_scala                        : "com.fasterxml.jackson.module:jackson-module-scala_2.11:$jackson_version",
        jaxb_api                                    : "jakarta.xml.bind:jakarta.xml.bind-api:$jaxb_api_version",
        jaxb_impl                                   : "com.sun.xml.bind:jaxb-impl:$jaxb_api_version",
        joda_time                                   : "joda-time:joda-time:2.10.10",
        jsonassert                                  : "org.skyscreamer:jsonassert:1.5.0",
        jsr305                                      : "com.google.code.findbugs:jsr305:$jsr305_version",
        junit                                       : "junit:junit:4.13.1",
        kafka                                       : "org.apache.kafka:kafka_2.11:$kafka_version",
        kafka_clients                               : "org.apache.kafka:kafka-clients:$kafka_version",
        mockito_core                                : "org.mockito:mockito-core:3.7.7",
        mongo_java_driver                           : "org.mongodb:mongo-java-driver:3.12.7",
        nemo_compiler_frontend_beam                 : "org.apache.nemo:nemo-compiler-frontend-beam:$nemo_version",
        netty_all                                   : "io.netty:netty-all:$netty_version",
        netty_handler                               : "io.netty:netty-handler:$netty_version",
        netty_tcnative_boringssl_static             : "io.netty:netty-tcnative-boringssl-static:2.0.33.Final",
        netty_transport_native_epoll                : "io.netty:netty-transport-native-epoll:$netty_version",
        postgres                                    : "org.postgresql:postgresql:$postgres_version",
        powermock                                   : "org.powermock:powermock-module-junit4:$powermock_version",
        powermock_mockito                           : "org.powermock:powermock-api-mockito2:$powermock_version",
        protobuf_java                               : "com.google.protobuf:protobuf-java:$protobuf_version",
        protobuf_java_util                          : "com.google.protobuf:protobuf-java-util:$protobuf_version",
        proto_google_cloud_bigquery_storage_v1      : "com.google.api.grpc:proto-google-cloud-bigquerystorage-v1", // google_cloud_platform_libraries_bom sets version
        proto_google_cloud_bigquerybeta2_storage_v1 : "com.google.api.grpc:proto-google-cloud-bigquerystorage-v1beta2", // google_cloud_platform_libraries_bom sets version
        proto_google_cloud_bigtable_admin_v2        : "com.google.api.grpc:proto-google-cloud-bigtable-admin-v2", // google_cloud_platform_libraries_bom sets version
        proto_google_cloud_bigtable_v2              : "com.google.api.grpc:proto-google-cloud-bigtable-v2", // google_cloud_platform_libraries_bom sets version
        proto_google_cloud_datacatalog_v1beta1      : "com.google.api.grpc:proto-google-cloud-datacatalog-v1beta1", // google_cloud_platform_libraries_bom sets version
        proto_google_cloud_datastore_v1             : "com.google.api.grpc:proto-google-cloud-datastore-v1", // google_cloud_platform_libraries_bom sets version
        proto_google_cloud_pubsub_v1                : "com.google.api.grpc:proto-google-cloud-pubsub-v1", // google_cloud_platform_libraries_bom sets version
        proto_google_cloud_pubsublite_v1            : "com.google.api.grpc:proto-google-cloud-pubsublite-v1:$google_cloud_pubsublite_version",
        proto_google_cloud_spanner_v1               : "com.google.api.grpc:proto-google-cloud-spanner-v1", // google_cloud_platform_libraries_bom sets version
        proto_google_cloud_spanner_admin_database_v1: "com.google.api.grpc:proto-google-cloud-spanner-admin-database-v1", // google_cloud_platform_libraries_bom sets version
        proto_google_common_protos                  : "com.google.api.grpc:proto-google-common-protos", // google_cloud_platform_libraries_bom sets version
        slf4j_api                                   : "org.slf4j:slf4j-api:$slf4j_version",
        slf4j_simple                                : "org.slf4j:slf4j-simple:$slf4j_version",
        slf4j_jdk14                                 : "org.slf4j:slf4j-jdk14:$slf4j_version",
        slf4j_log4j12                               : "org.slf4j:slf4j-log4j12:$slf4j_version",
        snappy_java                                 : "org.xerial.snappy:snappy-java:1.1.8.4",
        spark_core                                  : "org.apache.spark:spark-core_2.11:$spark_version",
        spark_network_common                        : "org.apache.spark:spark-network-common_2.11:$spark_version",
        spark_sql                                   : "org.apache.spark:spark-sql_2.11:$spark_version",
        spark_streaming                             : "org.apache.spark:spark-streaming_2.11:$spark_version",
        stax2_api                                   : "org.codehaus.woodstox:stax2-api:4.2.1",
        testcontainers_clickhouse                   : "org.testcontainers:clickhouse:$testcontainers_version",
        testcontainers_elasticsearch                : "org.testcontainers:elasticsearch:$testcontainers_version",
        testcontainers_kafka                        : "org.testcontainers:kafka:$testcontainers_version",
        testcontainers_localstack                   : "org.testcontainers:localstack:$testcontainers_version",
        testcontainers_postgresql                   : "org.testcontainers:postgresql:$testcontainers_version",
        testcontainers_gcloud                       : "org.testcontainers:gcloud:$testcontainers_version",
        vendored_bytebuddy_1_10_8                   : "org.apache.beam:beam-vendor-bytebuddy-1_10_8:0.1",
        vendored_grpc_1_26_0                        : "org.apache.beam:beam-vendor-grpc-1_26_0:0.3",
        vendored_guava_26_0_jre                     : "org.apache.beam:beam-vendor-guava-26_0-jre:0.1",
        vendored_calcite_1_20_0                     : "org.apache.beam:beam-vendor-calcite-1_20_0:0.1",
        woodstox_core_asl                           : "org.codehaus.woodstox:woodstox-core-asl:4.4.1",
        zstd_jni                                    : "com.github.luben:zstd-jni:1.4.5-2",
        quickcheck_core                             : "com.pholser:junit-quickcheck-core:$quickcheck_version",
      ],
      groovy: [
        groovy_all: "org.codehaus.groovy:groovy-all:2.4.13",
      ],
      // For generating pom.xml from archetypes.
      maven : [
        maven_compiler_plugin: "maven-plugins:maven-compiler-plugin:3.7.0",
        maven_exec_plugin    : "maven-plugins:maven-exec-plugin:1.6.0",
        maven_jar_plugin     : "maven-plugins:maven-jar-plugin:3.0.2",
        maven_shade_plugin   : "maven-plugins:maven-shade-plugin:3.1.0",
        maven_surefire_plugin: "maven-plugins:maven-surefire-plugin:3.0.0-M5",
      ],
    ]
  }
}
