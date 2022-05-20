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


plugins {
  id("com.gradle.enterprise") version "3.10.1" apply false
}


// Plugins which require online access should not be enabled when running in offline mode.
if (!gradle.startParameter.isOffline) {
  apply(plugin = "com.gradle.enterprise")
}

// JENKINS_HOME and BUILD_ID set automatically during Jenkins execution
val isJenkinsBuild = arrayOf("JENKINS_HOME", "BUILD_ID").all { System.getenv(it) != null }
// GITHUB_REPOSITORY and GITHUB_RUN_ID set automatically during Github Actions run
val isGithubActionsBuild = arrayOf("GITHUB_REPOSITORY", "GITHUB_RUN_ID").all { System.getenv(it) != null }
if (isJenkinsBuild || isGithubActionsBuild) {
  gradleEnterprise {
    buildScan {
      // Build Scan enabled and TOS accepted for Jenkins lab build. This does not apply to builds on
      // non-Jenkins machines. Developers need to separately enable and accept TOS to use build scans.
      termsOfServiceUrl = "https://gradle.com/terms-of-service"
      termsOfServiceAgree = "yes"
      publishAlways()
    }
  }
}

rootProject.name = "beam"

include(":release")
include(":release:go-licenses:go")
include(":release:go-licenses:java")
include(":release:go-licenses:py")

include(":examples:java")
include(":examples:java:twitter")
include(":examples:kotlin")
include(":examples:multi-language")
include(":model:fn-execution")
include(":model:job-management")
include(":model:pipeline")
include(":playground")
include(":playground:backend")
include(":playground:frontend")
include(":playground:backend:containers")
include(":playground:backend:containers:java")
include(":playground:backend:containers:go")
include(":playground:backend:containers:python")
include(":playground:backend:containers:router")
include(":playground:backend:containers:scio")
include(":playground:terraform")
include(":runners:core-construction-java")
include(":runners:core-java")
include(":runners:direct-java")
include(":runners:extensions-java:metrics")
/* Begin Flink Runner related settings */
// Flink 1.12
include(":runners:flink:1.12")
include(":runners:flink:1.12:job-server")
include(":runners:flink:1.12:job-server-container")
// Flink 1.13
include(":runners:flink:1.13")
include(":runners:flink:1.13:job-server")
include(":runners:flink:1.13:job-server-container")
// Flink 1.14
include(":runners:flink:1.14")
include(":runners:flink:1.14:job-server")
include(":runners:flink:1.14:job-server-container")
/* End Flink Runner related settings */
include(":runners:twister2")
include(":runners:google-cloud-dataflow-java")
include(":runners:google-cloud-dataflow-java:examples")
include(":runners:google-cloud-dataflow-java:examples-streaming")
include(":runners:java-fn-execution")
include(":runners:java-job-service")
include(":runners:jet")
include(":runners:local-java")
include(":runners:portability:java")
include(":runners:spark:2")
include(":runners:spark:2:job-server")
include(":runners:spark:2:job-server:container")
include(":runners:spark:3")
include(":runners:spark:3:job-server")
include(":runners:spark:3:job-server:container")
include(":runners:samza")
include(":runners:samza:job-server")
include(":sdks:go")
include(":sdks:go:container")
include(":sdks:go:examples")
include(":sdks:go:test")
include(":sdks:go:test:load")
include(":sdks:java:bom")
include(":sdks:java:bom:gcp")
include(":sdks:java:build-tools")
include(":sdks:java:container")
include(":sdks:java:container:java8")
include(":sdks:java:container:java11")
include(":sdks:java:container:java17")
include(":sdks:java:core")
include(":sdks:java:expansion-service")
include(":sdks:java:expansion-service:app")
include(":sdks:java:extensions:arrow")
include(":sdks:java:extensions:euphoria")
include(":sdks:java:extensions:kryo")
include(":sdks:java:extensions:google-cloud-platform-core")
include(":sdks:java:extensions:jackson")
include(":sdks:java:extensions:join-library")
include(":sdks:java:extensions:ml")
include(":sdks:java:extensions:protobuf")
include(":sdks:java:extensions:python")
include("sdks:java:extensions:sbe")
include(":sdks:java:extensions:schemaio-expansion-service")
include(":sdks:java:extensions:sketching")
include(":sdks:java:extensions:sorter")
include(":sdks:java:extensions:sql")
include(":sdks:java:extensions:sql:payloads")
include(":sdks:java:extensions:sql:perf-tests")
include(":sdks:java:extensions:sql:jdbc")
include(":sdks:java:extensions:sql:shell")
include(":sdks:java:extensions:sql:hcatalog")
include(":sdks:java:extensions:sql:datacatalog")
include(":sdks:java:extensions:sql:zetasql")
include(":sdks:java:extensions:sql:expansion-service")
include(":sdks:java:extensions:sql:udf")
include(":sdks:java:extensions:sql:udf-test-provider")
include(":sdks:java:extensions:zetasketch")
include(":sdks:java:fn-execution")
include(":sdks:java:harness")
include(":sdks:java:harness:jmh")
include(":sdks:java:io:amazon-web-services")
include(":sdks:java:io:amazon-web-services2")
include(":sdks:java:io:amqp")
include(":sdks:java:io:azure")
include(":sdks:java:io:cassandra")
include(":sdks:java:io:clickhouse")
include(":sdks:java:io:common")
include(":sdks:java:io:contextualtextio")
include(":sdks:java:io:debezium")
include(":sdks:java:io:debezium:expansion-service")
include(":sdks:java:io:elasticsearch")
include(":sdks:java:io:elasticsearch-tests:elasticsearch-tests-2")
include(":sdks:java:io:elasticsearch-tests:elasticsearch-tests-5")
include(":sdks:java:io:elasticsearch-tests:elasticsearch-tests-6")
include(":sdks:java:io:elasticsearch-tests:elasticsearch-tests-7")
include(":sdks:java:io:elasticsearch-tests:elasticsearch-tests-8")
include(":sdks:java:io:elasticsearch-tests:elasticsearch-tests-common")
include(":sdks:java:io:expansion-service")
include(":sdks:java:io:file-based-io-tests")
include(":sdks:java:io:bigquery-io-perf-tests")
include(":sdks:java:io:cdap")
include(":sdks:java:io:google-cloud-platform")
include(":sdks:java:io:google-cloud-platform:expansion-service")
include(":sdks:java:io:hadoop-common")
include(":sdks:java:io:hadoop-file-system")
include(":sdks:java:io:hadoop-format")
include(":sdks:java:io:hbase")
include(":sdks:java:io:hcatalog")
include(":sdks:java:io:jdbc")
include(":sdks:java:io:jms")
include(":sdks:java:io:kafka")
include(":sdks:java:io:kinesis")
include(":sdks:java:io:kinesis:expansion-service")
include(":sdks:java:io:kudu")
include(":sdks:java:io:mongodb")
include(":sdks:java:io:mqtt")
include(":sdks:java:io:neo4j")
include(":sdks:java:io:parquet")
include(":sdks:java:io:pulsar")
include(":sdks:java:io:rabbitmq")
include(":sdks:java:io:redis")
include(":sdks:java:io:solr")
include(":sdks:java:io:snowflake")
include(":sdks:java:io:snowflake:expansion-service")
include(":sdks:java:io:splunk")
include(":sdks:java:io:thrift")
include(":sdks:java:io:tika")
include(":sdks:java:io:xml")
include(":sdks:java:io:synthetic")
include(":sdks:java:io:influxdb")
include(":sdks:java:javadoc")
include(":sdks:java:maven-archetypes:examples")
include(":sdks:java:maven-archetypes:gcp-bom-examples")
include(":sdks:java:maven-archetypes:starter")
include(":sdks:java:testing:nexmark")
include(":sdks:java:testing:expansion-service")
include(":sdks:java:testing:jpms-tests")
include(":sdks:java:testing:kafka-service")
include(":sdks:java:testing:load-tests")
include(":sdks:java:testing:test-utils")
include(":sdks:java:testing:tpcds")
include(":sdks:java:testing:watermarks")
include(":sdks:python")
include(":sdks:python:apache_beam:testing:load_tests")
include(":sdks:python:container")
include(":sdks:python:container:py37")
include(":sdks:python:container:py38")
include(":sdks:python:container:py39")
include(":sdks:python:test-suites:dataflow")
include(":sdks:python:test-suites:dataflow:py37")
include(":sdks:python:test-suites:dataflow:py38")
include(":sdks:python:test-suites:dataflow:py39")
include(":sdks:python:test-suites:direct")
include(":sdks:python:test-suites:direct:py37")
include(":sdks:python:test-suites:direct:py38")
include(":sdks:python:test-suites:direct:py39")
include(":sdks:python:test-suites:direct:xlang")
include(":sdks:python:test-suites:portable:py37")
include(":sdks:python:test-suites:portable:py38")
include(":sdks:python:test-suites:portable:py39")
include(":sdks:python:test-suites:tox:pycommon")
include(":sdks:python:test-suites:tox:py37")
include(":sdks:python:test-suites:tox:py38")
include(":sdks:python:test-suites:tox:py39")
include(":vendor:grpc-1_43_2")
include(":vendor:bytebuddy-1_12_8")
include(":vendor:calcite-1_28_0")
include(":vendor:guava-26_0-jre")
include(":website")
include(":runners:google-cloud-dataflow-java:worker:legacy-worker")
include(":runners:google-cloud-dataflow-java:worker")
include(":runners:google-cloud-dataflow-java:worker:windmill")
// no dots allowed for project paths
include("beam-test-infra-metrics")
project(":beam-test-infra-metrics").projectDir = file(".test-infra/metrics")
include("beam-test-tools")
project(":beam-test-tools").projectDir = file(".test-infra/tools")
include("beam-test-jenkins")
project(":beam-test-jenkins").projectDir = file(".test-infra/jenkins")
include("beam-validate-runner")
project(":beam-validate-runner").projectDir = file(".test-infra/validate-runner")
