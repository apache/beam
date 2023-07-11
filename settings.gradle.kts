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
import com.gradle.enterprise.gradleplugin.internal.extension.BuildScanExtensionWithHiddenFeatures

plugins {
  id("com.gradle.enterprise") version "3.13.2"
  id("com.gradle.common-custom-user-data-gradle-plugin") version "1.10"
}


// JENKINS_HOME and BUILD_ID set automatically during Jenkins execution
val isJenkinsBuild = arrayOf("JENKINS_HOME", "BUILD_ID").all { System.getenv(it) != null }
// GITHUB_REPOSITORY and GITHUB_RUN_ID set automatically during Github Actions run
val isGithubActionsBuild = arrayOf("GITHUB_REPOSITORY", "GITHUB_RUN_ID").all { System.getenv(it) != null }
val isCi = isJenkinsBuild || isGithubActionsBuild

gradleEnterprise {
  server = "https://ge.apache.org"
  allowUntrustedServer = false

  buildScan {
    capture { isTaskInputFiles = true }
    isUploadInBackground = !isCi
    publishAlways()
    this as BuildScanExtensionWithHiddenFeatures
    publishIfAuthenticated()
    obfuscation {
      ipAddresses { addresses -> addresses.map { "0.0.0.0" } }
    }
  }
}

buildCache {
  local {
    isEnabled = true
  }
  remote<HttpBuildCache> {
    url = uri("https://beam-cache.apache.org/cache/")
    isAllowUntrustedServer = false
    credentials {
      username = System.getenv("GRADLE_ENTERPRISE_CACHE_USERNAME")
      password = System.getenv("GRADLE_ENTERPRISE_CACHE_PASSWORD")
    }
    isEnabled = true
    isPush = isCi
  }
}

rootProject.name = "beam"

include(":release")
include(":release:go-licenses:go")
include(":release:go-licenses:java")
include(":release:go-licenses:py")

include(":examples:java")
include(":examples:java:twitter")
include(":examples:java:cdap")
include(":examples:java:cdap:hubspot")
include(":examples:java:cdap:salesforce")
include(":examples:java:cdap:servicenow")
include(":examples:java:cdap:zendesk")
include(":examples:kotlin")
include(":examples:multi-language")
include(":learning")
include(":learning:tour-of-beam")
include(":learning:tour-of-beam:frontend")
include(":model:fn-execution")
include(":model:job-management")
include(":model:pipeline")
include(":playground")
include(":playground:backend")
include(":playground:frontend")
include(":playground:frontend:playground_components")
include(":playground:frontend:playground_components:tools:extract_symbols_java")
include(":playground:backend:containers")
include(":playground:backend:containers:java")
include(":playground:backend:containers:go")
include(":playground:backend:containers:python")
include(":playground:backend:containers:router")
include(":playground:backend:containers:scio")
include(":playground:terraform")
include(":playground:kafka-emulator")

include(":learning:tour-of-beam:frontend")

include(":runners:core-construction-java")
include(":runners:core-java")
include(":runners:direct-java")
include(":runners:extensions-java:metrics")
include(":learning")
include(":learning:tour-of-beam")
include(":learning:tour-of-beam:terraform")
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
// Flink 1.15
include(":runners:flink:1.15")
include(":runners:flink:1.15:job-server")
include(":runners:flink:1.15:job-server-container")
// Flink 1.16
include(":runners:flink:1.16")
include(":runners:flink:1.16:job-server")
include(":runners:flink:1.16:job-server-container")
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
include(":sdks:java:container:agent")
include(":sdks:java:container:java8")
include(":sdks:java:container:java11")
include(":sdks:java:container:java17")
include(":sdks:java:core")
include(":sdks:java:core:jmh")
include(":sdks:java:expansion-service")
include(":sdks:java:expansion-service:container")
include(":sdks:java:expansion-service:app")
include(":sdks:java:extensions:arrow")
include(":sdks:java:extensions:avro")
include(":sdks:java:extensions:euphoria")
include(":sdks:java:extensions:kryo")
include(":sdks:java:extensions:google-cloud-platform-core")
include(":sdks:java:extensions:jackson")
include(":sdks:java:extensions:join-library")
include(":sdks:java:extensions:ml")
include(":sdks:java:extensions:protobuf")
include(":sdks:java:extensions:python")
include(":sdks:java:extensions:sbe")
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
include(":sdks:java:extensions:timeseries")
include(":sdks:java:extensions:zetasketch")
include(":sdks:java:fn-execution")
include(":sdks:java:harness")
include(":sdks:java:harness:jmh")
include(":sdks:java:io:amazon-web-services")
include(":sdks:java:io:amazon-web-services2")
include(":sdks:java:io:amqp")
include(":sdks:java:io:azure")
include(":sdks:java:io:azure-cosmos")
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
include(":sdks:java:io:csv")
include(":sdks:java:io:file-schema-transform")
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
include(":sdks:java:io:sparkreceiver:2")
include(":sdks:java:io:snowflake")
include(":sdks:java:io:snowflake:expansion-service")
include(":sdks:java:io:splunk")
include(":sdks:java:io:thrift")
include(":sdks:java:io:tika")
include(":sdks:java:io:xml")
include(":sdks:java:io:synthetic")
include(":sdks:java:io:influxdb")
include(":sdks:java:io:singlestore")
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
include(":sdks:java:transform-service")
include(":sdks:java:transform-service:launcher")
include(":sdks:java:transform-service:controller-container")
include(":sdks:python")
include(":sdks:python:apache_beam:testing:load_tests")
include(":sdks:python:apache_beam:testing:benchmarks:nexmark")
include(":sdks:python:container")
include(":sdks:python:container:py38")
include(":sdks:python:container:py39")
include(":sdks:python:container:py310")
include(":sdks:python:container:py311")
include(":sdks:python:expansion-service-container")
include(":sdks:python:test-suites:dataflow")
include(":sdks:python:test-suites:dataflow:py38")
include(":sdks:python:test-suites:dataflow:py39")
include(":sdks:python:test-suites:dataflow:py310")
include(":sdks:python:test-suites:dataflow:py311")
include(":sdks:python:test-suites:direct")
include(":sdks:python:test-suites:direct:py38")
include(":sdks:python:test-suites:direct:py39")
include(":sdks:python:test-suites:direct:py310")
include(":sdks:python:test-suites:direct:py311")
include(":sdks:python:test-suites:direct:xlang")
include(":sdks:python:test-suites:portable:py38")
include(":sdks:python:test-suites:portable:py39")
include(":sdks:python:test-suites:portable:py310")
include(":sdks:python:test-suites:portable:py311")
include(":sdks:python:test-suites:tox:pycommon")
include(":sdks:python:test-suites:tox:py38")
include(":sdks:python:test-suites:tox:py39")
include(":sdks:python:test-suites:tox:py310")
include(":sdks:python:test-suites:tox:py311")
include(":sdks:python:test-suites:xlang")
include(":sdks:typescript")
include(":sdks:typescript:container")
include(":vendor:bytebuddy-1_12_8")
include(":vendor:grpc-1_54_0")
include(":vendor:calcite-1_28_0")
include(":vendor:guava-26_0-jre")
include(":website")
include(":runners:google-cloud-dataflow-java:worker")
include(":runners:google-cloud-dataflow-java:worker:windmill")
// no dots allowed for project paths
include("beam-test-infra-metrics")
project(":beam-test-infra-metrics").projectDir = file(".test-infra/metrics")
include("beam-test-infra-pipelines")
project(":beam-test-infra-pipelines").projectDir = file(".test-infra/pipelines")
include("beam-test-tools")
project(":beam-test-tools").projectDir = file(".test-infra/tools")
include("beam-test-jenkins")
project(":beam-test-jenkins").projectDir = file(".test-infra/jenkins")
include("beam-validate-runner")
project(":beam-validate-runner").projectDir = file(".test-infra/validate-runner")
include("com.google.api.gax.batching")
include("sdks:java:io:kafka:kafka-251")
findProject(":sdks:java:io:kafka:kafka-251")?.name = "kafka-251"
include("sdks:java:io:kafka:kafka-241")
findProject(":sdks:java:io:kafka:kafka-241")?.name = "kafka-241"
include("sdks:java:io:kafka:kafka-231")
findProject(":sdks:java:io:kafka:kafka-231")?.name = "kafka-231"
include("sdks:java:io:kafka:kafka-222")
findProject(":sdks:java:io:kafka:kafka-222")?.name = "kafka-222"
include("sdks:java:io:kafka:kafka-211")
findProject(":sdks:java:io:kafka:kafka-211")?.name = "kafka-211"
include("sdks:java:io:kafka:kafka-201")
findProject(":sdks:java:io:kafka:kafka-201")?.name = "kafka-201"
include("sdks:java:io:kafka:kafka-111")
findProject(":sdks:java:io:kafka:kafka-111")?.name = "kafka-111"
include("sdks:java:io:kafka:kafka-100")
findProject(":sdks:java:io:kafka:kafka-100")?.name = "kafka-100"
include("sdks:java:io:kafka:kafka-01103")
findProject(":sdks:java:io:kafka:kafka-01103")?.name = "kafka-01103"
